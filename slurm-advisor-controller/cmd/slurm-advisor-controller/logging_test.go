// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	crdbErrors "github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/config"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	collectorlog "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// A msg-only consumer must receive every admission count, even when no jobs
// were observed. Distinct counts catch swapped or duplicated summary values.
func TestEnvironmentAdmissionSummaryMessage(t *testing.T) {
	counts := []struct {
		field  string
		reason reconcile.EnvironmentAdmissionReason
		count  int
	}{
		{"matched", reconcile.EnvironmentAdmissionMatched, 1},
		{"controller_unspecified", reconcile.EnvironmentAdmissionControllerUnspecified, 2},
		{"prod_default", reconcile.EnvironmentAdmissionProductionDefault, 3},
		{"environment_mismatch", reconcile.EnvironmentAdmissionMismatch, 4},
		{"not_onboarded", reconcile.EnvironmentAdmissionNotOnboarded, 5},
		{"invalid_environment", reconcile.EnvironmentAdmissionInvalid, 6},
	}
	for _, populated := range []bool{false, true} {
		t.Run(fmt.Sprintf("populated=%t", populated), func(t *testing.T) {
			output := captureLoggerStderr(t)
			logger, shutdown, err := newLogger(context.Background(), config.Config{LogLevel: "info"})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
			snapshot := reconcile.ControllerSnapshot{
				EnvironmentAdmissions: make(map[cluster.JobAttemptRef]reconcile.EnvironmentAdmissionDecision),
			}
			if populated {
				for _, item := range counts {
					for i := 0; i < item.count; i++ {
						key := cluster.JobAttemptRef{JobID: fmt.Sprint(len(snapshot.EnvironmentAdmissions) + 1)}
						snapshot.EnvironmentAdmissions[key] = reconcile.EnvironmentAdmissionDecision{Reason: item.reason}
					}
				}
			}
			logEnvironmentAdmissionSummary(logger, "test", snapshot)
			logs := readLocalLogs(t, output)
			if len(logs) != 1 {
				t.Fatalf("stderr records = %d, want 1", len(logs))
			}
			msg, ok := logs[0]["msg"].(string)
			if !ok || !strings.Contains(msg, `controller_environment="test"`) {
				t.Fatalf("message missing controller environment: %#v", logs[0]["msg"])
			}
			if logs[0]["controller_environment"] != "test" {
				t.Errorf("structured environment = %#v, want test", logs[0]["controller_environment"])
			}
			for _, item := range counts {
				want := 0
				if populated {
					want = item.count
				}
				if !strings.Contains(" "+msg+" ", fmt.Sprintf(" %s=%d ", item.field, want)) {
					t.Errorf("message missing %s=%d: %q", item.field, want, msg)
				}
				if logs[0][item.field] != float64(want) {
					t.Errorf("structured %s = %#v, want %d", item.field, logs[0][item.field], want)
				}
			}
		})
	}
}

// These tests exercise the actual exporters so that authentication, endpoint
// routing, wire encoding, and shutdown flushing are covered together.
func TestNewLoggerExportsAndKeepsStderr(t *testing.T) {
	for _, tc := range []struct {
		name     string
		protocol string
		level    string
		tls      bool
	}{
		{name: "grpc", protocol: "grpc", level: "info"},
		{name: "http", protocol: "http", level: "info"},
		{name: "http/protobuf", protocol: "http/protobuf", level: "debug"},
		{name: "https", protocol: "http", level: "info", tls: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := captureLoggerStderr(t)
			requests := make(chan receivedLogExport, 4)
			var endpoint string
			if tc.protocol == "grpc" {
				endpoint = startLogGRPCCollector(t, requests)
			} else {
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/collector/v1/logs" || r.Method != http.MethodPost {
						t.Errorf("export route = %s %s, want POST /collector/v1/logs", r.Method, r.URL.Path)
					}
					if got := r.Header.Get("Content-Type"); got != "application/x-protobuf" {
						t.Errorf("Content-Type = %q, want application/x-protobuf", got)
					}
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					request := new(collectorlog.ExportLogsServiceRequest)
					if err := proto.Unmarshal(body, request); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					requests <- receivedLogExport{request: request, authorization: r.Header.Get("Authorization")}
					w.Header().Set("Content-Type", "application/x-protobuf")
					w.WriteHeader(http.StatusOK)
				}))
				if tc.tls {
					server.StartTLS()
					certificate := filepath.Join(t.TempDir(), "collector.pem")
					if err := os.WriteFile(certificate, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
						t.Fatal(err)
					}
					t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", certificate)
				} else {
					server.Start()
				}
				t.Cleanup(server.Close)
				endpoint = server.URL + "/collector"
			}

			logger, shutdown, err := newLogger(context.Background(), config.Config{
				LogLevel: tc.level, Mode: config.ModeObserve, Environment: "test",
				OTLP: config.OTLPConfig{Endpoint: endpoint, Protocol: tc.protocol, Token: "test-collector-token"},
			})
			if err != nil {
				t.Fatal(err)
			}
			logger.Info("reconcile complete", zap.Int("jobs", 3))
			logger.Debug("reconcile detail")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdown(ctx); err != nil {
				t.Fatalf("shutdown: %v", err)
			}
			exported := new(collectorlog.ExportLogsServiceRequest)
			for len(requests) > 0 {
				received := <-requests
				if received.authorization != "Bearer test-collector-token" {
					t.Errorf("Authorization = %q, want configured bearer token", received.authorization)
				}
				exported.ResourceLogs = append(exported.ResourceLogs, received.request.ResourceLogs...)
			}
			assertExportedLogs(t, exported, tc.level == "debug")
			logs := readLocalLogs(t, output)
			wantCount := 1
			if tc.level == "debug" {
				wantCount = 2
			}
			if len(logs) != wantCount {
				t.Fatalf("stderr records = %d, want %d", len(logs), wantCount)
			}
			if logs[0]["msg"] != "reconcile complete" || logs[0]["jobs"] != float64(3) || logs[0]["environment"] != "test" {
				t.Errorf("stderr record lost message or structured fields: %#v", logs[0])
			}
			if strings.Contains(string(mustReadLogOutput(t, output)), "test-collector-token") {
				t.Error("collector token leaked to stderr")
			}
		})
	}
}

func TestNewLoggerAppendsConciseErrorToOTLPOnly(t *testing.T) {
	output := captureLoggerStderr(t)
	requests := make(chan receivedLogExport, 4)
	endpoint := startLogGRPCCollector(t, requests)
	logger, shutdown, err := newLogger(context.Background(), config.Config{
		LogLevel: "info", Mode: config.ModeObserve, Environment: "test",
		OTLP: config.OTLPConfig{Endpoint: endpoint, Protocol: "grpc", Token: "test-collector-token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	failure := crdbErrors.Wrap(crdbErrors.New("invalid job id"), "requeue failed")
	logger.Warn("close client", zap.Error(failure), zap.NamedError("stacktrace", failure))
	logger.Error("controller sync failed", zap.Duration("duration", 2*time.Second), zap.Error(failure))
	logger.Warn("nested failure", zap.Namespace("request"), zap.Error(errors.New("request failed")))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	logs := readLocalLogs(t, output)
	if len(logs) != 3 {
		t.Fatalf("stderr records = %d, want 3", len(logs))
	}
	for i, want := range []string{"close client", "controller sync failed", "nested failure"} {
		if got := logs[i]["msg"]; got != want {
			t.Errorf("stderr message %d = %#v, want %q", i, got, want)
		}
	}
	for _, key := range []string{"error", "errorVerbose", "stacktrace", "stacktraceVerbose"} {
		if _, ok := logs[0][key]; !ok {
			t.Errorf("stderr record lost %q: %#v", key, logs[0])
		}
	}
	if logs[1]["duration"] != float64(2) {
		t.Errorf("stderr duration = %#v, want 2", logs[1]["duration"])
	}
	if _, ok := logs[1]["stacktrace"]; !ok {
		t.Errorf("stderr error record lost generated stacktrace: %#v", logs[1])
	}

	var records []*logspb.LogRecord
	for len(requests) > 0 {
		for _, resource := range (<-requests).request.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				records = append(records, scope.LogRecords...)
			}
		}
	}
	if len(records) != 3 {
		t.Fatalf("exported records = %d, want 3", len(records))
	}
	for i := 0; i < 2; i++ {
		message := records[i].Body.GetStringValue()
		if !strings.Contains(message, ` | error="requeue failed: invalid job id"`) {
			t.Errorf("OTLP message %d lacks concise error: %q", i, message)
		}
		if strings.Contains(message, "errorVerbose") || strings.Contains(message, "stacktrace") {
			t.Errorf("OTLP message %d includes verbose payload: %q", i, message)
		}
	}
	if got := records[2].Body.GetStringValue(); got != "nested failure" {
		t.Errorf("namespaced error changed OTLP message to %q", got)
	}
	for _, key := range []string{"exception.message", "exception.type", "stacktrace", "stacktraceVerbose"} {
		if got := logAttribute(records[0].Attributes, key); got == nil {
			t.Errorf("OTLP record lost %q attribute", key)
		}
	}
	if got := logAttribute(records[1].Attributes, "duration").GetIntValue(); got != int64(2*time.Second) {
		t.Errorf("OTLP duration = %d, want %d", got, 2*time.Second)
	}
	if got := logAttribute(records[1].Attributes, "exception.stacktrace"); got == nil {
		t.Error("OTLP error record lost generated stacktrace")
	}
}

func TestNewLoggerEntryErrorReplacesContextErrorInOTLPMessage(t *testing.T) {
	output := captureLoggerStderr(t)
	requests := make(chan receivedLogExport, 4)
	endpoint := startLogGRPCCollector(t, requests)
	logger, shutdown, err := newLogger(context.Background(), config.Config{
		LogLevel: "info", Mode: config.ModeObserve, Environment: "test",
		OTLP: config.OTLPConfig{Endpoint: endpoint, Protocol: "grpc", Token: "test-collector-token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	contextFailure := crdbErrors.Wrap(crdbErrors.New("old cause"), "old failure")
	contextLogger := logger.With(zap.Error(contextFailure))
	contextLogger.Warn("plain replacement", zap.Error(errors.New("plain failure")))
	contextLogger.Warn("context failure")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	logs := readLocalLogs(t, output)
	if len(logs) != 2 {
		t.Fatalf("stderr records = %d, want 2", len(logs))
	}
	if logs[0]["msg"] != "plain replacement" || logs[1]["msg"] != "context failure" {
		t.Errorf("stderr messages changed: %#v", logs)
	}

	var records []*logspb.LogRecord
	for len(requests) > 0 {
		for _, resource := range (<-requests).request.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				records = append(records, scope.LogRecords...)
			}
		}
	}
	if len(records) != 2 {
		t.Fatalf("exported records = %d, want 2", len(records))
	}
	if got := records[0].Body.GetStringValue(); got != `plain replacement | error="plain failure"` {
		t.Errorf("entry error did not replace context error: %q", got)
	}
	if got := records[1].Body.GetStringValue(); got != `context failure | error="old failure: old cause"` {
		t.Errorf("context error missing from OTLP message: %q", got)
	}
}

func TestRunFlushesStartupFailure(t *testing.T) {
	output := captureLoggerStderr(t)
	requests := make(chan receivedLogExport, 4)
	endpoint := startLogGRPCCollector(t, requests)
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := busy.Close(); err != nil {
			t.Error(err)
		}
	})
	for key, value := range map[string]string{
		"HEIMDALL_ENDPOINT":                     "unused.invalid:443",
		"SLURM_ADVISOR_MODE":                    "observe",
		"SLURM_ADVISOR_LOG_LEVEL":               "info",
		"SLURM_ADVISOR_LOG_LINKS_ENABLED":       "false",
		"SLURM_ADVISOR_AUDIT_TRAIL_DESTINATION": "logging",
		"SLURM_ADVISOR_METRICS_ENABLED":         "true",
		"SLURM_ADVISOR_METRICS_ADDR":            busy.Addr().String(),
		"OTEL_EXPORTER_OTLP_ENDPOINT":           endpoint,
		"OTEL_EXPORTER_OTLP_PROTOCOL":           "grpc",
		"OTEL_EXPORTER_OTLP_TOKEN":              "test-collector-token",
	} {
		t.Setenv(key, value)
	}
	if code := run(); code == 0 {
		t.Fatal("startup succeeded with an occupied metrics address")
	}
	var exportedFailure bool
	for len(requests) > 0 {
		for _, resource := range (<-requests).request.ResourceLogs {
			for _, scope := range resource.ScopeLogs {
				for _, record := range scope.LogRecords {
					if record.SeverityNumber == logspb.SeverityNumber_SEVERITY_NUMBER_ERROR &&
						logAttribute(record.Attributes, "metrics_addr").GetStringValue() == busy.Addr().String() {
						exportedFailure = true
					}
				}
			}
		}
	}
	if !exportedFailure {
		t.Errorf("startup returned before exporting its metrics failure; stderr: %s", mustReadLogOutput(t, output))
	}
	var localFailure bool
	for _, record := range readLocalLogs(t, output) {
		if record["level"] == "error" && record["metrics_addr"] == busy.Addr().String() {
			localFailure = true
		}
	}
	if !localFailure {
		t.Error("startup failure was not also logged to stderr")
	}
}

func TestNewLoggerRequiresBothCollectorEndpointAndToken(t *testing.T) {
	var exports atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		exports.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	for _, tc := range []struct {
		name     string
		endpoint string
		token    string
	}{
		{name: "disabled"},
		{name: "endpoint only", endpoint: server.URL},
		{name: "token only", token: "test-collector-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := captureLoggerStderr(t)
			logger, shutdown, err := newLogger(context.Background(), config.Config{
				LogLevel: "info", OTLP: config.OTLPConfig{Endpoint: tc.endpoint, Protocol: "http", Token: tc.token},
			})
			if err != nil {
				t.Fatal(err)
			}
			logger.Info("local logging available")
			if err := shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			logs := readLocalLogs(t, output)
			if len(logs) != 1 || logs[0]["msg"] != "local logging available" {
				t.Errorf("stderr records = %#v, want local message", logs)
			}
		})
	}
	if got := exports.Load(); got != 0 {
		t.Errorf("collector received %d exports without both endpoint and token", got)
	}
}

func TestNewLoggerCollectorFailureKeepsLocalLogsAndBoundsShutdown(t *testing.T) {
	output := captureLoggerStderr(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	logger, shutdown, err := newLogger(context.Background(), config.Config{
		LogLevel: "info",
		OTLP:     config.OTLPConfig{Endpoint: server.URL, Protocol: "http", Token: "test-collector-token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("collector unavailable")
	logs := readLocalLogs(t, output)
	if len(logs) != 1 || logs[0]["msg"] != "collector unavailable" {
		t.Fatalf("local logging depends on collector availability: %#v", logs)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- shutdown(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("shutdown error = %v, want context deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown ignored its deadline while collector was unavailable")
	}
}

type receivedLogExport struct {
	request       *collectorlog.ExportLogsServiceRequest
	authorization string
}

type recordingLogCollector struct {
	collectorlog.UnimplementedLogsServiceServer
	requests chan<- receivedLogExport
}

// startLogGRPCCollector serves until test cleanup stops the listener and RPCs.
func startLogGRPCCollector(t *testing.T, requests chan<- receivedLogExport) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	collectorlog.RegisterLogsServiceServer(server, &recordingLogCollector{requests: requests})
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()
	return "http://" + listener.Addr().String()
}

// Export records the wire request and credentials sent by the production gRPC exporter.
func (c *recordingLogCollector) Export(ctx context.Context, request *collectorlog.ExportLogsServiceRequest) (*collectorlog.ExportLogsServiceResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	c.requests <- receivedLogExport{request: request, authorization: strings.Join(md.Get("authorization"), ",")}
	return &collectorlog.ExportLogsServiceResponse{}, nil
}

func assertExportedLogs(t *testing.T, request *collectorlog.ExportLogsServiceRequest, debug bool) {
	t.Helper()
	var records []*logspb.LogRecord
	for _, resource := range request.ResourceLogs {
		if got := logAttribute(resource.GetResource().GetAttributes(), "service.name").GetStringValue(); got != "slurm-advisor-controller" {
			t.Errorf("service.name = %q, want slurm-advisor-controller", got)
		}
		for _, scope := range resource.ScopeLogs {
			records = append(records, scope.LogRecords...)
		}
	}
	wantCount := 1
	if debug {
		wantCount = 2
	}
	if len(records) != wantCount {
		t.Fatalf("exported records = %d, want %d", len(records), wantCount)
	}
	info := records[0]
	if info.Body.GetStringValue() != "reconcile complete" || info.SeverityNumber != logspb.SeverityNumber_SEVERITY_NUMBER_INFO {
		t.Errorf("exported info record = %v", info)
	}
	for key, want := range map[string]string{
		"component": "slurm-advisor-controller", "default_mode": "observe", "environment": "test",
	} {
		if got := logAttribute(info.Attributes, key).GetStringValue(); got != want {
			t.Errorf("attribute %s = %q, want %q", key, got, want)
		}
	}
	if got := logAttribute(info.Attributes, "jobs").GetIntValue(); got != 3 {
		t.Errorf("jobs = %d, want 3", got)
	}
	if debug && (records[1].Body.GetStringValue() != "reconcile detail" || records[1].SeverityNumber != logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG) {
		t.Errorf("exported debug record = %v", records[1])
	}
}

func logAttribute(attributes []*commonpb.KeyValue, key string) *commonpb.AnyValue {
	for _, attribute := range attributes {
		if attribute.Key == key {
			return attribute.Value
		}
	}
	return nil
}

// captureLoggerStderr must run before constructing a logger. These tests do not
// run in parallel because zap resolves the process stderr during construction.
func captureLoggerStderr(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stderr.jsonl")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stderr
	os.Stderr = file
	t.Cleanup(func() {
		os.Stderr = previous
		_ = file.Close()
	})
	return path
}

func mustReadLogOutput(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func readLocalLogs(t *testing.T, path string) []map[string]any {
	t.Helper()
	var records []map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(mustReadLogOutput(t, path))))
	for {
		var record map[string]any
		if err := decoder.Decode(&record); errors.Is(err, io.EOF) {
			return records
		} else if err != nil {
			t.Fatalf("decode stderr log: %v", err)
		}
		records = append(records, record)
	}
}
