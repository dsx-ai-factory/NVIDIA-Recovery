// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster/clustermock"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/config"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/heimdall/heimdallmock"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/loglinks"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/telemetry"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewControllerWiresActionMode(t *testing.T) {
	mocks := gomock.NewController(t)
	core, logs := observer.New(zap.WarnLevel)
	executor := clustermock.NewMockJobMutator(mocks)
	enricher := clustermock.NewMockActiveJobEnricher(mocks)
	controller := newController(
		config.Config{
			Mode:                     config.ModeAct,
			UserAllowlist:            []string{"alice", "bob"},
			AdviceMaxAge:             7 * time.Minute,
			FailedJobRecoveryTimeout: 18 * time.Second,
			JobUpsertWorkerPoolSize:  6,
			EnableEagerRequeue:       true,
			Safety: config.SafetyChecks{
				AcknowledgeSchedulerActions: true,
			},
		},
		heimdallmock.NewMockHeimdallClient(mocks),
		clustermock.NewMockJobReader(mocks),
		enricher,
		executor,
		state.NewMemorySeenJobStore(),
		nil,
		reconcile.NopObserver{},
		reconcile.NopAuditObserver{},
		zap.New(core),
	)

	if logs.Len() != 1 || logs.All()[0].ContextMap()["enable_eager_requeue"] != true {
		t.Fatalf("warnings = %#v, want explicit compatibility opt-in warning", logs.All())
	}
	if controller.ModePolicy.DefaultMode != cluster.JobModeAct || !controller.ModePolicy.AllowAct {
		t.Fatalf("ModePolicy = %#v, want allowed act default", controller.ModePolicy)
	}
	if !controller.Options.EnableEagerRequeue {
		t.Fatal("EnableEagerRequeue = false, want explicit eager recovery opt-in")
	}
	// Dropping the startup selector would silently widen an act default to all users.
	if !slices.Equal(controller.Options.UserAllowlist, []string{"alice", "bob"}) {
		t.Fatalf("UserAllowlist = %v, want configured rollout users", controller.Options.UserAllowlist)
	}
	if controller.ActionExecutor == nil {
		t.Fatal("ActionExecutor = nil, want configured executor")
	}
	if controller.ActiveJobEnricher != enricher {
		t.Fatal("ActiveJobEnricher was not wired into controller")
	}
	if controller.SeenJobs == nil {
		t.Fatal("SeenJobs = nil, want configured store")
	}
	if controller.Options.FailedJobRecoveryTimeout != 18*time.Second {
		t.Fatalf("FailedJobRecoveryTimeout = %s, want 18s", controller.Options.FailedJobRecoveryTimeout)
	}
	if controller.Options.AdviceMaxAge != 7*time.Minute {
		t.Fatalf("AdviceMaxAge = %s, want 7m", controller.Options.AdviceMaxAge)
	}
	if controller.Options.JobUpsertWorkerPoolSize != 6 {
		t.Fatalf(
			"JobUpsertWorkerPoolSize = %d, want 6",
			controller.Options.JobUpsertWorkerPoolSize,
		)
	}
}

func TestNewControllerWiresObserveMode(t *testing.T) {
	mocks := gomock.NewController(t)
	core, logs := observer.New(zap.WarnLevel)
	controller := newController(
		config.Config{Mode: config.ModeObserve},
		heimdallmock.NewMockHeimdallClient(mocks),
		clustermock.NewMockJobReader(mocks),
		nil,
		clustermock.NewMockJobMutator(mocks),
		state.NewMemorySeenJobStore(),
		nil,
		reconcile.NopObserver{},
		reconcile.NopAuditObserver{},
		zap.New(core),
	)

	if logs.Len() != 0 {
		t.Fatalf("warnings = %#v, want none with compatibility recovery disabled", logs.All())
	}
	if controller.Options.EnableEagerRequeue {
		t.Fatal("EnableEagerRequeue = true, want advice-driven recovery by default")
	}
	if controller.ModePolicy.DefaultMode != cluster.JobModeObserve || controller.ModePolicy.AllowAct {
		t.Fatalf("ModePolicy = %#v, want denied observe default", controller.ModePolicy)
	}
}

func TestNewControllerWiresIgnoreDefaultWithIndependentActionAllowance(t *testing.T) {
	mocks := gomock.NewController(t)
	controller := newController(
		config.Config{
			Mode: config.ModeIgnore,
			Safety: config.SafetyChecks{
				AcknowledgeSchedulerActions: true,
			},
		},
		heimdallmock.NewMockHeimdallClient(mocks),
		clustermock.NewMockJobReader(mocks),
		nil,
		clustermock.NewMockJobMutator(mocks),
		state.NewMemorySeenJobStore(),
		nil,
		reconcile.NopObserver{},
		reconcile.NopAuditObserver{},
		zap.NewNop(),
	)

	if controller.ModePolicy.DefaultMode != cluster.JobModeIgnore || !controller.ModePolicy.AllowAct {
		t.Fatalf("ModePolicy = %#v, want allowed ignore default", controller.ModePolicy)
	}
}

func TestNewControllerWiresObserver(t *testing.T) {
	mocks := gomock.NewController(t)
	observer := &recordingObserver{}
	auditObserver := &reconcile.NopAuditObserver{}
	materializer := &stubLogLinkMaterializer{}
	logger := zap.NewNop()
	controller := newController(
		config.Config{Mode: config.ModeObserve},
		heimdallmock.NewMockHeimdallClient(mocks),
		clustermock.NewMockJobReader(mocks),
		nil,
		clustermock.NewMockJobMutator(mocks),
		state.NewMemorySeenJobStore(),
		materializer,
		observer,
		auditObserver,
		logger,
	)

	if controller.Observer != observer {
		t.Fatal("Observer was not wired into controller")
	}
	if controller.AuditObserver != auditObserver {
		t.Fatal("AuditObserver was not wired into controller")
	}
	if controller.LogLinkMaterializer != materializer {
		t.Fatal("log-link materializer was not wired into controller")
	}
	if controller.Logger != logger {
		t.Fatal("logger was not wired into controller")
	}
}

func TestNewLogLinkMaterializerDisabledDoesNotValidateFilesystemSettings(t *testing.T) {
	materializer, recovery, err := newLogLinkMaterializer(config.Config{
		LogLinks: config.LogLinksConfig{
			Destination:         "/does/not/exist",
			ApplicationPatterns: []string{"["},
		},
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("newLogLinkMaterializer() failed while disabled: %v", err)
	}
	if materializer != nil {
		t.Fatal("materializer != nil, want nil while disabled")
	}
	if recovery != (loglinks.RecoveryReport{}) {
		t.Fatalf("recovery = %#v, want zero report", recovery)
	}
}

func TestNewLogLinkMaterializerBuildsEnabledMaterializer(t *testing.T) {
	sourceRoot := canonicalTempDir(t)
	destination := canonicalTempDir(t)
	cfg := config.WithDefaults(config.Config{
		HeimdallEndpoint: "127.0.0.1:8080",
		LogLinks: config.LogLinksConfig{
			Enabled:     true,
			Destination: destination,
			SourceRoots: []string{sourceRoot},
		},
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	materializer, recovery, err := newLogLinkMaterializer(cfg, zap.NewNop())
	if err != nil {
		t.Fatalf("newLogLinkMaterializer() failed: %v", err)
	}
	if materializer == nil {
		t.Fatal("materializer = nil, want enabled materializer")
	}
	if recovery != (loglinks.RecoveryReport{}) {
		t.Fatalf("recovery = %#v, want empty initial recovery", recovery)
	}
}

func TestNewLogLinkMaterializerReturnsStartupError(t *testing.T) {
	sourceRoot := canonicalTempDir(t)
	destination := canonicalTempDir(t)
	cfg := config.WithDefaults(config.Config{
		HeimdallEndpoint: "127.0.0.1:8080",
		LogLinks: config.LogLinksConfig{
			Enabled:             true,
			Destination:         destination,
			SourceRoots:         []string{sourceRoot},
			ApplicationPatterns: []string{"["},
		},
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() unexpectedly rejected startup configuration: %v", err)
	}

	materializer, _, err := newLogLinkMaterializer(cfg, zap.NewNop())
	if err == nil {
		t.Fatal("newLogLinkMaterializer() succeeded for invalid pattern, want startup error")
	}
	if materializer != nil {
		t.Fatal("materializer != nil after startup error")
	}
	if !strings.Contains(err.Error(), "initialize log-link filesystem state") {
		t.Fatalf("error = %q, want startup context", err)
	}
}

func TestLogLogLinkRecoveryEmitsOnePathFreeRecord(t *testing.T) {
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	logLogLinkRecovery(logger, loglinks.RecoveryReport{
		Owned:          4,
		InvalidEntries: 2,
		ManifestError:  true,
	})

	entries := recorded.All()
	if len(entries) != 1 {
		t.Fatalf("recovery log entries = %d, want 1", len(entries))
	}
	entry := entries[0]
	if entry.Message != "recovered Slurm log-link ownership manifest with issues" {
		t.Fatalf("message = %q, want recovery message", entry.Message)
	}
	if entry.Level != zapcore.WarnLevel {
		t.Fatalf("level = %s, want warn", entry.Level)
	}
	fields := entry.ContextMap()
	want := map[string]any{
		"owned_links":              int64(4),
		"invalid_manifest_entries": int64(2),
		"manifest_error":           true,
	}
	if len(fields) != len(want) {
		t.Fatalf("fields = %#v, want only aggregate recovery fields", fields)
	}
	for key, value := range want {
		if fields[key] != value {
			t.Fatalf("field %s = %#v, want %#v", key, fields[key], value)
		}
	}
}

func TestNewLoggerUsesConfiguredLevel(t *testing.T) {
	logger, shutdown, err := newLogger(context.Background(), config.Config{LogLevel: "debug"})
	if err != nil {
		t.Fatalf("newLogger() failed: %v", err)
	}
	defer logger.Sync()
	defer func() { _ = shutdown(context.Background()) }()

	if !logger.Core().Enabled(zapcore.DebugLevel) {
		t.Fatal("logger does not enable debug level")
	}
}

func TestStartMetricsServerDisabled(t *testing.T) {
	server, addr, err := startMetricsServer(context.Background(), config.Config{MetricsEnabled: false}, telemetry.Handler(prometheus.NewRegistry()), zap.NewNop())
	if err != nil {
		t.Fatalf("startMetricsServer() failed: %v", err)
	}
	if server != nil {
		t.Fatal("server != nil, want nil when metrics disabled")
	}
	if addr != "" {
		t.Fatalf("addr = %q, want empty when metrics disabled", addr)
	}
}

func TestMetricsMuxServesOnlyMetricsPath(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := telemetry.NewRecorder(reg)
	recorder.ObserveSync(reconcile.SyncObservation{
		Mode:     reconcile.ObserveMode,
		Outcome:  reconcile.ObservationOutcomeSuccess,
		Duration: time.Millisecond,
	})

	mux := newMetricsMux(telemetry.Handler(reg))
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() failed: %v", err)
	}
	if !strings.Contains(string(body), "slurm_advisor_controller_reconcile_loops_total") {
		t.Fatalf("metrics body missing controller metric:\n%s", string(body))
	}

	for _, path := range []string{"/", "/not-metrics"} {
		t.Run(path, func(t *testing.T) {
			resp := httptest.NewRecorder()
			mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, path, nil))
			if resp.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d", resp.Code, http.StatusNotFound)
			}
		})
	}
}

type recordingObserver struct {
	reconcile.NopObserver
}

type stubLogLinkMaterializer struct{}

func (*stubLogLinkMaterializer) Enqueue(
	time.Time,
	[]loglinks.Target,
) {
}

func (*stubLogLinkMaterializer) Drain() reconcile.LogLinkObservation {
	return reconcile.LogLinkObservation{}
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()

	path, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("resolve temporary directory: %v", err)
	}
	return path
}

func TestWarnUserActionSelection(t *testing.T) {
	for _, tt := range []struct {
		name     string
		mode     config.Mode
		users    []string
		warnings int
	}{
		{name: "act and list warn", mode: config.ModeAct, users: []string{"alice"}, warnings: 1},
		{name: "act without list", mode: config.ModeAct},
		{name: "observe and list", mode: config.ModeObserve, users: []string{"alice"}},
		{name: "ignore and list", mode: config.ModeIgnore, users: []string{"alice"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)
			warnUserActionSelection(config.Config{Mode: tt.mode, UserAllowlist: tt.users}, zap.New(core))
			if logs.Len() != tt.warnings {
				t.Fatalf("startup warnings = %d, want %d", logs.Len(), tt.warnings)
			}
		})
	}
}
