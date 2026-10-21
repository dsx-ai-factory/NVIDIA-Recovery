// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/config"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

func TestTracingProviderInitializesPropagationWithoutExporter(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
	})

	provider, err := NewTracingProvider(context.Background(), config.TracingConfig{Protocol: "http/protobuf"}, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTracingProvider() failed: %v", err)
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() failed: %v", err)
	}

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(context.Background(), carrier)
	if _, ok := otel.GetTextMapPropagator().(propagation.TraceContext); !ok {
		t.Fatalf("global propagator = %T, want W3C TraceContext", otel.GetTextMapPropagator())
	}
}

// Trace export is optional: a bad exporter setting must not stop scheduler
// reconciliation, and without an exporter no span may be marked sampled,
// because audit events would then name traces that are never sent.
func TestTracingWithoutWorkingExportKeepsRunningAndSamplesNothing(t *testing.T) {
	for name, cfg := range map[string]config.TracingConfig{
		"no endpoint":          {Protocol: "http/protobuf"},
		"unsupported protocol": {Endpoint: "http://collector:4318/v1/traces", Protocol: "http/json"},
	} {
		t.Run(name, func(t *testing.T) {
			previousProvider := otel.GetTracerProvider()
			t.Cleanup(func() { otel.SetTracerProvider(previousProvider) })

			provider, err := NewTracingProvider(context.Background(), cfg, zap.NewNop())
			if err != nil {
				t.Fatalf("NewTracingProvider() failed: %v", err)
			}
			t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

			recorder := NewRecorder(prometheus.NewRegistry())
			recovery := reconcile.RecoveryContext{
				SequenceID:  "f47ac10b-58cc-4372-a567-0e02b2c3d479",
				Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
				Operation:   reconcile.RecoveryOperationRestart,
			}
			parent, err := recorder.RestoreParent(context.Background(), recovery)
			if err != nil {
				t.Fatalf("RestoreParent() failed: %v", err)
			}
			_, span := recorder.StartReconcile(parent, reconcile.ReconcileSpanAttributes{Recovery: recovery, JobID: "42"})
			defer span.EndReconcile(reconcile.ReconcileSpanResult{Outcome: reconcile.RecoveryOutcomeApplied})
			if traceID, spanID := span.Correlation(); traceID != "" || spanID != "" {
				t.Fatalf("Correlation() = (%q, %q) without trace export, want empty", traceID, spanID)
			}
		})
	}
}

// A deployment configured only with the shared endpoint and token must reach
// the collector's trace path with the same credential the log exporter sends.
func TestTraceExportUsesTracePathAndBearerToken(t *testing.T) {
	type request struct{ path, authorization string }
	received := make(chan request, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case received <- request{r.URL.Path, r.Header.Get("Authorization")}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	t.Setenv("HEIMDALL_ENDPOINT", "127.0.0.1:8080")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", server.URL+"/otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_EXPORTER_OTLP_TOKEN", "trace-token")
	cfg, err := config.FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}

	previousProvider := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previousProvider) })
	provider, err := NewTracingProvider(context.Background(), cfg.Tracing, zap.NewNop())
	if err != nil {
		t.Fatalf("NewTracingProvider() failed: %v", err)
	}
	_, span := otel.Tracer("test").Start(context.Background(), "recovery")
	span.End()
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() failed: %v", err)
	}

	select {
	case got := <-received:
		if got.path != "/otlp/v1/traces" || got.authorization != "Bearer trace-token" {
			t.Fatalf("export request = %+v, want /otlp/v1/traces with bearer token", got)
		}
	default:
		t.Fatal("no trace export request reached the collector")
	}
}
