// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/config"
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

func TestTracingProviderRejectsUnsupportedExporterWithoutChangingControllerConfig(t *testing.T) {
	_, err := NewTracingProvider(context.Background(), config.TracingConfig{
		Endpoint: "http://collector:4318",
		Protocol: "http/json",
	}, zap.NewNop())
	if err == nil {
		t.Fatal("NewTracingProvider() accepted unsupported exporter protocol")
	}
}
