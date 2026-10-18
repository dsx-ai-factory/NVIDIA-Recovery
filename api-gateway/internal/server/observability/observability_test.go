// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"context"
	"net"
	"testing"
)

func TestCanEnableOTLP(t *testing.T) {
	tests := []struct {
		name string
		cfg  OtelConfig
		want bool
	}{
		{
			name: "no endpoint",
			cfg:  OtelConfig{Endpoint: ""},
			want: false,
		},
		{
			name: "endpoint with token",
			cfg:  OtelConfig{Endpoint: "https://otel.example.com", Token: "secret"},
			want: true,
		},
		{
			name: "endpoint with insecure",
			cfg:  OtelConfig{Endpoint: "http://localhost:4317", Insecure: true},
			want: true,
		},
		{
			name: "endpoint without auth",
			cfg:  OtelConfig{Endpoint: "https://otel.example.com"},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := canEnableOTLP(tt.cfg)
			if got != tt.want {
				t.Errorf("canEnableOTLP() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewOtelProvider_Disabled(t *testing.T) {
	p, err := NewOtelProvider(OtelConfig{Enabled: false}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p.IsTracingEnabled() {
		t.Error("expected tracing to be disabled")
	}
	if p.IsLoggingEnabled() {
		t.Error("expected logging to be disabled")
	}
	if p.IsMetricsEnabled() {
		t.Error("expected metrics to be disabled")
	}
	// Shutdown should be safe on a disabled provider
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

func TestNewOtelProvider_EnabledNoEndpoint(t *testing.T) {
	p, err := NewOtelProvider(OtelConfig{
		Enabled:        true,
		ServiceName:    "test-service",
		ServiceVersion: "v0.0.1",
		SampleRate:     1.0,
	}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.IsTracingEnabled() {
		t.Error("expected tracing to be enabled")
	}
	if p.otlpEnabled {
		t.Error("expected OTLP to be disabled when no endpoint is set")
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

func TestNewOtelProvider_EnabledWithEndpointInsecure(t *testing.T) {
	// Start a throwaway listener so the OTLP exporter has something to connect
	// to and shutdown doesn't log connection errors.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cerr := lis.Close(); cerr != nil {
			t.Logf("failed to close listener: %v", cerr)
		}
	}()

	p, err := NewOtelProvider(OtelConfig{
		Enabled:        true,
		ServiceName:    "test-service",
		ServiceVersion: "v0.0.1",
		SampleRate:     1.0,
		Endpoint:       "http://" + lis.Addr().String(),
		Insecure:       true,
	}, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !p.IsTracingEnabled() {
		t.Error("expected tracing to be enabled")
	}
	if !p.otlpEnabled {
		t.Error("expected OTLP to be enabled with insecure endpoint")
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

func TestShutdown_NilProvider(t *testing.T) {
	var p *OtelProvider
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected error shutting down nil provider: %v", err)
	}
}
