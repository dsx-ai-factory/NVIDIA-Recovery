// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	otlptracegrpc "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otlptracehttp "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

// TracingProvider wraps the OpenTelemetry TracerProvider.
type TracingProvider struct {
	provider *trace.TracerProvider
}

// newTracingProvider creates a TracerProvider. If otlpEnabled is true an OTLP
// exporter is configured; otherwise a no-op exporter is used so spans are still
// created locally (useful for propagation) but not exported.
func newTracingProvider(cfg OtelConfig, res *resource.Resource, otlpEnabled bool) (*TracingProvider, error) {
	sampler := buildSampler(cfg.SampleRate)

	var exporter trace.SpanExporter
	if otlpEnabled {
		var err error
		exporter, err = newTraceExporter(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create trace exporter: %w", err)
		}
		zap.L().Info("OTLP trace exporter created",
			zap.String("endpoint", cfg.Endpoint),
			zap.String("protocol", cfg.Protocol),
		)
	} else {
		exporter = &noOpSpanExporter{}
		zap.L().Info("Using no-op trace exporter (OTLP not enabled)")
	}

	tp := trace.NewTracerProvider(
		trace.WithResource(res),
		trace.WithSampler(sampler),
		// Always use BatchSpanProcessor — even with the no-op exporter this
		// avoids the synchronous per-span overhead of SimpleSpanProcessor.
		trace.WithBatcher(exporter),
	)

	otel.SetTracerProvider(tp)

	return &TracingProvider{provider: tp}, nil
}

// Shutdown gracefully shuts down the TracerProvider.
func (tp *TracingProvider) Shutdown(ctx context.Context) error {
	if tp == nil || tp.provider == nil {
		return nil
	}
	zap.L().Info("Shutting down tracing provider")
	return tp.provider.Shutdown(ctx)
}

// buildSampler returns a Sampler based on the configured rate.
func buildSampler(rate float64) trace.Sampler {
	switch {
	case rate <= 0:
		return trace.NeverSample()
	case rate >= 1.0:
		return trace.AlwaysSample()
	default:
		return trace.ParentBased(trace.TraceIDRatioBased(rate))
	}
}

// newTraceExporter creates either a gRPC or HTTP OTLP trace exporter based on
// the protocol in the config.
func newTraceExporter(cfg OtelConfig) (trace.SpanExporter, error) {
	ctx := context.Background()

	switch cfg.Protocol {
	case "http/protobuf", "http/json", "http":
		return newHTTPTraceExporter(ctx, cfg)
	default:
		// Default to gRPC (project convention; OTel spec defaults to http/protobuf)
		return newGRPCTraceExporter(ctx, cfg)
	}
}

func newGRPCTraceExporter(ctx context.Context, cfg OtelConfig) (trace.SpanExporter, error) {
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpointURL(cfg.Endpoint),
	}

	// Derive TLS from the URL scheme rather than the Insecure flag.
	// The Insecure flag controls whether a token is required (canEnableOTLP),
	// not TLS configuration.
	if strings.HasPrefix(cfg.Endpoint, "http://") {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}

	if cfg.Token != "" {
		opts = append(opts, otlptracegrpc.WithHeaders(map[string]string{
			"Authorization": "Bearer " + cfg.Token,
		}))
	}

	return otlptracegrpc.New(ctx, opts...)
}

func newHTTPTraceExporter(ctx context.Context, cfg OtelConfig) (trace.SpanExporter, error) {
	opts := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(cfg.Endpoint),
	}

	if strings.HasPrefix(cfg.Endpoint, "http://") {
		opts = append(opts, otlptracehttp.WithInsecure())
	}

	if cfg.Token != "" {
		opts = append(opts, otlptracehttp.WithHeaders(map[string]string{
			"Authorization": "Bearer " + cfg.Token,
		}))
	}

	return otlptracehttp.New(ctx, opts...)
}

// noOpSpanExporter is a placeholder exporter that discards all spans.
type noOpSpanExporter struct{}

func (e *noOpSpanExporter) ExportSpans(_ context.Context, _ []trace.ReadOnlySpan) error {
	return nil
}

func (e *noOpSpanExporter) Shutdown(_ context.Context) error {
	return nil
}
