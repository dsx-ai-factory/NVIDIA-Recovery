// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/config"
	"go.opentelemetry.io/otel"
	otlptracegrpc "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otlptracehttp "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.uber.org/zap"
)

const recoveryServiceName = "nvidia-recovery-slurm-advisor-controller"

// TracingProvider owns the process-wide OpenTelemetry trace provider.
type TracingProvider struct {
	provider *sdktrace.TracerProvider
}

// NewTracingProvider configures W3C propagation even when no exporter endpoint
// is present. This keeps recovery-context validation and continuation available
// in every deployment while making export opt-in through standard OTLP env vars.
func NewTracingProvider(ctx context.Context, cfg config.TracingConfig, logger *zap.Logger) (*TracingProvider, error) {
	if logger == nil {
		logger = zap.NewNop()
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(recoveryServiceName)),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create tracing resource")
	}

	options := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	}
	if cfg.Endpoint != "" {
		exporter, exportErr := newTraceExporter(ctx, cfg)
		if exportErr != nil {
			return nil, errors.Wrap(exportErr, "create OTLP trace exporter")
		}
		options = append(options, sdktrace.WithBatcher(exporter))
	}

	provider := sdktrace.NewTracerProvider(options...)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		logger.Warn("OpenTelemetry tracing error", zap.Error(err))
	}))

	logger.Info(
		"initialized OpenTelemetry tracing",
		zap.String("service_name", recoveryServiceName),
		zap.Bool("export_enabled", cfg.Endpoint != ""),
		zap.String("protocol", cfg.Protocol),
	)
	return &TracingProvider{provider: provider}, nil
}

func newTraceExporter(ctx context.Context, cfg config.TracingConfig) (sdktrace.SpanExporter, error) {
	switch cfg.Protocol {
	case "grpc":
		options := []otlptracegrpc.Option{otlptracegrpc.WithEndpointURL(cfg.Endpoint)}
		if strings.HasPrefix(cfg.Endpoint, "http://") {
			options = append(options, otlptracegrpc.WithInsecure())
		}
		return otlptracegrpc.New(ctx, options...)
	case "http", "http/protobuf":
		options := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(cfg.Endpoint)}
		if strings.HasPrefix(cfg.Endpoint, "http://") {
			options = append(options, otlptracehttp.WithInsecure())
		}
		return otlptracehttp.New(ctx, options...)
	default:
		return nil, errors.Newf("unsupported OTLP trace protocol %q", cfg.Protocol)
	}
}

// Shutdown flushes and stops the trace provider.
func (p *TracingProvider) Shutdown(ctx context.Context) error {
	if p == nil || p.provider == nil {
		return nil
	}
	return p.provider.Shutdown(ctx)
}
