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
//
// Trace export is optional, so an exporter that cannot be created is logged and
// dropped rather than stopping scheduler reconciliation. Without an exporter
// nothing is sampled: audit events record span IDs only for sampled spans, and
// no trace would ever exist under them.
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

	options := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	exportEnabled := false
	if cfg.Endpoint != "" {
		exporter, exportErr := newTraceExporter(ctx, cfg)
		if exportErr != nil {
			logger.Warn("create OTLP trace exporter; continuing without trace export", zap.Error(exportErr))
		} else {
			options = append(options, sdktrace.WithBatcher(exporter))
			exportEnabled = true
		}
	}
	if exportEnabled {
		options = append(options, sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())))
	} else {
		options = append(options, sdktrace.WithSampler(sdktrace.NeverSample()))
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
		zap.Bool("export_enabled", exportEnabled),
		zap.String("protocol", cfg.Protocol),
	)
	return &TracingProvider{provider: provider}, nil
}

func newTraceExporter(ctx context.Context, cfg config.TracingConfig) (sdktrace.SpanExporter, error) {
	switch cfg.Protocol {
	case "grpc":
		options := []otlptracegrpc.Option{otlptracegrpc.WithEndpointURL(cfg.Endpoint)}
		if cfg.Token != "" {
			options = append(options, otlptracegrpc.WithHeaders(bearerHeader(cfg.Token)))
		}
		if strings.HasPrefix(cfg.Endpoint, "http://") {
			options = append(options, otlptracegrpc.WithInsecure())
		}
		return otlptracegrpc.New(ctx, options...)
	case "http", "http/protobuf":
		options := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(cfg.Endpoint)}
		if cfg.Token != "" {
			options = append(options, otlptracehttp.WithHeaders(bearerHeader(cfg.Token)))
		}
		if strings.HasPrefix(cfg.Endpoint, "http://") {
			options = append(options, otlptracehttp.WithInsecure())
		}
		return otlptracehttp.New(ctx, options...)
	default:
		return nil, errors.Newf("unsupported OTLP trace protocol %q", cfg.Protocol)
	}
}

// bearerHeader authenticates like the controller's OTLP log exporter, so a
// deployment that supplies only OTEL_EXPORTER_OTLP_TOKEN exports both signals.
func bearerHeader(token string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + token}
}

// Shutdown flushes and stops the trace provider.
func (p *TracingProvider) Shutdown(ctx context.Context) error {
	if p == nil || p.provider == nil {
		return nil
	}
	return p.provider.Shutdown(ctx)
}
