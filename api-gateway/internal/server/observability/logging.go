// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"context"
	"fmt"
	"strings"

	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"

	otlploggrpc "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	otlploghttp "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
)

// LoggingProvider wraps the OpenTelemetry LoggerProvider.
type LoggingProvider struct {
	provider *sdklog.LoggerProvider
}

// newLoggingProvider creates a LoggerProvider backed by an OTLP log exporter
// (gRPC or HTTP) and wraps it in a LoggingProvider.
func newLoggingProvider(cfg OtelConfig, res *resource.Resource) (*LoggingProvider, error) {
	exporter, err := newLogExporter(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create log exporter: %w", err)
	}

	provider := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)

	zap.L().Info("OTLP log exporter created",
		zap.String("endpoint", cfg.Endpoint),
		zap.String("protocol", cfg.Protocol),
	)

	return &LoggingProvider{provider: provider}, nil
}

// newLogExporter creates either a gRPC or HTTP OTLP log exporter based on the
// protocol in the config.
func newLogExporter(cfg OtelConfig) (sdklog.Exporter, error) {
	ctx := context.Background()

	switch cfg.Protocol {
	case "http/protobuf", "http/json", "http":
		return newHTTPLogExporter(ctx, cfg)
	default:
		// Default to gRPC (project convention; OTel spec defaults to http/protobuf)
		return newGRPCLogExporter(ctx, cfg)
	}
}

func newGRPCLogExporter(ctx context.Context, cfg OtelConfig) (sdklog.Exporter, error) {
	opts := []otlploggrpc.Option{
		otlploggrpc.WithEndpointURL(cfg.Endpoint),
	}

	if strings.HasPrefix(cfg.Endpoint, "http://") {
		opts = append(opts, otlploggrpc.WithInsecure())
	}

	if cfg.Token != "" {
		opts = append(opts, otlploggrpc.WithHeaders(map[string]string{
			"Authorization": "Bearer " + cfg.Token,
		}))
	}

	return otlploggrpc.New(ctx, opts...)
}

func newHTTPLogExporter(ctx context.Context, cfg OtelConfig) (sdklog.Exporter, error) {
	opts := []otlploghttp.Option{
		otlploghttp.WithEndpointURL(cfg.Endpoint),
	}

	if strings.HasPrefix(cfg.Endpoint, "http://") {
		opts = append(opts, otlploghttp.WithInsecure())
	}

	if cfg.Token != "" {
		opts = append(opts, otlploghttp.WithHeaders(map[string]string{
			"Authorization": "Bearer " + cfg.Token,
		}))
	}

	return otlploghttp.New(ctx, opts...)
}

// Shutdown gracefully shuts down the LoggerProvider.
func (lp *LoggingProvider) Shutdown(ctx context.Context) error {
	if lp == nil || lp.provider == nil {
		return nil
	}
	zap.L().Info("Shutting down logging provider")
	return lp.provider.Shutdown(ctx)
}

// Provider returns the underlying LoggerProvider.
func (lp *LoggingProvider) Provider() *sdklog.LoggerProvider {
	if lp == nil {
		return nil
	}
	return lp.provider
}
