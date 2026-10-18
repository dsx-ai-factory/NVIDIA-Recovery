// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.uber.org/zap"
)

// OtelConfig holds the configuration for OpenTelemetry.
type OtelConfig struct {
	Enabled        bool
	ServiceName    string
	ServiceVersion string
	Insecure       bool
	SampleRate     float64
	Protocol       string // "grpc" or "http" (also accepts "http/protobuf", "http/json")
	Endpoint       string
	Token          string
}

// OtelProvider is the unified provider for OpenTelemetry tracing, logging, and metrics.
type OtelProvider struct {
	resource        *resource.Resource
	tracingProvider *TracingProvider
	loggingProvider *LoggingProvider
	metricsProvider *MetricsProvider
	otlpEnabled     bool
}

// NewOtelProvider creates a new unified OtelProvider.
// metricsEnabled controls whether Prometheus metrics are initialised.
func NewOtelProvider(cfg OtelConfig, metricsEnabled bool) (*OtelProvider, error) {
	if !cfg.Enabled {
		zap.L().Info("OpenTelemetry is disabled")
		return &OtelProvider{}, nil
	}

	// Build shared resource using the semconv version that matches the SDK's
	// default schema URL to avoid merge conflicts.
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create otel resource: %w", err)
	}

	otlpEnabled := canEnableOTLP(cfg)

	p := &OtelProvider{
		resource:    res,
		otlpEnabled: otlpEnabled,
	}

	// Always initialise tracing
	p.tracingProvider, err = newTracingProvider(cfg, res, otlpEnabled)
	if err != nil {
		return nil, fmt.Errorf("failed to create tracing provider: %w", err)
	}

	// Set global propagator
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Initialise OTLP log export if enabled
	if otlpEnabled {
		p.loggingProvider, err = newLoggingProvider(cfg, res)
		if err != nil {
			return nil, fmt.Errorf("failed to create logging provider: %w", err)
		}
	}

	// Initialise Prometheus metrics if enabled
	if metricsEnabled {
		p.metricsProvider, err = newMetricsProvider(res)
		if err != nil {
			return nil, fmt.Errorf("failed to create metrics provider: %w", err)
		}
	}

	zap.L().Info("OpenTelemetry initialised",
		zap.String("service_name", cfg.ServiceName),
		zap.String("service_version", cfg.ServiceVersion),
		zap.Bool("otlp_enabled", otlpEnabled),
		zap.Bool("metrics_enabled", metricsEnabled),
		zap.Float64("sample_rate", cfg.SampleRate),
	)

	return p, nil
}

// canEnableOTLP returns true if the endpoint is set and authentication is
// available (either a token or insecure mode).
func canEnableOTLP(cfg OtelConfig) bool {
	if cfg.Endpoint == "" {
		return false
	}
	if cfg.Token != "" || cfg.Insecure {
		return true
	}
	zap.L().Warn("OTLP endpoint is set but no token or insecure flag provided; OTLP export disabled. "+
		"Set HEIMDALL_OTEL_INSECURE=true for unauthenticated collectors, or provide OTEL_EXPORTER_OTLP_TOKEN.",
		zap.String("endpoint", cfg.Endpoint),
	)
	return false
}

// Shutdown gracefully shuts down all providers in reverse initialisation order.
func (p *OtelProvider) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}

	var errs []error
	if p.metricsProvider != nil {
		errs = append(errs, p.metricsProvider.Shutdown(ctx))
	}
	if p.loggingProvider != nil {
		errs = append(errs, p.loggingProvider.Shutdown(ctx))
	}
	if p.tracingProvider != nil {
		errs = append(errs, p.tracingProvider.Shutdown(ctx))
	}
	return errors.Join(errs...)
}

// IsTracingEnabled reports whether tracing is active.
func (p *OtelProvider) IsTracingEnabled() bool {
	return p != nil && p.tracingProvider != nil
}

// IsLoggingEnabled reports whether OTLP logging is active.
func (p *OtelProvider) IsLoggingEnabled() bool {
	return p != nil && p.loggingProvider != nil
}

// IsMetricsEnabled reports whether metrics export is active.
func (p *OtelProvider) IsMetricsEnabled() bool {
	return p != nil && p.metricsProvider != nil
}

// LoggingProvider returns the logging provider (may be nil).
func (p *OtelProvider) LoggingProvider() *LoggingProvider {
	if p == nil {
		return nil
	}
	return p.loggingProvider
}

// MetricsHandler returns an HTTP handler that serves Prometheus metrics.
func (p *OtelProvider) MetricsHandler() http.Handler {
	if p == nil || p.metricsProvider == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Metrics not enabled", http.StatusNotFound)
		})
	}
	return promhttp.Handler()
}
