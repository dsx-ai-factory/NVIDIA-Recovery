// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
)

// MetricsProvider wraps an OTel MeterProvider backed by a Prometheus exporter.
type MetricsProvider struct {
	provider *metric.MeterProvider
}

// newMetricsProvider creates a new MetricsProvider with a Prometheus exporter
// and registers it as the global OTel MeterProvider.
func newMetricsProvider(res *resource.Resource) (*MetricsProvider, error) {
	exporter, err := promexporter.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus exporter: %w", err)
	}

	provider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(exporter),
	)
	otel.SetMeterProvider(provider)

	zap.L().Info("OTel metrics initialized with Prometheus exporter")
	return &MetricsProvider{provider: provider}, nil
}

// Shutdown gracefully shuts down the metrics provider.
func (mp *MetricsProvider) Shutdown(ctx context.Context) error {
	if mp == nil || mp.provider == nil {
		return nil
	}
	return mp.provider.Shutdown(ctx)
}
