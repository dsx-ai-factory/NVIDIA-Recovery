// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type failingMeterProvider struct {
	metricnoop.MeterProvider
}

func (failingMeterProvider) Meter(string, ...otelmetric.MeterOption) otelmetric.Meter {
	return failingMeter{}
}

type failingMeter struct {
	metricnoop.Meter
}

func (failingMeter) Int64Counter(string, ...otelmetric.Int64CounterOption) (otelmetric.Int64Counter, error) {
	return nil, errors.New("instrument creation failed")
}

func setupTestMeterProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(previousProvider)
		_ = provider.Shutdown(context.Background())
	})

	return reader
}

func collectResourceMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()

	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatalf("Collect() error = %v", err)
	}

	return metrics
}

func int64SumMetricValue(t *testing.T, metrics metricdata.ResourceMetrics, name string) int64 {
	t.Helper()

	for _, scopeMetrics := range metrics.ScopeMetrics {
		for _, metric := range scopeMetrics.Metrics {
			if metric.Name != name {
				continue
			}

			sum, ok := metric.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q has type %T, want metricdata.Sum[int64]", name, metric.Data)
			}

			var total int64
			for _, point := range sum.DataPoints {
				total += point.Value
			}

			return total
		}
	}

	t.Fatalf("metric %q not found", name)
	return 0
}

func TestHttpMetricsMiddleware_PassesThrough(t *testing.T) {
	handler := HttpMetricsMiddleware("test-service")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("hello")); err != nil {
			t.Errorf("failed to write response: %v", err)
		}
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	if rec.Body.String() != "hello" {
		t.Errorf("expected body %q, got %q", "hello", rec.Body.String())
	}
}

func TestHttpMetricsResponseWriter_DefaultStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	w := newHTTPMetricsResponseWriter(rec)

	if w.statusCode != http.StatusOK {
		t.Errorf("expected default status %d, got %d", http.StatusOK, w.statusCode)
	}
}

func TestHttpMetricsResponseWriter_ByteCounting(t *testing.T) {
	rec := httptest.NewRecorder()
	w := newHTTPMetricsResponseWriter(rec)

	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if _, err := w.Write([]byte(" world")); err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	if w.bytesWritten != 11 {
		t.Errorf("expected 11 bytes written, got %d", w.bytesWritten)
	}
}

func TestRoutePrefix(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/v1/jobs/123", "/v1"},
		{"/unknown/foo/bar", "/unknown"},
		{"/healthz", "/healthz"},
		{"/", "/"},
		{"", "/"},
		{"/metrics", "/metrics"},
	}
	for _, tt := range tests {
		if got := routePrefix(tt.path); got != tt.want {
			t.Errorf("routePrefix(%q) = %q, want %q", tt.path, got, tt.want)
		}
	}
}

func TestHttpMetricsResponseWriter_Flush(t *testing.T) {
	rec := httptest.NewRecorder()
	w := newHTTPMetricsResponseWriter(rec)

	// httptest.ResponseRecorder implements http.Flusher, so Flush should not panic.
	w.Flush()
	if !rec.Flushed {
		t.Error("expected underlying writer to be flushed")
	}
}

func TestHttpMetricsResponseWriter_Unwrap(t *testing.T) {
	rec := httptest.NewRecorder()
	w := newHTTPMetricsResponseWriter(rec)

	if w.Unwrap() != rec {
		t.Error("Unwrap should return the underlying ResponseWriter")
	}
}

func TestHttpMetricsMiddleware_PanicsWhenInstrumentCreationFails(t *testing.T) {
	previousProvider := otel.GetMeterProvider()
	otel.SetMeterProvider(failingMeterProvider{})
	t.Cleanup(func() {
		otel.SetMeterProvider(previousProvider)
	})

	defer func() {
		if recover() == nil {
			t.Fatal("expected HttpMetricsMiddleware to panic when instrument creation fails")
		}
	}()

	_ = HttpMetricsMiddleware("test-service")
}

func TestHttpMetricsMiddleware_RecordsChunkedRequestBodyBytes(t *testing.T) {
	reader := setupTestMeterProvider(t)

	handler := HttpMetricsMiddleware("test-service")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if got, want := string(body), "chunked-body"; got != want {
			t.Fatalf("body = %q, want %q", got, want)
		}

		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader("chunked-body"))
	req.ContentLength = -1
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusCreated)
	}

	metrics := collectResourceMetrics(t, reader)
	if got, want := int64SumMetricValue(t, metrics, "http.server.request.body.size"), int64(len("chunked-body")); got != want {
		t.Fatalf("http.server.request.body.size = %d, want %d", got, want)
	}
}

func TestGrpcMetricsStatsHandler_ReturnsServerOption(t *testing.T) {
	// GrpcMetricsStatsHandler should return a valid grpc.ServerOption without panicking.
	opt := GrpcMetricsStatsHandler()
	if opt == nil {
		t.Fatal("expected non-nil grpc.ServerOption")
	}
}
