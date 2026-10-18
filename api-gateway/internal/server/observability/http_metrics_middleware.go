// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// httpMetricsResponseWriter wraps http.ResponseWriter to capture the status code
// and number of bytes written, while preserving http.Flusher and http.Hijacker
// interfaces required by reverse proxies (SSE, WebSocket).
type httpMetricsResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int64
}

type countingReadCloser struct {
	io.ReadCloser
	bytesRead int64
}

func (r *countingReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytesRead += int64(n)
	return n, err
}

type httpMetricsInstruments struct {
	requestsTotal   otelmetric.Int64Counter
	requestDuration otelmetric.Float64Histogram
	inFlight        otelmetric.Int64UpDownCounter
	requestBytes    otelmetric.Int64Counter
	responseBytes   otelmetric.Int64Counter
}

func newHTTPMetricsResponseWriter(w http.ResponseWriter) *httpMetricsResponseWriter {
	return &httpMetricsResponseWriter{
		ResponseWriter: w,
		statusCode:     http.StatusOK, // default if WriteHeader is never called
	}
}

func (w *httpMetricsResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *httpMetricsResponseWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytesWritten += int64(n)
	return n, err
}

// Flush delegates to the underlying ResponseWriter if it implements http.Flusher.
// This is required for SSE and chunked transfer encoding used by reverse proxies.
func (w *httpMetricsResponseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap returns the underlying ResponseWriter, allowing http.ResponseController
// and similar helpers to discover optional interfaces.
func (w *httpMetricsResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// routePrefix extracts a bounded route label from the URL path by returning the
// first path segment (e.g. "/v1/jobs/123" → "/v1"). For the gateway's known
// route prefixes (/v1, /healthz, /metrics) this provides useful
// per-route metrics without unbounded cardinality.
func routePrefix(path string) string {
	// Fast-path common roots
	if path == "/" || path == "" {
		return "/"
	}
	path = strings.TrimPrefix(path, "/")
	first, _, _ := strings.Cut(path, "/")
	return "/" + first
}

func newHTTPMetricsInstruments(meter otelmetric.Meter) (httpMetricsInstruments, error) {
	requestsTotal, err := meter.Int64Counter("http.server.request.count",
		otelmetric.WithDescription("Total number of HTTP requests"),
	)
	if err != nil {
		return httpMetricsInstruments{}, fmt.Errorf("create http.server.request.count counter: %w", err)
	}

	requestDuration, err := meter.Float64Histogram("http.server.request.duration",
		otelmetric.WithDescription("HTTP request duration in seconds"),
		otelmetric.WithUnit("s"),
	)
	if err != nil {
		return httpMetricsInstruments{}, fmt.Errorf("create http.server.request.duration histogram: %w", err)
	}

	inFlight, err := meter.Int64UpDownCounter("http.server.active_requests",
		otelmetric.WithDescription("Number of HTTP requests currently in flight"),
	)
	if err != nil {
		return httpMetricsInstruments{}, fmt.Errorf("create http.server.active_requests counter: %w", err)
	}

	requestBytes, err := meter.Int64Counter("http.server.request.body.size",
		otelmetric.WithDescription("Total bytes received in HTTP request bodies"),
		otelmetric.WithUnit("By"),
	)
	if err != nil {
		return httpMetricsInstruments{}, fmt.Errorf("create http.server.request.body.size counter: %w", err)
	}

	responseBytes, err := meter.Int64Counter("http.server.response.body.size",
		otelmetric.WithDescription("Total bytes sent in HTTP response bodies"),
		otelmetric.WithUnit("By"),
	)
	if err != nil {
		return httpMetricsInstruments{}, fmt.Errorf("create http.server.response.body.size counter: %w", err)
	}

	return httpMetricsInstruments{
		requestsTotal:   requestsTotal,
		requestDuration: requestDuration,
		inFlight:        inFlight,
		requestBytes:    requestBytes,
		responseBytes:   responseBytes,
	}, nil
}

// HttpMetricsMiddleware returns HTTP middleware that records Prometheus metrics
// for every request: request count, duration, status codes, in-flight gauge,
// and request/response byte counters.
func HttpMetricsMiddleware(serviceName string) func(http.Handler) http.Handler {
	meter := otel.Meter(serviceName)
	instruments, err := newHTTPMetricsInstruments(meter)
	if err != nil {
		zap.L().Panic("failed to initialize HTTP metrics instruments", zap.Error(err))
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			var requestBody *countingReadCloser
			if r.ContentLength <= 0 && r.Body != nil {
				requestBody = &countingReadCloser{ReadCloser: r.Body}
				r.Body = requestBody
			}

			instruments.inFlight.Add(r.Context(), 1)
			defer instruments.inFlight.Add(r.Context(), -1)

			wrapped := newHTTPMetricsResponseWriter(w)
			next.ServeHTTP(wrapped, r)

			duration := time.Since(start).Seconds()

			attrs := otelmetric.WithAttributes(
				attribute.String("http.request.method", r.Method),
				attribute.Int("http.response.status_code", wrapped.statusCode),
				attribute.String("http.route", routePrefix(r.URL.Path)),
			)

			instruments.requestsTotal.Add(r.Context(), 1, attrs)
			instruments.requestDuration.Record(r.Context(), duration, attrs)
			if r.ContentLength > 0 {
				instruments.requestBytes.Add(r.Context(), r.ContentLength, attrs)
			} else if requestBody != nil && requestBody.bytesRead > 0 {
				instruments.requestBytes.Add(r.Context(), requestBody.bytesRead, attrs)
			}
			instruments.responseBytes.Add(r.Context(), wrapped.bytesWritten, attrs)
		})
	}
}
