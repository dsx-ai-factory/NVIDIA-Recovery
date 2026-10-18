// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestExtractServiceFromMethod(t *testing.T) {
	tests := []struct {
		fullMethod string
		want       string
	}{
		{"/package.service/method", "service"},
		{"/heimdall.v1.HeimdallService/GetJob", "HeimdallService"},
	}

	for _, test := range tests {
		got := extractServiceFromMethod(test.fullMethod)
		if got != test.want {
			t.Errorf("extractServiceFromMethod(%q) = %q, want %q", test.fullMethod, got, test.want)
		}
	}
}

func TestExtractMethodFromMethod(t *testing.T) {
	tests := []struct {
		fullMethod string
		want       string
	}{
		{"/package.service/method", "method"},
		{"/heimdall.v1.HeimdallService/GetJob", "GetJob"},
	}

	for _, test := range tests {
		got := extractMethodFromMethod(test.fullMethod)
		if got != test.want {
			t.Errorf("extractMethodFromMethod(%q) = %q, want %q", test.fullMethod, got, test.want)
		}
	}
}

func TestHttpTracingMiddleware_UsesForwardedProtoForScheme(t *testing.T) {
	exporter, cleanup := setupTestTracer()
	defer cleanup()

	handler := HttpTracingMiddleware("test-service")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("X-Forwarded-Proto", "HTTPS, http")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}

	var scheme string
	for _, attr := range spans[0].Attributes {
		if attr.Key == attribute.Key("url.scheme") {
			scheme = attr.Value.AsString()
			break
		}
	}

	if scheme != "https" {
		t.Fatalf("url.scheme = %q, want %q", scheme, "https")
	}
}
