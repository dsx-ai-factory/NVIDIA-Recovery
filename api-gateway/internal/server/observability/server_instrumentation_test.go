// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGrpcServerStatsHandler_ReturnsServerOption(t *testing.T) {
	opt := GrpcServerStatsHandler()
	if opt == nil {
		t.Fatal("expected non-nil grpc.ServerOption")
	}
}

func TestHttpServerMiddleware_PassesThrough(t *testing.T) {
	handler := HttpServerMiddleware("test-service")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello"))
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
