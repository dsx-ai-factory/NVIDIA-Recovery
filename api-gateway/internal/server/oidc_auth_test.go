// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coreos/go-oidc/v3/oidc"
	"google.golang.org/grpc"
)

// This is a simplified test implementation that mocks the OIDC provider
// In a real scenario, you would use a testing library for OIDC

type mockIDToken struct {
	issuer string
	claims map[string]interface{}
}

func (m *mockIDToken) Claims(v interface{}) error {
	if claims, ok := v.(*map[string]interface{}); ok {
		*claims = m.claims
		return nil
	}
	return nil
}

func (m *mockIDToken) Issuer() string {
	return m.issuer
}

type mockOidcAuth struct {
	shouldValidate bool
	issuer         string
	claims         map[string]interface{}
}

func (m *mockOidcAuth) ParseValidToken(tokenString string) (interface{}, error) {
	if !m.shouldValidate {
		return nil, &oidc.TokenExpiredError{}
	}

	token := &mockIDToken{
		issuer: m.issuer,
		claims: m.claims,
	}
	return token, nil
}

func (m *mockOidcAuth) GrpcAuthInterceptorfunc(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Implementation for tests
	if !m.shouldValidate {
		return nil, &oidc.TokenExpiredError{}
	}
	return handler(ctx, req)
}

func (m *mockOidcAuth) HttpAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !m.shouldValidate {
			http.Error(w, "Invalid token", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func TestOidcHttpAuthMiddleware(t *testing.T) {
	// Create a mock OIDC auth that validates tokens
	validAuth := &mockOidcAuth{
		shouldValidate: true,
		issuer:         "https://test.example.com",
		claims: map[string]interface{}{
			"sub":   "user123",
			"email": "test@example.com",
		},
	}

	// Create a mock OIDC auth that rejects tokens
	invalidAuth := &mockOidcAuth{
		shouldValidate: false,
	}

	// Test handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Test with valid auth
	validMiddleware := validAuth.HttpAuthMiddleware(handler)
	req, err := http.NewRequest("GET", "/test", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer valid-token")

	recorder := httptest.NewRecorder()
	validMiddleware.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("Expected status code %d, got %d", http.StatusOK, recorder.Code)
	}

	// Test with invalid auth
	invalidMiddleware := invalidAuth.HttpAuthMiddleware(handler)
	req, err = http.NewRequest("GET", "/test", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer invalid-token")

	recorder = httptest.NewRecorder()
	invalidMiddleware.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("Expected status code %d, got %d", http.StatusUnauthorized, recorder.Code)
	}
}
