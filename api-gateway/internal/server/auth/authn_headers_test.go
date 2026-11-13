// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4/jwt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestHeaderIdentity checks the same assertion contract over HTTP and gRPC.
func TestHeaderIdentity(t *testing.T) {
	for _, tc := range []struct {
		name               string
		mechanism, client  []string
		accepted, rejected bool
	}{
		{name: "absent"},
		{"valid", []string{"service"}, []string{"subject-1"}, true, false},
		{"missing client", []string{"service"}, nil, false, true},
		{"missing mechanism", nil, []string{"subject-1"}, false, true},
		{"empty", []string{""}, []string{""}, false, true},
		{"duplicate client", []string{"service"}, []string{"a", "b"}, false, true},
		{"duplicate mechanism", []string{"a", "b"}, []string{"subject"}, false, true},
		{"joined duplicates", []string{"service"}, []string{"a,b"}, false, true},
		{"control characters", []string{"service"}, []string{"a\nb"}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
			r.Header[http.CanonicalHeaderKey(HeaderAuthMechanism)] = tc.mechanism
			r.Header[http.CanonicalHeaderKey(HeaderClientID)] = tc.client
			checker := HeaderBasedIdentityAuth{}
			check := func(ok bool, ctx context.Context, err error) {
				t.Helper()
				if ok != tc.accepted || (err != nil) != tc.rejected {
					t.Fatalf("accepted=%v error=%v", ok, err)
				}
				if ok {
					identity := IdentityFromContext(ctx)
					if identity == nil || identity.ClientID != "subject-1" || identity.UserID != "" || len(identity.Scopes) != 0 {
						t.Fatalf("unexpected identity: %+v", identity)
					}
				}
			}
			check(checker.CheckHttp(r.Context(), r))
			ctx := metadata.NewIncomingContext(t.Context(), metadata.MD{"x-nvr-auth-mechanism": tc.mechanism, "x-nvr-client-id": tc.client})
			check(checker.CheckGrpc(ctx, nil, nil))
		})
	}
}

// TestHeaderIdentityBoundary verifies default-off behavior, alias stripping, and OIDC fallback.
func TestHeaderIdentityBoundary(t *testing.T) {
	for _, trustHeaderBasedIdentity := range []bool{false, true} {
		chain := NewAuth()
		if trustHeaderBasedIdentity {
			chain.AddAuthenticator(HeaderBasedIdentityAuth{})
		}
		// With no fallback credentials, ordinary auth must deny this request.
		handler := HeaderIdentityBoundary(trustHeaderBasedIdentity, chain.HttpAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Grpc-Metadata-X-NVR-Client-Id") != "" || r.Header.Get("X-NVR-Scopes") != "" {
				t.Error("untrusted metadata survived")
			}
			w.WriteHeader(http.StatusNoContent)
		})))
		r := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		r.Header.Set(HeaderAuthMechanism, "service")
		r.Header.Set(HeaderClientID, "subject-1")
		r.Header.Set("Grpc-Metadata-X-NVR-Client-Id", "spoofed")
		r.Header.Set("X-NVR-Scopes", "admin")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		expected := http.StatusUnauthorized
		if trustHeaderBasedIdentity {
			expected = http.StatusNoContent
		}
		if w.Code != expected {
			t.Fatalf("trustHeaderBasedIdentity=%v status=%d", trustHeaderBasedIdentity, w.Code)
		}
	}
}

type headerTestStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s headerTestStream) Context() context.Context { return s.ctx }

// TestHeaderIdentityStream rejects unauthenticated streams and carries verified identity.
func TestHeaderIdentityStream(t *testing.T) {
	chain := NewAuth()
	chain.AddAuthenticator(HeaderBasedIdentityAuth{})
	for _, valid := range []bool{false, true} {
		md := metadata.MD{}
		if valid {
			md = metadata.Pairs("x-nvr-auth-mechanism", "service", "x-nvr-client-id", "subject-1")
		}
		ctx := metadata.NewIncomingContext(t.Context(), md)
		reached := false
		err := HeaderIdentityStreamInterceptor(true, chain)(nil, headerTestStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: "/example.Service/Watch"}, func(_ interface{}, stream grpc.ServerStream) error {
			reached = true
			if IdentityFromContext(stream.Context()).ClientID != "subject-1" {
				t.Error("identity lost")
			}
			return nil
		})
		if reached != valid || (!valid && status.Code(err) != codes.Unauthenticated) {
			t.Fatalf("valid=%v reached=%v err=%v", valid, reached, err)
		}
	}
}

// TestHeaderIdentityFallsBackToJWT proves absent assertions preserve token auth,
// while malformed assertions cannot be rescued by an otherwise valid token.
func TestHeaderIdentityFallsBackToJWT(t *testing.T) {
	key, jwksURL := testJWKS(t)
	token := signTestJWT(t, key, jwt.Claims{Issuer: "https://issuer.example.com", Subject: "human-1", Audience: jwt.Audience{"gateway"}, Expiry: jwt.NewNumericDate(time.Now().Add(time.Hour))}, nil)
	jwtAuth, err := NewJwtAuth(t.Context(), JwtAuthConfig{Issuer: "https://issuer.example.com", Audiences: []string{"gateway"}, JWKSURL: jwksURL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	chain := NewAuth()
	chain.AddAuthenticator(HeaderBasedIdentityAuth{})
	chain.AddAuthenticator(jwtAuth)
	for _, malformed := range []bool{false, true} {
		request := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		if malformed {
			request.Header.Set(HeaderClientID, "partial")
		}
		response := httptest.NewRecorder()
		chain.HttpAuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if IdentityFromContext(r.Context()).UserID != "human-1" {
				t.Error("normal identity lost")
			}
			w.WriteHeader(http.StatusNoContent)
		})).ServeHTTP(response, request)
		want := http.StatusNoContent
		if malformed {
			want = http.StatusUnauthorized
		}
		if response.Code != want {
			t.Fatalf("HTTP malformed=%v status=%d", malformed, response.Code)
		}
		md := metadata.Pairs("authorization", "Bearer "+token)
		if malformed {
			md.Set("x-nvr-client-id", "partial")
		}
		_, err := chain.GrpcAuthInterceptorfunc(metadata.NewIncomingContext(t.Context(), md), nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ interface{}) (interface{}, error) {
			if IdentityFromContext(ctx).UserID != "human-1" {
				t.Error("normal gRPC identity lost")
			}
			return nil, nil
		})
		if (err != nil) != malformed {
			t.Fatalf("gRPC malformed=%v err=%v", malformed, err)
		}
	}
}
