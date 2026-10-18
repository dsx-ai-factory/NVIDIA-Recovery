// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"google.golang.org/grpc/metadata"
)

func TestJwtAuthAcceptsValidJWKSBackedBearerToken(t *testing.T) {
	privateKey, jwksURL := testJWKS(t)
	token := signTestJWT(t, privateKey, jwt.Claims{
		Issuer:   "https://issuer.example.com",
		Subject:  "subject-123",
		Audience: jwt.Audience{"heimdall"},
		Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}, map[string]any{
		"client_id": "client-123",
		"scope":     "jobs:read jobs:write",
	})

	authn, err := NewJwtAuth(context.Background(), JwtAuthConfig{
		Issuer:        "https://issuer.example.com",
		Audiences:     []string{"heimdall"},
		JWKSURL:       jwksURL,
		ClientIDClaim: "client_id",
	}, nil)
	if err != nil {
		t.Fatalf("NewJwtAuth() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	ok, ctx, err := authn.CheckHttp(req.Context(), req)
	if err != nil {
		t.Fatalf("CheckHttp() error = %v", err)
	}
	if !ok {
		t.Fatal("CheckHttp() did not authenticate valid token")
	}

	identity := IdentityFromContext(ctx)
	if identity == nil {
		t.Fatal("expected identity in context")
	}
	if identity.Mechanism != AuthMechanismJWT {
		t.Fatalf("Mechanism = %q, want %q", identity.Mechanism, AuthMechanismJWT)
	}
	if identity.UserID != "subject-123" {
		t.Fatalf("UserID = %q, want subject-123", identity.UserID)
	}
	if identity.ClientID != "client-123" {
		t.Fatalf("ClientID = %q, want client-123", identity.ClientID)
	}
}

func TestJwtAuthRejectsInvalidAudience(t *testing.T) {
	privateKey, jwksURL := testJWKS(t)
	token := signTestJWT(t, privateKey, jwt.Claims{
		Issuer:   "https://issuer.example.com",
		Subject:  "subject-123",
		Audience: jwt.Audience{"other-service"},
		Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}, nil)

	authn, err := NewJwtAuth(context.Background(), JwtAuthConfig{
		Issuer:    "https://issuer.example.com",
		Audiences: []string{"heimdall"},
		JWKSURL:   jwksURL,
	}, nil)
	if err != nil {
		t.Fatalf("NewJwtAuth() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	ok, _, err := authn.CheckHttp(req.Context(), req)
	if err != nil {
		t.Fatalf("CheckHttp() error = %v", err)
	}
	if ok {
		t.Fatal("CheckHttp() authenticated token with wrong audience")
	}
}

func TestJwtAuthRejectsMissingExpiration(t *testing.T) {
	privateKey, jwksURL := testJWKS(t)
	token := signTestJWT(t, privateKey, jwt.Claims{
		Issuer:   "https://issuer.example.com",
		Subject:  "subject-123",
		Audience: jwt.Audience{"heimdall"},
	}, nil)

	authn, err := NewJwtAuth(context.Background(), JwtAuthConfig{
		Issuer:    "https://issuer.example.com",
		Audiences: []string{"heimdall"},
		JWKSURL:   jwksURL,
	}, nil)
	if err != nil {
		t.Fatalf("NewJwtAuth() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	ok, _, err := authn.CheckHttp(req.Context(), req)
	if err != nil {
		t.Fatalf("CheckHttp() error = %v", err)
	}
	if ok {
		t.Fatal("CheckHttp() authenticated token without exp claim")
	}
}

func TestJwtAuthRejectsMalformedBearerToken(t *testing.T) {
	_, jwksURL := testJWKS(t)
	authn, err := NewJwtAuth(context.Background(), JwtAuthConfig{
		Issuer:    "https://issuer.example.com",
		Audiences: []string{"heimdall"},
		JWKSURL:   jwksURL,
	}, nil)
	if err != nil {
		t.Fatalf("NewJwtAuth() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer not-a-jwt")

	ok, _, err := authn.CheckHttp(req.Context(), req)
	if err != nil {
		t.Fatalf("CheckHttp() error = %v", err)
	}
	if ok {
		t.Fatal("CheckHttp() authenticated malformed token")
	}
}

func TestJwtAuthAcceptsValidGrpcBearerToken(t *testing.T) {
	privateKey, jwksURL := testJWKS(t)
	token := signTestJWT(t, privateKey, validTestJWTClaims(), nil)
	authn := newTestJwtAuth(t, jwksURL)
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("authorization", "Bearer "+token),
	)

	ok, authenticatedCtx, err := authn.CheckGrpc(ctx, nil, nil)
	if err != nil {
		t.Fatalf("CheckGrpc() error = %v", err)
	}
	if !ok {
		t.Fatal("CheckGrpc() did not authenticate valid JWT")
	}
	identity := IdentityFromContext(authenticatedCtx)
	if identity == nil || identity.Mechanism != AuthMechanismJWT || identity.UserID != "subject-123" {
		t.Fatalf("IdentityFromContext() = %#v, want JWT identity for subject-123", identity)
	}
}

func TestJwtAuthAllowedSubjects(t *testing.T) {
	privateKey, jwksURL := testJWKS(t)
	const sacSubject = "system:serviceaccount:slinky:sac"
	tests := []struct {
		name            string
		subject         string
		issuer          string
		allowedSubjects []string
		subjectClaim    string
		extraClaims     map[string]any
		wantAccepted    bool
		wantUserID      string
	}{
		{name: "SAC", subject: sacSubject, allowedSubjects: []string{sacSubject}, wantAccepted: true, wantUserID: sacSubject},
		{name: "second allowed identity", subject: sacSubject, allowedSubjects: []string{"another-subject", sacSubject}, wantAccepted: true, wantUserID: sacSubject},
		{name: "other service account", subject: "system:serviceaccount:slinky:default", allowedSubjects: []string{sacSubject}},
		{name: "other namespace", subject: "system:serviceaccount:other:sac", allowedSubjects: []string{sacSubject}},
		{name: "subject prefix", subject: sacSubject + "-other", allowedSubjects: []string{sacSubject}},
		{name: "subject whitespace", subject: sacSubject + " ", allowedSubjects: []string{sacSubject}},
		{name: "missing subject", allowedSubjects: []string{sacSubject}},
		{name: "wrong issuer", subject: sacSubject, issuer: "https://other-issuer.example.com", allowedSubjects: []string{sacSubject}},
		{name: "no restriction", subject: "existing-client", wantAccepted: true, wantUserID: "existing-client"},
		{name: "empty restriction", subject: "existing-client", allowedSubjects: []string{}, wantAccepted: true, wantUserID: "existing-client"},
		{name: "mapped claim", subject: "different-subject", subjectClaim: "client_identity", extraClaims: map[string]any{"client_identity": sacSubject}, allowedSubjects: []string{sacSubject}, wantAccepted: true, wantUserID: sacSubject},
		{name: "mapped claim cannot bypass restriction", subject: sacSubject, subjectClaim: "client_identity", extraClaims: map[string]any{"client_identity": "other-client"}, allowedSubjects: []string{sacSubject}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authn, err := NewJwtAuth(context.Background(), JwtAuthConfig{
				Issuer: "https://issuer.example.com", Audiences: []string{"heimdall"}, JWKSURL: jwksURL,
				AllowedSubjects: tt.allowedSubjects, SubjectClaim: tt.subjectClaim,
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			claims := validTestJWTClaims()
			claims.Subject = tt.subject
			if tt.issuer != "" {
				claims.Issuer = tt.issuer
			}
			token := signTestJWT(t, privateKey, claims, tt.extraClaims)
			req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			grpcCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
			checks := map[string]func() (bool, context.Context, error){
				"HTTP": func() (bool, context.Context, error) { return authn.CheckHttp(req.Context(), req) },
				"gRPC": func() (bool, context.Context, error) { return authn.CheckGrpc(grpcCtx, nil, nil) },
			}
			for protocol, check := range checks {
				t.Run(protocol, func(t *testing.T) {
					accepted, ctx, err := check()
					if err != nil || accepted != tt.wantAccepted {
						t.Fatalf("accepted = %v, error = %v; want accepted = %v", accepted, err, tt.wantAccepted)
					}
					if accepted {
						identity := IdentityFromContext(ctx)
						if identity == nil {
							t.Fatal("accepted request has no authenticated identity")
						}
						if identity.UserID != tt.wantUserID {
							t.Fatalf("UserID = %q, want %q", identity.UserID, tt.wantUserID)
						}
					}
					if !accepted && ctx != nil {
						t.Fatal("rejected request returned an authenticated context")
					}
				})
			}
		})
	}
}

func TestJwtAuthRejectsInvalidRegisteredClaims(t *testing.T) {
	privateKey, jwksURL := testJWKS(t)
	authn := newTestJwtAuth(t, jwksURL)

	tests := []struct {
		name   string
		claims jwt.Claims
	}{
		{
			name: "wrong issuer",
			claims: jwt.Claims{
				Issuer:   "https://other-issuer.example.com",
				Subject:  "subject-123",
				Audience: jwt.Audience{"heimdall"},
				Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
			},
		},
		{
			name: "expired",
			claims: jwt.Claims{
				Issuer:   "https://issuer.example.com",
				Subject:  "subject-123",
				Audience: jwt.Audience{"heimdall"},
				Expiry:   jwt.NewNumericDate(time.Now().Add(-time.Hour)),
			},
		},
		{
			name: "not active yet",
			claims: jwt.Claims{
				Issuer:    "https://issuer.example.com",
				Subject:   "subject-123",
				Audience:  jwt.Audience{"heimdall"},
				Expiry:    jwt.NewNumericDate(time.Now().Add(2 * time.Hour)),
				NotBefore: jwt.NewNumericDate(time.Now().Add(time.Hour)),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token := signTestJWT(t, privateKey, tt.claims, nil)
			assertJWTRejected(t, authn, token)
		})
	}
}

func TestJwtAuthRejectsInvalidSignatureUnsupportedAlgorithmAndUnknownKey(t *testing.T) {
	privateKey, jwksURL := testJWKS(t)
	authn := newTestJwtAuth(t, jwksURL)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	tests := []struct {
		name  string
		token string
	}{
		{
			name:  "invalid signature",
			token: signTestJWT(t, otherKey, validTestJWTClaims(), nil),
		},
		{
			name: "unsupported algorithm",
			token: signTestJWTWithKey(
				t,
				jose.SigningKey{Algorithm: jose.HS256, Key: []byte("01234567890123456789012345678901")},
				"test-key",
				validTestJWTClaims(),
				nil,
			),
		},
		{
			name: "unknown key",
			token: signTestJWTWithKey(
				t,
				jose.SigningKey{Algorithm: jose.RS256, Key: privateKey},
				"unknown-key",
				validTestJWTClaims(),
				nil,
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertJWTRejected(t, authn, tt.token)
		})
	}
}

func TestJwtAuthIgnoresAPIKeyHeader(t *testing.T) {
	_, jwksURL := testJWKS(t)
	authn := newTestJwtAuth(t, jwksURL)
	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("X-Api-Key", "nvapi-not-supported")

	ok, _, err := authn.CheckHttp(req.Context(), req)
	if err != nil {
		t.Fatalf("CheckHttp() error = %v", err)
	}
	if ok {
		t.Fatal("CheckHttp() authenticated X-Api-Key credential")
	}
}

func testJWKS(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	keySet := jose.JSONWebKeySet{
		Keys: []jose.JSONWebKey{
			{
				Key:       &privateKey.PublicKey,
				KeyID:     "test-key",
				Algorithm: string(jose.RS256),
				Use:       "sig",
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(keySet); err != nil {
			t.Fatalf("Encode() error = %v", err)
		}
	}))
	t.Cleanup(server.Close)

	return privateKey, server.URL
}

func signTestJWT(t *testing.T, privateKey *rsa.PrivateKey, claims jwt.Claims, extraClaims map[string]any) string {
	t.Helper()
	return signTestJWTWithKey(
		t,
		jose.SigningKey{Algorithm: jose.RS256, Key: privateKey},
		"test-key",
		claims,
		extraClaims,
	)
}

func signTestJWTWithKey(
	t *testing.T,
	signingKey jose.SigningKey,
	keyID string,
	claims jwt.Claims,
	extraClaims map[string]any,
) string {
	t.Helper()

	options := (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", keyID)
	signer, err := jose.NewSigner(signingKey, options)
	if err != nil {
		t.Fatalf("NewSigner() error = %v", err)
	}

	builder := jwt.Signed(signer).Claims(claims)
	if extraClaims != nil {
		builder = builder.Claims(extraClaims)
	}
	token, err := builder.Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	return token
}

func validTestJWTClaims() jwt.Claims {
	return jwt.Claims{
		Issuer:   "https://issuer.example.com",
		Subject:  "subject-123",
		Audience: jwt.Audience{"heimdall"},
		Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
}

func newTestJwtAuth(t *testing.T, jwksURL string) *JwtAuth {
	t.Helper()

	authn, err := NewJwtAuth(context.Background(), JwtAuthConfig{
		Issuer:    "https://issuer.example.com",
		Audiences: []string{"heimdall"},
		JWKSURL:   jwksURL,
	}, nil)
	if err != nil {
		t.Fatalf("NewJwtAuth() error = %v", err)
	}
	return authn
}

func assertJWTRejected(t *testing.T, authn *JwtAuth, token string) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	ok, _, err := authn.CheckHttp(req.Context(), req)
	if err != nil {
		t.Fatalf("CheckHttp() error = %v", err)
	}
	if ok {
		t.Fatal("CheckHttp() authenticated invalid JWT")
	}
}
