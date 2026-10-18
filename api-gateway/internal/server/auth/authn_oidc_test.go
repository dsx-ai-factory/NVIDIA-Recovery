// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/zitadel/oidc/v3/pkg/client/rp"
	"github.com/zitadel/oidc/v3/pkg/oidc"
	"google.golang.org/grpc/metadata"
)

const (
	testOIDCIssuer   = "https://issuer.example.com"
	testOIDCClientID = "heimdall"
)

type testOIDCKeySet struct {
	publicKey *rsa.PublicKey
}

func (s testOIDCKeySet) VerifySignature(ctx context.Context, signature *jose.JSONWebSignature) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return signature.Verify(s.publicKey)
}

func TestOidcAuthAcceptsValidBearerToken(t *testing.T) {
	authn, privateKey := newTestOidcAuth(t)
	token := signTestOIDCToken(t, privateKey, testOIDCIssuer, time.Now().Add(time.Hour))

	req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	ok, ctx, err := authn.CheckHttp(req.Context(), req)
	if err != nil {
		t.Fatalf("CheckHttp() error = %v", err)
	}
	if !ok {
		t.Fatal("CheckHttp() did not authenticate valid OIDC token")
	}

	assertTestOIDCIdentity(t, ctx)
}

func TestOidcAuthAcceptsValidGrpcBearerToken(t *testing.T) {
	authn, privateKey := newTestOidcAuth(t)
	token := signTestOIDCToken(t, privateKey, testOIDCIssuer, time.Now().Add(time.Hour))
	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("authorization", "Bearer "+token),
	)

	ok, authenticatedCtx, err := authn.CheckGrpc(ctx, nil, nil)
	if err != nil {
		t.Fatalf("CheckGrpc() error = %v", err)
	}
	if !ok {
		t.Fatal("CheckGrpc() did not authenticate valid OIDC token")
	}

	assertTestOIDCIdentity(t, authenticatedCtx)
}

func TestOidcAuthRejectsInvalidBearerTokens(t *testing.T) {
	authn, privateKey := newTestOidcAuth(t)
	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}

	tests := []struct {
		name  string
		token string
	}{
		{name: "malformed", token: "not-a-jwt"},
		{
			name:  "wrong issuer",
			token: signTestOIDCToken(t, privateKey, "https://other-issuer.example.com", time.Now().Add(time.Hour)),
		},
		{
			name:  "expired",
			token: signTestOIDCToken(t, privateKey, testOIDCIssuer, time.Now().Add(-time.Hour)),
		},
		{
			name:  "invalid signature",
			token: signTestOIDCToken(t, otherKey, testOIDCIssuer, time.Now().Add(time.Hour)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/jobs", nil)
			req.Header.Set("Authorization", "Bearer "+tt.token)

			ok, _, err := authn.CheckHttp(req.Context(), req)
			if err != nil {
				t.Fatalf("CheckHttp() error = %v", err)
			}
			if ok {
				t.Fatal("CheckHttp() authenticated invalid OIDC token")
			}
		})
	}
}

func newTestOidcAuth(t *testing.T) (*OidcAuth, *rsa.PrivateKey) {
	t.Helper()

	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	verifier := rp.NewIDTokenVerifier(
		testOIDCIssuer,
		testOIDCClientID,
		testOIDCKeySet{publicKey: &privateKey.PublicKey},
		rp.WithSupportedSigningAlgorithms(string(jose.RS256)),
	)

	return NewOidcAuth(verifier, IdentityClaimConfig{ClientIDClaim: "client_id"}), privateKey
}

func signTestOIDCToken(t *testing.T, privateKey *rsa.PrivateKey, issuer string, expiry time.Time) string {
	t.Helper()

	claims := oidc.NewIDTokenClaims(
		issuer,
		"subject-123",
		[]string{testOIDCClientID},
		expiry,
		time.Now().Add(-time.Minute),
		"",
		"",
		nil,
		testOIDCClientID,
		0,
	)
	claims.Claims = map[string]any{
		"scope": "jobs:read jobs:write",
	}

	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: privateKey}, nil)
	if err != nil {
		t.Fatalf("NewSigner() error = %v", err)
	}
	token, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}
	return token
}

func assertTestOIDCIdentity(t *testing.T, ctx context.Context) {
	t.Helper()

	identity := IdentityFromContext(ctx)
	if identity == nil {
		t.Fatal("expected identity in context")
	}
	if identity.Mechanism != AuthMechanismOIDC {
		t.Fatalf("Mechanism = %q, want %q", identity.Mechanism, AuthMechanismOIDC)
	}
	if identity.UserID != "subject-123" {
		t.Fatalf("UserID = %q, want subject-123", identity.UserID)
	}
	if identity.ClientID != testOIDCClientID {
		t.Fatalf("ClientID = %q, want %q", identity.ClientID, testOIDCClientID)
	}
	if len(identity.Scopes) != 2 || identity.Scopes[0] != "jobs:read" || identity.Scopes[1] != "jobs:write" {
		t.Fatalf("Scopes = %v, want [jobs:read jobs:write]", identity.Scopes)
	}
}
