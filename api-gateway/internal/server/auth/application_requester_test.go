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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestApplicationRequesterRequiresVerifiedOwnerIdentity(t *testing.T) {
	for _, test := range []struct {
		name     string
		identity *Identity
		code     codes.Code
	}{
		{"anonymous", nil, codes.Unauthenticated},
		{"missing subject", &Identity{Mechanism: AuthMechanismJWT}, codes.Unauthenticated},
		{"service account", &Identity{Mechanism: AuthMechanismKubernetes, UserID: "alice"}, codes.PermissionDenied},
		{"unknown provider", &Identity{UserID: "alice"}, codes.PermissionDenied},
		{"whitespace", &Identity{Mechanism: AuthMechanismJWT, UserID: " alice"}, codes.PermissionDenied},
		{"oidc owner", &Identity{Mechanism: AuthMechanismOIDC, UserID: "alice"}, codes.OK},
		{"jwt owner", &Identity{Mechanism: AuthMechanismJWT, UserID: "Alice"}, codes.OK},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := ContextWithIdentity(context.Background(), test.identity)
			owner, err := ApplicationRequester(ctx)
			if status.Code(err) != test.code {
				t.Fatalf("error = %v, want %v", err, test.code)
			}
			if test.code == codes.OK && owner != test.identity.UserID {
				t.Fatalf("identity was rewritten: %q", owner)
			}
		})
	}
}

func TestApplicationRequesterUsesSignedConfiguredClaimOnBothTransports(t *testing.T) {
	key, jwks := testJWKS(t)
	verifier, err := NewJwtAuth(context.Background(), JwtAuthConfig{
		Issuer: "https://issuer.example.com", Audiences: []string{"heimdall"},
		JWKSURL: jwks, SubjectClaim: "preferred_username",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	token := signTestJWT(t, key, jwt.Claims{
		Issuer: "https://issuer.example.com", Subject: "opaque-subject", Audience: jwt.Audience{"heimdall"},
		Expiry: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}, map[string]any{"preferred_username": "alice"})
	req := httptest.NewRequest(http.MethodPost, "/v1/application-intentions", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	// Caller-supplied headers cannot override the verified claim.
	req.Header.Set("X-Requested-By", "bob")
	grpcContext := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token, "x-requested-by", "bob"))
	for _, check := range []func() (bool, context.Context, error){
		func() (bool, context.Context, error) { return verifier.CheckHttp(req.Context(), req) },
		func() (bool, context.Context, error) { return verifier.CheckGrpc(grpcContext, nil, nil) },
	} {
		ok, ctx, err := check()
		if err != nil || !ok {
			t.Fatalf("authentication: %v, %v", ok, err)
		}
		owner, err := ApplicationRequester(ctx)
		if err != nil || owner != "alice" {
			t.Fatalf("requester = %q, %v", owner, err)
		}
	}
}
