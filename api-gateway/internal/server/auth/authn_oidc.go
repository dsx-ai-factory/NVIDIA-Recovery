// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/zitadel/oidc/v3/pkg/client/rp"
	"github.com/zitadel/oidc/v3/pkg/oidc"
	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// OidcAuth implements the Authenticator interface using OIDC ID tokens.
type OidcAuth struct {
	tokenVerifier  *rp.IDTokenVerifier
	identityClaims IdentityClaimConfig
}

func NewOidcAuth(tokenVerifier *rp.IDTokenVerifier, identityClaims IdentityClaimConfig) *OidcAuth {
	return &OidcAuth{
		tokenVerifier:  tokenVerifier,
		identityClaims: identityClaims,
	}
}

func (a *OidcAuth) parseValidToken(ctx context.Context, tokenString string) (*oidc.IDTokenClaims, error) {
	idToken, err := rp.VerifyIDToken[*oidc.IDTokenClaims](ctx, tokenString, a.tokenVerifier)
	if err != nil {
		return nil, fmt.Errorf("failed to verify OIDC token: %w", err)
	}
	return idToken, nil
}

func (a *OidcAuth) CheckGrpc(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo) (bool, context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false, nil, nil
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return false, nil, nil
	}

	tokenString, ok := bearerToken(authHeaders[0])
	if !ok {
		return false, nil, nil
	}

	return a.checkToken(ctx, tokenString)
}

func (a *OidcAuth) CheckHttp(ctx context.Context, req *http.Request) (bool, context.Context, error) {
	tokenString, ok := bearerToken(req.Header.Get("Authorization"))
	if !ok {
		return false, nil, nil
	}

	return a.checkToken(ctx, tokenString)
}

func (a *OidcAuth) checkToken(ctx context.Context, tokenString string) (bool, context.Context, error) {
	claims, err := a.parseValidToken(ctx, tokenString)
	if err != nil {
		zap.L().Debug("OIDC token validation failed", zap.Error(err))
		return false, nil, nil
	}

	claimMap := oidcClaimsToMap(claims)
	identity := identityFromClaimMap(AuthMechanismOIDC, claimMap, a.identityClaims)
	if identity == nil {
		zap.L().Debug("OIDC token validation failed", zap.String("reason", "missing subject claim"))
		return false, nil, nil
	}
	ctxWithIdentity := ContextWithIdentity(ctx, identity)

	zap.L().Debug("OIDC token validation successful", zap.String("subject", identity.UserID))
	return true, ctxWithIdentity, nil
}

func oidcClaimsToMap(claims *oidc.IDTokenClaims) map[string]any {
	claimMap := map[string]any{}
	if claims == nil {
		return claimMap
	}

	for key, value := range claims.Claims {
		claimMap[key] = value
	}

	claimMap["iss"] = claims.Issuer
	claimMap["sub"] = claims.Subject
	claimMap["aud"] = []string(claims.Audience)
	claimMap["azp"] = claims.AuthorizedParty
	claimMap["client_id"] = claims.ClientID
	claimMap["email"] = claims.Email
	claimMap["preferred_username"] = claims.PreferredUsername

	return claimMap
}

func bearerToken(authHeader string) (string, bool) {
	parts := strings.Split(authHeader, " ")
	if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") || parts[1] == "" {
		return "", false
	}
	return parts[1], true
}
