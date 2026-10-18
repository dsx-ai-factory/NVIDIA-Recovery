// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// JwtAuthConfig defines a trusted JWT issuer and its accepted identities.
type JwtAuthConfig struct {
	Issuer       string
	Audiences    []string
	JWKSURL      string
	SubjectClaim string
	// AllowedSubjects restricts the configured subject claim to exact values.
	// An empty list accepts any nonempty subject from the trusted issuer.
	AllowedSubjects []string
	ClientIDClaim   string
	ScopesClaim     string
}

func (c JwtAuthConfig) identityClaims() IdentityClaimConfig {
	return IdentityClaimConfig{
		SubjectClaim:  c.SubjectClaim,
		ClientIDClaim: c.ClientIDClaim,
		ScopesClaim:   c.ScopesClaim,
	}
}

func (c JwtAuthConfig) Validate() error {
	if c.Issuer == "" {
		return fmt.Errorf("JWT issuer is required")
	}
	if len(c.Audiences) == 0 {
		return fmt.Errorf("at least one JWT audience is required")
	}
	if c.JWKSURL == "" {
		return fmt.Errorf("JWT JWKS URL is required")
	}
	return nil
}

type JwtAuth struct {
	config      JwtAuthConfig
	keySet      jose.JSONWebKeySet
	allowedAlgs []jose.SignatureAlgorithm
}

func NewJwtAuth(ctx context.Context, config JwtAuthConfig, httpClient *http.Client) (*JwtAuth, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: time.Minute}
	}

	keySet, err := fetchJWKS(ctx, httpClient, config.JWKSURL)
	if err != nil {
		return nil, err
	}

	return &JwtAuth{
		config: config,
		keySet: keySet,
		allowedAlgs: []jose.SignatureAlgorithm{
			jose.RS256,
			jose.RS384,
			jose.RS512,
			jose.PS256,
			jose.PS384,
			jose.PS512,
			jose.ES256,
			jose.ES384,
			jose.ES512,
			jose.EdDSA,
		},
	}, nil
}

func fetchJWKS(ctx context.Context, httpClient *http.Client, jwksURL string) (jose.JSONWebKeySet, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, jwksURL, nil)
	if err != nil {
		return jose.JSONWebKeySet{}, fmt.Errorf("create JWKS request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return jose.JSONWebKeySet{}, fmt.Errorf("fetch JWKS: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return jose.JSONWebKeySet{}, fmt.Errorf("fetch JWKS: unexpected status %d", resp.StatusCode)
	}

	var keySet jose.JSONWebKeySet
	if err := json.NewDecoder(resp.Body).Decode(&keySet); err != nil {
		return jose.JSONWebKeySet{}, fmt.Errorf("decode JWKS: %w", err)
	}
	if len(keySet.Keys) == 0 {
		return jose.JSONWebKeySet{}, fmt.Errorf("JWKS contains no keys")
	}

	return keySet, nil
}

func (a *JwtAuth) CheckGrpc(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo) (bool, context.Context, error) {
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

func (a *JwtAuth) CheckHttp(ctx context.Context, req *http.Request) (bool, context.Context, error) {
	tokenString, ok := bearerToken(req.Header.Get("Authorization"))
	if !ok {
		return false, nil, nil
	}

	return a.checkToken(ctx, tokenString)
}

func (a *JwtAuth) checkToken(ctx context.Context, tokenString string) (bool, context.Context, error) {
	token, err := jwt.ParseSigned(tokenString, a.allowedAlgs)
	if err != nil {
		zap.L().Debug("JWT parse failed", zap.Error(err))
		return false, nil, nil
	}

	claims, customClaims, err := a.verifiedClaims(token)
	if err != nil {
		zap.L().Debug("JWT validation failed", zap.Error(err))
		return false, nil, nil
	}

	if claims.Expiry == nil {
		zap.L().Debug("JWT claims validation failed", zap.String("reason", "missing exp claim"))
		return false, nil, nil
	}

	if err := claims.Validate(jwt.Expected{
		Issuer:      a.config.Issuer,
		AnyAudience: jwt.Audience(a.config.Audiences),
	}); err != nil {
		zap.L().Debug("JWT claims validation failed", zap.Error(err))
		return false, nil, nil
	}

	claimMap := jwtClaimsToMap(claims, customClaims)
	identity := identityFromClaimMap(AuthMechanismJWT, claimMap, a.config.identityClaims())
	if identity == nil {
		zap.L().Debug("JWT validation failed", zap.String("reason", "missing subject claim"))
		return false, nil, nil
	}
	if len(a.config.AllowedSubjects) > 0 && !slices.Contains(a.config.AllowedSubjects, identity.UserID) {
		zap.L().Debug("JWT validation failed", zap.String("reason", "subject not allowed"))
		return false, nil, nil
	}
	ctxWithIdentity := ContextWithIdentity(ctx, identity)

	return true, ctxWithIdentity, nil
}

func (a *JwtAuth) verifiedClaims(token *jwt.JSONWebToken) (jwt.Claims, map[string]any, error) {
	keys := a.keysForToken(token)
	if len(keys) == 0 {
		return jwt.Claims{}, nil, fmt.Errorf("no matching JWT verification keys")
	}

	var lastErr error
	for _, key := range keys {
		if key.Use != "" && key.Use != "sig" {
			continue
		}
		claims := jwt.Claims{}
		customClaims := map[string]any{}
		if err := token.Claims(key.Key, &claims, &customClaims); err != nil {
			lastErr = err
			continue
		}
		return claims, customClaims, nil
	}

	if lastErr != nil {
		return jwt.Claims{}, nil, lastErr
	}
	return jwt.Claims{}, nil, fmt.Errorf("no usable JWT verification keys")
}

func (a *JwtAuth) keysForToken(token *jwt.JSONWebToken) []jose.JSONWebKey {
	if len(token.Headers) == 0 || token.Headers[0].KeyID == "" {
		return a.keySet.Keys
	}
	return a.keySet.Key(token.Headers[0].KeyID)
}

func jwtClaimsToMap(claims jwt.Claims, customClaims map[string]any) map[string]any {
	claimMap := map[string]any{}
	for key, value := range customClaims {
		claimMap[key] = value
	}
	claimMap["iss"] = claims.Issuer
	claimMap["sub"] = claims.Subject
	claimMap["aud"] = []string(claims.Audience)
	claimMap["jti"] = claims.ID
	return claimMap
}
