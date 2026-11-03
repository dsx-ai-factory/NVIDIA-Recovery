// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"strings"
)

// AuthMechanism identifies the generic authenticator that accepted a request.
type AuthMechanism string

const (
	AuthMechanismOIDC       AuthMechanism = "oidc"
	AuthMechanismJWT        AuthMechanism = "jwt"
	AuthMechanismKubernetes AuthMechanism = "kubernetes"
)

// IdentityClaimConfig maps JWT/OIDC claims into Heimdall's generic identity
// context. Empty fields use standard OIDC/JWT claim defaults.
type IdentityClaimConfig struct {
	SubjectClaim  string
	ClientIDClaim string
	ScopesClaim   string
}

// Identity represents the authenticated caller. It is kept in request context
// for in-process authorization decisions only; it is not forwarded as headers.
type Identity struct {
	Mechanism AuthMechanism
	UserID    string
	ClientID  string
	Scopes    []string
}

type identityContextKey struct{}

func ContextWithIdentity(ctx context.Context, identity *Identity) context.Context {
	return context.WithValue(ctx, identityContextKey{}, identity)
}

func IdentityFromContext(ctx context.Context) *Identity {
	identity, _ := ctx.Value(identityContextKey{}).(*Identity)
	return identity
}

func identityFromClaimMap(mechanism AuthMechanism, claims map[string]any, config IdentityClaimConfig) *Identity {
	subjectClaim := config.SubjectClaim
	if subjectClaim == "" {
		subjectClaim = "sub"
	}
	userID := stringClaim(claims, subjectClaim)
	if userID == "" {
		return nil
	}

	scopes := configuredScopesFromClaims(claims, config.ScopesClaim)

	identity := &Identity{
		Mechanism: mechanism,
		UserID:    userID,
		Scopes:    scopes,
	}

	if config.ClientIDClaim != "" {
		identity.ClientID = stringClaim(claims, config.ClientIDClaim)
	}

	return identity
}

func configuredScopesFromClaims(claims map[string]any, scopesClaim string) []string {
	if scopesClaim != "" {
		return scopesFromClaim(claims[scopesClaim])
	}

	scopes := scopesFromClaim(claims["scope"])
	if len(scopes) > 0 {
		return scopes
	}
	return scopesFromClaim(claims["scp"])
}

func stringClaim(claims map[string]any, claimName string) string {
	value, ok := claims[claimName]
	if !ok {
		return ""
	}
	stringValue, _ := value.(string)
	return stringValue
}

func scopesFromClaim(value any) []string {
	switch scopes := value.(type) {
	case string:
		return strings.Fields(scopes)
	case []string:
		return nonEmptyStrings(scopes)
	case []any:
		values := make([]string, 0, len(scopes))
		for _, scope := range scopes {
			if s, ok := scope.(string); ok && s != "" {
				values = append(values, s)
			}
		}
		return values
	default:
		return nil
	}
}

func nonEmptyStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			result = append(result, value)
		}
	}
	return result
}
