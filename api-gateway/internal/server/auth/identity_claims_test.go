// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"reflect"
	"testing"
)

func TestIdentityFromClaimMapUsesConfiguredClaims(t *testing.T) {
	identity := identityFromClaimMap(
		AuthMechanismJWT,
		map[string]any{
			"sub":       "standard-subject",
			"email":     "user@example.com",
			"client_id": "client-123",
			"scp":       []any{"jobs:read", "jobs:write"},
		},
		IdentityClaimConfig{
			SubjectClaim:  "email",
			ClientIDClaim: "client_id",
			ScopesClaim:   "scp",
		},
	)

	if identity == nil {
		t.Fatal("expected identity")
	}
	if identity.Mechanism != AuthMechanismJWT {
		t.Fatalf("Mechanism = %q, want %q", identity.Mechanism, AuthMechanismJWT)
	}
	if identity.UserID != "user@example.com" {
		t.Fatalf("UserID = %q, want user@example.com", identity.UserID)
	}
	if identity.ClientID != "client-123" {
		t.Fatalf("ClientID = %q, want client-123", identity.ClientID)
	}
	if want := []string{"jobs:read", "jobs:write"}; !reflect.DeepEqual(identity.Scopes, want) {
		t.Fatalf("Scopes = %v, want %v", identity.Scopes, want)
	}
}

func TestIdentityFromClaimMapDefaultsToStandardClaims(t *testing.T) {
	identity := identityFromClaimMap(
		AuthMechanismOIDC,
		map[string]any{
			"sub":   "subject-123",
			"scope": "jobs:read jobs:write",
		},
		IdentityClaimConfig{},
	)

	if identity == nil {
		t.Fatal("expected identity")
	}
	if identity.UserID != "subject-123" {
		t.Fatalf("UserID = %q, want subject-123", identity.UserID)
	}
	if want := []string{"jobs:read", "jobs:write"}; !reflect.DeepEqual(identity.Scopes, want) {
		t.Fatalf("Scopes = %v, want %v", identity.Scopes, want)
	}
}

func TestIdentityFromClaimMapDefaultsToScpWhenScopeIsMissing(t *testing.T) {
	identity := identityFromClaimMap(
		AuthMechanismJWT,
		map[string]any{
			"sub": "subject-123",
			"scp": []any{"jobs:read", "jobs:write"},
		},
		IdentityClaimConfig{},
	)

	if identity == nil {
		t.Fatal("expected identity")
	}
	if want := []string{"jobs:read", "jobs:write"}; !reflect.DeepEqual(identity.Scopes, want) {
		t.Fatalf("Scopes = %v, want %v", identity.Scopes, want)
	}
}

func TestIdentityFromClaimMapReturnsNilWhenSubjectIsMissing(t *testing.T) {
	identity := identityFromClaimMap(
		AuthMechanismJWT,
		map[string]any{"scope": "jobs:read"},
		IdentityClaimConfig{},
	)

	if identity != nil {
		t.Fatalf("identityFromClaimMap() = %#v, want nil", identity)
	}
}
