// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"reflect"
	"testing"
)

func TestIdentityContext(t *testing.T) {
	identity := &Identity{
		Mechanism: AuthMechanismOIDC,
		UserID:    "user123",
		ClientID:  "client456",
		Scopes:    []string{"read", "write"},
	}

	ctx := ContextWithIdentity(context.Background(), identity)
	retrieved := IdentityFromContext(ctx)

	if retrieved == nil {
		t.Fatal("expected identity to be retrieved, got nil")
	}
	if retrieved.Mechanism != AuthMechanismOIDC {
		t.Errorf("expected Mechanism %q, got %q", AuthMechanismOIDC, retrieved.Mechanism)
	}
	if retrieved.UserID != "user123" {
		t.Errorf("expected UserID %q, got %q", "user123", retrieved.UserID)
	}
	if retrieved.ClientID != "client456" {
		t.Errorf("expected ClientID %q, got %q", "client456", retrieved.ClientID)
	}
	if !reflect.DeepEqual(retrieved.Scopes, []string{"read", "write"}) {
		t.Errorf("expected Scopes %v, got %v", []string{"read", "write"}, retrieved.Scopes)
	}
}

func TestIdentityFromContextReturnsNilWhenMissing(t *testing.T) {
	retrieved := IdentityFromContext(context.Background())
	if retrieved != nil {
		t.Errorf("expected nil, got %v", retrieved)
	}
}
