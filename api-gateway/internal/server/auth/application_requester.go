// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ApplicationRequester returns a verified end-user identity for ownership checks.
// Operators must map the subject claim to the scheduler username. A service
// account identity or an anonymous internal listener cannot establish job ownership.
func ApplicationRequester(ctx context.Context) (string, error) {
	identity := IdentityFromContext(ctx)
	if identity == nil || identity.UserID == "" {
		return "", status.Error(codes.Unauthenticated, "verified user identity is required")
	}
	if identity.Mechanism != AuthMechanismOIDC && identity.Mechanism != AuthMechanismJWT {
		return "", status.Error(codes.PermissionDenied, "a verified job-owner identity is required")
	}
	if !utf8.ValidString(identity.UserID) || utf8.RuneCountInString(identity.UserID) > 256 ||
		strings.ContainsFunc(identity.UserID, unicode.IsSpace) {
		return "", status.Error(codes.PermissionDenied, "identity cannot be used as a scheduler username")
	}
	return identity.UserID, nil
}
