// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"fmt"
	"net/http"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Authenticator is the interface for authentication providers
//
// Multiple authenticators can be configured at once and will be checked in order until
// one returns true. At least one authenticator must succeed otherwise the request will be a 401.
//
// Authenticators return a context on success based on the original request context but populated
// with information on the authenticated user.
type Authenticator interface {
	CheckGrpc(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo) (bool, context.Context, error)
	CheckHttp(ctx context.Context, req *http.Request) (bool, context.Context, error)
}

// Auth is the interface for authentication providers

type Auth struct {
	sessionMiddleware func(next http.Handler) http.Handler
	authenticators    []Authenticator
}

func NewAuth() *Auth {
	return &Auth{}
}

func (a *Auth) IsEmpty() bool {
	return len(a.authenticators) == 0
}

func (a *Auth) AddAuthenticator(authenticator Authenticator) {
	a.authenticators = append(a.authenticators, authenticator)
}

func (a *Auth) SetSessionMiddleware(sessionMiddleware func(next http.Handler) http.Handler) {
	a.sessionMiddleware = sessionMiddleware
}

func (a *Auth) GrpcAuthInterceptorfunc(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	for _, authenticator := range a.authenticators {
		ok, ctx, err := authenticator.CheckGrpc(ctx, req, info)
		if err != nil {
			return nil, err
		}
		if ok {
			return handler(ctx, req)
		}
	}
	return nil, status.Errorf(codes.Unauthenticated, "authentication failed")
}

func (a *Auth) HttpAuthMiddleware(next http.Handler) http.Handler {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, authenticator := range a.authenticators {
			zap.L().Debug("Attempting authentication with authenticator", zap.String("authenticator", fmt.Sprintf("%T", authenticator)))

			ok, ctx, err := authenticator.CheckHttp(r.Context(), r)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}
			if ok {
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
		}

		zap.L().Debug("Authentication failed: all authenticators failed")
		http.Error(w, "authentication failed", http.StatusUnauthorized)
	})

	if a.sessionMiddleware != nil {
		return a.sessionMiddleware(handler)
	}

	return handler
}
