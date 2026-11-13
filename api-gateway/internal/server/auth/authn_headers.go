// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"net/http"
	"strings"
	"unicode"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// HeaderAuthMechanism identifies the authentication performed by a trusted upstream authenticator.
	HeaderAuthMechanism = "X-NVR-Auth-Mechanism"
	// HeaderClientID contains the caller identifier verified by a trusted upstream authenticator.
	HeaderClientID = "X-NVR-Client-Id"
)

// HeaderBasedIdentityAuth accepts identity asserted by a trusted upstream authenticator.
// Register it only on listeners configured to trust header-based identity.
type HeaderBasedIdentityAuth struct{}

// IsIdentityHeader identifies the two supported assertion fields.
func IsIdentityHeader(key string) bool {
	return strings.EqualFold(key, HeaderAuthMechanism) || strings.EqualFold(key, HeaderClientID)
}

// headerIdentity rejects partial, duplicate, empty, or whitespace-bearing assertions.
func headerIdentity(get func(string) []string) (*Identity, error) {
	mechanism, client := get(HeaderAuthMechanism), get(HeaderClientID)
	if len(mechanism) == 0 && len(client) == 0 {
		return nil, nil
	}

	valid := func(values []string) bool {
		if len(values) != 1 {
			return false
		}

		value := values[0]
		return value != "" &&
			!strings.Contains(value, ",") &&
			// Prevent whitespaces and control characters.
			strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) < 0
	}
	if !valid(mechanism) || !valid(client) {
		return nil, errors.New("authentication failed")
	}
	return &Identity{Mechanism: AuthMechanism(mechanism[0]), ClientID: client[0]}, nil
}

// CheckHttp implements the existing authenticator interface for trusted HTTP assertions.
func (HeaderBasedIdentityAuth) CheckHttp(ctx context.Context, r *http.Request) (bool, context.Context, error) {
	identity, err := headerIdentity(r.Header.Values)
	if err != nil || identity == nil {
		return false, ctx, err
	}
	return true, ContextWithIdentity(ctx, identity), nil
}

// CheckGrpc implements the existing authenticator interface for trusted gRPC assertions.
func (HeaderBasedIdentityAuth) CheckGrpc(ctx context.Context, _ interface{}, _ *grpc.UnaryServerInfo) (bool, context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	identity, err := headerIdentity(func(key string) []string { return md.Get(strings.ToLower(key)) })
	if err != nil {
		return false, ctx, status.Error(codes.Unauthenticated, "authentication failed")
	}
	if identity == nil {
		return false, ctx, nil
	}
	return true, ContextWithIdentity(ctx, identity), nil
}

// shouldDropIdentityHeader reports whether a header must be removed under the listener
// trust policy. Metadata aliases and unsupported identity fields are always rejected.
func shouldDropIdentityHeader(key string, trustHeaderBasedIdentity bool) bool {
	lower := strings.ToLower(key)
	if strings.HasPrefix(lower, "grpc-metadata-x-nvr-") {
		return true
	}
	return strings.HasPrefix(lower, "x-nvr-") && (!trustHeaderBasedIdentity || !IsIdentityHeader(key))
}

// HeaderIdentityBoundary strips assertions on ordinary listeners and all metadata aliases.
func HeaderIdentityBoundary(trustHeaderBasedIdentity bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for key := range r.Header {
			if shouldDropIdentityHeader(key, trustHeaderBasedIdentity) {
				delete(r.Header, key)
			}
		}
		next.ServeHTTP(w, r)
	})
}

func sanitizeIdentityMetadata(ctx context.Context, trustHeaderBasedIdentity bool) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	md = md.Copy()
	for key := range md {
		if shouldDropIdentityHeader(key, trustHeaderBasedIdentity) {
			delete(md, key)
		}
	}
	return metadata.NewIncomingContext(ctx, md)
}

// HeaderIdentityUnaryInterceptor enforces the identity namespace boundary for every RPC.
func HeaderIdentityUnaryInterceptor(trustHeaderBasedIdentity bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return handler(sanitizeIdentityMetadata(ctx, trustHeaderBasedIdentity), req)
	}
}

type identityServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *identityServerStream) Context() context.Context { return s.ctx }

// HeaderIdentityStreamInterceptor sanitizes metadata and applies the endpoint's auth chain.
func HeaderIdentityStreamInterceptor(trustHeaderBasedIdentity bool, chain *Auth) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := sanitizeIdentityMetadata(stream.Context(), trustHeaderBasedIdentity)
		if chain == nil {
			return handler(srv, &identityServerStream{stream, ctx})
		}
		_, err := chain.GrpcAuthInterceptorfunc(ctx, nil, &grpc.UnaryServerInfo{Server: srv, FullMethod: info.FullMethod},
			func(authenticated context.Context, _ interface{}) (interface{}, error) {
				return nil, handler(srv, &identityServerStream{stream, authenticated})
			})
		return err
	}
}
