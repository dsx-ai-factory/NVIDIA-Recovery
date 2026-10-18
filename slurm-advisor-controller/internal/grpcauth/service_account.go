// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package grpcauth provides shared credentials for API Gateway RPCs.
package grpcauth

import (
	"context"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/credentials"
)

// ServiceAccountTokenCredentials loads a projected ServiceAccount token before
// each RPC so Kubernetes token rotation takes effect without reconnecting.
type ServiceAccountTokenCredentials struct {
	tokenPath  string
	requireTLS bool
}

var _ credentials.PerRPCCredentials = ServiceAccountTokenCredentials{}

// NewServiceAccountTokenCredentials creates credentials backed by a projected
// ServiceAccount token file.
func NewServiceAccountTokenCredentials(tokenPath string) ServiceAccountTokenCredentials {
	return ServiceAccountTokenCredentials{tokenPath: tokenPath}
}

// GetRequestMetadata attaches the current token as bearer authorization metadata.
func (c ServiceAccountTokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	token, err := os.ReadFile(c.tokenPath)
	if err != nil {
		return nil, errors.Wrap(err, "read service account token")
	}

	trimmedToken := strings.TrimSpace(string(token))
	if trimmedToken == "" {
		return nil, errors.New("service account token is empty")
	}

	return map[string]string{"authorization": "Bearer " + trimmedToken}, nil
}

// RequireTransportSecurity requires encrypted transport when TLS is configured.
func (c ServiceAccountTokenCredentials) RequireTransportSecurity() bool {
	return c.requireTLS
}
