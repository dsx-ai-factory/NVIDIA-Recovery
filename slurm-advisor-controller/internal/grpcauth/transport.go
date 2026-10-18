// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package grpcauth

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/tlsconfig"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// DialOptions builds shared gateway transport and rotating bearer credentials.
// The zero configuration preserves plaintext internal connections. TLS always
// verifies the server, and its bearer credentials refuse insecure transport.
func DialOptions(cfg tlsconfig.Config, tokenPath string) ([]grpc.DialOption, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	transport := insecure.NewCredentials()
	if cfg.Enabled {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.ServerName}
		if cfg.CAFile != "" {
			pem, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, errors.Wrap(err, "read gateway TLS CA file")
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(pem) {
				return nil, errors.New("gateway TLS CA file contains no certificates")
			}
			tlsConfig.RootCAs = roots
		}
		transport = credentials.NewTLS(tlsConfig)
	}
	options := []grpc.DialOption{grpc.WithTransportCredentials(transport)}
	if tokenPath != "" {
		token := NewServiceAccountTokenCredentials(tokenPath)
		token.requireTLS = cfg.Enabled
		options = append(options, grpc.WithPerRPCCredentials(token))
	}
	return options, nil
}
