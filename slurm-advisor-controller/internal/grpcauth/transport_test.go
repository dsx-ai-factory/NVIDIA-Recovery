// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package grpcauth

import (
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/tlsconfig"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestDialOptionsRejectInvalidTrustConfiguration(t *testing.T) {
	invalidCA := filepath.Join(t.TempDir(), "invalid.pem")
	if err := os.WriteFile(invalidCA, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []tlsconfig.Config{
		{CAFile: invalidCA},
		{ServerName: "gateway.example"},
		{Enabled: true, CAFile: invalidCA},
		{Enabled: true, CAFile: filepath.Join(t.TempDir(), "missing")},
	} {
		if _, err := DialOptions(cfg, ""); err == nil {
			t.Fatalf("DialOptions(%+v) accepted invalid trust configuration", cfg)
		}
	}
}

func TestTLSBearerCredentialsRejectPlaintextOverride(t *testing.T) {
	options, err := DialOptions(tlsconfig.Config{Enabled: true}, "/unused/token")
	if err != nil {
		t.Fatal(err)
	}
	options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient("localhost:443", options...)
	if err == nil {
		_ = conn.Close()
		t.Fatal("TLS bearer credentials allowed plaintext transport")
	}
}
