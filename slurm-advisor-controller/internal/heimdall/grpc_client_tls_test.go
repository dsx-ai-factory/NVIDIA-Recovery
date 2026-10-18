// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package heimdall

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/tlsconfig"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

// TestDialGRPCClientTLS exercises the production client against a real TLS
// listener: trusted certificates work, untrusted/wrong-name servers receive no
// bearer token, and token rotation works without recreating the connection.
func TestDialGRPCClientTLS(t *testing.T) {
	fixture := httptest.NewTLSServer(nil)
	certificate := fixture.TLS.Certificates[0]
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fixture.Certificate().Raw})
	fixture.Close()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name      string
		config    tlsconfig.Config
		success   bool
		plaintext bool
	}{
		{"trusted endpoint name", tlsconfig.Config{Enabled: true, CAFile: caFile}, true, false},
		{"trusted explicit name", tlsconfig.Config{Enabled: true, CAFile: caFile, ServerName: "example.com"}, true, false},
		{"system roots reject unknown CA", tlsconfig.Config{Enabled: true}, false, false},
		{"plaintext endpoint", tlsconfig.Config{Enabled: true, CAFile: caFile}, false, true},
		{"wrong server name", tlsconfig.Config{Enabled: true, CAFile: caFile, ServerName: "wrong.example"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			received := make(chan string, 2)
			serverOptions := []grpc.ServerOption{}
			if !tc.plaintext {
				serverOptions = append(serverOptions, grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}})))
			}
			serverOptions = append(serverOptions, grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				for _, value := range metadata.ValueFromIncomingContext(ctx, "authorization") {
					received <- value
				}
				return handler(ctx, req)
			}))
			server := grpc.NewServer(serverOptions...)
			pb.RegisterHeimdallServiceServer(server, dialTestServer{})
			go func() { _ = server.Serve(listener) }()
			defer server.Stop()
			tokenFile := writeDialTokenFile(t, "first-token")
			client, closeClient, err := DialGRPCClient(context.Background(), listener.Addr().String(), tokenFile, tc.config, zap.NewNop())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = closeClient() }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = client.CreateOrUpdateJob(ctx, &pb.CreateOrUpdateJobRequest{JobId: "first"})
			if !tc.success {
				if err == nil {
					t.Fatal("RPC succeeded with invalid server trust")
				}
				select {
				case token := <-received:
					t.Fatalf("untrusted server received token %q", token)
				default:
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := <-received; got != "Bearer first-token" {
				t.Fatalf("authorization = %q", got)
			}
			if err := os.WriteFile(tokenFile, []byte("second-token"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := client.CreateOrUpdateJob(ctx, &pb.CreateOrUpdateJobRequest{JobId: "second"}); err != nil {
				t.Fatal(err)
			}
			if got := <-received; got != "Bearer second-token" {
				t.Fatalf("rotated authorization = %q", got)
			}
		})
	}
}
