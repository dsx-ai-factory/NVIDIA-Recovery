// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package heimdall

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/tlsconfig"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

func TestDialGRPCClientAttachesRotatedServiceAccountToken(t *testing.T) {
	tokenPath := writeDialTokenFile(t, "first-token")
	client, authorizations, cleanup := newDialTestClient(t, tokenPath)
	defer cleanup()

	if _, err := client.CreateOrUpdateJob(context.Background(), &pb.CreateOrUpdateJobRequest{JobId: "first"}); err != nil {
		t.Fatalf("first CreateOrUpdateJob() failed: %v", err)
	}
	if err := os.WriteFile(tokenPath, []byte("second-token\n"), 0o600); err != nil {
		t.Fatalf("rotate token file: %v", err)
	}
	if _, err := client.CreateOrUpdateJob(context.Background(), &pb.CreateOrUpdateJobRequest{JobId: "second"}); err != nil {
		t.Fatalf("second CreateOrUpdateJob() failed: %v", err)
	}

	if diff := cmp.Diff([]string{"Bearer first-token", "Bearer second-token"}, *authorizations); diff != "" {
		t.Fatalf("authorization metadata mismatch (-want +got):\n%s", diff)
	}
}

func TestDialGRPCClientLeavesAuthorizationAbsentWithoutTokenPath(t *testing.T) {
	client, authorizations, cleanup := newDialTestClient(t, "")
	defer cleanup()

	if _, err := client.CreateOrUpdateJob(context.Background(), &pb.CreateOrUpdateJobRequest{JobId: "no-token"}); err != nil {
		t.Fatalf("CreateOrUpdateJob() failed: %v", err)
	}

	if len(*authorizations) != 0 {
		t.Fatalf("authorization metadata = %q, want absent", *authorizations)
	}
}

func newDialTestClient(t *testing.T, tokenPath string) (*GRPCClient, *[]string, func()) {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	authorizations := []string{}
	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		authorizations = append(authorizations, metadata.ValueFromIncomingContext(ctx, "authorization")...)
		return handler(ctx, req)
	}))
	pb.RegisterHeimdallServiceServer(grpcServer, dialTestServer{})
	go func() { _ = grpcServer.Serve(listener) }()

	client, closeClient, err := dialGRPCClient(context.Background(), "passthrough:///bufnet", tokenPath, tlsconfig.Config{}, zap.NewNop(),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("dialGRPCClient() failed: %v", err)
	}

	return client, &authorizations, func() {
		_ = closeClient()
		grpcServer.Stop()
		_ = listener.Close()
	}
}

func writeDialTokenFile(t *testing.T, token string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
	return path
}

type dialTestServer struct {
	pb.UnimplementedHeimdallServiceServer
}

func (dialTestServer) CreateOrUpdateJob(context.Context, *pb.CreateOrUpdateJobRequest) (*pb.CreateOrUpdateJobResponse, error) {
	return &pb.CreateOrUpdateJobResponse{}, nil
}
