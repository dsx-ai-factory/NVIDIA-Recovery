// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package heimdall

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/tlsconfig"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestDialGRPCClientLogsReadyWithoutRPC(t *testing.T) {
	listener := newReadinessListener(t)
	var rpcCalls atomic.Int32
	server := grpc.NewServer(grpc.UnaryInterceptor(func(context.Context, any, *grpc.UnaryServerInfo, grpc.UnaryHandler) (any, error) {
		rpcCalls.Add(1)
		return nil, status.Error(codes.Unauthenticated, "test gateway rejects requests")
	}))
	pb.RegisterHeimdallServiceServer(server, dialTestServer{})
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()

	core, logs := observer.New(zap.InfoLevel)
	ready := make(chan struct{}, 1)
	logger := zap.New(core, zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Message == "API Gateway connection established" {
			select {
			case ready <- struct{}{}:
			default:
			}
		}
		return nil
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, closeClient, err := DialGRPCClient(ctx, listener.Addr().String(), "", tlsconfig.Config{}, logger)
	if err != nil {
		t.Fatalf("DialGRPCClient(): %v", err)
	}
	t.Cleanup(func() { _ = closeClient() })
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("no connection-success log after the gRPC handshake")
	}
	if got := rpcCalls.Load(); got != 0 {
		t.Fatalf("RPC calls before readiness log = %d, want 0", got)
	}

	// Transport readiness must not depend on successful RPC authorization.
	for range 2 {
		_, err := client.CreateOrUpdateJob(ctx, &pb.CreateOrUpdateJobRequest{JobId: "test-job"})
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("CreateOrUpdateJob() error = %v, want Unauthenticated", err)
		}
	}
	if err := closeClient(); err != nil {
		t.Fatalf("close client: %v", err)
	}
	entries := logs.All()
	if len(entries) != 1 {
		t.Fatalf("connection logs = %d, want 1", len(entries))
	}
	if entry := entries[0]; entry.Level != zapcore.InfoLevel || entry.ContextMap()["heimdall_endpoint"] != listener.Addr().String() {
		t.Fatalf("connection log = %+v, want INFO with gateway endpoint", entry)
	}
}

func TestDialGRPCClientDoesNotLogReadyForTCPConnectionOnly(t *testing.T) {
	listener := newReadinessListener(t)
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	core, logs := observer.New(zap.InfoLevel)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, closeClient, err := DialGRPCClient(ctx, listener.Addr().String(), "", tlsconfig.Config{}, zap.New(core))
	if err != nil {
		t.Fatalf("DialGRPCClient(): %v", err)
	}
	t.Cleanup(func() { _ = closeClient() })
	// Accept TCP without responding to the HTTP/2 handshake. Closing the client
	// must stop the waiting watcher without logging a successful gRPC connection.
	select {
	case conn := <-accepted:
		t.Cleanup(func() { _ = conn.Close() })
	case <-ctx.Done():
		t.Fatal("client did not initiate its TCP connection")
	}
	if err := closeClient(); err != nil {
		t.Fatalf("close client before readiness: %v", err)
	}
	if logs.Len() != 0 {
		t.Fatal("TCP connection without gRPC readiness emitted a success log")
	}
}

func TestGatewayReadinessWatcherStopsOnCancellation(t *testing.T) {
	conn, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient(): %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	core, logs := observer.New(zap.InfoLevel)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		logGatewayConnectionReady(ctx, conn, "unused", zap.New(core))
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("readiness watcher did not stop on cancellation")
	}
	if logs.Len() != 0 {
		t.Fatal("cancelled readiness watcher emitted a success log")
	}
}

func newReadinessListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}
