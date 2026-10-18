// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/mockheimdall"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()
	logger = logger.With(zap.String("component", "mock-heimdall-gateway"))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	grpcAddr := envOrDefault("MOCK_HEIMDALL_GRPC_ADDR", ":8080")
	debugAddr := envOrDefault("MOCK_HEIMDALL_DEBUG_ADDR", ":8081")

	server := mockheimdall.NewServer(nil)
	grpcServer := grpc.NewServer()
	pb.RegisterHeimdallServiceServer(grpcServer, server)

	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		logger.Fatal("listen gRPC", zap.String("grpc_addr", grpcAddr), zap.Error(err))
	}
	go func() {
		logger.Info("mock Heimdall gRPC listening", zap.String("grpc_addr", grpcAddr))
		if err := grpcServer.Serve(listener); err != nil {
			logger.Warn("mock Heimdall gRPC stopped", zap.String("grpc_addr", grpcAddr), zap.Error(err))
		}
	}()

	debugMux := http.NewServeMux()
	server.RegisterDebugHandlers(debugMux)
	debugServer := &http.Server{Addr: debugAddr, Handler: debugMux}
	go func() {
		logger.Info("mock Heimdall debug HTTP listening", zap.String("debug_addr", debugAddr))
		if err := debugServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Warn("mock Heimdall debug HTTP stopped", zap.String("debug_addr", debugAddr), zap.Error(err))
		}
	}()

	<-ctx.Done()
	grpcServer.GracefulStop()
	if err := debugServer.Shutdown(context.Background()); err != nil {
		logger.Warn("shutdown debug HTTP", zap.String("debug_addr", debugAddr), zap.Error(err))
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}
