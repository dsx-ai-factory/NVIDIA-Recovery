// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Command kubernetes-job-controller publishes Kubernetes workload observations to Recovery.
package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type config struct {
	Namespace  string `envconfig:"KUBERNETES_JOB_NAMESPACE" required:"true"`
	Gateway    string `envconfig:"KUBERNETES_JOB_GATEWAY_ENDPOINT" required:"true"`
	Kubeconfig string `envconfig:"KUBECONFIG"`
}

func main() {
	logger, err := zap.NewProduction()
	if err != nil {
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, logger); err != nil {
		logger.Error("workload discovery stopped", zap.Error(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *zap.Logger) error {
	var cfg config
	if err := envconfig.Process("", &cfg); err != nil {
		return errors.Wrap(err, "read controller configuration")
	}
	if strings.TrimSpace(cfg.Namespace) == "" || strings.TrimSpace(cfg.Gateway) == "" {
		return errors.New("namespace and internal gateway endpoint are required")
	}
	var kube *rest.Config
	var err error
	if cfg.Kubeconfig == "" {
		kube, err = rest.InClusterConfig()
	} else {
		kube, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	}
	if err != nil {
		return errors.Wrap(err, "load Kubernetes configuration")
	}
	// Use the existing internal gateway listener for same-cluster publication.
	conn, err := grpc.NewClient(cfg.Gateway, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return errors.Wrap(err, "initialize internal gateway client")
	}
	defer func() { _ = conn.Close() }()
	mgr, err := newManager(ctx, kube, cfg.Namespace, pb.NewHeimdallServiceClient(conn), logger)
	if err != nil {
		return err
	}
	return mgr.Start(ctx)
}
