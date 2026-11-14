// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package heimdall

import (
	"context"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
)

const DefaultPageLimit int32 = 100

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -destination=heimdallmock/mock_client.go -package=heimdallmock . HeimdallClient

type HeimdallClient interface {
	CreateOrUpdateJob(ctx context.Context, req *pb.CreateOrUpdateJobRequest) (*pb.CreateOrUpdateJobResponse, error)
	GetJobs(ctx context.Context, opts ...PaginationOption) ([]*pb.Job, error)
	ListAdvice(ctx context.Context, opts ...PaginationOption) ([]*pb.Advice, error)
	UpdateAdviceReconciliationStatus(ctx context.Context, adviceID string, status *pb.AdviceReconciliationStatus) (*pb.AdviceReconciliationStatus, error)
	RecordRecoveryMilestone(ctx context.Context, req *pb.RecordRecoveryMilestoneRequest) (*pb.RecordRecoveryMilestoneResponse, error)
}

type PaginationOptions struct {
	PageLimit  int32
	OnlyActive bool
}

type PaginationOption func(*PaginationOptions)

func WithPageLimit(limit int32) PaginationOption {
	return func(opts *PaginationOptions) {
		opts.PageLimit = limit
	}
}

// WithOnlyActiveAdvice limits Advice to work that has not reached terminal status.
func WithOnlyActiveAdvice() PaginationOption {
	return func(opts *PaginationOptions) {
		opts.OnlyActive = true
	}
}

func ResolvePaginationOptions(opts ...PaginationOption) PaginationOptions {
	resolved := PaginationOptions{PageLimit: DefaultPageLimit}
	for _, opt := range opts {
		opt(&resolved)
	}
	if resolved.PageLimit <= 0 {
		resolved.PageLimit = DefaultPageLimit
	}

	return resolved
}
