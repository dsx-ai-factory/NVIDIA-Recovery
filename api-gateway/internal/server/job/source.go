// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package job

import (
	"context"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type JobSource interface {
	CreateApplicationIntention(ctx context.Context, req *pb.CreateApplicationIntentionRequest) (*pb.CreateApplicationIntentionResponse, error)
	CreateOrUpdateJob(ctx context.Context, req *pb.CreateOrUpdateJobRequest) (*pb.CreateOrUpdateJobResponse, error)
	GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error)
	GetJobs(ctx context.Context, req *pb.GetJobsRequest) (*pb.GetJobsResponse, error)
	GetJobsBySchedulerId(ctx context.Context, req *pb.GetJobsBySchedulerIdRequest) (*pb.GetJobsBySchedulerIdResponse, error)
	GetWorkflow(ctx context.Context, req *pb.GetWorkflowRequest) (*pb.GetWorkflowResponse, error)
	GetWorkflows(ctx context.Context, req *pb.GetWorkflowsRequest) (*pb.GetWorkflowsResponse, error)
	ListAdvice(ctx context.Context, req *pb.ListAdviceRequest) (*pb.ListAdviceResponse, error)
	UpdateAdviceReconciliationStatus(ctx context.Context, req *pb.UpdateAdviceReconciliationStatusRequest) (*pb.UpdateAdviceReconciliationStatusResponse, error)
	GetRecoverySequence(ctx context.Context, req *pb.GetRecoverySequenceRequest) (*pb.GetRecoverySequenceResponse, error)
	RecordRecoveryMilestone(ctx context.Context, req *pb.RecordRecoveryMilestoneRequest) (*pb.RecordRecoveryMilestoneResponse, error)
}

type NotImplementedJobSource struct{}

func NewNotImplementedJobSource() JobSource {
	return &NotImplementedJobSource{}
}

func (s *NotImplementedJobSource) CreateApplicationIntention(ctx context.Context, req *pb.CreateApplicationIntentionRequest) (*pb.CreateApplicationIntentionResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateApplicationIntention not implemented")
}

func (s *NotImplementedJobSource) CreateOrUpdateJob(ctx context.Context, req *pb.CreateOrUpdateJobRequest) (*pb.CreateOrUpdateJobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateOrUpdateJob not implemented")
}

func (s *NotImplementedJobSource) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJob not implemented")
}

func (s *NotImplementedJobSource) GetJobs(ctx context.Context, req *pb.GetJobsRequest) (*pb.GetJobsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJobs not implemented")
}

func (s *NotImplementedJobSource) GetWorkflow(ctx context.Context, req *pb.GetWorkflowRequest) (*pb.GetWorkflowResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWorkflow not implemented")
}

func (s *NotImplementedJobSource) GetWorkflows(ctx context.Context, req *pb.GetWorkflowsRequest) (*pb.GetWorkflowsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetWorkflows not implemented")
}

func (s *NotImplementedJobSource) GetJobsBySchedulerId(ctx context.Context, req *pb.GetJobsBySchedulerIdRequest) (*pb.GetJobsBySchedulerIdResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetJobsBySchedulerId not implemented")
}

func (s *NotImplementedJobSource) ListAdvice(ctx context.Context, req *pb.ListAdviceRequest) (*pb.ListAdviceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ListAdvice not implemented")
}

func (s *NotImplementedJobSource) UpdateAdviceReconciliationStatus(ctx context.Context, req *pb.UpdateAdviceReconciliationStatusRequest) (*pb.UpdateAdviceReconciliationStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateAdviceReconciliationStatus not implemented")
}

func (s *NotImplementedJobSource) GetRecoverySequence(ctx context.Context, req *pb.GetRecoverySequenceRequest) (*pb.GetRecoverySequenceResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetRecoverySequence not implemented")
}

func (s *NotImplementedJobSource) RecordRecoveryMilestone(ctx context.Context, req *pb.RecordRecoveryMilestoneRequest) (*pb.RecordRecoveryMilestoneResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RecordRecoveryMilestone not implemented")
}
