// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package job

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type JobsSyntheticSource struct {
}

func NewJobsSyntheticSource() JobSource {
	return &JobsSyntheticSource{}
}

func (s *JobsSyntheticSource) CreateApplicationIntention(ctx context.Context, req *pb.CreateApplicationIntentionRequest) (*pb.CreateApplicationIntentionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "application intentions are not available from the synthetic source")
}

func (s *JobsSyntheticSource) GetRecoverySequence(ctx context.Context, req *pb.GetRecoverySequenceRequest) (*pb.GetRecoverySequenceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "recovery sequences are not available from the synthetic source")
}

func (s *JobsSyntheticSource) RecordRecoveryMilestone(ctx context.Context, req *pb.RecordRecoveryMilestoneRequest) (*pb.RecordRecoveryMilestoneResponse, error) {
	return nil, status.Error(codes.Unimplemented, "recovery milestones are not available from the synthetic source")
}

func (s *JobsSyntheticSource) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	// Parse heimdall_id to extract job_id and attempt_index
	parts := strings.Split(req.HeimdallJobId, "-")
	if len(parts) < 2 {
		return nil, status.Error(codes.InvalidArgument, "invalid heimdall job id format")
	}

	jobId := parts[0]
	attemptIndex := int32(0)
	if len(parts) > 1 {
		parsed, err := strconv.Atoi(parts[1])
		if err == nil {
			attemptIndex = int32(parsed)
		}
	}

	nodes := []string{"node1", "node2", "node3"}
	gpuCountPerNode := int32(8)

	createdAt := time.Now().UTC().Add(-time.Duration(rand.Float64()*24*7) * time.Hour) // Random time in past week
	lastSeen := time.Now().UTC().Add(-time.Duration(rand.Float64()*60) * time.Minute)  // Random time in past hour
	updatedAt := lastSeen

	// Generate random state
	states := []pb.JobStatus{
		pb.JobStatus_JOB_STATUS_RUNNING,
		pb.JobStatus_JOB_STATUS_COMPLETED,
		pb.JobStatus_JOB_STATUS_FAILED,
		pb.JobStatus_JOB_STATUS_STOPPED,
	}
	actualState := states[rand.Intn(len(states))]

	resp := pb.GetJobResponse{
		Job: &pb.Job{
			HeimdallJobId:   req.HeimdallJobId,
			JobId:           jobId,
			WorkflowId:      fmt.Sprintf("workflow-%s", jobId),
			AttemptIndex:    attemptIndex,
			Nodes:           nodes,
			GpuCountPerNode: &gpuCountPerNode,
			State:           actualState,
			CreatedAt:       timestamppb.New(createdAt),
			LastSeen:        timestamppb.New(lastSeen),
			UpdatedAt:       timestamppb.New(updatedAt),
			// Optional fields
			NodeConstraints: nil,
		},
	}
	return &resp, nil
}

func (s *JobsSyntheticSource) GetJobs(ctx context.Context, req *pb.GetJobsRequest) (*pb.GetJobsResponse, error) {
	jobs := make([]*pb.Job, 0, 10)
	states := []pb.JobStatus{
		pb.JobStatus_JOB_STATUS_RUNNING,
		pb.JobStatus_JOB_STATUS_COMPLETED,
		pb.JobStatus_JOB_STATUS_FAILED,
		pb.JobStatus_JOB_STATUS_STOPPED,
	}

	for i := 0; i < 10; i++ {
		jobId := fmt.Sprintf("job-%d", i)
		attemptIndex := int32(i % 3)
		heimdallId := fmt.Sprintf("%s-%d", jobId, attemptIndex)

		job, err := s.GetJob(ctx, &pb.GetJobRequest{HeimdallJobId: heimdallId})
		if err != nil {
			return nil, err
		}

		job.Job.JobId = jobId
		job.Job.AttemptIndex = attemptIndex
		job.Job.WorkflowId = fmt.Sprintf("workflow-%d", i%3)
		job.Job.State = states[i%len(states)]

		if len(req.JobIds) > 0 && !slices.Contains(req.JobIds, job.Job.HeimdallJobId) {
			continue
		}
		if len(req.WorkflowIds) > 0 && !slices.Contains(req.WorkflowIds, job.Job.WorkflowId) {
			continue
		}

		if req.JmState != nil && *req.JmState != "" {
			expectedState := parseJobStatus(*req.JmState)
			if expectedState != pb.JobStatus_JOB_STATUS_UNSPECIFIED && job.Job.State != expectedState {
				continue
			}
		}

		jobs = append(jobs, job.Job)
	}

	if req.Limit <= 0 {
		return &pb.GetJobsResponse{Jobs: jobs, Count: int32(len(jobs))}, nil
	}

	pagination, err := resolveNumericPagination(req.Limit, req.PageNumber, req.PageToken)
	if err != nil {
		return nil, err
	}
	page, nextPageToken, err := paginateItems(jobs, pagination)
	if err != nil {
		return nil, err
	}

	return &pb.GetJobsResponse{
		Jobs:          page,
		Count:         int32(len(page)),
		NextPageToken: nextPageToken,
	}, nil
}

// CreateOrUpdateJob implements JobSource.
func (s *JobsSyntheticSource) CreateOrUpdateJob(ctx context.Context, req *pb.CreateOrUpdateJobRequest) (*pb.CreateOrUpdateJobResponse, error) {
	// Generate heimdall_id if not provided
	heimdallId := ""
	if req.HeimdallJobId != nil && *req.HeimdallJobId != "" {
		heimdallId = *req.HeimdallJobId
	} else {
		heimdallId = fmt.Sprintf("%s-%d", req.JobId, req.AttemptIndex)
	}

	var actualNodeConstraints *pb.NodeConstraints
	if req.NodeConstraints != nil {
		actualNodeConstraints = req.NodeConstraints
	}

	job := &pb.Job{
		HeimdallJobId:        heimdallId,
		JobId:                req.JobId,
		WorkflowId:           req.WorkflowId,
		AttemptIndex:         req.AttemptIndex,
		Nodes:                req.AllocatedNodes,
		GpuCountPerNode:      &req.GpuCountPerNode,
		NodeConstraints:      actualNodeConstraints,
		AdditionalProperties: req.AdditionalProperties,
		RecoveryConfig:       req.RecoveryConfig,
		State:                req.State,
		CreatedAt:            timestamppb.Now(),
		LastSeen:             timestamppb.Now(),
		UpdatedAt:            timestamppb.Now(),
		JobName:              req.JobName,
	}

	return &pb.CreateOrUpdateJobResponse{
		Job: job,
	}, nil
}

// GetJobsBySchedulerId implements JobSource.
func (s *JobsSyntheticSource) GetJobsBySchedulerId(ctx context.Context, req *pb.GetJobsBySchedulerIdRequest) (*pb.GetJobsBySchedulerIdResponse, error) {
	jobs := make([]*pb.Job, 0, 3)

	// Generate multiple jobs with different attempt indices for the same scheduler job ID
	for attemptIndex := 0; attemptIndex < 3; attemptIndex++ {
		heimdallId := fmt.Sprintf("%s-%d", req.SchedulerJobId, attemptIndex)

		jobResp, err := s.GetJob(ctx, &pb.GetJobRequest{HeimdallJobId: heimdallId})
		if err != nil {
			// Skip if job doesn't exist
			continue
		}

		// Ensure the job ID matches what was requested
		jobResp.Job.JobId = req.SchedulerJobId
		jobs = append(jobs, jobResp.Job)
	}

	if req.Limit <= 0 {
		return &pb.GetJobsBySchedulerIdResponse{Jobs: jobs, Count: int32(len(jobs))}, nil
	}

	pagination, err := resolveNumericPagination(req.Limit, nil, req.Page)
	if err != nil {
		return nil, err
	}
	page, nextPageToken, err := paginateItems(jobs, pagination)
	if err != nil {
		return nil, err
	}

	return &pb.GetJobsBySchedulerIdResponse{
		Jobs:          page,
		Count:         int32(len(page)),
		NextPageToken: nextPageToken,
	}, nil
}

func (s *JobsSyntheticSource) generateSyntheticWorkflow(id string) *pb.Workflow {
	endTime := time.Now().UTC()
	startTime := endTime.Add(-time.Duration(rand.Float32()*21600.0) * time.Second) // Random duration up to 6 hours

	workflow := &pb.Workflow{
		WorkflowId:          id,
		SchedulerWorkflowId: id,
		Name:                id,
		StartTime:           timestamppb.New(startTime),
		EndTime:             timestamppb.New(endTime),
	}

	return workflow
}

func (s *JobsSyntheticSource) GetWorkflow(ctx context.Context, req *pb.GetWorkflowRequest) (*pb.GetWorkflowResponse, error) {
	workflow := s.generateSyntheticWorkflow(req.WorkflowId)

	return &pb.GetWorkflowResponse{Workflow: workflow}, nil
}

func (s *JobsSyntheticSource) GetWorkflows(ctx context.Context, req *pb.GetWorkflowsRequest) (*pb.GetWorkflowsResponse, error) {
	workflows := make([]*pb.Workflow, 0, 3)

	for n := range 3 {
		workflow := s.generateSyntheticWorkflow(fmt.Sprintf("workflow-%d", n))
		workflows = append(workflows, workflow)
	}

	if req.Limit <= 0 {
		return &pb.GetWorkflowsResponse{Workflows: workflows, Count: int32(len(workflows))}, nil
	}

	pagination, err := resolveNumericPagination(req.Limit, req.PageNumber, req.PageToken)
	if err != nil {
		return nil, err
	}
	page, nextPageToken, err := paginateItems(workflows, pagination)
	if err != nil {
		return nil, err
	}

	return &pb.GetWorkflowsResponse{
		Workflows:     page,
		Count:         int32(len(page)),
		NextPageToken: nextPageToken,
	}, nil
}

// ListAdvice implements JobSource.
func (s *JobsSyntheticSource) ListAdvice(ctx context.Context, req *pb.ListAdviceRequest) (*pb.ListAdviceResponse, error) {
	advice := make([]*pb.Advice, 0, 5)

	for i := 0; i < 5; i++ {
		jobId := fmt.Sprintf("job-%d", i)
		attemptIndex := int32(i % 3)
		heimdallId := fmt.Sprintf("%s-%d", jobId, attemptIndex)

		// Generate random desired state
		var desiredState *pb.AdviceState
		if rand.Float32() > 0.5 {
			states := []pb.AdviceState{
				pb.AdviceState_ADVICE_STATE_RESTARTED,
				pb.AdviceState_ADVICE_STATE_TERMINATED,
			}
			state := states[rand.Intn(len(states))]
			desiredState = &state
		}

		// Generate random node constraints
		var nodeConstraints *pb.NodeConstraints
		if rand.Float32() > 0.5 {
			nodeConstraints = &pb.NodeConstraints{
				Exclusions: []string{fmt.Sprintf("bad-node-%d", rand.Intn(10))},
			}
		}

		// Create advice recommendation
		adviceRecommendation := &pb.AdviceRecommendation{
			State:           desiredState,
			NodeConstraints: nodeConstraints,
		}

		adviceItem := &pb.Advice{
			HeimdallJobId: heimdallId,
			JobId:         jobId,
			AttemptIndex:  attemptIndex,
			Advice:        adviceRecommendation,
			Generation:    1,
		}

		advice = append(advice, adviceItem)
	}

	if req.Limit <= 0 {
		return &pb.ListAdviceResponse{Advice: advice, Count: int32(len(advice))}, nil
	}

	pagination, err := resolveNumericPagination(req.Limit, nil, req.Page)
	if err != nil {
		return nil, err
	}
	page, nextPageToken, err := paginateItems(advice, pagination)
	if err != nil {
		return nil, err
	}

	return &pb.ListAdviceResponse{
		Advice:        page,
		Count:         int32(len(page)),
		NextPageToken: nextPageToken,
	}, nil
}

// UpdateAdviceReconciliationStatus implements JobSource for synthetic mode.
func (s *JobsSyntheticSource) UpdateAdviceReconciliationStatus(_ context.Context, req *pb.UpdateAdviceReconciliationStatusRequest) (*pb.UpdateAdviceReconciliationStatusResponse, error) {
	return &pb.UpdateAdviceReconciliationStatusResponse{Status: req.GetStatus()}, nil
}
