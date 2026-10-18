// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/job"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type recordingWorkflowSource struct {
	job.JobSource
	calls int
	limit int32
}

type recordingJobSource struct {
	job.JobSource
	calls int
}

type recordingAdviceSource struct {
	job.JobSource
	listCalls   int
	statusCalls int
}

func (s *recordingAdviceSource) ListAdvice(_ context.Context, _ *pb.ListAdviceRequest) (*pb.ListAdviceResponse, error) {
	s.listCalls++
	return &pb.ListAdviceResponse{}, nil
}

func (s *recordingAdviceSource) UpdateAdviceReconciliationStatus(
	_ context.Context,
	req *pb.UpdateAdviceReconciliationStatusRequest,
) (*pb.UpdateAdviceReconciliationStatusResponse, error) {
	s.statusCalls++
	return &pb.UpdateAdviceReconciliationStatusResponse{Status: req.GetStatus()}, nil
}

func (s *recordingJobSource) GetJobs(_ context.Context, _ *pb.GetJobsRequest) (*pb.GetJobsResponse, error) {
	s.calls++
	return &pb.GetJobsResponse{}, nil
}

func (s *recordingWorkflowSource) GetWorkflows(_ context.Context, req *pb.GetWorkflowsRequest) (*pb.GetWorkflowsResponse, error) {
	s.calls++
	s.limit = req.Limit
	return &pb.GetWorkflowsResponse{}, nil
}

func TestListAdviceOnlyActiveDoesNotRequireControllerScope(t *testing.T) {
	source := &recordingAdviceSource{JobSource: job.NewNotImplementedJobSource()}
	server := NewHeimdallServer(source)
	onlyActive := true

	_, err := server.ListAdvice(context.Background(), &pb.ListAdviceRequest{OnlyActive: &onlyActive})

	if err != nil {
		t.Fatalf("ListAdvice() failed: %v", err)
	}
	if source.listCalls != 1 {
		t.Fatalf("source calls = %d, want 1", source.listCalls)
	}
}

func TestUpdateAdviceReconciliationStatusValidatesGeneration(t *testing.T) {
	source := &recordingAdviceSource{JobSource: job.NewNotImplementedJobSource()}
	server := NewHeimdallServer(source)

	_, err := server.UpdateAdviceReconciliationStatus(context.Background(), &pb.UpdateAdviceReconciliationStatusRequest{
		HeimdallJobId: "advice-a",
		Status: &pb.AdviceReconciliationStatus{
			Disposition: pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED,
			Reason:      pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED,
		},
	})

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error code = %s, want InvalidArgument: %v", status.Code(err), err)
	}
	if source.statusCalls != 0 {
		t.Fatalf("source calls = %d, want 0", source.statusCalls)
	}
}

func TestGetWorkflowsLimit(t *testing.T) {
	tests := []struct {
		name      string
		limit     int32
		wantLimit int32
		wantCalls int
		wantCode  codes.Code
	}{
		{name: "defaults to 100", limit: 0, wantLimit: 100, wantCalls: 1},
		{name: "accepts maximum", limit: 250, wantLimit: 250, wantCalls: 1},
		{name: "rejects above maximum", limit: 251, wantCalls: 0, wantCode: codes.InvalidArgument},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workflowSource := &recordingWorkflowSource{JobSource: job.NewNotImplementedJobSource()}
			server := NewHeimdallServer(workflowSource)

			_, err := server.GetWorkflows(context.Background(), &pb.GetWorkflowsRequest{Limit: test.limit})
			if status.Code(err) != test.wantCode {
				t.Fatalf("error code = %s, want %s: %v", status.Code(err), test.wantCode, err)
			}
			if workflowSource.calls != test.wantCalls {
				t.Fatalf("source calls = %d, want %d", workflowSource.calls, test.wantCalls)
			}
			if workflowSource.calls > 0 && workflowSource.limit != test.wantLimit {
				t.Fatalf("source limit = %d, want %d", workflowSource.limit, test.wantLimit)
			}
		})
	}
}

func TestGetJobsStateFilters(t *testing.T) {
	for _, tt := range []struct {
		name     string
		req      *pb.GetJobsRequest
		wantCode codes.Code
	}{
		{name: "unfiltered", req: &pb.GetJobsRequest{}},
		{name: "invalid end time", req: &pb.GetJobsRequest{EndTimeFrom: &timestamppb.Timestamp{Seconds: 253402300800}}, wantCode: codes.InvalidArgument},
		{name: "non-final", req: &pb.GetJobsRequest{CompletedJobs: new(false)}},
		{name: "final", req: &pb.GetJobsRequest{CompletedJobs: new(true)}},
		{name: "disabled reconciliation filter", req: &pb.GetJobsRequest{CompletedJobs: new(false), ReconciliationEligibleJobs: new(false)}},
		{name: "state and eligible", req: &pb.GetJobsRequest{JmState: new("RUNNING"), ReconciliationEligibleJobs: new(true)}, wantCode: codes.InvalidArgument},
		{name: "state and non-final", req: &pb.GetJobsRequest{JmState: new("RUNNING"), CompletedJobs: new(false)}, wantCode: codes.InvalidArgument},
		{name: "state and final", req: &pb.GetJobsRequest{JmState: new("FAILED"), CompletedJobs: new(true)}, wantCode: codes.InvalidArgument},
		{name: "eligible and non-final", req: &pb.GetJobsRequest{ReconciliationEligibleJobs: new(true), CompletedJobs: new(false)}, wantCode: codes.InvalidArgument},
		{name: "eligible and final", req: &pb.GetJobsRequest{ReconciliationEligibleJobs: new(true), CompletedJobs: new(true)}, wantCode: codes.InvalidArgument},
	} {
		t.Run(tt.name, func(t *testing.T) {
			jobSource := &recordingJobSource{JobSource: job.NewNotImplementedJobSource()}
			server := NewHeimdallServer(jobSource)
			_, err := server.GetJobs(context.Background(), tt.req)
			if status.Code(err) != tt.wantCode {
				t.Fatalf("error code = %s, want %s: %v", status.Code(err), tt.wantCode, err)
			}
			wantCalls := 1
			if tt.wantCode != codes.OK {
				wantCalls = 0
			}
			if jobSource.calls != wantCalls {
				t.Fatalf("source calls = %d, want %d", jobSource.calls, wantCalls)
			}
		})
	}
}

func TestValidateHeimdallJobID(t *testing.T) {
	tests := []struct {
		heimdallJobID string
		wantCode      codes.Code
	}{
		{heimdallJobID: "", wantCode: codes.InvalidArgument},
		{heimdallJobID: "test_job-0"},
	}

	for _, test := range tests {
		if got := status.Code(validateHeimdallJobId(test.heimdallJobID)); got != test.wantCode {
			t.Fatalf("error code = %s, want %s", got, test.wantCode)
		}
	}
}

func TestValidateWorkflowID(t *testing.T) {
	tests := []struct {
		name       string
		workflowID string
		wantCode   codes.Code
	}{
		{name: "missing", wantCode: codes.InvalidArgument},
		{name: "malformed UTF-8", workflowID: string([]byte{0xff}), wantCode: codes.InvalidArgument},
		{name: "256 multibyte scalars", workflowID: strings.Repeat("界", 256)},
		{name: "257 multibyte scalars", workflowID: strings.Repeat("界", 257), wantCode: codes.InvalidArgument},
		{name: "opaque characters preserved", workflowID: " /%_\\cafe\u0301 "},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := status.Code(validateWorkflowID(test.workflowID)); got != test.wantCode {
				t.Fatalf("error code = %s, want %s", got, test.wantCode)
			}
		})
	}
}

type recordingRecoverySource struct {
	job.JobSource
	beginCalls     int
	milestoneCalls int
}

func (s *recordingRecoverySource) GetRecoverySequence(_ context.Context, _ *pb.GetRecoverySequenceRequest) (*pb.GetRecoverySequenceResponse, error) {
	s.beginCalls++
	return &pb.GetRecoverySequenceResponse{}, nil
}

func (s *recordingRecoverySource) RecordRecoveryMilestone(_ context.Context, _ *pb.RecordRecoveryMilestoneRequest) (*pb.RecordRecoveryMilestoneResponse, error) {
	s.milestoneCalls++
	return &pb.RecordRecoveryMilestoneResponse{}, nil
}

func TestGetRecoverySequenceValidation(t *testing.T) {
	valid := func() *pb.GetRecoverySequenceRequest {
		return &pb.GetRecoverySequenceRequest{
			SchedulerSystem:    "slurm",
			SchedulerScope:     "production",
			JobId:              "1234",
			SourceAttemptIndex: 2,
			Operation:          pb.RecoveryOperation_RECOVERY_OPERATION_RESTART,
		}
	}

	tests := map[string]func(*pb.GetRecoverySequenceRequest){
		"missing scheduler system": func(request *pb.GetRecoverySequenceRequest) { request.SchedulerSystem = "" },
		"missing scheduler scope":  func(request *pb.GetRecoverySequenceRequest) { request.SchedulerScope = "" },
		"missing job id":           func(request *pb.GetRecoverySequenceRequest) { request.JobId = "" },
		"negative attempt":         func(request *pb.GetRecoverySequenceRequest) { request.SourceAttemptIndex = -1 },
		"missing operation": func(request *pb.GetRecoverySequenceRequest) {
			request.Operation = pb.RecoveryOperation_RECOVERY_OPERATION_UNSPECIFIED
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			source := &recordingRecoverySource{JobSource: job.NewNotImplementedJobSource()}
			server := NewHeimdallServer(source)
			request := valid()
			mutate(request)

			_, err := server.GetRecoverySequence(context.Background(), request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
			if source.beginCalls != 0 {
				t.Fatalf("source calls = %d, want 0", source.beginCalls)
			}
		})
	}

	source := &recordingRecoverySource{JobSource: job.NewNotImplementedJobSource()}
	server := NewHeimdallServer(source)
	if _, err := server.GetRecoverySequence(context.Background(), valid()); err != nil {
		t.Fatalf("valid request failed: %v", err)
	}
	if source.beginCalls != 1 {
		t.Fatalf("source calls = %d, want 1", source.beginCalls)
	}
}

func TestRecordRecoveryMilestoneValidation(t *testing.T) {
	valid := func() *pb.RecordRecoveryMilestoneRequest {
		completesSequence := true
		return &pb.RecordRecoveryMilestoneRequest{
			SequenceId:        "0198b9cf-4ce7-72ad-bd18-9ba8938ee2eb",
			IdempotencyKey:    "release-1234-3-1",
			Operation:         pb.RecoveryOperation_RECOVERY_OPERATION_RESTART,
			Action:            pb.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_RELEASE,
			Outcome:           pb.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_SUCCESS,
			CompletesSequence: &completesSequence,
			CompletedAt:       timestamppb.New(time.Date(2026, 8, 18, 1, 2, 4, 0, time.UTC)),
		}
	}

	tests := map[string]func(*pb.RecordRecoveryMilestoneRequest){
		"missing sequence id":     func(request *pb.RecordRecoveryMilestoneRequest) { request.SequenceId = "" },
		"missing idempotency key": func(request *pb.RecordRecoveryMilestoneRequest) { request.IdempotencyKey = "" },
		"missing operation": func(request *pb.RecordRecoveryMilestoneRequest) {
			request.Operation = pb.RecoveryOperation_RECOVERY_OPERATION_UNSPECIFIED
		},
		"missing action": func(request *pb.RecordRecoveryMilestoneRequest) {
			request.Action = pb.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_UNSPECIFIED
		},
		"missing outcome": func(request *pb.RecordRecoveryMilestoneRequest) {
			request.Outcome = pb.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_UNSPECIFIED
		},
		"missing completion intent": func(request *pb.RecordRecoveryMilestoneRequest) {
			request.CompletesSequence = nil
		},
		"missing completion": func(request *pb.RecordRecoveryMilestoneRequest) { request.CompletedAt = nil },
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			source := &recordingRecoverySource{JobSource: job.NewNotImplementedJobSource()}
			server := NewHeimdallServer(source)
			request := valid()
			mutate(request)

			_, err := server.RecordRecoveryMilestone(context.Background(), request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
			if source.milestoneCalls != 0 {
				t.Fatalf("source calls = %d, want 0", source.milestoneCalls)
			}
		})
	}

	source := &recordingRecoverySource{JobSource: job.NewNotImplementedJobSource()}
	server := NewHeimdallServer(source)
	if _, err := server.RecordRecoveryMilestone(context.Background(), valid()); err != nil {
		t.Fatalf("valid request failed: %v", err)
	}
	if source.milestoneCalls != 1 {
		t.Fatalf("source calls = %d, want 1", source.milestoneCalls)
	}

	nonTerminal := valid()
	*nonTerminal.CompletesSequence = false
	if _, err := server.RecordRecoveryMilestone(context.Background(), nonTerminal); err != nil {
		t.Fatalf("explicit false completion intent failed: %v", err)
	}
	if source.milestoneCalls != 2 {
		t.Fatalf("source calls = %d, want 2", source.milestoneCalls)
	}
}
