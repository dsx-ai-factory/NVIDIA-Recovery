// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"math"
	"unicode/utf8"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/job"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/util"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	health "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// heimdallServer implements the HeimdallServiceServer interface.
type heimdallServer struct {
	pb.UnimplementedHeimdallServiceServer // Embed for forward compatibility

	jobSource job.JobSource
}

var (
	_               = pb.HeimdallServiceServer(&heimdallServer{})
	_               = health.HealthServer(&heimdallServer{})
	defaultPageSize = int32(100)
	maxPageSize     = int32(250)
)

func NewHeimdallServer(jobSource job.JobSource) pb.HeimdallServiceServer {
	return &heimdallServer{
		jobSource: jobSource,
	}
}

func (s *heimdallServer) Check(ctx context.Context, in *health.HealthCheckRequest) (*health.HealthCheckResponse, error) {
	return &health.HealthCheckResponse{Status: health.HealthCheckResponse_SERVING}, nil
}

func (s *heimdallServer) Watch(in *health.HealthCheckRequest, _ health.Health_WatchServer) error {
	// Example of how to register both methods but only implement the Check method.
	return status.Error(codes.Unimplemented, "unimplemented")
}

func (s *heimdallServer) List(ctx context.Context, in *health.HealthListRequest) (*health.HealthListResponse, error) {
	statuses := make(map[string]*health.HealthCheckResponse)
	statuses["heimdall"] = &health.HealthCheckResponse{
		Status: health.HealthCheckResponse_SERVING,
	}
	return &health.HealthListResponse{
		Statuses: statuses,
	}, nil
}

func (s *heimdallServer) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.GetJobResponse, error) {
	ctx, _ = util.GetZapLoggerFromContextWith(ctx, zap.String("heimdall_job_id", req.HeimdallJobId))

	if err := validateHeimdallJobId(req.HeimdallJobId); err != nil {
		return nil, err
	}

	return s.jobSource.GetJob(ctx, req)
}

func (s *heimdallServer) GetJobs(ctx context.Context, req *pb.GetJobsRequest) (*pb.GetJobsResponse, error) {
	if err := validateJobIds(req.JobIds); err != nil {
		return nil, err
	}

	if err := validateWorkflowIds(req.WorkflowIds); err != nil {
		return nil, err
	}
	stateFilters := 0
	if req.JmState != nil && *req.JmState != "" {
		stateFilters++
	}
	if req.GetReconciliationEligibleJobs() {
		stateFilters++
	}
	if req.CompletedJobs != nil {
		stateFilters++
	}
	if stateFilters > 1 {
		return nil, status.Error(codes.InvalidArgument, "jm_state, completed_jobs, and reconciliation_eligible_jobs are mutually exclusive")
	}
	if req.EndTimeFrom != nil {
		if err := req.EndTimeFrom.CheckValid(); err != nil {
			return nil, status.Error(codes.InvalidArgument, "end_time_from must be a valid timestamp")
		}
	}

	// Default limit if not specified
	if req.Limit <= 0 {
		req.Limit = defaultPageSize
	}

	if err := validateLimit(req.Limit); err != nil {
		return nil, err
	}

	return s.jobSource.GetJobs(ctx, req)
}

func (s *heimdallServer) GetWorkflow(ctx context.Context, req *pb.GetWorkflowRequest) (*pb.GetWorkflowResponse, error) {
	if err := validateWorkflowID(req.WorkflowId); err != nil {
		return nil, err
	}
	return s.jobSource.GetWorkflow(ctx, req)
}

func (s *heimdallServer) GetWorkflows(ctx context.Context, req *pb.GetWorkflowsRequest) (*pb.GetWorkflowsResponse, error) {
	if req.Limit <= 0 {
		req.Limit = defaultPageSize
	}

	if err := validateLimit(req.Limit); err != nil {
		return nil, err
	}

	return s.jobSource.GetWorkflows(ctx, req)
}

func (s *heimdallServer) CreateOrUpdateJob(ctx context.Context, req *pb.CreateOrUpdateJobRequest) (*pb.CreateOrUpdateJobResponse, error) {
	if err := validateJobId(req.JobId); err != nil {
		return nil, err
	}

	if err := validateWorkflowID(req.WorkflowId); err != nil {
		return nil, err
	}

	// Protobuf accepts unknown numeric enum values on both public transports.
	// Unspecified remains valid for partial updates that do not change state.
	if _, ok := pb.JobStatus_name[int32(req.State)]; !ok {
		return nil, status.Error(codes.InvalidArgument, "unsupported job state")
	}

	return s.jobSource.CreateOrUpdateJob(ctx, req)
}

func (s *heimdallServer) GetJobsBySchedulerId(ctx context.Context, req *pb.GetJobsBySchedulerIdRequest) (*pb.GetJobsBySchedulerIdResponse, error) {
	if err := validateSchedulerJobId(req.SchedulerJobId); err != nil {
		return nil, err
	}

	// Default limit if not specified
	if req.Limit <= 0 {
		req.Limit = defaultPageSize
	}

	if err := validateLimit(req.Limit); err != nil {
		return nil, err
	}

	return s.jobSource.GetJobsBySchedulerId(ctx, req)
}

func (s *heimdallServer) ListAdvice(ctx context.Context, req *pb.ListAdviceRequest) (*pb.ListAdviceResponse, error) {
	// Default limit if not specified
	if req.Limit <= 0 {
		req.Limit = defaultPageSize
	}

	if err := validateLimit(req.Limit); err != nil {
		return nil, err
	}
	return s.jobSource.ListAdvice(ctx, req)
}

func (s *heimdallServer) UpdateAdviceReconciliationStatus(ctx context.Context, req *pb.UpdateAdviceReconciliationStatusRequest) (*pb.UpdateAdviceReconciliationStatusResponse, error) {
	if err := validateHeimdallJobId(req.GetHeimdallJobId()); err != nil {
		return nil, err
	}
	reconciliationStatus := req.GetStatus()
	if reconciliationStatus == nil {
		return nil, status.Error(codes.InvalidArgument, "reconciliation status must be provided")
	}
	if reconciliationStatus.GetObservedGeneration() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "observed generation must be positive")
	}
	if reconciliationStatus.GetObservedGeneration() > math.MaxInt32 {
		return nil, status.Error(codes.InvalidArgument, "observed generation exceeds the Job Manager contract")
	}
	if reconciliationStatus.GetDisposition() == pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "reconciliation disposition must be provided")
	}
	if reconciliationStatus.GetReason() == pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, "reconciliation reason must be provided")
	}
	if reconciliationStatus.GetObservedAt() == nil || reconciliationStatus.GetObservedAt().CheckValid() != nil {
		return nil, status.Error(codes.InvalidArgument, "valid observed_at must be provided")
	}
	if reconciliationStatus.GetTransitionedAt() == nil || reconciliationStatus.GetTransitionedAt().CheckValid() != nil {
		return nil, status.Error(codes.InvalidArgument, "valid transitioned_at must be provided")
	}
	if utf8.RuneCountInString(reconciliationStatus.GetDiagnosticMessage()) > 1024 {
		return nil, status.Error(codes.InvalidArgument, "diagnostic message cannot exceed 1024 characters")
	}
	return s.jobSource.UpdateAdviceReconciliationStatus(ctx, req)
}

func (s *heimdallServer) GetRecoverySequence(ctx context.Context, req *pb.GetRecoverySequenceRequest) (*pb.GetRecoverySequenceResponse, error) {
	if err := validateRequiredBoundedString("scheduler system", req.SchedulerSystem, 32); err != nil {
		return nil, err
	}
	if err := validateRequiredBoundedString("scheduler scope", req.SchedulerScope, 256); err != nil {
		return nil, err
	}
	if err := validateRequiredBoundedString("job id", req.JobId, 256); err != nil {
		return nil, err
	}
	if req.SourceAttemptIndex < 0 {
		return nil, status.Error(codes.InvalidArgument, "source attempt index must not be negative")
	}
	if !validRecoveryOperation(req.Operation) {
		return nil, status.Error(codes.InvalidArgument, "a supported recovery operation must be provided")
	}

	return s.jobSource.GetRecoverySequence(ctx, req)
}

func (s *heimdallServer) RecordRecoveryMilestone(ctx context.Context, req *pb.RecordRecoveryMilestoneRequest) (*pb.RecordRecoveryMilestoneResponse, error) {
	if err := validateRequiredBoundedString("sequence id", req.SequenceId, 64); err != nil {
		return nil, err
	}
	if err := validateRequiredBoundedString("idempotency key", req.IdempotencyKey, 128); err != nil {
		return nil, err
	}
	if !validRecoveryOperation(req.Operation) {
		return nil, status.Error(codes.InvalidArgument, "a supported recovery operation must be provided")
	}
	if !validRecoverySchedulerAction(req.Action) {
		return nil, status.Error(codes.InvalidArgument, "a supported recovery scheduler action must be provided")
	}
	if !validRecoverySchedulerActionOutcome(req.Outcome) {
		return nil, status.Error(codes.InvalidArgument, "a supported recovery scheduler action outcome must be provided")
	}
	if req.CompletesSequence == nil {
		return nil, status.Error(codes.InvalidArgument, "completes sequence must be provided")
	}
	if req.GetCompletesSequence() && req.Outcome != pb.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_SUCCESS {
		return nil, status.Error(codes.InvalidArgument, "only successful actions can complete a recovery sequence")
	}
	if req.CompletedAt == nil {
		return nil, status.Error(codes.InvalidArgument, "completed at must be provided")
	}
	if err := req.CompletedAt.CheckValid(); err != nil {
		return nil, status.Error(codes.InvalidArgument, "completed at must be a valid timestamp")
	}

	return s.jobSource.RecordRecoveryMilestone(ctx, req)
}

func validateLimit(limit int32) error {
	if limit <= 0 {
		return status.Errorf(codes.InvalidArgument, "limit must be greater than 0")
	}
	if limit > maxPageSize {
		return status.Errorf(codes.InvalidArgument, "limit must be less than or equal to %d", maxPageSize)
	}
	return nil
}

func validateHeimdallJobId(heimdallJobId string) error {
	if heimdallJobId == "" {
		return status.Errorf(codes.InvalidArgument, "heimdall job id must be provided")
	}
	return nil
}

func validateJobId(jobId string) error {
	if jobId == "" {
		return status.Errorf(codes.InvalidArgument, "job id must be provided")
	}
	return nil
}

func validateJobIds(jobIds []string) error {
	for _, jobId := range jobIds {
		if err := validateJobId(jobId); err != nil {
			return err
		}
	}
	return nil
}

func validateSchedulerJobId(schedulerJobId string) error {
	if schedulerJobId == "" {
		return status.Errorf(codes.InvalidArgument, "scheduler job id must be provided")
	}
	return nil
}

func validateWorkflowID(workflowID string) error {
	if workflowID == "" {
		return status.Error(codes.InvalidArgument, "workflow ID must be provided")
	}
	if !utf8.ValidString(workflowID) {
		return status.Error(codes.InvalidArgument, "workflow ID must be valid UTF-8")
	}
	if utf8.RuneCountInString(workflowID) > 256 {
		return status.Error(codes.InvalidArgument, "workflow ID must contain at most 256 Unicode scalars")
	}
	return nil
}

func validateWorkflowIds(workflowIds []string) error {
	for _, workflowId := range workflowIds {
		if err := validateWorkflowID(workflowId); err != nil {
			return err
		}
	}
	return nil
}

func validateRequiredBoundedString(name, value string, maxRunes int) error {
	if value == "" {
		return status.Errorf(codes.InvalidArgument, "%s must be provided", name)
	}
	return validateOptionalBoundedString(name, value, maxRunes)
}

func validateOptionalBoundedString(name, value string, maxRunes int) error {
	if !utf8.ValidString(value) {
		return status.Errorf(codes.InvalidArgument, "%s must be valid UTF-8", name)
	}
	if utf8.RuneCountInString(value) > maxRunes {
		return status.Errorf(codes.InvalidArgument, "%s must contain at most %d Unicode scalars", name, maxRunes)
	}
	return nil
}

func validRecoveryOperation(operation pb.RecoveryOperation) bool {
	switch operation {
	case pb.RecoveryOperation_RECOVERY_OPERATION_RESTART,
		pb.RecoveryOperation_RECOVERY_OPERATION_TERMINATE,
		pb.RecoveryOperation_RECOVERY_OPERATION_DAG_RELEASE:
		return true
	default:
		return false
	}
}

func validRecoverySchedulerAction(action pb.RecoverySchedulerAction) bool {
	switch action {
	case pb.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_REQUEUE_HOLD,
		pb.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_UPDATE_CONSTRAINTS,
		pb.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_RELEASE,
		pb.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_CANCEL:
		return true
	default:
		return false
	}
}

func validRecoverySchedulerActionOutcome(outcome pb.RecoverySchedulerActionOutcome) bool {
	switch outcome {
	case pb.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_PLANNED,
		pb.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_SUCCESS,
		pb.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_ERROR,
		pb.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_ALREADY_SATISFIED:
		return true
	default:
		return false
	}
}
