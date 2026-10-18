// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package jobmanager

import (
	"context"
	"fmt"
	"net/http"
	"time"

	jobmanagerBindings "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/jobmanager"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var tracer trace.Tracer

type SlurmJobExecutionStatus = jobmanagerBindings.SlurmJobExecutionStatusOutput
type JobExecutionStatuses = jobmanagerBindings.JobExecutionStatuses
type GetWorkflowsResponse = jobmanagerBindings.GetWorkflowsResponse
type Advice = jobmanagerBindings.Advice
type AdviceOutput = jobmanagerBindings.AdviceOutput
type AdviceReconciliationStatus = jobmanagerBindings.AdviceReconciliationStatus
type ListAdviceResponse = jobmanagerBindings.ListAdviceResponse
type GetRecoverySequenceResponse = jobmanagerBindings.GetRecoverySequenceResponse
type RecordRecoveryMilestoneRequest = jobmanagerBindings.RecordRecoveryMilestoneRequest
type RecordRecoveryMilestoneResponse = jobmanagerBindings.RecordRecoveryMilestoneResponse

type GetJobsOption func(*jobmanagerBindings.ApiGetJobsRequest)
type GetWorkflowsOption func(*jobmanagerBindings.ApiGetWorkflowsRequest)
type GetAdviceOption func(*jobmanagerBindings.ApiGetAdviceRequest)

func init() {
	tracer = otel.Tracer("jobmanager/client")
}

type JobManagerClient struct {
	jobManager *jobmanagerBindings.APIClient
}

type Pagination struct {
	Limit  int32
	Offset int32
}

type TokenPagination struct {
	Token string
	Limit int32
}

type JobRunId struct {
	JobId        string
	AttemptIndex int32
}

func NewJobManagerClient(jobManagerEndpoint string) *JobManagerClient {
	cfg := jobmanagerBindings.NewConfiguration()
	cfg.Servers = jobmanagerBindings.ServerConfigurations{
		{
			URL: jobManagerEndpoint,
		},
	}

	return &JobManagerClient{
		jobManager: jobmanagerBindings.NewAPIClient(cfg),
	}
}

func (c *JobManagerClient) GetJob(ctx context.Context, jobRunId JobRunId) (*SlurmJobExecutionStatus, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.GetJob")
	defer span.End()

	request := c.jobManager.DefaultAPI.GetJobs(ctx)

	request = request.JobId(jobRunId.JobId)
	request = request.JobAttemptIndex(fmt.Sprintf("%d", jobRunId.AttemptIndex))

	response, _, err := c.jobManager.DefaultAPI.GetJobsExecute(request)
	if err != nil {
		return nil, err
	}

	if len(response.Statuses) == 0 {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("no job found for job ID: %s-%d", jobRunId.JobId, jobRunId.AttemptIndex))
	}

	if len(response.Statuses) > 1 {
		return nil, status.Error(codes.Internal, fmt.Sprintf("multiple jobs found for job ID: %s-%d", jobRunId.JobId, jobRunId.AttemptIndex))
	}

	return &response.Statuses[0], nil
}

// GetJobByHeimdallId retrieves a job by its heimdall_job_id (UUID)
func (c *JobManagerClient) GetJobByHeimdallId(ctx context.Context, heimdallJobId string) (*SlurmJobExecutionStatus, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.GetJobByHeimdallId")
	defer span.End()

	request := c.jobManager.DefaultAPI.GetJobs(ctx)
	request = request.HeimdallJobId(heimdallJobId)

	response, _, err := c.jobManager.DefaultAPI.GetJobsExecute(request)
	if err != nil {
		return nil, err
	}

	if len(response.Statuses) == 0 {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("no job found for heimdall_job_id: %s", heimdallJobId))
	}

	if len(response.Statuses) > 1 {
		return nil, status.Error(codes.Internal, fmt.Sprintf("multiple jobs found for heimdall_job_id: %s", heimdallJobId))
	}

	return &response.Statuses[0], nil
}

func WithJobIds(jobIDs []JobRunId) GetJobsOption {
	return func(request *jobmanagerBindings.ApiGetJobsRequest) {
		for _, jobRunId := range jobIDs {
			*request = request.JobId(jobRunId.JobId)
			*request = request.JobAttemptIndex(fmt.Sprintf("%d", jobRunId.AttemptIndex))
		}
	}
}

func WithJobID(jobID string) GetJobsOption {
	return func(request *jobmanagerBindings.ApiGetJobsRequest) {
		*request = request.JobId(jobID)
	}
}

func WithJobName(jobName string) GetJobsOption {
	return func(request *jobmanagerBindings.ApiGetJobsRequest) {
		*request = request.JobName(jobName)
	}
}

func WithWorkflowID(workflowID string) GetJobsOption {
	return func(request *jobmanagerBindings.ApiGetJobsRequest) {
		*request = request.WorkflowId(workflowID)
	}
}

func WithJobState(state string) GetJobsOption {
	return func(request *jobmanagerBindings.ApiGetJobsRequest) {
		*request = request.State(state)
	}
}

func WithReconciliationEligibleJobs(reconciliationEligibleJobs bool) GetJobsOption {
	return func(request *jobmanagerBindings.ApiGetJobsRequest) {
		*request = request.ReconciliationEligibleJobs(reconciliationEligibleJobs)
	}
}

// WithCompletedJobs selects final or non-final attempts in Job Manager before pagination.
func WithCompletedJobs(completedJobs bool) GetJobsOption {
	return func(request *jobmanagerBindings.ApiGetJobsRequest) {
		*request = request.CompletedJobs(completedJobs)
	}
}

// WithJobEndTimeFrom filters on persisted end time in Job Manager's database.
func WithJobEndTimeFrom(since time.Time) GetJobsOption {
	return func(request *jobmanagerBindings.ApiGetJobsRequest) {
		*request = request.TimeFilterTarget(jobmanagerBindings.JOBTIMEFILTERTARGET_END_TIME).TimeFilterFrom(since)
	}
}

func WithJobsPagination(pagination *Pagination) GetJobsOption {
	return func(request *jobmanagerBindings.ApiGetJobsRequest) {
		if pagination != nil {
			*request = request.Limit(pagination.Limit).Offset(pagination.Offset)
		}
	}
}

func (c *JobManagerClient) GetJobs(ctx context.Context, opts ...GetJobsOption) (*JobExecutionStatuses, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.GetJobs")
	defer span.End()

	request := c.jobManager.DefaultAPI.GetJobs(ctx)

	for _, opt := range opts {
		opt(&request)
	}

	response, _, err := c.jobManager.DefaultAPI.GetJobsExecute(request)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func WithWorkflowsPagination(pagination *Pagination) GetWorkflowsOption {
	return func(request *jobmanagerBindings.ApiGetWorkflowsRequest) {
		if pagination != nil {
			*request = request.Limit(pagination.Limit).Offset(pagination.Offset)
		}
	}
}

func WithExactWorkflowID(workflowID string) GetWorkflowsOption {
	return func(request *jobmanagerBindings.ApiGetWorkflowsRequest) {
		*request = request.WorkflowId(workflowID).WorkflowIdMatch("exact")
	}
}

func (c *JobManagerClient) GetWorkflows(ctx context.Context, opts ...GetWorkflowsOption) (*GetWorkflowsResponse, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.GetWorkflows")
	defer span.End()

	request := c.jobManager.DefaultAPI.GetWorkflows(ctx)

	for _, opt := range opts {
		opt(&request)
	}

	response, _, err := c.jobManager.DefaultAPI.GetWorkflowsExecute(request)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// UpsertJob creates or updates a job in the Job Manager.
// It returns the created/updated job and whether it was created (true) or updated (false).
func (c *JobManagerClient) UpsertJob(ctx context.Context, input *jobmanagerBindings.SlurmJobExecutionStatusInput) (*SlurmJobExecutionStatus, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.UpsertJob")
	defer span.End()

	request := c.jobManager.DefaultAPI.PostJobs(ctx)
	request = request.SlurmJobExecutionStatusInput(*input)

	response, httpResponse, err := c.jobManager.DefaultAPI.PostJobsExecute(request)
	if err != nil {
		if httpResponse != nil &&
			(httpResponse.StatusCode == http.StatusBadRequest ||
				httpResponse.StatusCode == http.StatusUnprocessableEntity) {
			return nil, status.Error(codes.InvalidArgument, "job manager rejected job input")
		}
		return nil, err
	}

	return response, nil
}

func WithAdvicePagination(pagination *TokenPagination) GetAdviceOption {
	return func(request *jobmanagerBindings.ApiGetAdviceRequest) {
		if pagination != nil {
			*request = request.PageToken(pagination.Token).PageSize(pagination.Limit)
		}
	}
}

// WithOnlyActiveAdvice requests Advice that can still require reconciliation.
func WithOnlyActiveAdvice() GetAdviceOption {
	return func(request *jobmanagerBindings.ApiGetAdviceRequest) {
		*request = request.OnlyActive(true)
	}
}

// GetAdvice retrieves advice from the job manager
func (c *JobManagerClient) GetAdvice(ctx context.Context, opts ...GetAdviceOption) (*ListAdviceResponse, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.GetAdvice")
	defer span.End()

	request := c.jobManager.DefaultAPI.GetAdvice(ctx)

	for _, opt := range opts {
		opt(&request)
	}

	response, _, err := c.jobManager.DefaultAPI.GetAdviceExecute(request)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// UpdateAdviceReconciliationStatus records one generation-scoped controller observation.
func (c *JobManagerClient) UpdateAdviceReconciliationStatus(
	ctx context.Context,
	adviceID string,
	reconciliationStatus AdviceReconciliationStatus,
) (*AdviceReconciliationStatus, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.UpdateAdviceReconciliationStatus")
	defer span.End()

	request := c.jobManager.DefaultAPI.PutAdviceReconciliationStatus(ctx, adviceID)
	request = request.AdviceReconciliationStatus(reconciliationStatus)
	response, httpResponse, err := c.jobManager.DefaultAPI.PutAdviceReconciliationStatusExecute(request)
	if err != nil {
		if httpResponse != nil {
			switch httpResponse.StatusCode {
			case http.StatusBadRequest, http.StatusUnprocessableEntity:
				return nil, status.Error(codes.InvalidArgument, "job manager rejected Advice reconciliation status")
			case http.StatusNotFound:
				return nil, status.Error(codes.NotFound, "Advice not found")
			}
		}
		return nil, err
	}

	return response, nil
}

func (c *JobManagerClient) GetRecoverySequence(ctx context.Context, schedulerSystem, schedulerScope, jobID string, sourceAttemptIndex int32, operation jobmanagerBindings.RecoveryOperation) (*GetRecoverySequenceResponse, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.GetRecoverySequence")
	defer span.End()

	request := c.jobManager.DefaultAPI.GetRecoverySequence(ctx)
	request = request.SchedulerSystem(schedulerSystem).SchedulerScope(schedulerScope).JobId(jobID).SourceAttemptIndex(sourceAttemptIndex).Operation(operation)
	response, httpResponse, err := request.Execute()
	if err != nil {
		return nil, mapRecoveryRequestError(httpResponse, err)
	}

	return response, nil
}

func (c *JobManagerClient) RecordRecoveryMilestone(ctx context.Context, input *RecordRecoveryMilestoneRequest) (*RecordRecoveryMilestoneResponse, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.RecordRecoveryMilestone")
	defer span.End()

	request := c.jobManager.DefaultAPI.RecordRecoveryMilestone(ctx)
	request = request.RecordRecoveryMilestoneRequest(*input)
	response, httpResponse, err := request.Execute()
	if err != nil {
		return nil, mapRecoveryRequestError(httpResponse, err)
	}

	return response, nil
}

func mapRecoveryRequestError(response *http.Response, err error) error {
	if response != nil {
		switch response.StatusCode {
		case http.StatusBadRequest, http.StatusUnprocessableEntity:
			return status.Error(codes.InvalidArgument, "job manager rejected recovery input")
		case http.StatusNotFound:
			return status.Error(codes.NotFound, "recovery sequence not found")
		}
	}
	return err
}
