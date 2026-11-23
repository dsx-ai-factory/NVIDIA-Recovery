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

// NodeEvents is the downstream persisted attribution result collection.
type NodeEvents = jobmanagerBindings.NodeEvents

type ApplicationIntention = jobmanagerBindings.ApplicationIntention
type ApplicationIntentionInput = jobmanagerBindings.ApplicationIntentionInput

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

func (c *JobManagerClient) CreateApplicationIntention(ctx context.Context, input ApplicationIntentionInput) (*ApplicationIntention, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.CreateApplicationIntention")
	defer span.End()

	response, _, err := c.jobManager.DefaultAPI.PostApplicationIntention(ctx).
		ApplicationIntentionInput(input).
		Execute()
	if err != nil {
		return nil, err
	}
	return response, nil
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

// ErrorAttributionsParams captures the filters for GetErrorAttributions.
type ErrorAttributionsParams struct {
	// NodeNames selects node-scoped results.
	NodeNames []string
	// JobIds selects scheduler jobs.
	JobIds []string
	// StartTimestamp is the inclusive lower event-time bound.
	StartTimestamp *time.Time
	// EndTimestamp is the inclusive upper event-time bound.
	EndTimestamp *time.Time
	// AnomalyUuid selects one anomaly.
	AnomalyUuid *string
	// Limit bounds the downstream page size.
	Limit *int32
	// Offset selects a zero-based page offset.
	Offset *int32
}

// GetErrorAttributions retrieves error attributions from the job manager.
func (c *JobManagerClient) GetErrorAttributions(ctx context.Context, p ErrorAttributionsParams) (*NodeEvents, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.GetErrorAttributions")
	defer span.End()

	request := c.jobManager.DefaultAPI.GetErrorAttributions(ctx)
	if len(p.NodeNames) > 0 {
		request = request.NodeNames(strSliceToPtr(p.NodeNames))
	}
	if len(p.JobIds) > 0 {
		request = request.JobIds(strSliceToPtr(p.JobIds))
	}
	if p.StartTimestamp != nil {
		request = request.StartTimestamp(*p.StartTimestamp)
	}
	if p.EndTimestamp != nil {
		request = request.EndTimestamp(*p.EndTimestamp)
	}
	if p.AnomalyUuid != nil {
		request = request.AnomalyUuid(*p.AnomalyUuid)
	}
	if p.Limit != nil {
		request = request.Limit(*p.Limit)
	}
	if p.Offset != nil {
		request = request.Offset(*p.Offset)
	}

	response, httpResponse, err := c.jobManager.DefaultAPI.GetErrorAttributionsExecute(request)
	if err != nil {
		return nil, mapAttributionRequestError(httpResponse, err)
	}
	return response, nil
}

// JobErrorAttributionsParams captures the filters for GetJobErrorAttributions.
type JobErrorAttributionsParams struct {
	// JobId is the scheduler job identifier.
	JobId string
	// JobAttemptIndex selects an attempt; -1 means latest.
	JobAttemptIndex *int32
	// AnomalyUuid selects one anomaly.
	AnomalyUuid *string
}

// GetJobErrorAttributions retrieves error attributions for a single job.
func (c *JobManagerClient) GetJobErrorAttributions(ctx context.Context, p JobErrorAttributionsParams) (*NodeEvents, error) {
	ctx, span := tracer.Start(ctx, "jobmanager.GetJobErrorAttributions")
	defer span.End()

	request := c.jobManager.DefaultAPI.GetJobErrorAttributions(ctx).JobId(p.JobId)
	if p.JobAttemptIndex != nil {
		request = request.JobAttemptIndex(*p.JobAttemptIndex)
	}
	if p.AnomalyUuid != nil {
		request = request.AnomalyUuid(*p.AnomalyUuid)
	}

	response, httpResponse, err := c.jobManager.DefaultAPI.GetJobErrorAttributionsExecute(request)
	if err != nil {
		return nil, mapAttributionRequestError(httpResponse, err)
	}
	return response, nil
}

// strSliceToPtr adapts repeated query values to the generated nullable element type.
func strSliceToPtr(values []string) []*string {
	result := make([]*string, len(values))
	for i := range values {
		result[i] = &values[i]
	}
	return result
}

func mapAttributionRequestError(response *http.Response, err error) error {
	if response != nil {
		switch response.StatusCode {
		case http.StatusBadRequest, http.StatusUnprocessableEntity:
			return status.Error(codes.InvalidArgument, "job manager rejected attribution filters")
		case http.StatusNotFound:
			return status.Error(codes.NotFound, "attribution results not found")
		}
	}
	return err
}
