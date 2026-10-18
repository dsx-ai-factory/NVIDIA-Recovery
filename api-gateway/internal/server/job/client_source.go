// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package job

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	v1 "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	jobmanagerBindings "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/jobmanager"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/jobmanager"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/util"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type JobClientSource struct {
	jobManager *jobmanager.JobManagerClient
}

var _ JobSource = &JobClientSource{}

func NewJobClientSource(jobManagerEndpoint string) *JobClientSource {
	return &JobClientSource{
		jobManager: jobmanager.NewJobManagerClient(jobManagerEndpoint),
	}
}

// GetJob implements JobSource.
func (j *JobClientSource) GetJob(ctx context.Context, req *v1.GetJobRequest) (*v1.GetJobResponse, error) {
	logger := util.GetZapLoggerFromContext(ctx)
	var job *jobmanager.SlurmJobExecutionStatus
	var err error

	// Check use_heimdall_id flag - default to legacy if not set or false
	if req.UseHeimdallId != nil && *req.UseHeimdallId {
		// New path: query by heimdall_job_id directly (UUID)
		job, err = j.jobManager.GetJobByHeimdallId(ctx, req.HeimdallJobId)
	} else {
		// Legacy path (default): parse as job_id-attempt_index
		jobRunId, parseErr := parseJobAndAttemptIndex(req.HeimdallJobId)
		if parseErr != nil {
			return nil, parseErr
		}

		logger.Info("getting legacy job", zap.String("job_id", jobRunId.JobId), zap.Int32("attempt_index", jobRunId.AttemptIndex))
		job, err = j.jobManager.GetJob(ctx, jobRunId)
	}

	if err != nil {
		return nil, err
	}

	jobPb, err := slurmJobExecutionStatusToJobPb(ctx, job)
	if err != nil {
		return nil, err
	}

	return &v1.GetJobResponse{
		Job: jobPb,
	}, nil
}

// GetJobs implements JobSource.
func (j *JobClientSource) GetJobs(ctx context.Context, req *v1.GetJobsRequest) (*v1.GetJobsResponse, error) {
	var opts []jobmanager.GetJobsOption
	var pagination numericPagination

	if req.Limit > 0 {
		var err error
		pagination, err = resolveNumericPagination(req.Limit, req.PageNumber, req.PageToken)
		if err != nil {
			return nil, err
		}

		backendPagination := &jobmanager.Pagination{
			Limit:  pagination.limit,
			Offset: pagination.offset,
		}
		opts = append(opts, jobmanager.WithJobsPagination(backendPagination))
	}

	jobRunIds := make([]jobmanager.JobRunId, len(req.JobIds))
	for i, jobID := range req.JobIds {
		jobRunId, err := parseJobAndAttemptIndex(jobID)
		if err != nil {
			return nil, err
		}
		jobRunIds[i] = jobRunId
	}

	if len(jobRunIds) > 0 {
		opts = append(opts, jobmanager.WithJobIds(jobRunIds))
	}

	if len(req.WorkflowIds) > 0 {
		if len(req.WorkflowIds) > 1 {
			return nil, status.Error(codes.InvalidArgument, "only one workflow ID is supported")
		}

		opts = append(opts, jobmanager.WithWorkflowID(req.WorkflowIds[0]))
	}

	if req.JmState != nil && *req.JmState != "" {
		opts = append(opts, jobmanager.WithJobState(*req.JmState))
	}
	if req.ReconciliationEligibleJobs != nil && *req.ReconciliationEligibleJobs {
		opts = append(opts, jobmanager.WithReconciliationEligibleJobs(true))
	}
	if req.CompletedJobs != nil {
		opts = append(opts, jobmanager.WithCompletedJobs(*req.CompletedJobs))
	}
	if req.EndTimeFrom != nil {
		opts = append(opts, jobmanager.WithJobEndTimeFrom(req.EndTimeFrom.AsTime()))
	}

	jobs, err := j.jobManager.GetJobs(ctx, opts...)
	if err != nil {
		return nil, err
	}

	jobPbs := make([]*v1.Job, len(jobs.Statuses))
	for i, job := range jobs.Statuses {
		jobPb, err := slurmJobExecutionStatusToJobPb(ctx, &job)
		if err != nil {
			return nil, err
		}
		jobPbs[i] = jobPb
	}

	var nextPageToken *string
	if req.Limit > 0 {
		nextPageToken, err = pagination.nextToken(int32(len(jobs.Statuses)), jobs.ItemCount)
		if err != nil {
			return nil, err
		}
	}

	return &v1.GetJobsResponse{
		Jobs:          jobPbs,
		Count:         int32(len(jobPbs)),
		NextPageToken: nextPageToken,
	}, nil
}

// CreateOrUpdateJob implements JobSource.
func (j *JobClientSource) CreateOrUpdateJob(ctx context.Context, req *v1.CreateOrUpdateJobRequest) (*v1.CreateOrUpdateJobResponse, error) {
	logger := util.GetZapLoggerFromContext(ctx)

	// Use heimdall_id if provided, otherwise leave empty
	heimdallId := ""
	if req.HeimdallJobId != nil && *req.HeimdallJobId != "" {
		heimdallId = *req.HeimdallJobId
	}

	logger.Info("creating or updating job",
		zap.String("heimdall_id", heimdallId),
		zap.String("job_id", req.JobId),
		zap.String("job_name", stringPtrValue(req.JobName)),
		zap.String("workflow_id", req.WorkflowId),
		zap.Int32("attempt_index", req.AttemptIndex))

	// Convert protobuf request to Job Manager format
	input, err := convertCreateOrUpdateRequestToJobManagerInput(req)
	if err != nil {
		return nil, err
	}

	// Call Job Manager to persist the job
	jobManagerOutput, err := j.jobManager.UpsertJob(ctx, input)
	if err != nil {
		logger.Error("failed to create/update job in Job Manager", zap.Error(err))
		return nil, err
	}

	// Determine if job was created or updated by checking if it existed before
	// For now, we check if the job existed by attempting to get it first
	// TODO: The Job Manager API should return a 'created' flag in the response
	// Convert Job Manager output back to protobuf format
	job := convertJobManagerOutputToJob(jobManagerOutput, heimdallId)

	return &v1.CreateOrUpdateJobResponse{
		Job: job,
	}, nil
}

// Helper function to safely get string pointer value
func stringPtrValue(ptr *string) string {
	if ptr != nil {
		return *ptr
	}
	return ""
}

// convertCreateOrUpdateRequestToJobManagerInput converts a protobuf CreateOrUpdateJobRequest to Job Manager format
func convertCreateOrUpdateRequestToJobManagerInput(req *v1.CreateOrUpdateJobRequest) (*jobmanagerBindings.SlurmJobExecutionStatusInput, error) {
	input := jobmanagerBindings.NewSlurmJobExecutionStatusInput(req.JobId, req.AttemptIndex, req.WorkflowId)

	// Set heimdall_job_id if provided
	if req.HeimdallJobId != nil && *req.HeimdallJobId != "" {
		input.HeimdallJobId = *jobmanagerBindings.NewNullableString(req.HeimdallJobId)
		input.Filename = *jobmanagerBindings.NewNullableString(req.HeimdallJobId)
	}

	// Set optional fields
	if req.JobName != nil {
		input.JobName = *jobmanagerBindings.NewNullableString(req.JobName)
	}

	// Convert state
	if req.State != v1.JobStatus_JOB_STATUS_UNSPECIFIED {
		stateStr := convertJobStatusToString(req.State)
		input.State = *jobmanagerBindings.NewNullableString(&stateStr)
	}

	// Set allocated nodes
	if len(req.AllocatedNodes) > 0 {
		input.AllocatedNodeList = req.AllocatedNodes
	}

	// Set resource counts
	if req.GpuCountPerNode > 0 {
		gpuCount := req.GpuCountPerNode
		input.TotalGpuCount = *jobmanagerBindings.NewNullableInt32(&gpuCount)
	}

	// Set node constraints if provided
	if req.NodeConstraints != nil {
		if len(req.NodeConstraints.Inclusions) > 0 {
			input.InclusionNodeList = req.NodeConstraints.Inclusions
		}
		if len(req.NodeConstraints.Exclusions) > 0 {
			input.ExclusionNodeList = req.NodeConstraints.Exclusions
		}
	}

	// Convert scheduler metadata from protobuf Struct to interface{} map
	if req.SchedulerMetadata != nil {
		input.SchedulerMetadata = req.SchedulerMetadata.AsMap()
	}

	if req.RecoveryConfig != nil {
		apsSpec, err := recoveryConfigToApsSpecInput(req.RecoveryConfig)
		if err != nil {
			return nil, err
		}
		input.SetApsSpec(apsSpec)
	}

	return input, nil
}

// convertJobManagerOutputToJob converts Job Manager output to protobuf Job format
func convertJobManagerOutputToJob(output *jobmanagerBindings.SlurmJobExecutionStatusOutput, heimdallId string) *v1.Job {
	job := &v1.Job{
		HeimdallJobId: heimdallId,
		JobId:         output.JobId,
		WorkflowId:    output.WorkflowId,
		AttemptIndex:  output.JobAttemptIndex,
	}

	// Convert optional fields
	if output.JobName.IsSet() {
		job.JobName = output.JobName.Get()
	}

	// Nullable backend fields may be explicitly set to null. Preserve the
	// unspecified state instead of dereferencing a nil value on partial updates.
	if state := output.State.Get(); state != nil {
		job.State = parseJobStatus(*state)
	}

	// Convert allocated nodes
	if len(output.AllocatedNodeList) > 0 {
		job.Nodes = output.AllocatedNodeList
	}

	// Convert GPU count
	if output.TotalGpuCount.IsSet() {
		job.GpuCountPerNode = output.TotalGpuCount.Get()
	}

	// Convert node constraints
	if len(output.InclusionNodeList) > 0 || len(output.ExclusionNodeList) > 0 {
		job.NodeConstraints = &v1.NodeConstraints{
			Inclusions: output.InclusionNodeList,
			Exclusions: output.ExclusionNodeList,
		}
	}

	if output.ApsSpec.IsSet() && output.ApsSpec.Get() != nil {
		job.RecoveryConfig = apsSpecOutputToRecoveryConfig(*output.ApsSpec.Get())
	}

	if output.SchedulerMetadata != nil {
		job.SchedulerMetadata, _ = structpb.NewStruct(output.SchedulerMetadata)
	}

	// Convert timestamps
	if timestamp := output.LastUpdatedAt.Get(); timestamp != nil {
		job.LastSeen = timestamppb.New(*timestamp)
		job.UpdatedAt = timestamppb.New(*timestamp)
	}

	// Set creation time (use update time as fallback)
	if job.UpdatedAt != nil {
		job.CreatedAt = job.UpdatedAt
	} else {
		job.CreatedAt = timestamppb.Now()
	}

	return job
}

func recoveryConfigToApsSpecInput(config *v1.JobRecoveryConfig) (jobmanagerBindings.ApsSlurmJobSpecInput, error) {
	apsSpec := jobmanagerBindings.ApsSlurmJobSpecInput{}

	switch config.AutoResumeMode {
	case v1.JobRecoveryConfig_AUTO_RESUME_MODE_UNSPECIFIED:
	case v1.JobRecoveryConfig_AUTO_RESUME_MODE_REQUEUE:
		apsSpec.SetAutoResumeMode(jobmanagerBindings.AUTORESUMEMODEENUM_REQUEUE)
	case v1.JobRecoveryConfig_AUTO_RESUME_MODE_SINGLETON_DEPENDENCY:
		apsSpec.SetAutoResumeMode(jobmanagerBindings.AUTORESUMEMODEENUM_SINGLETON_DEPENDENCY)
	default:
		return jobmanagerBindings.ApsSlurmJobSpecInput{}, status.Errorf(codes.InvalidArgument, "unsupported auto_resume_mode: %s", config.AutoResumeMode)
	}

	if config.MaxRequeueTimes != nil {
		if *config.MaxRequeueTimes <= 0 {
			return jobmanagerBindings.ApsSlurmJobSpecInput{}, status.Error(codes.InvalidArgument, "max_requeue_times must be positive")
		}
		apsSpec.SetMaxRequeueTimes(*config.MaxRequeueTimes)
	}
	if config.RequeueOnCompletion != nil {
		apsSpec.SetRequeueOnCompletion(*config.RequeueOnCompletion)
	}
	if config.LogFilePathsSharedAcrossAttempts != nil {
		apsSpec.SetLogFilePathsSharedAcrossAttempts(*config.LogFilePathsSharedAcrossAttempts)
	}
	if config.Environment != nil {
		apsSpec.SetEnvironment(*config.Environment)
	}
	if config.AnomalyDetectionConfig != nil {
		detection := jobmanagerBindings.AnomalyDetectionConfig{}
		if config.AnomalyDetectionConfig.LogBasedCrashEnabled != nil {
			detection.SetLogBasedCrashEnabled(*config.AnomalyDetectionConfig.LogBasedCrashEnabled)
		}
		if config.AnomalyDetectionConfig.LogBasedTimeoutEnabled != nil {
			detection.SetLogBasedTimeoutEnabled(*config.AnomalyDetectionConfig.LogBasedTimeoutEnabled)
		}
		if threshold := config.AnomalyDetectionConfig.LogBasedTimeoutThresholdPretraining; threshold != nil {
			if *threshold <= 0 {
				return jobmanagerBindings.ApsSlurmJobSpecInput{}, status.Error(codes.InvalidArgument, "log_based_timeout_threshold_pretraining must be positive")
			}
			detection.SetLogBasedTimeoutThresholdPretraining(*threshold)
		}
		if threshold := config.AnomalyDetectionConfig.LogBasedTimeoutThresholdTrainingloop; threshold != nil {
			if *threshold <= 0 {
				return jobmanagerBindings.ApsSlurmJobSpecInput{}, status.Error(codes.InvalidArgument, "log_based_timeout_threshold_trainingloop must be positive")
			}
			detection.SetLogBasedTimeoutThresholdTrainingloop(*threshold)
		}
		apsSpec.SetAnomalyDetectionConfig(detection)
	}
	if len(config.DagDependencies) > 0 {
		dependencies := make([]jobmanagerBindings.DagDependencyStep, 0, len(config.DagDependencies))
		for _, dependency := range config.DagDependencies {
			if dependency == nil {
				return jobmanagerBindings.ApsSlurmJobSpecInput{}, status.Error(codes.InvalidArgument, "dag_dependencies must not contain null entries")
			}
			var target jobmanagerBindings.Target
			switch value := dependency.Target.(type) {
			case *v1.JobDagDependency_JobId:
				target = jobmanagerBindings.JobReferenceByIdAsTarget(jobmanagerBindings.NewJobReferenceById(value.JobId))
			case *v1.JobDagDependency_JobName:
				target = jobmanagerBindings.JobReferenceByNameAsTarget(jobmanagerBindings.NewJobReferenceByName(value.JobName))
			default:
				return jobmanagerBindings.ApsSlurmJobSpecInput{}, status.Error(codes.InvalidArgument, "dag dependency target must contain job_id or job_name")
			}
			step := jobmanagerBindings.NewDagDependencyStep(target)
			switch dependency.Trigger {
			case v1.JobDagDependency_TRIGGER_CONDITION_UNSPECIFIED:
			case v1.JobDagDependency_TRIGGER_CONDITION_COMPLETED:
				step.SetTrigger(jobmanagerBindings.DAGTRIGGERCONDITIONENUM_COMPLETED)
			default:
				return jobmanagerBindings.ApsSlurmJobSpecInput{}, status.Errorf(codes.InvalidArgument, "unsupported DAG trigger condition: %s", dependency.Trigger)
			}
			dependencies = append(dependencies, *step)
		}
		apsSpec.SetDagDependencies(dependencies)
	}
	switch config.DagAction {
	case v1.JobRecoveryConfig_DAG_ACTION_UNSPECIFIED:
	case v1.JobRecoveryConfig_DAG_ACTION_RELEASE:
		apsSpec.SetDagAction(jobmanagerBindings.DAGACTIONENUM_RELEASE)
	default:
		return jobmanagerBindings.ApsSlurmJobSpecInput{}, status.Errorf(codes.InvalidArgument, "unsupported DAG action: %s", config.DagAction)
	}

	return apsSpec, nil
}

func apsSpecOutputToRecoveryConfig(apsSpec jobmanagerBindings.ApsSlurmJobSpecOutput) *v1.JobRecoveryConfig {
	config := &v1.JobRecoveryConfig{}

	if autoResumeMode, ok := apsSpec.GetAutoResumeModeOk(); ok && autoResumeMode != nil {
		switch *autoResumeMode {
		case jobmanagerBindings.AUTORESUMEMODEENUM_REQUEUE:
			config.AutoResumeMode = v1.JobRecoveryConfig_AUTO_RESUME_MODE_REQUEUE
		case jobmanagerBindings.AUTORESUMEMODEENUM_SINGLETON_DEPENDENCY:
			config.AutoResumeMode = v1.JobRecoveryConfig_AUTO_RESUME_MODE_SINGLETON_DEPENDENCY
		}
	}
	if maxRequeueTimes, ok := apsSpec.GetMaxRequeueTimesOk(); ok && maxRequeueTimes != nil {
		config.MaxRequeueTimes = maxRequeueTimes
	}
	if requeueOnCompletion, ok := apsSpec.GetRequeueOnCompletionOk(); ok && requeueOnCompletion != nil {
		config.RequeueOnCompletion = requeueOnCompletion
	}
	if sharedPaths, ok := apsSpec.GetLogFilePathsSharedAcrossAttemptsOk(); ok && sharedPaths != nil {
		config.LogFilePathsSharedAcrossAttempts = sharedPaths
	}
	if environment, ok := apsSpec.GetEnvironmentOk(); ok && environment != nil {
		config.Environment = environment
	}
	if detection, ok := apsSpec.GetAnomalyDetectionConfigOk(); ok && detection != nil {
		config.AnomalyDetectionConfig = &v1.JobAnomalyDetectionConfig{
			LogBasedCrashEnabled:                 detection.LogBasedCrashEnabled,
			LogBasedTimeoutEnabled:               detection.LogBasedTimeoutEnabled,
			LogBasedTimeoutThresholdPretraining:  detection.LogBasedTimeoutThresholdPretraining,
			LogBasedTimeoutThresholdTrainingloop: detection.LogBasedTimeoutThresholdTrainingloop,
		}
	}
	for _, dependency := range apsSpec.GetDagDependencies() {
		mapped := &v1.JobDagDependency{}
		switch target := dependency.Target.GetActualInstance().(type) {
		case *jobmanagerBindings.JobReferenceById:
			mapped.Target = &v1.JobDagDependency_JobId{JobId: target.Id}
		case *jobmanagerBindings.JobReferenceByName:
			mapped.Target = &v1.JobDagDependency_JobName{JobName: target.Name}
		default:
			continue
		}
		if trigger, ok := dependency.GetTriggerOk(); ok && trigger != nil && *trigger == jobmanagerBindings.DAGTRIGGERCONDITIONENUM_COMPLETED {
			mapped.Trigger = v1.JobDagDependency_TRIGGER_CONDITION_COMPLETED
		}
		config.DagDependencies = append(config.DagDependencies, mapped)
	}
	if action, ok := apsSpec.GetDagActionOk(); ok && action != nil && *action == jobmanagerBindings.DAGACTIONENUM_RELEASE {
		config.DagAction = v1.JobRecoveryConfig_DAG_ACTION_RELEASE
	}

	return config
}

// convertJobStatusToString converts a protobuf JobStatus enum to a string
func convertJobStatusToString(status v1.JobStatus) string {
	switch status {
	case v1.JobStatus_JOB_STATUS_PENDING:
		return "PENDING"
	case v1.JobStatus_JOB_STATUS_RUNNING:
		return "RUNNING"
	case v1.JobStatus_JOB_STATUS_STOPPED:
		return "STOPPED"
	case v1.JobStatus_JOB_STATUS_COMPLETED:
		return "COMPLETED"
	case v1.JobStatus_JOB_STATUS_FAILED:
		return "FAILED"
	case v1.JobStatus_JOB_STATUS_CANCELLED:
		return "CANCELLED"
	case v1.JobStatus_JOB_STATUS_DELETED:
		return "DELETED"
	default:
		return "UNSPECIFIED"
	}
}

// parseJobStatus converts a string status to protobuf JobStatus enum
func parseJobStatus(status string) v1.JobStatus {
	switch status {
	case "PENDING":
		return v1.JobStatus_JOB_STATUS_PENDING
	case "RUNNING":
		return v1.JobStatus_JOB_STATUS_RUNNING
	case "STOPPED":
		return v1.JobStatus_JOB_STATUS_STOPPED
	case "COMPLETED":
		return v1.JobStatus_JOB_STATUS_COMPLETED
	case "FAILED":
		return v1.JobStatus_JOB_STATUS_FAILED
	case "CANCELLED":
		return v1.JobStatus_JOB_STATUS_CANCELLED
	case "DELETED":
		return v1.JobStatus_JOB_STATUS_DELETED
	default:
		return v1.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

// GetJobsBySchedulerId implements JobSource.
func (j *JobClientSource) GetJobsBySchedulerId(ctx context.Context, req *v1.GetJobsBySchedulerIdRequest) (*v1.GetJobsBySchedulerIdResponse, error) {
	var opts []jobmanager.GetJobsOption
	var pagination numericPagination

	if req.Limit > 0 {
		var err error
		pagination, err = resolveNumericPagination(req.Limit, nil, req.Page)
		if err != nil {
			return nil, err
		}

		backendPagination := &jobmanager.Pagination{
			Limit:  pagination.limit,
			Offset: pagination.offset,
		}
		opts = append(opts, jobmanager.WithJobsPagination(backendPagination))
	}

	opts = append(opts, jobmanager.WithJobID(req.SchedulerJobId))

	jobs, err := j.jobManager.GetJobs(ctx, opts...)
	if err != nil {
		return nil, err
	}

	jobPbs := make([]*v1.Job, 0, len(jobs.Statuses))
	for _, job := range jobs.Statuses {
		// Only include jobs that actually match the scheduler job ID
		if job.JobId == req.SchedulerJobId {
			jobPb, err := slurmJobExecutionStatusToJobPb(ctx, &job)
			if err != nil {
				return nil, err
			}
			jobPbs = append(jobPbs, jobPb)
		}
	}

	var nextPageToken *string
	if req.Limit > 0 {
		nextPageToken, err = pagination.nextToken(int32(len(jobPbs)), jobs.ItemCount)
		if err != nil {
			return nil, err
		}
	}

	return &v1.GetJobsBySchedulerIdResponse{
		Jobs:          jobPbs,
		Count:         int32(len(jobPbs)),
		NextPageToken: nextPageToken,
	}, nil
}

// GetWorkflow implements JobSource.
func (j *JobClientSource) GetWorkflow(ctx context.Context, req *v1.GetWorkflowRequest) (*v1.GetWorkflowResponse, error) {
	workflows, err := j.jobManager.GetWorkflows(ctx, jobmanager.WithExactWorkflowID(req.WorkflowId))
	if err != nil {
		return nil, err
	}

	if len(workflows.Workflows) == 0 {
		return nil, status.Error(codes.NotFound, "workflow not found")
	}
	if len(workflows.Workflows) > 1 {
		return nil, status.Error(codes.Internal, "multiple workflows matched an exact workflow ID")
	}

	return &v1.GetWorkflowResponse{
		Workflow: workflowProjectionToPB(&workflows.Workflows[0]),
	}, nil
}

func workflowProjectionToPB(workflow *jobmanagerBindings.Workflow) *v1.Workflow {
	var startTime *timestamppb.Timestamp
	var endTime *timestamppb.Timestamp
	if workflow.StartTime.IsSet() && workflow.StartTime.Get() != nil {
		startTime = timestamppb.New(*workflow.StartTime.Get())
	}
	if workflow.EndTime.IsSet() && workflow.EndTime.Get() != nil {
		endTime = timestamppb.New(*workflow.EndTime.Get())
	}

	return &v1.Workflow{
		WorkflowId:          workflow.Id,
		SchedulerWorkflowId: workflow.Id,
		Name:                workflow.Id,
		StartTime:           startTime,
		EndTime:             endTime,
	}
}

// GetWorkflows implements JobSource.
func (j *JobClientSource) GetWorkflows(ctx context.Context, req *v1.GetWorkflowsRequest) (*v1.GetWorkflowsResponse, error) {
	var opts []jobmanager.GetWorkflowsOption
	var pagination numericPagination

	if req.Limit > 0 {
		var err error
		pagination, err = resolveNumericPagination(req.Limit, req.PageNumber, req.PageToken)
		if err != nil {
			return nil, err
		}

		backendPagination := &jobmanager.Pagination{
			Limit:  pagination.limit,
			Offset: pagination.offset,
		}
		opts = append(opts, jobmanager.WithWorkflowsPagination(backendPagination))
	}

	workflows, err := j.jobManager.GetWorkflows(ctx, opts...)
	if err != nil {
		return nil, err
	}

	workflowPbs := make([]*v1.Workflow, len(workflows.Workflows))
	for i, workflow := range workflows.Workflows {
		workflowPbs[i] = workflowProjectionToPB(&workflow)
	}

	var nextPageToken *string
	if req.Limit > 0 {
		nextPageToken, err = pagination.nextToken(int32(len(workflows.Workflows)), workflows.ItemCount)
		if err != nil {
			return nil, err
		}
	}

	return &v1.GetWorkflowsResponse{
		Workflows:     workflowPbs,
		Count:         int32(len(workflowPbs)),
		NextPageToken: nextPageToken,
	}, nil
}

func parseJobAndAttemptIndex(jobID string) (jobmanager.JobRunId, error) {
	// Use right partition to find the last hyphen, so job names with hyphens are supported
	lastIndex := strings.LastIndex(jobID, "-")
	if lastIndex == -1 {
		return jobmanager.JobRunId{}, status.Error(codes.InvalidArgument, "invalid job ID")
	}

	jobId := jobID[:lastIndex]
	attemptIndexStr := jobID[lastIndex+1:]

	attemptIndex, err := strconv.ParseInt(attemptIndexStr, 10, 32)
	if err != nil {
		return jobmanager.JobRunId{}, status.Error(codes.InvalidArgument, "invalid job ID")
	}

	return jobmanager.JobRunId{
		JobId:        jobId,
		AttemptIndex: int32(attemptIndex),
	}, nil
}

func slurmJobExecutionStatusToJobPb(ctx context.Context, job *jobmanager.SlurmJobExecutionStatus) (*v1.Job, error) {
	_, logger := util.GetZapLoggerFromContextWith(ctx, zap.String("job_id", job.JobId))

	jobName := ""
	if job.JobName.IsSet() && job.JobName.Get() != nil {
		jobName = *job.JobName.Get()
	}

	nodes := job.AllocatedNodeList

	nodeCount := 0
	if job.NodeCount.IsSet() && job.NodeCount.Get() != nil {
		nodeCount = int(*job.NodeCount.Get())
	} else {
		nodeCount = len(job.AllocatedNodeList)
	}

	gpuCountPerNode := 0
	if job.TotalGpuCount.IsSet() && job.TotalGpuCount.Get() != nil && nodeCount > 0 {
		gpuCountPerNode = int(*job.TotalGpuCount.Get()) / nodeCount
	}

	// See: https://slurm.schedmd.com/job_state_codes.html#states for all possible slurm job states.
	actualState := v1.JobStatus_JOB_STATUS_UNSPECIFIED
	if job.State.IsSet() && job.State.Get() != nil {
		stateStr := *job.State.Get()
		// TODO: Double check these states are correct
		switch stateStr {
		case "RUNNING":
			actualState = v1.JobStatus_JOB_STATUS_RUNNING
		case "COMPLETED":
			actualState = v1.JobStatus_JOB_STATUS_COMPLETED
		case "FAILED", "BOOT_FAIL", "DEADLINE", "NODE_FAIL", "OUT_OF_MEMORY", "TIMEOUT":
			actualState = v1.JobStatus_JOB_STATUS_FAILED
		case "CANCELLED":
			actualState = v1.JobStatus_JOB_STATUS_CANCELLED
		case "STOPPED", "PREEMPTED", "SUSPENDED":
			actualState = v1.JobStatus_JOB_STATUS_STOPPED
		case "PENDING":
			actualState = v1.JobStatus_JOB_STATUS_PENDING
		default:
			logger.Warn("unknown job state", zap.String("state", stateStr))
		}
	}

	// TODO: Check these are UTC timestamps
	var createdAt *timestamppb.Timestamp
	if job.SubmitTime.IsSet() && job.SubmitTime.Get() != nil {
		createdAt = timestamppb.New(*job.SubmitTime.Get())
	}

	// Job Manager currently exposes one persisted source timestamp. Use it for
	// both public freshness fields rather than making a read look like a new
	// scheduler observation. Leave both fields absent when the source timestamp
	// is unavailable.
	var lastSeen *timestamppb.Timestamp
	var updatedAt *timestamppb.Timestamp
	if job.LastUpdatedAt.IsSet() && job.LastUpdatedAt.Get() != nil {
		lastUpdatedAt := *job.LastUpdatedAt.Get()
		lastSeen = timestamppb.New(lastUpdatedAt)
		updatedAt = timestamppb.New(lastUpdatedAt)
	}

	heimdallJobId := ""
	if job.HeimdallJobId.IsSet() && job.HeimdallJobId.Get() != nil {
		heimdallJobId = *job.HeimdallJobId.Get()
	}

	jobPb := &v1.Job{
		HeimdallJobId: heimdallJobId,
		JobRunId:      fmt.Sprintf("%s-%d", job.JobId, job.JobAttemptIndex),
		JobId:         job.JobId,
		JobName:       &jobName,
		WorkflowId:    job.WorkflowId,
		AttemptIndex:  job.JobAttemptIndex,
		Nodes:         nodes,
		GpuCountPerNode: func() *int32 {
			if gpuCountPerNode > 0 {
				v := int32(gpuCountPerNode)
				return &v
			}
			return nil
		}(),
		State:     actualState,
		CreatedAt: createdAt,
		LastSeen:  lastSeen,
		UpdatedAt: updatedAt,
		// Node constraints and additional properties are not available from JobManager
		NodeConstraints: nil,
	}

	if job.ApsSpec.IsSet() && job.ApsSpec.Get() != nil {
		jobPb.RecoveryConfig = apsSpecOutputToRecoveryConfig(*job.ApsSpec.Get())
	}

	// Transform scheduler metadata
	if job.SchedulerMetadata != nil {
		var err error
		jobPb.SchedulerMetadata, err = structpb.NewStruct(job.SchedulerMetadata)
		if err != nil {
			// Log error but continue - scheduler metadata is optional
			logger.Warn("failed to convert scheduler metadata", zap.Error(err))
		}
	}

	return jobPb, nil
}

// ListAdvice implements JobSource.
func (j *JobClientSource) ListAdvice(ctx context.Context, req *v1.ListAdviceRequest) (*v1.ListAdviceResponse, error) {
	// Set up pagination
	var pagination *jobmanager.TokenPagination
	var opts []jobmanager.GetAdviceOption

	pageToken := ""
	if req.Page != nil && *req.Page != "" {
		pageToken = *req.Page
	}

	limit := 100
	if req.Limit > 0 {
		limit = int(req.Limit)
	}

	pagination = &jobmanager.TokenPagination{
		Token: pageToken,
		Limit: int32(limit),
	}

	opts = append(opts, jobmanager.WithAdvicePagination(pagination))
	if req.GetOnlyActive() {
		opts = append(opts, jobmanager.WithOnlyActiveAdvice())
	}

	// Call job manager to get advice
	adviceResp, err := j.jobManager.GetAdvice(ctx, opts...)
	if err != nil {
		return nil, err
	}

	// Transform job manager advice to protobuf advice
	pbAdvice := make([]*v1.Advice, 0, len(adviceResp.Advice))
	for _, jmAdvice := range adviceResp.Advice {
		pbAdv, err := transformAdviceToProto(&jmAdvice)
		if err != nil {
			return nil, err
		}
		pbAdvice = append(pbAdvice, pbAdv)
	}

	// Handle next page token
	var nextPageToken *string
	if adviceResp.NextPageToken.IsSet() {
		tokenPtr := adviceResp.NextPageToken.Get()
		if tokenPtr != nil {
			nextPageToken = tokenPtr
		}
	}

	return &v1.ListAdviceResponse{
		Advice:        pbAdvice,
		Count:         adviceResp.ItemCount,
		NextPageToken: nextPageToken,
	}, nil
}

// transformAdviceToProto converts job manager Advice to protobuf Advice
func transformAdviceToProto(jmAdvice *jobmanager.AdviceOutput) (*v1.Advice, error) {
	pbAdvice := &v1.Advice{
		HeimdallJobId: jmAdvice.Id,
		JobId:         jmAdvice.JobId,
		AttemptIndex:  jmAdvice.RestartCount,
		Generation:    int64(jmAdvice.GetGeneration()),
	}

	// Transform advice recommendation
	pbRecommendation := &v1.AdviceRecommendation{}
	if jmAdvice.Advice.State.IsSet() {
		statePtr := jmAdvice.Advice.State.Get()
		if statePtr != nil {
			// Map string state to AdviceState enum
			state := mapStateToAdviceState(string(*statePtr))
			pbRecommendation.State = &state
		}
	}

	if jmAdvice.Advice.NodeConstraints.IsSet() {
		ncPtr := jmAdvice.Advice.NodeConstraints.Get()
		if ncPtr != nil {
			pbRecommendation.NodeConstraints = &v1.NodeConstraints{
				Exclusions: ncPtr.Exclusions,
				Inclusions: ncPtr.Inclusions,
			}
		}
	}
	pbAdvice.Advice = pbRecommendation

	// Transform issues
	if len(jmAdvice.Issues) > 0 {
		pbAdvice.Issues = make([]*v1.Issue, 0, len(jmAdvice.Issues))
		for _, jmIssue := range jmAdvice.Issues {
			pbIssue := &v1.Issue{
				IssueType:        jmIssue.IssueType,
				Timestamp:        timestamppb.New(jmIssue.Timestamp),
				RelevantLogLines: jmIssue.RelevantLogLines,
			}
			if jmIssue.DetectedBy.IsSet() {
				detectedByPtr := jmIssue.DetectedBy.Get()
				if detectedByPtr != nil {
					pbIssue.DetectedBy = *detectedByPtr
				}
			}

			// Transform remediation actions
			if len(jmIssue.RemediationActions) > 0 {
				pbIssue.RemediationActions = make([]*v1.RemediationAction, 0, len(jmIssue.RemediationActions))
				for _, jmAction := range jmIssue.RemediationActions {
					pbAction := &v1.RemediationAction{
						ActionType: jmAction.ActionType,
						Timestamp:  timestamppb.New(jmAction.Timestamp),
						Reason:     jmAction.Reason,
					}

					// Transform resource details
					if len(jmAction.ResourceDetails) > 0 {
						pbAction.ResourceDetails = make([]*v1.ResourceDetail, 0, len(jmAction.ResourceDetails))
						for _, jmDetail := range jmAction.ResourceDetails {
							pbDetail := &v1.ResourceDetail{
								ResourceType: jmDetail.ResourceType,
								ResourceId:   jmDetail.ResourceId,
							}
							if jmDetail.Details.IsSet() {
								detailsPtr := jmDetail.Details.Get()
								if detailsPtr != nil {
									pbDetail.Details = detailsPtr
								}
							}
							pbAction.ResourceDetails = append(pbAction.ResourceDetails, pbDetail)
						}
					}

					pbIssue.RemediationActions = append(pbIssue.RemediationActions, pbAction)
				}
			}

			pbAdvice.Issues = append(pbAdvice.Issues, pbIssue)
		}
	}

	// Transform timestamps
	if jmAdvice.CreatedAt.IsSet() {
		createdTime := jmAdvice.CreatedAt.Get()
		if createdTime != nil {
			pbAdvice.CreatedAt = timestamppb.New(*createdTime)
		}
	}

	if jmAdvice.UpdatedAt.IsSet() {
		updatedTime := jmAdvice.UpdatedAt.Get()
		if updatedTime != nil {
			pbAdvice.UpdatedAt = timestamppb.New(*updatedTime)
		}
	}
	if jmAdvice.DesiredStateChangedAt.IsSet() {
		desiredStateChangedTime := jmAdvice.DesiredStateChangedAt.Get()
		if desiredStateChangedTime != nil {
			pbAdvice.DesiredStateChangedAt = timestamppb.New(*desiredStateChangedTime)
		}
	}

	if reconciliationStatus := jmAdvice.ReconciliationStatus.Get(); reconciliationStatus != nil {
		pbAdvice.ReconciliationStatus = transformAdviceReconciliationStatusToProto(reconciliationStatus)
	}

	// Transform scheduler metadata
	if jmAdvice.SchedulerMetadata != nil {
		pbAdvice.SchedulerMetadata, _ = structpb.NewStruct(jmAdvice.SchedulerMetadata)
		// Ignore error - scheduler metadata is optional
	}

	if recoveryContext, ok := jmAdvice.GetRecoveryContextOk(); ok && recoveryContext != nil {
		pbRecoveryContext, err := recoveryContextToProto(recoveryContext)
		if err != nil {
			return nil, err
		}
		pbAdvice.RecoveryContext = pbRecoveryContext
	}

	return pbAdvice, nil
}

// GetRecoverySequence implements JobSource.
func (j *JobClientSource) GetRecoverySequence(ctx context.Context, req *v1.GetRecoverySequenceRequest) (*v1.GetRecoverySequenceResponse, error) {
	operation, err := recoveryOperationToJobManager(req.Operation)
	if err != nil {
		return nil, err
	}

	response, err := j.jobManager.GetRecoverySequence(ctx, req.SchedulerSystem, req.SchedulerScope, req.JobId, req.SourceAttemptIndex, operation)
	if err != nil {
		return nil, err
	}
	recoveryContext, err := recoveryContextToProto(&response.RecoveryContext)
	if err != nil {
		return nil, err
	}

	return &v1.GetRecoverySequenceResponse{RecoveryContext: recoveryContext}, nil
}

// RecordRecoveryMilestone implements JobSource.
func (j *JobClientSource) RecordRecoveryMilestone(ctx context.Context, req *v1.RecordRecoveryMilestoneRequest) (*v1.RecordRecoveryMilestoneResponse, error) {
	operation, err := recoveryOperationToJobManager(req.Operation)
	if err != nil {
		return nil, err
	}
	action, err := recoverySchedulerActionToJobManager(req.Action)
	if err != nil {
		return nil, err
	}
	outcome, err := recoverySchedulerActionOutcomeToJobManager(req.Outcome)
	if err != nil {
		return nil, err
	}

	input := jobmanagerBindings.NewRecordRecoveryMilestoneRequest(
		req.SequenceId,
		req.IdempotencyKey,
		operation,
		action,
		outcome,
		req.GetCompletesSequence(),
		req.CompletedAt.AsTime(),
	)
	response, err := j.jobManager.RecordRecoveryMilestone(ctx, input)
	if err != nil {
		return nil, err
	}

	pbResponse := &v1.RecordRecoveryMilestoneResponse{
		Duplicate:         response.Duplicate,
		SequenceCompleted: response.SequenceCompleted,
	}
	if duration, ok := response.GetEndToEndDurationSecondsOk(); ok && duration != nil {
		value := float64(*duration)
		pbResponse.EndToEndDurationSeconds = &value
	}
	return pbResponse, nil
}

func recoveryContextToProto(recoveryContext *jobmanagerBindings.RecoveryContext) (*v1.RecoveryContext, error) {
	operation, err := recoveryOperationFromJobManager(recoveryContext.Operation)
	if err != nil {
		return nil, err
	}
	pbContext := &v1.RecoveryContext{
		SequenceId:  recoveryContext.SequenceId,
		Traceparent: recoveryContext.Traceparent,
		TriggeredAt: timestamppb.New(recoveryContext.TriggeredAt),
		Operation:   operation,
	}
	if tracestate, ok := recoveryContext.GetTracestateOk(); ok && tracestate != nil {
		pbContext.Tracestate = tracestate
	}
	return pbContext, nil
}

func recoveryOperationToJobManager(operation v1.RecoveryOperation) (jobmanagerBindings.RecoveryOperation, error) {
	switch operation {
	case v1.RecoveryOperation_RECOVERY_OPERATION_RESTART:
		return jobmanagerBindings.RECOVERYOPERATION_RESTART, nil
	case v1.RecoveryOperation_RECOVERY_OPERATION_TERMINATE:
		return jobmanagerBindings.RECOVERYOPERATION_TERMINATE, nil
	case v1.RecoveryOperation_RECOVERY_OPERATION_DAG_RELEASE:
		return jobmanagerBindings.RECOVERYOPERATION_DAG_RELEASE, nil
	default:
		return "", status.Error(codes.InvalidArgument, "unsupported recovery operation")
	}
}

func recoveryOperationFromJobManager(operation jobmanagerBindings.RecoveryOperation) (v1.RecoveryOperation, error) {
	switch operation {
	case jobmanagerBindings.RECOVERYOPERATION_RESTART:
		return v1.RecoveryOperation_RECOVERY_OPERATION_RESTART, nil
	case jobmanagerBindings.RECOVERYOPERATION_TERMINATE:
		return v1.RecoveryOperation_RECOVERY_OPERATION_TERMINATE, nil
	case jobmanagerBindings.RECOVERYOPERATION_DAG_RELEASE:
		return v1.RecoveryOperation_RECOVERY_OPERATION_DAG_RELEASE, nil
	default:
		return v1.RecoveryOperation_RECOVERY_OPERATION_UNSPECIFIED, status.Error(codes.Internal, "job manager returned an unsupported recovery operation")
	}
}

func transformAdviceReconciliationStatusToProto(jmStatus *jobmanagerBindings.AdviceReconciliationStatus) *v1.AdviceReconciliationStatus {
	if jmStatus == nil {
		return nil
	}

	result := &v1.AdviceReconciliationStatus{
		ObservedGeneration: int64(jmStatus.ObservedGeneration),
		Disposition:        mapReconciliationDispositionToProto(jmStatus.Disposition),
		Reason:             mapReconciliationReasonToProto(jmStatus.Reason),
		ObservedAt:         timestamppb.New(jmStatus.ObservedAt),
		TransitionedAt:     timestamppb.New(jmStatus.TransitionedAt),
	}
	if diagnosticMessage, ok := jmStatus.GetDiagnosticMessageOk(); ok && diagnosticMessage != nil {
		result.DiagnosticMessage = diagnosticMessage
	}
	return result
}

func mapReconciliationDispositionToProto(disposition jobmanagerBindings.AdviceReconciliationDisposition) v1.AdviceReconciliationDisposition {
	switch disposition {
	case jobmanagerBindings.ADVICERECONCILIATIONDISPOSITION_PENDING:
		return v1.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_PENDING
	case jobmanagerBindings.ADVICERECONCILIATIONDISPOSITION_CONVERGED:
		return v1.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED
	case jobmanagerBindings.ADVICERECONCILIATIONDISPOSITION_ABANDONED:
		return v1.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED
	default:
		return v1.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_UNSPECIFIED
	}
}

func mapReconciliationReasonToProto(reason jobmanagerBindings.AdviceReconciliationReason) v1.AdviceReconciliationReason {
	switch reason {
	case jobmanagerBindings.ADVICERECONCILIATIONREASON_DESIRED_STATE_OBSERVED:
		return v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED
	case jobmanagerBindings.ADVICERECONCILIATIONREASON_ALREADY_SATISFIED:
		return v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_ALREADY_SATISFIED
	case jobmanagerBindings.ADVICERECONCILIATIONREASON_ADVICE_EXPIRED:
		return v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_ADVICE_EXPIRED
	case jobmanagerBindings.ADVICERECONCILIATIONREASON_STALE_ATTEMPT:
		return v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_STALE_ATTEMPT
	case jobmanagerBindings.ADVICERECONCILIATIONREASON_TARGET_STATE_FINAL:
		return v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_TARGET_STATE_FINAL
	case jobmanagerBindings.ADVICERECONCILIATIONREASON_SUCCESSOR_ALREADY_RUNNING:
		return v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_SUCCESSOR_ALREADY_RUNNING
	case jobmanagerBindings.ADVICERECONCILIATIONREASON_INVALID_RECOMMENDATION:
		return v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_INVALID_RECOMMENDATION
	case jobmanagerBindings.ADVICERECONCILIATIONREASON_UNSUPPORTED_RECOMMENDATION:
		return v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_UNSUPPORTED_RECOMMENDATION
	default:
		return v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_UNSPECIFIED
	}
}

// UpdateAdviceReconciliationStatus implements JobSource.
func (j *JobClientSource) UpdateAdviceReconciliationStatus(ctx context.Context, req *v1.UpdateAdviceReconciliationStatusRequest) (*v1.UpdateAdviceReconciliationStatusResponse, error) {
	jmStatus := transformAdviceReconciliationStatusToJobManager(req.GetStatus())
	persisted, err := j.jobManager.UpdateAdviceReconciliationStatus(ctx, req.GetHeimdallJobId(), jmStatus)
	if err != nil {
		return nil, err
	}

	return &v1.UpdateAdviceReconciliationStatusResponse{
		Status: transformAdviceReconciliationStatusToProto(persisted),
	}, nil
}

func transformAdviceReconciliationStatusToJobManager(status *v1.AdviceReconciliationStatus) jobmanagerBindings.AdviceReconciliationStatus {
	result := jobmanagerBindings.AdviceReconciliationStatus{
		ObservedGeneration: int32(status.GetObservedGeneration()),
		Disposition:        mapReconciliationDispositionToJobManager(status.GetDisposition()),
		Reason:             mapReconciliationReasonToJobManager(status.GetReason()),
		ObservedAt:         status.GetObservedAt().AsTime(),
		TransitionedAt:     status.GetTransitionedAt().AsTime(),
	}
	if status.DiagnosticMessage != nil {
		result.SetDiagnosticMessage(status.GetDiagnosticMessage())
	}

	return result
}

func mapReconciliationDispositionToJobManager(disposition v1.AdviceReconciliationDisposition) jobmanagerBindings.AdviceReconciliationDisposition {
	switch disposition {
	case v1.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_PENDING:
		return jobmanagerBindings.ADVICERECONCILIATIONDISPOSITION_PENDING
	case v1.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED:
		return jobmanagerBindings.ADVICERECONCILIATIONDISPOSITION_CONVERGED
	case v1.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED:
		return jobmanagerBindings.ADVICERECONCILIATIONDISPOSITION_ABANDONED
	default:
		return ""
	}
}

func mapReconciliationReasonToJobManager(reason v1.AdviceReconciliationReason) jobmanagerBindings.AdviceReconciliationReason {
	switch reason {
	case v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED:
		return jobmanagerBindings.ADVICERECONCILIATIONREASON_DESIRED_STATE_OBSERVED
	case v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_ALREADY_SATISFIED:
		return jobmanagerBindings.ADVICERECONCILIATIONREASON_ALREADY_SATISFIED
	case v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_ADVICE_EXPIRED:
		return jobmanagerBindings.ADVICERECONCILIATIONREASON_ADVICE_EXPIRED
	case v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_STALE_ATTEMPT:
		return jobmanagerBindings.ADVICERECONCILIATIONREASON_STALE_ATTEMPT
	case v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_TARGET_STATE_FINAL:
		return jobmanagerBindings.ADVICERECONCILIATIONREASON_TARGET_STATE_FINAL
	case v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_SUCCESSOR_ALREADY_RUNNING:
		return jobmanagerBindings.ADVICERECONCILIATIONREASON_SUCCESSOR_ALREADY_RUNNING
	case v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_INVALID_RECOMMENDATION:
		return jobmanagerBindings.ADVICERECONCILIATIONREASON_INVALID_RECOMMENDATION
	case v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_UNSUPPORTED_RECOMMENDATION:
		return jobmanagerBindings.ADVICERECONCILIATIONREASON_UNSUPPORTED_RECOMMENDATION
	default:
		return ""
	}
}

func recoverySchedulerActionToJobManager(action v1.RecoverySchedulerAction) (jobmanagerBindings.RecoverySchedulerAction, error) {
	switch action {
	case v1.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_REQUEUE_HOLD:
		return jobmanagerBindings.RECOVERYSCHEDULERACTION_SLURM_REQUEUE_HOLD, nil
	case v1.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_UPDATE_CONSTRAINTS:
		return jobmanagerBindings.RECOVERYSCHEDULERACTION_SLURM_UPDATE_CONSTRAINTS, nil
	case v1.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_RELEASE:
		return jobmanagerBindings.RECOVERYSCHEDULERACTION_SLURM_RELEASE, nil
	case v1.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_CANCEL:
		return jobmanagerBindings.RECOVERYSCHEDULERACTION_SLURM_CANCEL, nil
	default:
		return "", status.Error(codes.InvalidArgument, "unsupported recovery scheduler action")
	}
}

func recoverySchedulerActionOutcomeToJobManager(outcome v1.RecoverySchedulerActionOutcome) (jobmanagerBindings.RecoverySchedulerActionOutcome, error) {
	switch outcome {
	case v1.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_PLANNED:
		return jobmanagerBindings.RECOVERYSCHEDULERACTIONOUTCOME_PLANNED, nil
	case v1.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_SUCCESS:
		return jobmanagerBindings.RECOVERYSCHEDULERACTIONOUTCOME_SUCCESS, nil
	case v1.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_ERROR:
		return jobmanagerBindings.RECOVERYSCHEDULERACTIONOUTCOME_ERROR, nil
	case v1.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_ALREADY_SATISFIED:
		return jobmanagerBindings.RECOVERYSCHEDULERACTIONOUTCOME_ALREADY_SATISFIED, nil
	default:
		return "", status.Error(codes.InvalidArgument, "unsupported recovery scheduler action outcome")
	}
}

// mapStateToAdviceState maps string state to protobuf AdviceState enum
func mapStateToAdviceState(state string) v1.AdviceState {
	switch state {
	case "Restarted":
		return v1.AdviceState_ADVICE_STATE_RESTARTED
	case "Terminated":
		return v1.AdviceState_ADVICE_STATE_TERMINATED
	case "Released":
		return v1.AdviceState_ADVICE_STATE_RELEASED
	default:
		return v1.AdviceState_ADVICE_STATE_UNSPECIFIED
	}
}
