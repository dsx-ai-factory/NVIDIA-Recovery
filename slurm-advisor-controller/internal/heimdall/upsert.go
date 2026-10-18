// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package heimdall

import (
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

type UpsertRequestOptions struct {
	Environment string
}

const maxWorkflowIDRunes = 256

// ErrInvalidWorkflowID identifies records that do not carry an API-safe workflow identity.
var ErrInvalidWorkflowID = errors.New("job execution record has an invalid workflow ID")

func BuildCreateOrUpdateJobRequest(job cluster.JobExecutionRecord, opts UpsertRequestOptions) (*pb.CreateOrUpdateJobRequest, error) {
	if err := validateWorkflowID(job.WorkflowID); err != nil {
		return nil, err
	}
	heimdallJobID := GenerateHeimdallJobID(job.JobID, job.AttemptIndex)

	metadata, err := buildSchedulerMetadata(job, opts)
	if err != nil {
		return nil, err
	}

	req := &pb.CreateOrUpdateJobRequest{
		HeimdallJobId:  &heimdallJobID,
		JobId:          job.JobID,
		WorkflowId:     job.WorkflowID,
		AttemptIndex:   job.AttemptIndex,
		AllocatedNodes: sortedUnique(job.AllocatedNodes),
		NodeConstraints: &pb.NodeConstraints{
			Exclusions: sortedUnique(job.NodeConstraints.Exclusions),
			Inclusions: sortedUnique(job.NodeConstraints.Inclusions),
		},
		State:             mapJobState(job.State),
		SchedulerMetadata: metadata,
		RecoveryConfig:    mapRecoveryConfig(job.RecoveryConfig),
	}
	if job.JobName != "" {
		jobName := job.JobName
		req.JobName = &jobName
	}

	return req, nil
}

func mapJobState(state cluster.JobState) pb.JobStatus {
	switch state {
	case cluster.JobStatePending:
		return pb.JobStatus_JOB_STATUS_PENDING
	case cluster.JobStateRunning:
		return pb.JobStatus_JOB_STATUS_RUNNING
	case cluster.JobStateStopped:
		return pb.JobStatus_JOB_STATUS_STOPPED
	case cluster.JobStateCompleted:
		return pb.JobStatus_JOB_STATUS_COMPLETED
	case cluster.JobStateFailed, cluster.JobStateRequeued:
		return pb.JobStatus_JOB_STATUS_FAILED
	case cluster.JobStateCancelled:
		return pb.JobStatus_JOB_STATUS_CANCELLED
	case cluster.JobStateDeleted:
		return pb.JobStatus_JOB_STATUS_DELETED
	default:
		return pb.JobStatus_JOB_STATUS_UNSPECIFIED
	}
}

func mapRecoveryConfig(config *cluster.RecoveryConfig) *pb.JobRecoveryConfig {
	if config == nil {
		return nil
	}

	out := &pb.JobRecoveryConfig{
		MaxRequeueTimes:                  config.MaxRequeueTimes,
		RequeueOnCompletion:              config.RequeueOnCompletion,
		LogFilePathsSharedAcrossAttempts: config.LogFilePathsSharedAcrossAttempts,
		Environment:                      config.Environment,
	}
	if config.AnomalyDetection != nil {
		out.AnomalyDetectionConfig = &pb.JobAnomalyDetectionConfig{
			LogBasedCrashEnabled:                 config.AnomalyDetection.LogBasedCrashEnabled,
			LogBasedTimeoutEnabled:               config.AnomalyDetection.LogBasedTimeoutEnabled,
			LogBasedTimeoutThresholdPretraining:  config.AnomalyDetection.LogBasedTimeoutThresholdPretraining,
			LogBasedTimeoutThresholdTrainingloop: config.AnomalyDetection.LogBasedTimeoutThresholdTrainingloop,
		}
	}
	out.DagDependencies, out.DagAction = mapDAGPolicy(config)
	switch config.AutoResumeMode {
	case cluster.AutoResumeModeRequeue:
		out.AutoResumeMode = pb.JobRecoveryConfig_AUTO_RESUME_MODE_REQUEUE
	case cluster.AutoResumeModeSingletonDependency:
		out.AutoResumeMode = pb.JobRecoveryConfig_AUTO_RESUME_MODE_SINGLETON_DEPENDENCY
	default:
		out.AutoResumeMode = pb.JobRecoveryConfig_AUTO_RESUME_MODE_UNSPECIFIED
	}

	return out
}

func mapDAGPolicy(config *cluster.RecoveryConfig) ([]*pb.JobDagDependency, pb.JobRecoveryConfig_DagAction) {
	action := pb.JobRecoveryConfig_DAG_ACTION_UNSPECIFIED
	switch config.DAGAction {
	case cluster.DAGActionUnset:
	case cluster.DAGActionRelease:
		action = pb.JobRecoveryConfig_DAG_ACTION_RELEASE
	default:
		return nil, pb.JobRecoveryConfig_DAG_ACTION_UNSPECIFIED
	}

	dependencies := make([]*pb.JobDagDependency, 0, len(config.DAGDependencies))
	for _, dependency := range config.DAGDependencies {
		if dependency.Trigger != cluster.DAGTriggerConditionCompleted {
			return nil, pb.JobRecoveryConfig_DAG_ACTION_UNSPECIFIED
		}
		mapped := &pb.JobDagDependency{
			Trigger: pb.JobDagDependency_TRIGGER_CONDITION_COMPLETED,
		}
		switch dependency.TargetType {
		case cluster.DAGTargetTypeID:
			mapped.Target = &pb.JobDagDependency_JobId{JobId: dependency.TargetIdentifier}
		case cluster.DAGTargetTypeName:
			mapped.Target = &pb.JobDagDependency_JobName{JobName: dependency.TargetIdentifier}
		default:
			return nil, pb.JobRecoveryConfig_DAG_ACTION_UNSPECIFIED
		}
		dependencies = append(dependencies, mapped)
	}
	return dependencies, action
}

func GenerateHeimdallJobID(jobID string, attemptIndex int32) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("%s-%d", jobID, attemptIndex))).String()
}

func validateWorkflowID(workflowID string) error {
	switch {
	case workflowID == "":
		return errors.Wrap(ErrInvalidWorkflowID, "workflow ID is empty")
	case !utf8.ValidString(workflowID):
		return errors.Wrap(ErrInvalidWorkflowID, "workflow ID is not valid UTF-8")
	case utf8.RuneCountInString(workflowID) > maxWorkflowIDRunes:
		return errors.Wrap(ErrInvalidWorkflowID, "workflow ID exceeds 256 Unicode scalar values")
	default:
		return nil
	}
}

func buildSchedulerMetadata(job cluster.JobExecutionRecord, opts UpsertRequestOptions) (*structpb.Struct, error) {
	metadata := map[string]any{}
	for key, value := range job.SchedulerMetadata {
		metadata[key] = value
	}

	metadata["scheduler"] = cluster.SchedulerSlurm
	metadata["slurm_job_id"] = job.JobID
	metadata["slurm_attempt_index"] = job.AttemptIndex
	metadata["managed_by"] = cluster.ManagedBySlurmAdvisor
	if job.RawJobID != "" {
		metadata["slurm_raw_job_id"] = job.RawJobID
	}
	if job.ArrayJobID != "" {
		metadata["slurm_array_job_id"] = job.ArrayJobID
	}
	if job.ArrayTaskID != "" {
		metadata["slurm_array_task_id"] = job.ArrayTaskID
	}
	if job.RawState != "" {
		metadata["slurm_state"] = job.RawState
	}
	// Slurm conversion owns whether a reason is fresh enough to publish.
	// In particular, requeue-managed sacct observations retain Reason for
	// reconciliation while intentionally omitting it from scheduler metadata.
	if job.Cluster != "" {
		metadata["slurm_cluster"] = job.Cluster
	}
	if job.Partition != "" {
		metadata["slurm_partition"] = job.Partition
	}
	if job.User != "" {
		metadata["slurm_user"] = job.User
	}
	if job.Command != "" {
		metadata["slurm_command"] = job.Command
	}
	if job.NodeCount > 0 {
		metadata["slurm_node_count"] = job.NodeCount
	}
	if job.TotalGPUCount > 0 {
		metadata["slurm_total_gpu_count"] = job.TotalGPUCount
	}
	if job.TotalCPUCount > 0 {
		metadata["slurm_total_cpu_count"] = job.TotalCPUCount
	}
	if job.TotalMemoryMB > 0 {
		metadata["slurm_total_memory_mb"] = job.TotalMemoryMB
	}
	if job.SubmitTime != nil {
		metadata["slurm_submit_time"] = formatMetadataTime(job.SubmitTime)
	}
	if job.StartTime != nil {
		metadata["slurm_start_time"] = formatMetadataTime(job.StartTime)
	}
	if job.EndTime != nil {
		metadata["slurm_end_time"] = formatMetadataTime(job.EndTime)
	}
	if len(job.FailedNodes) > 0 {
		metadata["slurm_failed_nodes"] = sortedUnique(job.FailedNodes)
	}
	if opts.Environment != "" {
		metadata["environment"] = opts.Environment
	}

	normalized, err := normalizeMetadata(metadata)
	if err != nil {
		return nil, err
	}
	out, err := structpb.NewStruct(normalized)
	if err != nil {
		return nil, errors.Wrap(err, "build scheduler metadata")
	}

	return out, nil
}

func formatMetadataTime(value *time.Time) string {
	return value.UTC().Format(time.RFC3339)
}

func sortedUnique(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	slices.Sort(out)

	return out
}

func normalizeMetadata(metadata map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(metadata))
	for key, value := range metadata {
		normalized, err := normalizeMetadataValue(value)
		if err != nil {
			return nil, errors.Wrapf(err, "metadata %q", key)
		}
		out[key] = normalized
	}

	return out, nil
}

func normalizeMetadataValue(value any) (any, error) {
	switch typed := value.(type) {
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out, nil
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			normalized, err := normalizeMetadataValue(item)
			if err != nil {
				return nil, err
			}
			out = append(out, normalized)
		}
		return out, nil
	case map[string]any:
		return normalizeMetadata(typed)
	default:
		return value, nil
	}
}
