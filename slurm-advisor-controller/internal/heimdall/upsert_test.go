// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package heimdall

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

func TestBuildCreateOrUpdateJobRequestUsesStableHeimdallID(t *testing.T) {
	submitTime := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	startTime := time.Date(2026, 6, 24, 12, 1, 0, 0, time.UTC)
	jobName := "train-llm"
	heimdallJobID := "7afd1bcf-df8f-5b70-a845-571e8b2e7e16"
	metadata := structpb.NewStructValue(mustStruct(t, map[string]any{
		"environment":           "prod",
		"managed_by":            cluster.ManagedBySlurmAdvisor,
		"scheduler":             cluster.SchedulerSlurm,
		"slurm_attempt_index":   float64(2),
		"slurm_cluster":         "cluster-a",
		"slurm_command":         "/workspace/train.sh",
		"slurm_job_id":          "12345",
		"slurm_node_count":      float64(2),
		"slurm_partition":       "normal",
		"slurm_raw_job_id":      "12345.batch",
		"slurm_reason":          "Resources",
		"slurm_start_time":      "2026-06-24T12:01:00Z",
		"slurm_state":           "RUNNING",
		"slurm_submit_time":     "2026-06-24T12:00:00Z",
		"slurm_total_cpu_count": float64(16),
		"slurm_total_gpu_count": float64(8),
		"slurm_total_memory_mb": float64(64000),
		"slurm_user":            "alice",
	}))
	want := &pb.CreateOrUpdateJobRequest{
		HeimdallJobId:  &heimdallJobID,
		JobId:          "12345",
		WorkflowId:     "workflow-a",
		AttemptIndex:   2,
		JobName:        &jobName,
		AllocatedNodes: []string{"node1", "node2"},
		NodeConstraints: &pb.NodeConstraints{
			Exclusions: []string{"node8", "node9"},
			Inclusions: []string{"node3", "node4"},
		},
		State:             pb.JobStatus_JOB_STATUS_RUNNING,
		SchedulerMetadata: metadata.GetStructValue(),
	}

	got, err := BuildCreateOrUpdateJobRequest(cluster.JobExecutionRecord{
		JobID:        "12345",
		RawJobID:     "12345.batch",
		AttemptIndex: 2,
		JobName:      jobName,
		WorkflowID:   "workflow-a",
		State:        cluster.JobStateRunning,
		RawState:     "RUNNING",
		Reason:       "Resources",
		SchedulerMetadata: map[string]any{
			"slurm_reason": "Resources",
		},
		Cluster:        "cluster-a",
		Partition:      "normal",
		User:           "alice",
		Command:        "/workspace/train.sh",
		NodeCount:      2,
		TotalCPUCount:  16,
		TotalGPUCount:  8,
		TotalMemoryMB:  64000,
		SubmitTime:     &submitTime,
		StartTime:      &startTime,
		AllocatedNodes: []string{"node2", "node1"},
		NodeConstraints: cluster.NodeConstraints{
			Inclusions: []string{"node4", "node3"},
			Exclusions: []string{"node9", "node8"},
		},
	}, UpsertRequestOptions{Environment: "prod"})
	if err != nil {
		t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
	}

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("BuildCreateOrUpdateJobRequest() mismatch (-want +got):\n%s", diff)
	}
}

func TestMapJobStateReportsRequeuedAttemptAsFailed(t *testing.T) {
	if got := mapJobState(cluster.JobStateRequeued); got != pb.JobStatus_JOB_STATUS_FAILED {
		t.Fatalf("mapJobState(requeued) = %q, want %q", got, pb.JobStatus_JOB_STATUS_FAILED)
	}
}

func TestBuildCreateOrUpdateJobRequestDoesNotPublishInternalReason(t *testing.T) {
	got, err := BuildCreateOrUpdateJobRequest(cluster.JobExecutionRecord{
		JobID:      "12345",
		WorkflowID: "workflow-a",
		Reason:     "NonZeroExitCode",
	}, UpsertRequestOptions{})
	if err != nil {
		t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
	}
	if value, exists := got.GetSchedulerMetadata().AsMap()["slurm_reason"]; exists {
		t.Fatalf("slurm_reason = %#v, want omitted without explicit scheduler metadata", value)
	}
}

func TestBuildCreateOrUpdateJobRequestPreservesAPSEnvironment(t *testing.T) {
	environment := "QA/Canary 01"
	got, err := BuildCreateOrUpdateJobRequest(cluster.JobExecutionRecord{
		JobID: "12345", WorkflowID: "workflow-a",
		RecoveryConfig: &cluster.RecoveryConfig{Environment: &environment},
	}, UpsertRequestOptions{Environment: "controller-backend"})
	if err != nil {
		t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
	}
	if got.GetRecoveryConfig().Environment == nil || *got.GetRecoveryConfig().Environment != environment {
		t.Fatalf("RecoveryConfig.Environment = %v, want %q", got.GetRecoveryConfig().Environment, environment)
	}
	if got.GetSchedulerMetadata().GetFields()["environment"].GetStringValue() != "controller-backend" {
		t.Fatal("scheduler metadata environment no longer identifies the controller backend")
	}
}

func TestUpsertRequestOptionsOnlyCarriesEnvironmentMetadata(t *testing.T) {
	optionType := reflect.TypeOf(UpsertRequestOptions{})
	if _, ok := optionType.FieldByName("ClusterID"); ok {
		t.Fatal("UpsertRequestOptions exposes ClusterID, want only environment metadata")
	}
	if _, ok := optionType.FieldByName("ControllerInstanceID"); ok {
		t.Fatal("UpsertRequestOptions exposes ControllerInstanceID, want only environment metadata")
	}
	if _, ok := optionType.FieldByName("Environment"); !ok {
		t.Fatal("UpsertRequestOptions does not expose Environment")
	}
}

func TestBuildCreateOrUpdateJobRequestConvertsMetadataLists(t *testing.T) {
	got, err := BuildCreateOrUpdateJobRequest(cluster.JobExecutionRecord{
		JobID:       "12345",
		JobName:     "job-name-fallback",
		WorkflowID:  "workflow-metadata",
		FailedNodes: []string{"node9", "node8"},
		SchedulerMetadata: map[string]any{
			"custom_labels": []string{"alpha", "beta"},
		},
	}, UpsertRequestOptions{})
	if err != nil {
		t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
	}

	if got.GetWorkflowId() != "workflow-metadata" {
		t.Fatalf("WorkflowId = %q, want workflow-metadata", got.GetWorkflowId())
	}
	fields := got.GetSchedulerMetadata().GetFields()
	failedNodes := fields["slurm_failed_nodes"].GetListValue().GetValues()
	if len(failedNodes) != 2 || failedNodes[0].GetStringValue() != "node8" || failedNodes[1].GetStringValue() != "node9" {
		t.Fatalf("slurm_failed_nodes metadata = %v, want node8,node9", fields["slurm_failed_nodes"])
	}
	customLabels := fields["custom_labels"].GetListValue().GetValues()
	if len(customLabels) != 2 || customLabels[0].GetStringValue() != "alpha" || customLabels[1].GetStringValue() != "beta" {
		t.Fatalf("custom_labels metadata = %v, want alpha,beta", fields["custom_labels"])
	}
}

func TestBuildCreateOrUpdateJobRequestIncludesArrayEndTimeAndFiltersNodes(t *testing.T) {
	endTime := time.Date(2026, 6, 24, 12, 2, 0, 0, time.UTC)

	got, err := BuildCreateOrUpdateJobRequest(cluster.JobExecutionRecord{
		JobID:       "12345_7",
		ArrayJobID:  "12345",
		ArrayTaskID: "7",
		WorkflowID:  "array-workflow",
		EndTime:     &endTime,
		AllocatedNodes: []string{
			"node2",
			"",
			"node1",
			"node1",
		},
		NodeConstraints: cluster.NodeConstraints{
			Inclusions: []string{"node4", "", "node3", "node3"},
			Exclusions: []string{"node9", "", "node8", "node9"},
		},
	}, UpsertRequestOptions{})
	if err != nil {
		t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
	}

	if diff := cmp.Diff([]string{"node1", "node2"}, got.GetAllocatedNodes()); diff != "" {
		t.Fatalf("AllocatedNodes mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"node3", "node4"}, got.GetNodeConstraints().GetInclusions()); diff != "" {
		t.Fatalf("Inclusions mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"node8", "node9"}, got.GetNodeConstraints().GetExclusions()); diff != "" {
		t.Fatalf("Exclusions mismatch (-want +got):\n%s", diff)
	}

	fields := got.GetSchedulerMetadata().GetFields()
	if fields["slurm_array_job_id"].GetStringValue() != "12345" {
		t.Fatalf("slurm_array_job_id metadata = %v, want 12345", fields["slurm_array_job_id"])
	}
	if fields["slurm_array_task_id"].GetStringValue() != "7" {
		t.Fatalf("slurm_array_task_id metadata = %v, want 7", fields["slurm_array_task_id"])
	}
	if fields["slurm_end_time"].GetStringValue() != "2026-06-24T12:02:00Z" {
		t.Fatalf("slurm_end_time metadata = %v, want 2026-06-24T12:02:00Z", fields["slurm_end_time"])
	}
	if got.GetWorkflowId() != "array-workflow" {
		t.Fatalf("WorkflowId = %q, want explicit array workflow", got.GetWorkflowId())
	}
}

func TestBuildCreateOrUpdateJobRequestRejectsInvalidWorkflowID(t *testing.T) {
	tests := map[string]string{
		"missing":       "",
		"invalid UTF-8": string([]byte{'w', 0xff}),
		"oversized":     strings.Repeat("\U0001f680", 257),
	}

	for name, workflowID := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := BuildCreateOrUpdateJobRequest(cluster.JobExecutionRecord{
				JobID:      "12345",
				JobName:    "must-not-be-used-as-fallback",
				WorkflowID: workflowID,
			}, UpsertRequestOptions{})
			if !errors.Is(err, ErrInvalidWorkflowID) {
				t.Fatalf("BuildCreateOrUpdateJobRequest() error = %v, want invalid workflow ID", err)
			}
			if got != nil {
				t.Fatalf("BuildCreateOrUpdateJobRequest() = %#v, want nil request", got)
			}
		})
	}
}

func TestBuildCreateOrUpdateJobRequestPreservesValidWorkflowIDVerbatim(t *testing.T) {
	values := []string{
		"  workflow/e\u0301:?#[reserved]/\ufffd  ",
		strings.Repeat("\U0001f680", 256),
	}

	for _, workflowID := range values {
		got, err := BuildCreateOrUpdateJobRequest(cluster.JobExecutionRecord{
			JobID:      "12345",
			WorkflowID: workflowID,
		}, UpsertRequestOptions{})
		if err != nil {
			t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
		}
		if got.GetWorkflowId() != workflowID {
			t.Fatalf("WorkflowId = %q, want verbatim %q", got.GetWorkflowId(), workflowID)
		}
	}
}

func TestBuildCreateOrUpdateJobRequestDoesNotPopulateGpuOrAdditionalProperties(t *testing.T) {
	got, err := BuildCreateOrUpdateJobRequest(cluster.JobExecutionRecord{
		JobID:        "12345",
		AttemptIndex: 0,
		WorkflowID:   "workflow-a",
		State:        cluster.JobStateRunning,
	}, UpsertRequestOptions{})
	if err != nil {
		t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
	}

	if got.GetGpuCountPerNode() != 0 {
		t.Fatalf("GpuCountPerNode = %d, want 0", got.GetGpuCountPerNode())
	}
	additionalProperties := got.ProtoReflect().Descriptor().Fields().ByName("additional_properties")
	if got.ProtoReflect().Has(additionalProperties) {
		t.Fatalf("AdditionalProperties is populated, want nil")
	}
}

func TestBuildCreateOrUpdateJobRequestMapsRecoveryConfig(t *testing.T) {
	maxRequeues := int32(3)
	requeueOnCompletion := true
	sharedLogPaths := true
	logBasedCrashEnabled := false
	logBasedTimeoutEnabled := true
	timeoutThreshold := int32(30)

	got, err := BuildCreateOrUpdateJobRequest(cluster.JobExecutionRecord{
		JobID:        "12345",
		AttemptIndex: 0,
		WorkflowID:   "workflow-a",
		State:        cluster.JobStateRunning,
		RecoveryConfig: &cluster.RecoveryConfig{
			AutoResumeMode:                   cluster.AutoResumeModeRequeue,
			MaxRequeueTimes:                  &maxRequeues,
			RequeueOnCompletion:              &requeueOnCompletion,
			LogFilePathsSharedAcrossAttempts: &sharedLogPaths,
			AnomalyDetection: &cluster.AnomalyDetectionConfig{
				LogBasedCrashEnabled:                &logBasedCrashEnabled,
				LogBasedTimeoutEnabled:              &logBasedTimeoutEnabled,
				LogBasedTimeoutThresholdPretraining: &timeoutThreshold,
			},
		},
	}, UpsertRequestOptions{})
	if err != nil {
		t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
	}

	config := got.GetRecoveryConfig()
	if config == nil {
		t.Fatal("RecoveryConfig = nil, want config")
	}
	if config.AutoResumeMode != pb.JobRecoveryConfig_AUTO_RESUME_MODE_REQUEUE {
		t.Fatalf("AutoResumeMode = %s, want REQUEUE", config.AutoResumeMode)
	}
	if config.MaxRequeueTimes == nil || config.GetMaxRequeueTimes() != 3 {
		t.Fatalf("MaxRequeueTimes = %v, want 3", config.MaxRequeueTimes)
	}
	if config.RequeueOnCompletion == nil || !config.GetRequeueOnCompletion() {
		t.Fatalf("RequeueOnCompletion = %v, want true", config.RequeueOnCompletion)
	}
	if config.LogFilePathsSharedAcrossAttempts == nil || !config.GetLogFilePathsSharedAcrossAttempts() {
		t.Fatalf(
			"LogFilePathsSharedAcrossAttempts = %v, want true",
			config.LogFilePathsSharedAcrossAttempts,
		)
	}
	detection := config.GetAnomalyDetectionConfig()
	if detection == nil || detection.LogBasedCrashEnabled == nil || detection.GetLogBasedCrashEnabled() {
		t.Fatalf("AnomalyDetectionConfig = %#v, want log-based crash false with presence", detection)
	}
	if detection.LogBasedTimeoutEnabled == nil || !detection.GetLogBasedTimeoutEnabled() {
		t.Fatalf("LogBasedTimeoutEnabled = %v, want true", detection.LogBasedTimeoutEnabled)
	}
	if detection.GetLogBasedTimeoutThresholdPretraining() != 30 {
		t.Fatalf("LogBasedTimeoutThresholdPretraining = %d, want 30", detection.GetLogBasedTimeoutThresholdPretraining())
	}
}

func TestBuildCreateOrUpdateJobRequestMapsDAGPolicy(t *testing.T) {
	got, err := BuildCreateOrUpdateJobRequest(cluster.JobExecutionRecord{
		JobID:        "12345",
		WorkflowID:   "workflow-a",
		DAGChild:     true,
		AttemptIndex: 0,
		RecoveryConfig: &cluster.RecoveryConfig{
			DAGDependencies: []cluster.DAGDependency{
				{TargetType: cluster.DAGTargetTypeID, TargetIdentifier: "111", Trigger: cluster.DAGTriggerConditionCompleted},
				{TargetType: cluster.DAGTargetTypeName, TargetIdentifier: "prepare", Trigger: cluster.DAGTriggerConditionCompleted},
			},
			DAGAction: cluster.DAGActionRelease,
		},
	}, UpsertRequestOptions{})
	if err != nil {
		t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
	}

	config := got.GetRecoveryConfig()
	if config == nil {
		t.Fatal("RecoveryConfig = nil, want DAG policy")
	}
	if config.GetDagAction() != pb.JobRecoveryConfig_DAG_ACTION_RELEASE {
		t.Fatalf("DagAction = %v, want RELEASE", config.GetDagAction())
	}
	dependencies := config.GetDagDependencies()
	if len(dependencies) != 2 {
		t.Fatalf("len(DagDependencies) = %d, want 2", len(dependencies))
	}
	if dependencies[0].GetJobId() != "111" || dependencies[0].GetTrigger() != pb.JobDagDependency_TRIGGER_CONDITION_COMPLETED {
		t.Fatalf("first dependency = %#v, want completed ID target 111", dependencies[0])
	}
	if dependencies[1].GetJobName() != "prepare" || dependencies[1].GetTrigger() != pb.JobDagDependency_TRIGGER_CONDITION_COMPLETED {
		t.Fatalf("second dependency = %#v, want completed name target prepare", dependencies[1])
	}
}

func TestMapDAGPolicyOmitsPartialOrUnsupportedPolicy(t *testing.T) {
	tests := map[string]*cluster.RecoveryConfig{
		"unsupported target": {
			DAGDependencies: []cluster.DAGDependency{
				{TargetType: cluster.DAGTargetTypeID, TargetIdentifier: "111", Trigger: cluster.DAGTriggerConditionCompleted},
				{TargetType: cluster.DAGTargetType("uuid"), TargetIdentifier: "222", Trigger: cluster.DAGTriggerConditionCompleted},
			},
			DAGAction: cluster.DAGActionRelease,
		},
		"unsupported trigger": {
			DAGDependencies: []cluster.DAGDependency{
				{TargetType: cluster.DAGTargetTypeID, TargetIdentifier: "111", Trigger: cluster.DAGTriggerCondition("failed")},
			},
			DAGAction: cluster.DAGActionRelease,
		},
		"unsupported action": {
			DAGDependencies: []cluster.DAGDependency{
				{TargetType: cluster.DAGTargetTypeID, TargetIdentifier: "111", Trigger: cluster.DAGTriggerConditionCompleted},
			},
			DAGAction: cluster.DAGAction("cancel"),
		},
	}

	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			dependencies, action := mapDAGPolicy(config)
			if len(dependencies) != 0 || action != pb.JobRecoveryConfig_DAG_ACTION_UNSPECIFIED {
				t.Fatalf("mapDAGPolicy() = (%#v, %v), want complete omission", dependencies, action)
			}
		})
	}
}

func mustStruct(t *testing.T, values map[string]any) *structpb.Struct {
	t.Helper()

	out, err := structpb.NewStruct(values)
	if err != nil {
		t.Fatalf("NewStruct() failed: %v", err)
	}

	return out
}
