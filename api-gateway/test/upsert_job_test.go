// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/job"
)

// TestCreateOrUpdateJobSynthetic tests the CreateOrUpdateJob endpoint with synthetic job source
func TestCreateOrUpdateJobSynthetic(t *testing.T) {
	jobSource := job.NewJobsSyntheticSource()
	ctx := context.Background()

	// Test with custom heimdall_id (K8s CRD style)
	heimdallId := "kubeflow-pytorch-simple-test"
	jobName := "kubeflow-pytorch-simple"

	req := &pb.CreateOrUpdateJobRequest{
		HeimdallJobId: &heimdallId,
		JobName:       &jobName,
		JobId:         "kubeflow-pytorch-simple",
		WorkflowId:    "test-workflow-001",
		AttemptIndex:  0,
		AllocatedNodes: []string{
			"runai-cluster-system-0-0",
			"runai-cluster-system-0-1",
		},
		State:           pb.JobStatus_JOB_STATUS_RUNNING,
		GpuCountPerNode: 8,
	}

	resp, err := jobSource.CreateOrUpdateJob(ctx, req)
	if err != nil {
		t.Fatalf("UpsertJob failed: %v", err)
	}

	// Verify response
	if resp.Job == nil {
		t.Fatal("Expected job in response, got nil")
	}

	if resp.Job.HeimdallJobId != heimdallId {
		t.Errorf("Expected heimdall_id %s, got %s", heimdallId, resp.Job.HeimdallJobId)
	}

	if resp.Job.JobId != "kubeflow-pytorch-simple" {
		t.Errorf("Expected job_id kubeflow-pytorch-simple, got %s", resp.Job.JobId)
	}

	if resp.Job.JobName == nil || *resp.Job.JobName != jobName {
		t.Errorf("Expected job_name %s, got %v", jobName, resp.Job.JobName)
	}

	if resp.Job.WorkflowId != "test-workflow-001" {
		t.Errorf("Expected workflow_id test-workflow-001, got %s", resp.Job.WorkflowId)
	}

	if resp.Job.AttemptIndex != 0 {
		t.Errorf("Expected attempt_index 0, got %d", resp.Job.AttemptIndex)
	}

	if len(resp.Job.Nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(resp.Job.Nodes))
	}

	if resp.Job.State != pb.JobStatus_JOB_STATUS_RUNNING {
		t.Errorf("Expected state RUNNING, got %v", resp.Job.State)
	}

	t.Logf("Successfully created job: %s", resp.Job.HeimdallJobId)
}

// TestCreateOrUpdateJobAutoGenerateId tests CreateOrUpdateJob with auto-generated heimdall_id
func TestCreateOrUpdateJobAutoGenerateId(t *testing.T) {
	jobSource := job.NewJobsSyntheticSource()
	ctx := context.Background()

	jobName := "tensorflow-training"

	req := &pb.CreateOrUpdateJobRequest{
		// HeimdallJobId not provided - should be auto-generated
		JobName:      &jobName,
		JobId:        "tf-job-123",
		WorkflowId:   "tf-workflow",
		AttemptIndex: 1,
		AllocatedNodes: []string{
			"node-1",
			"node-2",
			"node-3",
		},
		State:           pb.JobStatus_JOB_STATUS_PENDING,
		GpuCountPerNode: 4,
	}

	resp, err := jobSource.CreateOrUpdateJob(ctx, req)
	if err != nil {
		t.Fatalf("CreateOrUpdateJob failed: %v", err)
	}

	// Verify heimdall_id was auto-generated in format: job_id-attempt_index
	expectedId := "tf-job-123-1"
	if resp.Job.HeimdallJobId != expectedId {
		t.Errorf("Expected auto-generated heimdall_id %s, got %s", expectedId, resp.Job.HeimdallJobId)
	}

	if resp.Job.JobName == nil || *resp.Job.JobName != jobName {
		t.Errorf("Expected job_name %s, got %v", jobName, resp.Job.JobName)
	}

	t.Logf("Successfully created job with auto-generated ID: %s", resp.Job.HeimdallJobId)
}

// TestCreateOrUpdateJobMinimalRequest tests CreateOrUpdateJob with minimal required fields
func TestCreateOrUpdateJobMinimalRequest(t *testing.T) {
	jobSource := job.NewJobsSyntheticSource()
	ctx := context.Background()

	req := &pb.CreateOrUpdateJobRequest{
		JobId:        "minimal-job",
		WorkflowId:   "minimal-workflow",
		AttemptIndex: 0,
		State:        pb.JobStatus_JOB_STATUS_RUNNING,
	}

	resp, err := jobSource.CreateOrUpdateJob(ctx, req)
	if err != nil {
		t.Fatalf("CreateOrUpdateJob with minimal fields failed: %v", err)
	}

	if resp.Job == nil {
		t.Fatal("Expected job in response, got nil")
	}

	expectedId := "minimal-job-0"
	if resp.Job.HeimdallJobId != expectedId {
		t.Errorf("Expected heimdall_id %s, got %s", expectedId, resp.Job.HeimdallJobId)
	}

	t.Logf("Successfully created job with minimal fields: %s", resp.Job.HeimdallJobId)
}

func TestCreateOrUpdateJobSyntheticRecoveryConfig(t *testing.T) {
	jobSource := job.NewJobsSyntheticSource()
	ctx := context.Background()

	maxRequeueTimes := int32(3)
	requeueOnCompletion := false
	resp, err := jobSource.CreateOrUpdateJob(ctx, &pb.CreateOrUpdateJobRequest{
		JobId:        "recovery-job",
		WorkflowId:   "recovery-workflow",
		AttemptIndex: 0,
		State:        pb.JobStatus_JOB_STATUS_PENDING,
		RecoveryConfig: &pb.JobRecoveryConfig{
			AutoResumeMode:      pb.JobRecoveryConfig_AUTO_RESUME_MODE_REQUEUE,
			MaxRequeueTimes:     &maxRequeueTimes,
			RequeueOnCompletion: &requeueOnCompletion,
		},
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateJob failed: %v", err)
	}
	if resp.Job == nil || resp.Job.RecoveryConfig == nil {
		t.Fatalf("RecoveryConfig missing from response: %#v", resp.Job)
	}
	if resp.Job.RecoveryConfig.AutoResumeMode != pb.JobRecoveryConfig_AUTO_RESUME_MODE_REQUEUE {
		t.Fatalf("AutoResumeMode = %v, want REQUEUE", resp.Job.RecoveryConfig.AutoResumeMode)
	}
	if resp.Job.RecoveryConfig.MaxRequeueTimes == nil || *resp.Job.RecoveryConfig.MaxRequeueTimes != maxRequeueTimes {
		t.Fatalf("MaxRequeueTimes = %v, want %d", resp.Job.RecoveryConfig.MaxRequeueTimes, maxRequeueTimes)
	}
	if resp.Job.RecoveryConfig.RequeueOnCompletion == nil || *resp.Job.RecoveryConfig.RequeueOnCompletion != false {
		t.Fatalf("RequeueOnCompletion = %v, want false with presence", resp.Job.RecoveryConfig.RequeueOnCompletion)
	}
}
