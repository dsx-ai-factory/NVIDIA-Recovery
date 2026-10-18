// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"errors"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

func TestJobMutationsUseValidatedWFCCommands(t *testing.T) {
	ctx := context.Background()
	executor := &recordingExecutor{
		results: map[string][]byte{
			"squeue\x00-j\x0012345\x00-s\x00-o\x00%i\x00-h": []byte("12345.batch\n12345.extern\n12345.0\n"),
		},
	}
	slurmCluster := &slurmCluster{rwCommands: executor, roCommands: executor}

	if err := slurmCluster.JobKill(ctx, "12345"); err != nil {
		t.Fatalf("JobKill() failed: %v", err)
	}
	if err := slurmCluster.JobHold(ctx, "12345"); err != nil {
		t.Fatalf("JobHold() failed: %v", err)
	}
	if err := slurmCluster.JobRelease(ctx, "12345"); err != nil {
		t.Fatalf("JobRelease() failed: %v", err)
	}
	if err := slurmCluster.JobSignal(ctx, "12345", "SIGTERM"); err != nil {
		t.Fatalf("JobSignal() failed: %v", err)
	}
	if err := slurmCluster.JobNodeUpdate(ctx, "12345", []string{"node3", "node4"}, []string{"node1", "node2"}, 4); err != nil {
		t.Fatalf("JobNodeUpdate() failed: %v", err)
	}
	if err := slurmCluster.JobRequeueHold(ctx, "12345"); err != nil {
		t.Fatalf("JobRequeueHold() failed: %v", err)
	}
	if err := slurmCluster.JobUpdateEligibleNow(ctx, "12345"); err != nil {
		t.Fatalf("JobUpdateEligibleNow() failed: %v", err)
	}

	assertCommandArgs(t, executor.calls, [][]string{
		{"scancel", "12345"},
		{"scontrol", "hold", "12345"},
		{"scontrol", "release", "12345"},
		{"squeue", "-j", "12345", "-s", "-o", "%i", "-h"},
		{"scancel", "--ctld", "--signal=SIGTERM", "12345.0"},
		{"scontrol", "update", "JobID=12345", "NumNodes=4", "ReqNodeList=node1,node2", "ExcNodeList=node3,node4"},
		{"scontrol", "requeuehold", "12345"},
		{"scontrol", "update", "JobID=12345", "EligibleTime=Now"},
	})
}

func TestJobReleasePreservesConcreteArrayElementID(t *testing.T) {
	executor := &recordingExecutor{}
	slurmCluster := &slurmCluster{rwCommands: executor, roCommands: executor}

	if err := slurmCluster.JobRelease(context.Background(), "12345_2"); err != nil {
		t.Fatalf("JobRelease() failed: %v", err)
	}
	assertCommandArgs(t, executor.calls, [][]string{{"scontrol", "release", "12345_2"}})
}

func TestJobNodeUpdateRejectsInvalidRequests(t *testing.T) {
	slurmCluster := &slurmCluster{rwCommands: &recordingExecutor{}, roCommands: &recordingExecutor{}}
	ctx := context.Background()

	tests := []struct {
		name       string
		exclusions []string
		inclusions []string
		nodes      int
		wantErr    error
	}{
		{name: "empty", wantErr: cluster.ErrEmptyNodeUpdate},
		{name: "invalid nodes", nodes: -1, wantErr: cluster.ErrInvalidNodeCount},
		{name: "overlap", exclusions: []string{"node1"}, inclusions: []string{"node1"}, wantErr: cluster.ErrNodeUpdateOverlap},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := slurmCluster.JobNodeUpdate(ctx, "12345", tt.exclusions, tt.inclusions, tt.nodes)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("JobNodeUpdate() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetJobsConvertsTypedSqueueData(t *testing.T) {
	executor := &recordingExecutor{
		results: map[string][]byte{
			"squeue\x00--json": []byte(`{
				"jobs": [{
					"job_id": 12345,
					"name": "test_job",
					"job_state": "RUNNING",
					"state_reason": "None",
					"submit_time": 1623456789,
					"start_time": 1623456790,
					"end_time": 0,
					"cluster": "test_cluster",
					"partition": "test_partition",
					"resv_name": "test_reservation",
					"user_name": "test_user",
					"comment": "{\"APS\":{\"workflow_id\":\"workflow-a\",\"auto_resume_mode\":\"requeue\",\"max_requeue_times\":3}}",
					"command": "test_command",
					"dependency": "test_dependency",
					"tasks": {"number": 4},
					"node_count": {"number": 2},
					"nodes": "node[1-2]",
					"required_nodes": "node1",
					"excluded_nodes": "node3",
					"tres_req_str": "cpu=4,mem=8T,node=2,billing=4,gres/gpu=2",
					"tres_alloc_str": "cpu=8,mem=16G,node=2,billing=8,gres/gpu=4",
					"restart_cnt": 1
				}]
			}`),
		},
	}
	slurmCluster := &slurmCluster{rwCommands: executor, roCommands: executor}

	jobs, err := slurmCluster.GetJobs(context.Background())
	if err != nil {
		t.Fatalf("GetJobs() failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1", len(jobs))
	}

	job := jobs[0]
	if job.JobID != "12345" || job.AttemptIndex != 1 || job.WorkflowID != "workflow-a" {
		t.Fatalf("identity = %q/%d/%q, want 12345/1/workflow-a", job.JobID, job.AttemptIndex, job.WorkflowID)
	}
	if job.State != cluster.JobStateRunning || job.RawState != "RUNNING" {
		t.Fatalf("state = %q/%q, want running/RUNNING", job.State, job.RawState)
	}
	if got := job.SchedulerMetadata["slurm_reason"]; got != "None" {
		t.Fatalf("slurm_reason = %#v, want None", got)
	}
	if !slices.Equal(job.AllocatedNodes, []string{"node1", "node2"}) {
		t.Fatalf("AllocatedNodes = %v, want node1,node2", job.AllocatedNodes)
	}
	if !slices.Equal(job.NodeConstraints.Inclusions, []string{"node1"}) {
		t.Fatalf("Inclusions = %v, want node1", job.NodeConstraints.Inclusions)
	}
	if !slices.Equal(job.NodeConstraints.Exclusions, []string{"node3"}) {
		t.Fatalf("Exclusions = %v, want node3", job.NodeConstraints.Exclusions)
	}
	if job.TotalCPUCount != 8 || job.TotalMemoryMB != 16384 || job.TotalGPUCount != 4 {
		t.Fatalf("resources = cpu=%v mem=%v gpu=%v, want 8/16384/4", job.TotalCPUCount, job.TotalMemoryMB, job.TotalGPUCount)
	}
	if job.RecoveryConfig == nil || job.RecoveryConfig.AutoResumeMode != cluster.AutoResumeModeRequeue {
		t.Fatalf("RecoveryConfig = %#v, want requeue config", job.RecoveryConfig)
	}
	if job.RecoveryConfig.MaxRequeueTimes == nil || *job.RecoveryConfig.MaxRequeueTimes != 3 {
		t.Fatalf("MaxRequeueTimes = %v, want 3", job.RecoveryConfig.MaxRequeueTimes)
	}
	assertCommandArgs(t, executor.calls, [][]string{{"squeue", "--json"}})
}

func TestActiveJobsAcceptsMultiElementStateList(t *testing.T) {
	executor := &recordingExecutor{results: map[string][]byte{
		"squeue\x00--json": []byte(
			`{"jobs":[{"job_id":12345,"job_state":["RUNNING","COMPLETING"]}]}`,
		),
	}}
	slurmCluster := &slurmCluster{roCommands: executor}

	jobs, err := slurmCluster.ActiveJobs(context.Background())
	if err != nil {
		t.Fatalf("ActiveJobs() failed: %v", err)
	}
	if len(jobs) != 1 ||
		jobs[0].State != cluster.JobStateRunning ||
		jobs[0].RawState != "RUNNING" {
		t.Fatalf("jobs = %#v, want one running/RUNNING job", jobs)
	}
	assertCommandArgs(t, executor.calls, [][]string{{"squeue", "--json"}})
}

func TestParseSqueueOutputAcceptsStringVersionComponents(t *testing.T) {
	_, err := parseSqueueOutput([]byte(`{
		"meta": {
			"slurm": {
				"version": {"major": "25", "minor": "05", "micro": "8"},
				"release": "25.05.8"
			}
		},
		"jobs": []
	}`))
	if err != nil {
		t.Fatalf("parseSqueueOutput() failed for Slurm 25.05 metadata: %v", err)
	}
}

func TestSqueueHostlistConversionExpandsAllNodeFields(t *testing.T) {
	executor := &recordingExecutor{
		results: map[string][]byte{
			"squeue\x00--json": []byte(`{
				"jobs": [{
					"job_id": 12345,
					"nodes": "gpu-node-[001-002]",
					"required_nodes": "rack[1-2]node[01-02]",
					"excluded_nodes": "gpu-node-[010,012-013]"
				}]
			}`),
		},
	}
	slurmCluster := &slurmCluster{rwCommands: executor, roCommands: executor}

	jobs, err := slurmCluster.GetJobs(context.Background())
	if err != nil {
		t.Fatalf("GetJobs() failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1", len(jobs))
	}

	job := jobs[0]
	if want := []string{"gpu-node-001", "gpu-node-002"}; !slices.Equal(job.AllocatedNodes, want) {
		t.Fatalf("AllocatedNodes = %#v, want %#v", job.AllocatedNodes, want)
	}
	if want := []string{"rack1node01", "rack1node02", "rack2node01", "rack2node02"}; !slices.Equal(job.NodeConstraints.Inclusions, want) {
		t.Fatalf("Inclusions = %#v, want %#v", job.NodeConstraints.Inclusions, want)
	}
	if want := []string{"gpu-node-010", "gpu-node-012", "gpu-node-013"}; !slices.Equal(job.NodeConstraints.Exclusions, want) {
		t.Fatalf("Exclusions = %#v, want %#v", job.NodeConstraints.Exclusions, want)
	}
}

func TestSqueueHostlistConversionRejectsMalformedExcludedNodes(t *testing.T) {
	executor := &recordingExecutor{
		results: map[string][]byte{
			"squeue\x00--json": []byte(`{
				"jobs": [{
					"job_id": 12345,
					"nodes": "gpu-node-001",
					"excluded_nodes": "node[1-]"
				}]
			}`),
		},
	}
	slurmCluster := &slurmCluster{rwCommands: executor, roCommands: executor}

	jobs, err := slurmCluster.GetJobs(context.Background())
	if err == nil {
		t.Fatalf("GetJobs() error = nil, jobs = %#v, want malformed excluded_nodes error", jobs)
	}
	if jobs != nil {
		t.Fatalf("GetJobs() jobs = %#v, want nil on conversion error", jobs)
	}
	for _, part := range []string{"squeue", "12345", "excluded_nodes"} {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("GetJobs() error = %q, want context %q", err, part)
		}
	}
}

func TestSacctHostlistConversionExpandsAllocatedNodes(t *testing.T) {
	jobs, err := observedJobsFromSacct([]byte(`{
		"jobs": [{
			"job_id": 12345,
			"state": {"current": "COMPLETED"},
			"nodes": "rack[1-2]node[01-02]"
		}]
	}`))
	if err != nil {
		t.Fatalf("observedJobsFromSacct() failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1", len(jobs))
	}
	want := []string{"rack1node01", "rack1node02", "rack2node01", "rack2node02"}
	if !slices.Equal(jobs[0].AllocatedNodes, want) {
		t.Fatalf("AllocatedNodes = %#v, want %#v", jobs[0].AllocatedNodes, want)
	}
}

func TestSacctHostlistConversionRejectsMalformedNodes(t *testing.T) {
	jobs, err := observedJobsFromSacct([]byte(`{
		"jobs": [{
			"job_id": 12345,
			"state": {"current": "COMPLETED"},
			"nodes": "node[1-]"
		}]
	}`))
	if err == nil {
		t.Fatalf("observedJobsFromSacct() error = nil, jobs = %#v, want malformed nodes error", jobs)
	}
	if jobs != nil {
		t.Fatalf("observedJobsFromSacct() jobs = %#v, want nil on conversion error", jobs)
	}
	for _, part := range []string{"sacct", "12345", "nodes"} {
		if !strings.Contains(err.Error(), part) {
			t.Fatalf("observedJobsFromSacct() error = %q, want context %q", err, part)
		}
	}
}

func TestSacctResourcesFallBackToRequestedTotalsWithoutAllocation(t *testing.T) {
	jobs, err := observedJobsFromSacct([]byte(`{
		"jobs": [{
			"job_id": 12345,
			"state": {"current": "PENDING"},
			"tres": {"requested": {"total": [
				{"type": "cpu", "name": "", "count": 16},
				{"type": "mem", "name": "", "count": 64000},
				{"type": "gres", "name": "gpu", "count": 8}
			]}}
		}]
	}`))
	if err != nil {
		t.Fatalf("observedJobsFromSacct() failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1", len(jobs))
	}
	job := jobs[0]
	if job.TotalCPUCount != 16 || job.TotalMemoryMB != 64000 || job.TotalGPUCount != 8 {
		t.Fatalf("requested resources = cpu=%v memory=%v gpu=%v, want 16/64000/8", job.TotalCPUCount, job.TotalMemoryMB, job.TotalGPUCount)
	}
}

func TestSacctRequeueMetadataPreservesSqueueStateReason(t *testing.T) {
	jobs, err := observedJobsFromSacct([]byte(`{
		"jobs": [{
			"job_id": 12345,
			"state": {"current": "FAILED", "reason": "NonZeroExitCode"},
			"comment": {"job": "{\"APS\":{\"auto_resume_mode\":\"requeue\"}}"}
		}]
	}`))
	if err != nil {
		t.Fatalf("observedJobsFromSacct() failed: %v", err)
	}
	metadata := jobs[0].SchedulerMetadata
	if jobs[0].Reason != "NonZeroExitCode" {
		t.Fatalf("Reason = %q, want retained for reconciliation", jobs[0].Reason)
	}
	if _, exists := metadata["slurm_reason"]; exists {
		t.Fatalf("slurm_reason = %#v, want omitted for requeue accounting update", metadata["slurm_reason"])
	}
}

func TestObservedJobsFromSqueuePreservesHoldState(t *testing.T) {
	jobs, err := observedJobsFromSqueue([]byte(`{
		"jobs": [{
			"job_id": 12345,
			"job_state": ["PENDING", "REQUEUED"],
			"state_reason": "Dependency",
			"hold": true
		}]
	}`))
	if err != nil {
		t.Fatalf("observedJobsFromSqueue() failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1", len(jobs))
	}
	if jobs[0].State != cluster.JobStateRequeued {
		t.Errorf("jobs[0].State = %q, want requeued", jobs[0].State)
	}
	if !jobs[0].Held {
		t.Error("jobs[0].Held = false, want true")
	}
	if jobs[0].Reason != "Dependency" {
		t.Errorf("jobs[0].Reason = %q, want Dependency", jobs[0].Reason)
	}
}

func TestObservedJobsFromSqueueMarksOnlyValidHeimdallCommentsManaged(t *testing.T) {
	for name, tt := range map[string]struct {
		comment string
		want    bool
	}{
		"APS object": {
			comment: `{"APS":{}}`,
			want:    true,
		},
		"HEIMDALL object": {
			comment: `{"HEIMDALL":{"workflow_id":"workflow-a"}}`,
			want:    true,
		},
		"missing": {},
		"malformed": {
			comment: `{"APS":`,
		},
		"unrelated object": {
			comment: `{"OTHER":{}}`,
		},
		"APS non-object": {
			comment: `{"APS":"managed"}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			jobs, err := observedJobsFromSqueue([]byte(`{"jobs":[{"job_id":12345,"comment":` + strconv.Quote(tt.comment) + `}]}`))
			if err != nil {
				t.Fatalf("observedJobsFromSqueue() failed: %v", err)
			}
			if len(jobs) != 1 {
				t.Fatalf("len(jobs) = %d, want 1", len(jobs))
			}
			if jobs[0].HeimdallManaged != tt.want {
				t.Fatalf("HeimdallManaged = %t, want %t", jobs[0].HeimdallManaged, tt.want)
			}
		})
	}
}

func TestObservedJobsFromSacctMarksValidHeimdallCommentManaged(t *testing.T) {
	jobs, err := observedJobsFromSacct([]byte(`{
		"jobs": [{
			"job_id": 12345,
			"comment": {"job": "{\"APS\":{}}"}
		}]
	}`))
	if err != nil {
		t.Fatalf("observedJobsFromSacct() failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1", len(jobs))
	}
	if !jobs[0].HeimdallManaged {
		t.Fatal("HeimdallManaged = false, want true")
	}
}

func TestObservedJobsParseJobModeWithExistingCommentRules(t *testing.T) {
	tests := []struct {
		name        string
		comment     string
		wantManaged bool
		wantRequest cluster.JobModeRequest
	}{
		{name: "missing comment"},
		{
			name:        "APS without mode",
			comment:     ` { "APS" : { "workflow_id" : "workflow-a" } } `,
			wantManaged: true,
			wantRequest: cluster.JobModeRequest{Valid: true},
		},
		{
			name:        "APS lowercase mode",
			comment:     `{"APS":{"mode":"ignore"}}`,
			wantManaged: true,
			wantRequest: cluster.JobModeRequest{Mode: cluster.JobModeIgnore, Specified: true, Valid: true},
		},
		{
			name:        "HEIMDALL uppercase value",
			comment:     `{"HEIMDALL":{"mode":"ACT"}}`,
			wantManaged: true,
			wantRequest: cluster.JobModeRequest{Mode: cluster.JobModeAct, Specified: true, Valid: true},
		},
		{
			name:        "mode value whitespace is invalid",
			comment:     `{"APS":{"mode":" observe "}}`,
			wantManaged: true,
			wantRequest: cluster.JobModeRequest{Mode: cluster.JobMode(" observe "), Specified: true},
		},
		{
			name:        "mode key remains case sensitive",
			comment:     `{"APS":{"Mode":"act"}}`,
			wantManaged: true,
			wantRequest: cluster.JobModeRequest{Valid: true},
		},
		{
			name:        "non-string mode is invalid",
			comment:     `{"APS":{"mode":true}}`,
			wantManaged: true,
			wantRequest: cluster.JobModeRequest{Specified: true},
		},
		{
			name:    "malformed comment",
			comment: `{"APS":`,
		},
	}

	converters := map[string]func(string) ([]cluster.JobExecutionRecord, error){
		"squeue": func(comment string) ([]cluster.JobExecutionRecord, error) {
			return observedJobsFromSqueue([]byte(`{"jobs":[{"job_id":12345,"comment":` + strconv.Quote(comment) + `}]}`))
		},
		"sacct": func(comment string) ([]cluster.JobExecutionRecord, error) {
			return observedJobsFromSacct([]byte(`{"jobs":[{"job_id":12345,"comment":{"job":` + strconv.Quote(comment) + `}}]}`))
		},
	}

	for _, tt := range tests {
		for source, convert := range converters {
			t.Run(source+"/"+tt.name, func(t *testing.T) {
				jobs, err := convert(tt.comment)
				if err != nil {
					t.Fatalf("convert() failed: %v", err)
				}
				if len(jobs) != 1 {
					t.Fatalf("len(jobs) = %d, want 1", len(jobs))
				}
				if jobs[0].HeimdallManaged != tt.wantManaged {
					t.Errorf("HeimdallManaged = %t, want %t", jobs[0].HeimdallManaged, tt.wantManaged)
				}
				if jobs[0].ModeRequest != tt.wantRequest {
					t.Errorf("ModeRequest = %#v, want %#v", jobs[0].ModeRequest, tt.wantRequest)
				}
			})
		}
	}
}

func TestObservedJobsPreserveOptionalEnvironmentString(t *testing.T) {
	tests := []struct {
		name        string
		comment     string
		wantManaged bool
		wantRequest cluster.JobEnvironmentRequest
		wantConfig  *string
	}{
		{name: "missing comment"},
		{name: "absent environment", comment: `{"APS":{}}`, wantManaged: true, wantRequest: cluster.JobEnvironmentRequest{Valid: true}},
		{name: "null environment", comment: `{"APS":{"environment":null}}`, wantManaged: true, wantRequest: cluster.JobEnvironmentRequest{Valid: true}},
		{name: "empty environment", comment: `{"APS":{"environment":""}}`, wantManaged: true, wantRequest: cluster.JobEnvironmentRequest{Specified: true, Valid: true}, wantConfig: stringPointer("")},
		{name: "arbitrary exact environment", comment: `{"APS":{"environment":"QA/Canary 01"}}`, wantManaged: true, wantRequest: cluster.JobEnvironmentRequest{Environment: "QA/Canary 01", Specified: true, Valid: true}, wantConfig: stringPointer("QA/Canary 01")},
		{name: "HEIMDALL alias", comment: `{"HEIMDALL":{"environment":"blue"}}`, wantManaged: true, wantRequest: cluster.JobEnvironmentRequest{Environment: "blue", Specified: true, Valid: true}, wantConfig: stringPointer("blue")},
		{name: "non-string environment", comment: `{"APS":{"environment":7}}`, wantManaged: true, wantRequest: cluster.JobEnvironmentRequest{Specified: true}},
	}

	converters := map[string]func(string) ([]cluster.JobExecutionRecord, error){
		"squeue": func(comment string) ([]cluster.JobExecutionRecord, error) {
			return observedJobsFromSqueue([]byte(`{"jobs":[{"job_id":12345,"comment":` + strconv.Quote(comment) + `}]}`))
		},
		"sacct": func(comment string) ([]cluster.JobExecutionRecord, error) {
			return observedJobsFromSacct([]byte(`{"jobs":[{"job_id":12345,"comment":{"job":` + strconv.Quote(comment) + `}}]}`))
		},
	}

	for _, tt := range tests {
		for source, convert := range converters {
			t.Run(source+"/"+tt.name, func(t *testing.T) {
				jobs, err := convert(tt.comment)
				if err != nil {
					t.Fatalf("convert() failed: %v", err)
				}
				if jobs[0].HeimdallManaged != tt.wantManaged || jobs[0].EnvironmentRequest != tt.wantRequest {
					t.Fatalf("managed/request = %t/%#v, want %t/%#v", jobs[0].HeimdallManaged, jobs[0].EnvironmentRequest, tt.wantManaged, tt.wantRequest)
				}
				if tt.wantConfig == nil {
					if jobs[0].RecoveryConfig != nil && jobs[0].RecoveryConfig.Environment != nil {
						t.Fatalf("RecoveryConfig.Environment = %v, want nil", jobs[0].RecoveryConfig.Environment)
					}
				} else if jobs[0].RecoveryConfig == nil || jobs[0].RecoveryConfig.Environment == nil || *jobs[0].RecoveryConfig.Environment != *tt.wantConfig {
					t.Fatalf("RecoveryConfig = %#v, want environment %q", jobs[0].RecoveryConfig, *tt.wantConfig)
				}
			})
		}
	}
}

func stringPointer(value string) *string { return &value }

func TestRecoveryConfigPreservesAnomalyDetectionPolicy(t *testing.T) {
	config := recoveryConfigFromComment(`{"APS":{"anomaly_detection_config":{"log_based_crash_enabled":false,"log_based_timeout_enabled":true,"log_based_timeout_threshold_pretraining":30,"log_based_timeout_threshold_trainingloop":45}}}`)
	if config == nil || config.AnomalyDetection == nil {
		t.Fatalf("RecoveryConfig = %#v, want anomaly detection config", config)
	}
	detection := config.AnomalyDetection
	if detection.LogBasedCrashEnabled == nil || *detection.LogBasedCrashEnabled {
		t.Fatalf("LogBasedCrashEnabled = %v, want false with presence", detection.LogBasedCrashEnabled)
	}
	if detection.LogBasedTimeoutEnabled == nil || !*detection.LogBasedTimeoutEnabled {
		t.Fatalf("LogBasedTimeoutEnabled = %v, want true", detection.LogBasedTimeoutEnabled)
	}
	if detection.LogBasedTimeoutThresholdPretraining == nil || *detection.LogBasedTimeoutThresholdPretraining != 30 {
		t.Fatalf("LogBasedTimeoutThresholdPretraining = %v, want 30", detection.LogBasedTimeoutThresholdPretraining)
	}
	if detection.LogBasedTimeoutThresholdTrainingloop == nil || *detection.LogBasedTimeoutThresholdTrainingloop != 45 {
		t.Fatalf("LogBasedTimeoutThresholdTrainingloop = %v, want 45", detection.LogBasedTimeoutThresholdTrainingloop)
	}
}

func TestRecoveryConfigPreservesSharedLogPathPolicy(t *testing.T) {
	config := recoveryConfigFromComment(`{"APS":{"log_file_paths_shared_across_attempts":true}}`)
	if config == nil || config.LogFilePathsSharedAcrossAttempts == nil || !*config.LogFilePathsSharedAcrossAttempts {
		t.Fatalf("RecoveryConfig = %#v, want shared log paths enabled", config)
	}
}

func TestRecoveryConfigPreservesDAGPolicy(t *testing.T) {
	config := recoveryConfigFromComment(`{"APS":{"dag_dependencies":[{"target":{"type":"id","id":"123"}},{"target":{"type":"name","name":"prepare"},"trigger":"completed"}],"dag_action":"release"}}`)
	if config == nil {
		t.Fatal("RecoveryConfig = nil, want DAG policy")
	}
	if config.DAGAction != cluster.DAGActionRelease {
		t.Fatalf("DAGAction = %q, want release", config.DAGAction)
	}
	want := []cluster.DAGDependency{
		{TargetType: cluster.DAGTargetTypeID, TargetIdentifier: "123", Trigger: cluster.DAGTriggerConditionCompleted},
		{TargetType: cluster.DAGTargetTypeName, TargetIdentifier: "prepare", Trigger: cluster.DAGTriggerConditionCompleted},
	}
	if !slices.Equal(config.DAGDependencies, want) {
		t.Fatalf("DAGDependencies = %#v, want %#v", config.DAGDependencies, want)
	}
}

func TestRecoveryConfigRejectsPartialOrUnsupportedDAGPolicy(t *testing.T) {
	comments := map[string]string{
		"non-list dependencies": `{"APS":{"auto_resume_mode":"requeue","dag_dependencies":{},"dag_action":"release"}}`,
		"non-object dependency": `{"APS":{"auto_resume_mode":"requeue","dag_dependencies":["parent"],"dag_action":"release"}}`,
		"missing target":        `{"APS":{"auto_resume_mode":"requeue","dag_dependencies":[{}],"dag_action":"release"}}`,
		"unsupported target":    `{"APS":{"auto_resume_mode":"requeue","dag_dependencies":[{"target":{"type":"uuid","id":"123"}}],"dag_action":"release"}}`,
		"missing identifier":    `{"APS":{"auto_resume_mode":"requeue","dag_dependencies":[{"target":{"type":"id"}}],"dag_action":"release"}}`,
		"unsupported trigger":   `{"APS":{"auto_resume_mode":"requeue","dag_dependencies":[{"target":{"type":"id","id":"123"},"trigger":"failed"}],"dag_action":"release"}}`,
		"unsupported action":    `{"APS":{"auto_resume_mode":"requeue","dag_dependencies":[{"target":{"type":"id","id":"123"}}],"dag_action":"cancel"}}`,
	}

	for name, comment := range comments {
		t.Run(name, func(t *testing.T) {
			config := recoveryConfigFromComment(comment)
			if config == nil || config.AutoResumeMode != cluster.AutoResumeModeRequeue {
				t.Fatalf("RecoveryConfig = %#v, want preserved non-DAG recovery policy", config)
			}
			if len(config.DAGDependencies) != 0 || config.DAGAction != cluster.DAGActionUnset {
				t.Fatalf("DAG policy = (%#v, %q), want fail-closed omission", config.DAGDependencies, config.DAGAction)
			}
		})
	}
}

func TestObservedJobsDeriveDAGChildFromNonEmptyDependencies(t *testing.T) {
	for name, tt := range map[string]struct {
		comment string
		want    bool
	}{
		"APS dependencies": {
			comment: `{"APS":{"dag_dependencies":[{"target":{"type":"id","id":"123"},"trigger":"completed"}]}}`,
			want:    true,
		},
		"HEIMDALL dependencies": {
			comment: `{"HEIMDALL":{"dag_dependencies":[{"target":{"type":"name","name":"parent"}}]}}`,
			want:    true,
		},
		"empty dependencies":   {comment: `{"APS":{"dag_dependencies":[]}}`},
		"null dependencies":    {comment: `{"APS":{"dag_dependencies":null}}`},
		"missing dependencies": {comment: `{"APS":{"workflow_id":"workflow-a"}}`},
		"malformed comment":    {comment: `{"APS":`},
	} {
		t.Run(name, func(t *testing.T) {
			jobs, err := observedJobsFromSqueue([]byte(`{"jobs":[{"job_id":12345,"comment":` + strconv.Quote(tt.comment) + `}]}`))
			if err != nil {
				t.Fatalf("observedJobsFromSqueue() failed: %v", err)
			}
			if len(jobs) != 1 {
				t.Fatalf("len(jobs) = %d, want 1", len(jobs))
			}
			if jobs[0].DAGChild != tt.want {
				t.Fatalf("DAGChild = %t, want %t", jobs[0].DAGChild, tt.want)
			}
		})
	}
}

func TestFinalizedJobsRunsSacctForDisappearedRecords(t *testing.T) {
	executor := &recordingExecutor{
		results: map[string][]byte{
			"sacct\x00-Xj\x0012345\x00--duplicates\x00--json": []byte(`{"jobs":[{"allocation_nodes":1,"job_id":12345,"name":"train","state":{"current":"COMPLETED","reason":"None"},"cluster":"cluster-a","partition":"normal","reservation":{"name":"reservation-a"},"user":"alice","nodes":"c1","tres":{"allocated":[{"type":"cpu","name":"","count":16},{"type":"mem","name":"","count":64000},{"type":"gres","name":"gpu","count":8}]},"time":{"submission":1722495600,"start":1722495660,"end":1722495700},"comment":{"job":"{\"APS\":{\"workflow_name\":\"train\"}}"}}]}`),
		},
	}
	slurmCluster := newSlurmCluster(Options{})
	slurmCluster.rwCommands = executor
	slurmCluster.roCommands = executor

	jobs, err := slurmCluster.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 0}})
	if err != nil {
		t.Fatalf("FinalizedJobs() failed: %v", err)
	}
	if len(jobs) != 1 || jobs[0].State != cluster.JobStateCompleted {
		t.Fatalf("jobs = %#v, want one completed job", jobs)
	}
	job := jobs[0]
	if job.Cluster != "cluster-a" || job.Partition != "normal" || job.Reservation != "reservation-a" || job.User != "alice" {
		t.Fatalf("accounting metadata = %#v, want cluster/partition/reservation/user", job)
	}
	if job.NodeCount != 1 || job.TotalCPUCount != 16 || job.TotalMemoryMB != 64000 || job.TotalGPUCount != 8 {
		t.Fatalf("accounting resources = nodes=%d cpu=%v memory=%v gpu=%v, want 1/16/64000/8", job.NodeCount, job.TotalCPUCount, job.TotalMemoryMB, job.TotalGPUCount)
	}
	assertCommandArgs(t, executor.calls, [][]string{{"sacct", "-Xj", "12345", "--duplicates", "--json"}})
}

func TestFinalizedJobsAcceptsMultiElementStateList(t *testing.T) {
	executor := &recordingExecutor{results: map[string][]byte{
		"sacct\x00-Xj\x0012345\x00--duplicates\x00--json": []byte(
			`{"jobs":[{"job_id":12345,"state":{"current":["FAILED","NODE_FAIL"]}}]}`,
		),
	}}
	slurmCluster := newSlurmCluster(Options{})
	slurmCluster.roCommands = executor
	ref := cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 0}

	jobs, err := slurmCluster.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{ref})
	if err != nil {
		t.Fatalf("FinalizedJobs() failed: %v", err)
	}
	if len(jobs) != 1 ||
		jobs[0].State != cluster.JobStateFailed ||
		jobs[0].RawState != "FAILED" {
		t.Fatalf("jobs = %#v, want one failed/FAILED job", jobs)
	}
	assertCommandArgs(
		t,
		executor.calls,
		[][]string{{"sacct", "-Xj", "12345", "--duplicates", "--json"}},
	)
}

func TestExpandNodeListMatchesValidatedWFCHostlistCases(t *testing.T) {
	tests := []struct {
		value string
		want  []string
	}{
		{value: "n01p[001-003]", want: []string{"n01p001", "n01p002", "n01p003"}},
		{value: "n01p[001-002],n02p[010-011]", want: []string{"n01p001", "n01p002", "n02p010", "n02p011"}},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			got, err := ExpandNodeList(tt.value)
			if err != nil {
				t.Fatalf("ExpandNodeList(%q) failed: %v", tt.value, err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ExpandNodeList(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

type recordingExecutor struct {
	results map[string][]byte
	err     error
	calls   [][]string
}

func (e *recordingExecutor) Execute(cmd *exec.Cmd) (commandOutput, error) {
	e.calls = append(e.calls, append([]string(nil), cmd.Args...))
	if e.err != nil {
		return commandOutput{}, e.err
	}
	return commandOutput{stdout: e.results[commandKey(cmd.Args)]}, nil
}

func commandKey(args []string) string {
	return strings.Join(args, "\x00")
}

func assertCommandArgs(t *testing.T, got, want [][]string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("len(calls) = %d, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			t.Fatalf("calls[%d] = %v, want %v", i, got[i], want[i])
		}
	}
}
