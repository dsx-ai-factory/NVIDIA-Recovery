// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"errors"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestControllerFiltersBootstrapEnvironmentBeforeAccounting(t *testing.T) {
	config := func(environment string) *pb.JobRecoveryConfig {
		return &pb.JobRecoveryConfig{Environment: &environment}
	}
	tests := []struct {
		name        string
		environment string
		config      *pb.JobRecoveryConfig
		admit       bool
	}{
		{name: "named exact match", environment: "nvr-test", config: config("nvr-test"), admit: true},
		{name: "named rejects prod", environment: "nvr-test", config: config("prod")},
		{name: "named rejects missing config", environment: "nvr-test"},
		{name: "named rejects absent environment", environment: "nvr-test", config: &pb.JobRecoveryConfig{}},
		{name: "named rejects empty environment", environment: "nvr-test", config: config("")},
		{name: "case sensitive", environment: "nvr-test", config: config("NVR-TEST")},
		{name: "no whitespace trimming", environment: "nvr-test", config: config(" nvr-test ")},
		{name: "prod exact match", environment: "prod", config: config("prod"), admit: true},
		{name: "prod accepts missing config", environment: "prod", admit: true},
		{name: "prod accepts absent environment", environment: "prod", config: &pb.JobRecoveryConfig{}, admit: true},
		{name: "prod accepts empty environment", environment: "prod", config: config(""), admit: true},
		{name: "prod rejects other environment", environment: "prod", config: config("nvr-test")},
		{name: "unspecified accepts named", environment: "unspecified", config: config("nvr-test"), admit: true},
		{name: "unspecified accepts missing config", environment: "unspecified", admit: true},
		{name: "default accepts named", config: config("prod"), admit: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 1}
			job := &pb.Job{
				JobId: key.JobID, AttemptIndex: key.AttemptIndex, RecoveryConfig: tt.config,
				// Backend ownership is deliberately opposite to APS admission.
				// It must not replace the job's declared routing environment.
				SchedulerMetadata: &structpb.Struct{Fields: map[string]*structpb.Value{
					"environment": structpb.NewStringValue("nvr-test"),
				}},
			}
			if tt.admit {
				job.SchedulerMetadata.Fields["environment"] = structpb.NewStringValue("other-backend")
			}
			reader := newMockJobReader(t)
			reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
			if tt.admit {
				// Missing accounting records must remain discoverable next tick.
				reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{key}).Return(nil, nil).Times(2)
			}
			client := &fakeControllerClient{getJobsResponses: map[string]*pb.GetJobsResponse{
				"": {Jobs: []*pb.Job{job}},
			}}
			seen := state.NewMemorySeenJobStore()
			controller := Controller{
				JobReader: reader, HeimdallClient: client, SeenJobs: seen,
				Options:        ControllerOptions{Environment: tt.environment},
				ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeObserve},
				ActionExecutor: newMockJobMutator(t),
			}
			for tick := range 2 {
				if _, err := controller.SyncOnce(context.Background()); err != nil {
					t.Fatalf("tick %d: SyncOnce() failed: %v", tick, err)
				}
				var want []state.SeenJobRecord
				if tt.admit {
					want = []state.SeenJobRecord{key}
				}
				assertSeenRecords(t, seen, want)
			}
			if len(client.getJobsCalls) != 1 {
				t.Fatalf("GetJobs calls = %d, want one bootstrap", len(client.getJobsCalls))
			}
			assertRequestsEqual(t, client.upsertRequests, nil)
		})
	}
}

func TestControllerBootstrapTimeoutRetainsOnlyMatchingAttempts(t *testing.T) {
	matchingEnvironment, rejectedEnvironment := "nvr-test", "prod"
	key := cluster.JobAttemptRef{JobID: "matching", AttemptIndex: 1}
	reader := newMockJobReader(t)
	reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
	reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{key}).
		Return(nil, context.DeadlineExceeded).Times(2)
	client := &fakeControllerClient{getJobsResponses: map[string]*pb.GetJobsResponse{
		"": {Jobs: []*pb.Job{
			{JobId: key.JobID, AttemptIndex: key.AttemptIndex, RecoveryConfig: &pb.JobRecoveryConfig{Environment: &matchingEnvironment}},
			{JobId: "production", RecoveryConfig: &pb.JobRecoveryConfig{Environment: &rejectedEnvironment}},
			{JobId: "untagged"},
		}},
	}}
	seen := state.NewMemorySeenJobStore()
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: seen,
		Options: ControllerOptions{Environment: matchingEnvironment},
	}
	for tick := range 2 {
		if _, err := controller.SyncOnce(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("tick %d: SyncOnce() error = %v, want deadline exceeded", tick, err)
		}
		assertSeenRecords(t, seen, []state.SeenJobRecord{key})
	}
	if len(client.getJobsCalls) != 1 {
		t.Fatalf("GetJobs calls = %d, want one bootstrap despite accounting timeouts", len(client.getJobsCalls))
	}
}

func TestControllerBootstrapEnvironmentDoesNotOverrideFreshSlurm(t *testing.T) {
	for _, tt := range []struct {
		name    string
		attempt int32
		later   bool
	}{
		{name: "same attempt at startup"},
		{name: "newer attempt at startup", attempt: 1},
		{name: "same attempt on later poll", later: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			oldEnvironment := "prod"
			job := managedJob(cluster.JobExecutionRecord{
				JobID: "12345", AttemptIndex: tt.attempt, WorkflowID: "test-workflow", State: cluster.JobStateRunning,
				EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "nvr-test", Specified: true, Valid: true},
			})
			reader := newMockJobReader(t)
			if tt.later {
				reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil)
			} else {
				reader.EXPECT().ActiveJobs(gomock.Any()).Return([]cluster.JobExecutionRecord{job}, nil)
			}
			reader.EXPECT().ActiveJobs(gomock.Any()).Return([]cluster.JobExecutionRecord{job}, nil)
			client := &fakeControllerClient{getJobsResponses: map[string]*pb.GetJobsResponse{
				"": {Jobs: []*pb.Job{{JobId: job.JobID, RecoveryConfig: &pb.JobRecoveryConfig{Environment: &oldEnvironment}}}},
			}}
			seen := state.NewMemorySeenJobStore()
			controller := Controller{
				JobReader: reader, HeimdallClient: client, SeenJobs: seen,
				Environment: "nvr-test", ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve},
			}
			for range 2 {
				if _, err := controller.SyncOnce(context.Background()); err != nil {
					t.Fatalf("SyncOnce() failed: %v", err)
				}
			}
			assertSeenRecords(t, seen, []state.SeenJobRecord{jobAttemptRef(job)})
			wantUpserts := 2
			if tt.later {
				wantUpserts = 1
			}
			if len(client.upsertRequests) != wantUpserts {
				t.Fatalf("upsert count = %d, want %d", len(client.upsertRequests), wantUpserts)
			}
			for _, request := range client.upsertRequests {
				if request.GetJobId() != job.JobID || request.GetAttemptIndex() != tt.attempt {
					t.Fatalf("upsert = %v, want fresh Slurm attempt %v", request, jobAttemptRef(job))
				}
			}
		})
	}
}

func TestControllerBootstrapAdviceAccountingRespectsIndependentSources(t *testing.T) {
	for _, tt := range []struct {
		name        string
		environment string
		seen        bool
		accounting  bool
		wantSkip    AdviceSkipReason
	}{
		{name: "foreign advice cannot restore rejected backend candidate", environment: "prod", wantSkip: AdviceSkipEnvironmentMismatch},
		{name: "seen attempt survives foreign advice", environment: "prod", seen: true, accounting: true, wantSkip: AdviceSkipEnvironmentMismatch},
		{name: "owned advice independently discovers attempt", environment: "nvr-test", accounting: true, wantSkip: AdviceSkipMissingSnapshotJob},
		{name: "legacy advice without environment remains discoverable", accounting: true, wantSkip: AdviceSkipMissingSnapshotJob},
	} {
		t.Run(tt.name, func(t *testing.T) {
			key := cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 1}
			production := "prod"
			advice := newAdvice(t, adviceFixture{
				adviceID: "release", jobID: key.JobID, attemptIndex: key.AttemptIndex,
				state:    adviceState(pb.AdviceState_ADVICE_STATE_RELEASED),
				metadata: map[string]any{"scheduler": cluster.SchedulerSlurm, "environment": tt.environment},
			})
			client := &fakeControllerClient{
				getJobsResponses: map[string]*pb.GetJobsResponse{"": {Jobs: []*pb.Job{{
					JobId: key.JobID, AttemptIndex: key.AttemptIndex,
					RecoveryConfig: &pb.JobRecoveryConfig{Environment: &production},
				}}}},
				listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{advice}}},
			}
			reader := newMockJobReader(t)
			reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
			if tt.accounting {
				reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{key}).Return(nil, nil).Times(2)
			}
			var seenKeys []state.SeenJobRecord
			if tt.seen {
				seenKeys = []state.SeenJobRecord{key}
			}
			seen := state.NewMemorySeenJobStore(seenKeys...)
			controller := Controller{
				JobReader: reader, HeimdallClient: client, SeenJobs: seen,
				Options:        ControllerOptions{Environment: "nvr-test"},
				ActionExecutor: newMockJobMutator(t), ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve},
			}
			for range 2 {
				result, err := controller.SyncOnce(context.Background())
				if err != nil {
					t.Fatalf("SyncOnce() failed: %v", err)
				}
				if len(result.Advice.Skips) != 1 || result.Advice.Skips[0].Reason != tt.wantSkip {
					t.Fatalf("Advice skips = %v, want %s", result.Advice.Skips, tt.wantSkip)
				}
				assertSeenRecords(t, seen, seenKeys)
			}
			assertRequestsEqual(t, client.upsertRequests, nil)
		})
	}
}
