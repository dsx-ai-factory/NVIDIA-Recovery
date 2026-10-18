// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"errors"
	"slices"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestControllerUserBootstrapRetainsAllEnvironmentAdmittedJobs(t *testing.T) {
	blue, green := "blue", "green"
	var jobs []*pb.Job
	for _, candidate := range []struct {
		id          string
		user        *structpb.Value
		environment *string
	}{
		{id: "alice", user: structpb.NewStringValue("alice"), environment: &blue},
		{id: "malformed", user: structpb.NewNumberValue(123), environment: &blue},
		{id: "missing", environment: &blue},
		{id: "other", user: structpb.NewStringValue("bob"), environment: &blue},
		{id: "wrong-environment", user: structpb.NewStringValue("alice"), environment: &green},
	} {
		job := &pb.Job{JobId: candidate.id, RecoveryConfig: &pb.JobRecoveryConfig{Environment: candidate.environment}}
		if candidate.user != nil {
			job.SchedulerMetadata = &structpb.Struct{Fields: map[string]*structpb.Value{"slurm_user": candidate.user}}
		}
		jobs = append(jobs, job)
	}
	want := []cluster.JobAttemptRef{{JobID: "alice"}, {JobID: "malformed"}, {JobID: "missing"}, {JobID: "other"}}
	reader := newMockJobReader(t)
	reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
	// A failed accounting query must retain even jobs whose username cannot be
	// reconstructed at bootstrap, so fresh scheduler data can resolve their mode.
	reader.EXPECT().FinalizedJobs(gomock.Any(), want).Return(nil, context.DeadlineExceeded).Times(2)
	client := &fakeControllerClient{getJobsResponses: map[string]*pb.GetJobsResponse{"": {Jobs: jobs}}}
	seen := state.NewMemorySeenJobStore()
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: seen,
		Options:        ControllerOptions{Environment: blue, UserAllowlist: []string{"alice"}},
		ActionExecutor: newMockJobMutator(t),
	}
	for tick := range 2 {
		if _, err := controller.SyncOnce(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("tick %d: error = %v, want accounting timeout", tick, err)
		}
		assertSeenRecords(t, seen, want)
	}
	if len(client.getJobsCalls) != 1 {
		t.Fatalf("GetJobs calls = %d, want one bootstrap", len(client.getJobsCalls))
	}
}

func TestControllerUserMonitoringAndAdviceActionSelection(t *testing.T) {
	var jobs []cluster.JobExecutionRecord
	var advice []*pb.Advice
	for _, candidate := range []struct {
		id, user, environment string
		explicitMode          cluster.JobMode
	}{
		{id: "selected", user: "alice", environment: "blue"},
		{id: "unselected", user: "bob", environment: "blue"},
		{id: "unknown", environment: "blue"},
		{id: "explicit-act", user: "bob", environment: "blue", explicitMode: cluster.JobModeAct},
		{id: "explicit-observe", user: "alice", environment: "blue", explicitMode: cluster.JobModeObserve},
		{id: "explicit-ignore", user: "alice", environment: "blue", explicitMode: cluster.JobModeIgnore},
		{id: "wrong-environment", user: "alice", environment: "green", explicitMode: cluster.JobModeAct},
	} {
		job := managedJob(cluster.JobExecutionRecord{
			JobID: candidate.id, User: candidate.user, WorkflowID: "workflow", State: cluster.JobStateRunning,
			EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: candidate.environment, Specified: true, Valid: true},
		})
		if candidate.explicitMode != "" {
			job.ModeRequest = cluster.JobModeRequest{Mode: candidate.explicitMode, Specified: true, Valid: true}
		}
		jobs = append(jobs, job)
		advice = append(advice, newAdvice(t, adviceFixture{
			adviceID: "terminate-" + job.JobID, jobID: job.JobID,
			state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
			// Deliberately disagree with fresh scheduler identity: Advice cannot select
			// active mode using its own stale username metadata.
			metadata: map[string]any{"scheduler": cluster.SchedulerSlurm, "slurm_user": "alice", "environment": "blue"},
		}))
	}
	reader := newMockJobReader(t)
	expectActiveJobs(reader, jobs)
	mutator := newMockJobMutator(t)
	mutator.EXPECT().JobKill(gomock.Any(), "selected").Return(nil)
	mutator.EXPECT().JobKill(gomock.Any(), "explicit-act").Return(nil)
	client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: advice}}}
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(),
		Options:    ControllerOptions{Environment: "blue", UserAllowlist: []string{"alice"}},
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve, AllowAct: true}, ActionExecutor: mutator,
	}
	if _, err := controller.SyncOnce(context.Background()); err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	var published []string
	for _, request := range client.upsertRequests {
		published = append(published, request.GetJobId())
	}
	slices.Sort(published)
	want := []string{"explicit-act", "explicit-observe", "selected", "unknown", "unselected"}
	if !slices.Equal(published, want) {
		t.Fatalf("published jobs = %v, want %v", published, want)
	}
	if len(client.statusUpdates) != 0 {
		t.Fatalf("issued actions and passive plans must await fresh confirmation: statuses=%v", client.statusUpdates)
	}
}

func TestControllerUserAdviceCandidatesUseFreshAccountingPolicy(t *testing.T) {
	for _, tt := range []struct {
		name         string
		metadataUser any
		freshUser    string
		explicitAct  bool
		wantAct      bool
	}{
		{name: "outside list is still queried", metadataUser: "bob", freshUser: "bob"},
		{name: "unknown metadata is still queried", freshUser: "alice", wantAct: true},
		{name: "malformed metadata is still queried", metadataUser: 123, freshUser: "alice", wantAct: true},
		{name: "fresh username overrides stale selection", metadataUser: "alice", freshUser: "bob"},
		{name: "explicit act outside list", metadataUser: "bob", freshUser: "bob", explicitAct: true, wantAct: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			job := managedJob(cluster.JobExecutionRecord{JobID: "job", User: tt.freshUser, WorkflowID: "workflow", State: cluster.JobStatePending, Held: true, Reason: "JobHeldUser"})
			if tt.explicitAct {
				job.ModeRequest = cluster.JobModeRequest{Mode: cluster.JobModeAct, Specified: true, Valid: true}
			}
			metadata := map[string]any{"scheduler": cluster.SchedulerSlurm}
			if tt.metadataUser != nil {
				metadata["slurm_user"] = tt.metadataUser
			}
			advice := newAdvice(t, adviceFixture{adviceID: "release", jobID: job.JobID, state: adviceState(pb.AdviceState_ADVICE_STATE_RELEASED), metadata: metadata})
			reader := newMockJobReader(t)
			expectActiveJobs(reader, nil)
			expectFinalizedJobs(reader, []cluster.JobAttemptRef{jobAttemptRef(job)}, []cluster.JobExecutionRecord{job})
			mutator := newMockJobMutator(t)
			if tt.wantAct {
				mutator.EXPECT().JobRelease(gomock.Any(), job.JobID).Return(nil)
			}
			client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{advice}}}}
			controller := Controller{JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(), Options: ControllerOptions{UserAllowlist: []string{"alice"}}, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve, AllowAct: true}, ActionExecutor: mutator}
			result, err := controller.SyncOnce(context.Background())
			if err != nil {
				t.Fatalf("SyncOnce() failed: %v", err)
			}
			if len(result.Snapshot.AccountingJobs) != 1 || len(client.upsertRequests) != 1 {
				t.Fatalf("accounting/publication = %v / %v, want candidate monitored", result.Snapshot.AccountingJobs, client.upsertRequests)
			}
		})
	}
}

func TestControllerUserLaterExplicitActRecoversFailedJobOnce(t *testing.T) {
	job := managedJob(cluster.JobExecutionRecord{JobID: "job", User: "bob", WorkflowID: "workflow", State: cluster.JobStateFailed, RecoveryConfig: &cluster.RecoveryConfig{AutoResumeMode: cluster.AutoResumeModeRequeue}})
	reader := newMockJobReader(t)
	mutator := newMockJobMutator(t)
	mutator.EXPECT().JobRequeueHold(gomock.Any(), job.JobID).Return(nil).Times(1)
	controller := Controller{JobReader: reader, HeimdallClient: &fakeControllerClient{}, SeenJobs: state.NewMemorySeenJobStore(), Options: ControllerOptions{EnableEagerRequeue: true, UserAllowlist: []string{"alice"}}, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve, AllowAct: true}, ActionExecutor: mutator}
	for tick := range 3 {
		if tick == 1 {
			job.ModeRequest = cluster.JobModeRequest{Mode: cluster.JobModeAct, Specified: true, Valid: true}
		}
		expectActiveJobs(reader, []cluster.JobExecutionRecord{job})
		expectFinalizedJobs(reader, []cluster.JobAttemptRef{jobAttemptRef(job)}, []cluster.JobExecutionRecord{job})
		result, err := controller.SyncOnce(context.Background())
		if err != nil {
			t.Fatalf("tick %d: SyncOnce() failed: %v", tick, err)
		}
		if tick < 2 && (len(result.FailedJobs.Requeues) != 1 || result.FailedJobs.Requeues[0].Applied != (tick == 1)) {
			t.Fatalf("tick %d: requeues = %+v, want one with applied=%t", tick, result.FailedJobs.Requeues, tick == 1)
		}
		if tick == 2 && len(result.FailedJobs.Requeues) != 0 {
			t.Fatalf("successful recovery repeated: %+v", result.FailedJobs.Requeues)
		}
	}
}

func TestControllerUserNewerUnselectedAttemptBlocksStaleAdvice(t *testing.T) {
	old := managedJob(cluster.JobExecutionRecord{JobID: "job", User: "alice", WorkflowID: "workflow", State: cluster.JobStateRunning})
	current := old
	current.AttemptIndex, current.User = 1, "bob"
	advice := newAdvice(t, adviceFixture{adviceID: "terminate-old", jobID: old.JobID, state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED), metadata: map[string]any{"scheduler": cluster.SchedulerSlurm, "slurm_user": "alice"}})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{old, current})
	client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{advice}}}}
	controller := Controller{JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(), Options: ControllerOptions{UserAllowlist: []string{"alice"}}, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve, AllowAct: true}, ActionExecutor: newMockJobMutator(t)}
	result, err := controller.SyncOnce(context.Background())
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	if len(result.Advice.Skips) != 1 || result.Advice.Skips[0].Reason != AdviceSkipStaleAttempt {
		t.Fatalf("Advice skips = %v, want stale attempt", result.Advice.Skips)
	}
	if !slices.ContainsFunc(client.upsertRequests, func(request *pb.CreateOrUpdateJobRequest) bool { return request.GetAttemptIndex() == 1 }) {
		t.Fatalf("published = %v, missing latest monitored attempt", client.upsertRequests)
	}
}
