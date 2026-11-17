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

func TestControllerArrayCommandRetrySurvivesBootstrap(t *testing.T) {
	ctx := context.Background()
	failed := managedJob(cluster.JobExecutionRecord{
		JobID: "4200_0", AttemptIndex: 1, WorkflowID: "array", State: cluster.JobStateFailed,
	})
	sibling := managedJob(cluster.JobExecutionRecord{
		JobID: "4200_1", WorkflowID: "array", State: cluster.JobStateFailed,
	})
	restart := newAdvice(t, adviceFixture{
		adviceID: "retry", jobID: failed.JobID, attemptIndex: failed.AttemptIndex,
		state:    adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	siblingRestart := newAdvice(t, adviceFixture{
		adviceID: "sibling", jobID: sibling.JobID,
		state:    adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{
		"": {Advice: []*pb.Advice{restart, siblingRestart}},
	}}
	reader := newMockJobReader(t)
	mutator := newMockJobMutator(t)
	commandErr := errors.New("temporary requeue failure")
	gomock.InOrder(
		mutator.EXPECT().JobRequeueHold(gomock.Any(), failed.JobID).Return(commandErr),
		mutator.EXPECT().JobRequeueHold(gomock.Any(), sibling.JobID).Return(nil),
		mutator.EXPECT().JobRelease(gomock.Any(), sibling.JobID).Return(nil),
		mutator.EXPECT().JobRequeueHold(gomock.Any(), failed.JobID).Return(nil),
		mutator.EXPECT().JobRelease(gomock.Any(), failed.JobID).Return(nil),
	)
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(),
		ActionExecutor: mutator, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{jobAttemptRef(failed), jobAttemptRef(sibling)}, []cluster.JobExecutionRecord{failed, sibling})
	if _, err := controller.SyncOnce(ctx); !errors.Is(err, commandErr) {
		t.Fatalf("first tick error = %v, want transient command failure", err)
	}
	if len(client.statusUpdates) != 0 {
		t.Fatalf("command results must not establish convergence: %#v", client.statusUpdates)
	}

	siblingSuccessor := sibling
	siblingSuccessor.AttemptIndex = 1
	siblingSuccessor.State = cluster.JobStateRunning
	client.getJobsResponses = map[string]*pb.GetJobsResponse{"": {Jobs: []*pb.Job{
		{JobId: sibling.JobID, AttemptIndex: 1, State: pb.JobStatus_JOB_STATUS_RUNNING},
	}}}
	controller = Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(),
		ActionExecutor: mutator, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{jobAttemptRef(failed), jobAttemptRef(sibling), jobAttemptRef(siblingSuccessor)},
		[]cluster.JobExecutionRecord{failed, sibling, siblingSuccessor})
	if _, err := controller.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(client.getJobsCalls) != 2 {
		t.Fatalf("bootstrap calls = %d, want one per controller", len(client.getJobsCalls))
	}
	if len(client.statusUpdates) != 1 || client.statusUpdates[0].AdviceID != "sibling" || client.statusUpdates[0].Status.GetDisposition() != pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED {
		t.Fatalf("only sibling successor is observed: %#v", client.statusUpdates)
	}
	failedPublications := 0
	for _, request := range client.upsertRequests {
		if request.GetJobId() == failed.JobID {
			failedPublications++
			if request.GetAttemptIndex() != 1 {
				t.Fatalf("command retry advanced attempt: %v", request)
			}
		}
	}
	if failedPublications != 2 {
		t.Fatalf("failed-task publications = %d, want same attempt on both ticks", failedPublications)
	}
}

func TestControllerDelayedAdviceUsesAccountingSuccessor(t *testing.T) {
	for _, identity := range []struct {
		name       string
		jobID      string
		otherJobID string
	}{
		{name: "standalone", jobID: "4200", otherJobID: "4201"},
		{name: "array task", jobID: "4200_0", otherJobID: "4200_1"},
	} {
		t.Run(identity.name, func(t *testing.T) {
			tests := []struct {
				name             string
				state            pb.AdviceState
				successorAttempt int32
				reason           pb.AdviceReconciliationReason
				sourcePurged     bool
			}{
				{"restart already advanced", pb.AdviceState_ADVICE_STATE_RESTARTED, 1, pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED, false},
				{"restart source purged", pb.AdviceState_ADVICE_STATE_RESTARTED, 1, pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED, true},
				{"restart too old", pb.AdviceState_ADVICE_STATE_RESTARTED, 2, pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_STALE_ATTEMPT, false},
				{"terminate old attempt", pb.AdviceState_ADVICE_STATE_TERMINATED, 1, pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_STALE_ATTEMPT, false},
				{"release old attempt", pb.AdviceState_ADVICE_STATE_RELEASED, 1, pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_STALE_ATTEMPT, false},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					source := managedJob(cluster.JobExecutionRecord{
						JobID: identity.jobID, WorkflowID: "array", State: cluster.JobStateFailed,
					})
					successor := source
					successor.AttemptIndex = test.successorAttempt
					successor.State = cluster.JobStateRunning
					sibling := managedJob(cluster.JobExecutionRecord{
						JobID: identity.otherJobID, WorkflowID: "array", State: cluster.JobStateFailed,
					})
					reader := newMockJobReader(t)
					expectActiveJobs(reader, nil)
					accounting := []cluster.JobExecutionRecord{successor, sibling}
					if !test.sourcePurged {
						accounting = append(accounting, source)
					}
					expectFinalizedJobs(reader, []cluster.JobAttemptRef{{JobID: source.JobID}, {JobID: sibling.JobID}},
						accounting)
					advice := newAdvice(t, adviceFixture{
						adviceID: "delayed", jobID: source.JobID, state: adviceState(test.state),
						metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
					})
					siblingAdvice := newAdvice(t, adviceFixture{
						adviceID: "sibling", jobID: sibling.JobID, state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
						metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
					})
					client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{
						"": {Advice: []*pb.Advice{advice, siblingAdvice}},
					}}
					controller := Controller{
						JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(),
						ActionExecutor: newMockJobMutator(t), ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
					}
					result, err := controller.SyncOnce(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					assertSnapshotKeys(t, result.Snapshot, []cluster.JobAttemptRef{
						{JobID: source.JobID}, {JobID: source.JobID, AttemptIndex: test.successorAttempt}, {JobID: sibling.JobID},
					})
					if len(client.upsertRequests) != len(accounting) {
						t.Fatalf("upserts = %d, want %d observed accounting records", len(client.upsertRequests), len(accounting))
					}
					if test.sourcePurged {
						for _, request := range client.upsertRequests {
							if request.GetJobId() == source.JobID && request.GetAttemptIndex() == source.AttemptIndex {
								t.Fatalf("purged source was published: %v", request)
							}
						}
					}
					if len(client.statusUpdates) != 2 {
						t.Fatalf("status updates = %#v, want both Advice outcomes", client.statusUpdates)
					}
					for _, update := range client.statusUpdates {
						wantReason := pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED
						wantDisposition := pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED
						if update.AdviceID == "delayed" {
							wantReason = test.reason
							if test.reason == pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_STALE_ATTEMPT {
								wantDisposition = pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED
							}
						}
						if update.Status.GetReason() != wantReason || update.Status.GetDisposition() != wantDisposition {
							t.Fatalf("unexpected outcome for %s: %v", update.AdviceID, update.Status)
						}
					}
				})
			}

		})
	}
}

func TestControllerRejectsConflictingArrayBootstrap(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"slurm_array_job_id": "4200", "slurm_array_task_id": "1"})
	if err != nil {
		t.Fatal(err)
	}
	sibling := cluster.JobAttemptRef{JobID: "4200_1", AttemptIndex: 2}
	reader := newMockJobReader(t)
	reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
	reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{sibling}).Return(nil, nil).Times(2)
	client := &fakeControllerClient{getJobsResponses: map[string]*pb.GetJobsResponse{
		"": {Jobs: []*pb.Job{
			{JobId: "4200_0", SchedulerMetadata: metadata},
			{JobId: sibling.JobID, AttemptIndex: sibling.AttemptIndex},
		}},
	}}
	seen := state.NewMemorySeenJobStore()
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: seen,
		ActionExecutor: newMockJobMutator(t), ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve},
	}
	for range 2 {
		if _, err := controller.SyncOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		assertSeenRecords(t, seen, []state.SeenJobRecord{sibling})
	}
	if len(client.getJobsCalls) != 1 {
		t.Fatalf("bootstrap calls = %d, want 1", len(client.getJobsCalls))
	}
}

func TestControllerRejectsConflictingArrayAdvice(t *testing.T) {
	for _, sourceActive := range []bool{false, true} {
		name := "missing source"
		if sourceActive {
			name = "active source"
		}
		t.Run(name, func(t *testing.T) {
			jobs := []cluster.JobExecutionRecord{managedJob(cluster.JobExecutionRecord{
				JobID: "4200_1", WorkflowID: "array", State: cluster.JobStateRunning,
			})}
			if sourceActive {
				jobs = append(jobs, managedJob(cluster.JobExecutionRecord{
					JobID: "4200_0", WorkflowID: "array", State: cluster.JobStateCompleted,
				}))
			}
			reader := newMockJobReader(t)
			expectActiveJobs(reader, jobs)
			if sourceActive {
				expectFinalizedJobs(reader, []cluster.JobAttemptRef{{JobID: "4200_0"}}, nil)
			}
			bad := newAdvice(t, adviceFixture{
				adviceID: "bad", jobID: "4200_0", state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
				metadata: map[string]any{"scheduler": cluster.SchedulerSlurm, "slurm_job_id": "4200_1"},
			})
			good := newAdvice(t, adviceFixture{
				adviceID: "good", jobID: "4200_1", state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
				metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
			})
			client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{bad, good}}}}
			mutator := newMockJobMutator(t)
			mutator.EXPECT().JobKill(gomock.Any(), "4200_1").Return(nil)
			controller := Controller{
				JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(),
				ActionExecutor: mutator, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			}
			result, err := controller.SyncOnce(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(client.statusUpdates) != 0 {
				t.Fatalf("unexpected terminal statuses: %#v", client.statusUpdates)
			}
			if len(result.Advice.Skips) != 1 || result.Advice.Skips[0].Reason != AdviceSkipInvalidIdentity {
				t.Fatalf("advice skips = %#v", result.Advice.Skips)
			}
		})
	}
}

func TestControllerPrefersActiveStateAtEqualAttempt(t *testing.T) {
	for _, identity := range []struct {
		name       string
		jobID      string
		otherJobID string
	}{
		{name: "standalone", jobID: "4200", otherJobID: "4201"},
		{name: "array task", jobID: "4200_0", otherJobID: "4200_1"},
	} {
		t.Run(identity.name, func(t *testing.T) {
			for _, accountingAttempt := range []int32{0, 1} {
				name := "equal attempt"
				if accountingAttempt == 1 {
					name = "newer accounting attempt"
				}
				t.Run(name, func(t *testing.T) {
					active := managedJob(cluster.JobExecutionRecord{
						JobID: identity.jobID, WorkflowID: "array", State: cluster.JobStateRunning, Reason: "NODE_FAIL",
					})
					sibling := managedJob(cluster.JobExecutionRecord{JobID: identity.otherJobID, WorkflowID: "array", State: cluster.JobStateRunning})
					accounting := active
					accounting.AttemptIndex = accountingAttempt
					accounting.State = cluster.JobStateFailed
					reader := newMockJobReader(t)
					expectActiveJobs(reader, []cluster.JobExecutionRecord{active, sibling})
					requests := []cluster.JobAttemptRef{{JobID: active.JobID}}
					seen := state.NewMemorySeenJobStore()
					if accountingAttempt == 1 {
						newer := cluster.JobAttemptRef{JobID: active.JobID, AttemptIndex: 1}
						requests = append(requests, newer)
						seen = state.NewMemorySeenJobStore(newer)
					}
					expectFinalizedJobs(reader, requests, []cluster.JobExecutionRecord{accounting})
					advice := newAdvice(t, adviceFixture{
						adviceID: "terminate", jobID: active.JobID, state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
						metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
					})
					client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{advice}}}}
					mutator := newMockJobMutator(t)
					if accountingAttempt == 0 {
						mutator.EXPECT().JobKill(gomock.Any(), active.JobID).Return(nil)
					}
					controller := Controller{
						JobReader: reader, HeimdallClient: client, SeenJobs: seen,
						ActionExecutor: mutator, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
					}
					if _, err := controller.SyncOnce(context.Background()); err != nil {
						t.Fatal(err)
					}
					if accountingAttempt == 0 && len(client.statusUpdates) != 0 {
						t.Fatalf("command success must not converge: %#v", client.statusUpdates)
					}
					if accountingAttempt == 1 && (len(client.statusUpdates) != 1 || client.statusUpdates[0].Status.GetReason() != pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_STALE_ATTEMPT) {
						t.Fatalf("newer accounting must protect successor: %#v", client.statusUpdates)
					}
				})
			}

		})
	}
}
