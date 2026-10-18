// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"go.uber.org/mock/gomock"
)

func TestControllerPublishesPartialAccountingAndRetriesUnresolvedJobs(t *testing.T) {
	now := time.Unix(1700000000, 0)
	expiredAt := now.Add(-time.Hour)
	active := managedJob(cluster.JobExecutionRecord{JobID: "active", WorkflowID: "workflow", State: cluster.JobStateRunning})
	completed := managedJob(cluster.JobExecutionRecord{JobID: "completed", WorkflowID: "workflow", State: cluster.JobStateCompleted})
	failed := managedJob(cluster.JobExecutionRecord{JobID: "failed", WorkflowID: "workflow", State: cluster.JobStateFailed})
	failed.ModeRequest = cluster.JobModeRequest{Mode: cluster.JobModeAct, Specified: true, Valid: true}
	missing := managedJob(cluster.JobExecutionRecord{JobID: "missing", WorkflowID: "workflow", State: cluster.JobStateCompleted})
	accountingErr := errors.New("one accounting request failed")
	reader := newMockJobReader(t)
	reader.EXPECT().ActiveJobs(gomock.Any()).Return([]cluster.JobExecutionRecord{active}, nil).Times(2)
	firstAccounting := reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{
		jobAttemptRef(completed), jobAttemptRef(failed), jobAttemptRef(missing),
	}).Return([]cluster.JobExecutionRecord{completed, failed}, accountingErr)
	reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{
		jobAttemptRef(failed), jobAttemptRef(missing),
	}).Return([]cluster.JobExecutionRecord{failed, missing}, nil).After(firstAccounting)
	expiredAdvice := newAdvice(t, adviceFixture{
		adviceID: "expired", jobID: active.JobID, createdAt: &expiredAt,
		state:    adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	client := &fakeControllerClient{
		getJobsResponses: map[string]*pb.GetJobsResponse{"": {Jobs: []*pb.Job{
			{JobId: completed.JobID}, {JobId: failed.JobID}, {JobId: missing.JobID},
		}}},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{expiredAdvice}}},
	}
	seen := state.NewMemorySeenJobStore()
	mutator := newMockJobMutator(t)
	observer := &recordingObserver{}
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: seen, ActionExecutor: mutator,
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Options:    ControllerOptions{AdviceMaxAge: time.Minute, EnableEagerRequeue: true},
		Observer:   observer, Now: func() time.Time { return now },
	}

	result, err := controller.SyncOnce(context.Background())
	if !errors.Is(err, accountingErr) {
		t.Fatalf("first SyncOnce() error = %v, want accounting failure", err)
	}
	var published []string
	for _, request := range client.upsertRequests {
		published = append(published, request.GetJobId())
	}
	slices.Sort(published)
	if !slices.Equal(published, []string{active.JobID, completed.JobID, failed.JobID}) {
		t.Fatalf("published jobs = %v, want every successful fresh observation", published)
	}
	if len(result.Snapshot.AccountingJobs) != 2 {
		t.Fatalf("accounting snapshot = %v, want both successful results", result.Snapshot.AccountingJobs)
	}
	assertSeenRecords(t, seen, []state.SeenJobRecord{jobAttemptRef(active), jobAttemptRef(failed), jobAttemptRef(missing)})
	if len(result.FailedJobs.Requeues) != 0 || len(result.Advice.TerminalOutcomes) != 0 || len(client.statusUpdates) != 0 {
		t.Fatalf("incomplete accounting must defer recovery and expired Advice: failed=%+v advice=%+v statuses=%+v", result.FailedJobs, result.Advice, client.statusUpdates)
	}
	if observer.observations[0].Outcome != ObservationOutcomeError {
		t.Fatalf("sync outcome = %s, want accounting failure reported", observer.observations[0].Outcome)
	}
	assertSourceObservation(t, observer.observations[0].Sources, SourceSlurmAccounting, ObservationOutcomeError, 0)
	assertStageObservation(t, observer.observations[0].Stages, StageJobStatusPolling, ObservationOutcomeError)
	assertStageObservation(t, observer.observations[0].Stages, StageJobReconciliation, ObservationOutcomeSuccess)

	// No scheduler call is permitted until a complete accounting read succeeds.
	mutator.EXPECT().JobRequeueHold(gomock.Any(), failed.JobID).Return(nil)
	result, err = controller.SyncOnce(context.Background())
	if err != nil {
		t.Fatalf("retry SyncOnce() failed: %v", err)
	}
	if len(result.FailedJobs.Requeues) != 1 || !result.FailedJobs.Requeues[0].Applied {
		t.Fatalf("retry recoveries = %+v, want the retained failed job recovered", result.FailedJobs.Requeues)
	}
	if len(client.statusUpdates) != 1 || client.statusUpdates[0].Status.GetReason() != pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_ADVICE_EXPIRED {
		t.Fatalf("retry Advice statuses = %+v, want deferred expiration published", client.statusUpdates)
	}
	assertSeenRecords(t, seen, []state.SeenJobRecord{jobAttemptRef(active)})
	if len(client.getJobsCalls) != 1 {
		t.Fatalf("bootstrap reads = %d, want one; retries must use retained seen jobs", len(client.getJobsCalls))
	}
}

func TestControllerPreservesAccountingAndPublicationErrors(t *testing.T) {
	completed := managedJob(cluster.JobExecutionRecord{JobID: "completed", WorkflowID: "workflow", State: cluster.JobStateCompleted})
	keys := []cluster.JobAttemptRef{jobAttemptRef(completed), {JobID: "missing"}}
	accountingErr := errors.New("accounting unavailable")
	publicationErr := errors.New("job publication unavailable")
	reader := newMockJobReader(t)
	expectActiveJobs(reader, nil)
	reader.EXPECT().FinalizedJobs(gomock.Any(), keys).Return([]cluster.JobExecutionRecord{completed}, accountingErr)
	client := &fakeControllerClient{upsertErrByJobID: map[string]error{completed.JobID: publicationErr}}
	seen := state.NewMemorySeenJobStore(keys...)
	controller := Controller{JobReader: reader, HeimdallClient: client, SeenJobs: seen}

	_, err := controller.SyncOnce(context.Background())
	if !errors.Is(err, accountingErr) || !errors.Is(err, publicationErr) {
		t.Fatalf("SyncOnce() error = %v, want both accounting and publication failures", err)
	}
	if len(client.upsertRequests) != 1 || client.upsertRequests[0].GetJobId() != completed.JobID {
		t.Fatalf("publication requests = %v, want the successful accounting observation attempted", client.upsertRequests)
	}
	assertSeenRecords(t, seen, keys)
}

func TestControllerDoesNotPublishAccountingAfterParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	active := managedJob(cluster.JobExecutionRecord{JobID: "active", WorkflowID: "workflow", State: cluster.JobStateRunning})
	completed := managedJob(cluster.JobExecutionRecord{JobID: "completed", WorkflowID: "workflow", State: cluster.JobStateCompleted})
	key := jobAttemptRef(completed)
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{active})
	reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{key}).DoAndReturn(
		func(context.Context, []cluster.JobAttemptRef) ([]cluster.JobExecutionRecord, error) {
			cancel()
			return []cluster.JobExecutionRecord{completed}, nil
		},
	)
	client := &fakeControllerClient{}
	seen := state.NewMemorySeenJobStore(key)
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: seen,
		ActionExecutor: newMockJobMutator(t), ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	_, err := controller.SyncOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SyncOnce() error = %v, want parent cancellation", err)
	}
	if len(client.upsertRequests) != 0 || len(client.statusUpdates) != 0 {
		t.Fatalf("publication after cancellation: jobs=%v advice=%v", client.upsertRequests, client.statusUpdates)
	}
	assertSeenRecords(t, seen, []state.SeenJobRecord{key})
}
