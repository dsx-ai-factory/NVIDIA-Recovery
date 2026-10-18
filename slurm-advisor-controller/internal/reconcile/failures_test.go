// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/mock/gomock"
)

func TestFailedJobRequeueSkipReasonHonorsAPSPolicy(t *testing.T) {
	t.Parallel()

	one := int32(1)
	tests := map[string]struct {
		job  cluster.JobExecutionRecord
		want FailedJobRequeueSkipReason
	}{
		"first failure is below explicit limit": {
			job:  failedJobWithRecovery(0, cluster.AutoResumeModeRequeue, &one),
			want: FailedJobRequeueSkipNone,
		},
		"requeued failure is at explicit limit": {
			job:  failedJobWithRecovery(1, cluster.AutoResumeModeRequeue, &one),
			want: FailedJobRequeueSkipLimitReached,
		},
		"attempt beyond explicit limit remains ineligible": {
			job:  failedJobWithRecovery(2, cluster.AutoResumeModeRequeue, &one),
			want: FailedJobRequeueSkipLimitReached,
		},
		"singleton dependency does not authorize failed recovery": {
			job:  failedJobWithRecovery(0, cluster.AutoResumeModeSingletonDependency, &one),
			want: FailedJobRequeueSkipRecoveryMode,
		},
		"omitted APS config preserves system default": {
			job:  managedJob(cluster.JobExecutionRecord{JobID: "123", State: cluster.JobStateFailed}),
			want: FailedJobRequeueSkipNone,
		},
		"omitted job limit preserves system-default recovery": {
			job:  failedJobWithRecovery(7, cluster.AutoResumeModeRequeue, nil),
			want: FailedJobRequeueSkipNone,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := failedJobRequeueSkipReason(tt.job); got != tt.want {
				t.Fatalf("failedJobRequeueSkipReason() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReconcileFailedJobsAppliesOnlyBelowAPSRequeueLimit(t *testing.T) {
	one := int32(1)
	snapshot := newControllerSnapshot(time.Now())
	addAccountingJob(&snapshot, failedJobWithRecovery(0, cluster.AutoResumeModeRequeue, &one))
	addAccountingJob(&snapshot, failedJobWithRecovery(1, cluster.AutoResumeModeRequeue, &one))
	executor := newMockJobMutator(t)
	executor.EXPECT().JobRequeueHold(gomock.Any(), "job-0").Return(nil)
	observation := &SyncObservation{}
	controller := Controller{
		ActionExecutor: executor,
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.reconcileFailedJobs(context.Background(), snapshot, observation)
	if err != nil {
		t.Fatalf("reconcileFailedJobs() failed: %v", err)
	}
	want := []FailedJobRequeue{
		{
			RequeueID:       "failed-job-requeuehold:job-0",
			JobID:           "job-0",
			MaxRequeueTimes: &one,
			Applied:         true,
			Mode:            cluster.JobModeAct,
		},
		{
			RequeueID:       "failed-job-requeuehold:job-1",
			JobID:           "job-1",
			AttemptIndex:    1,
			MaxRequeueTimes: &one,
			Mode:            cluster.JobModeAct,
			SkipReason:      FailedJobRequeueSkipLimitReached,
		},
	}
	if diff := cmp.Diff(want, result.Requeues); diff != "" {
		t.Fatalf("failed job requeues mismatch (-want +got):\n%s", diff)
	}
	assertFailedRequeueObservation(t, observation.FailedRequeues, ObservationOutcomePlanned, 1)
	assertFailedRequeueObservation(t, observation.FailedRequeues, ObservationOutcomeApplied, 1)
	assertFailedRequeueObservation(t, observation.FailedRequeues, ObservationOutcomeSkippedRequeueLimit, 1)
}

func TestReconcileFailedJobsRecordsRecoveryModeSkip(t *testing.T) {
	snapshot := newControllerSnapshot(time.Now())
	addAccountingJob(&snapshot, failedJobWithRecovery(0, cluster.AutoResumeModeSingletonDependency, nil))
	observation := &SyncObservation{}
	controller := Controller{
		ActionExecutor: newMockJobMutator(t),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.reconcileFailedJobs(context.Background(), snapshot, observation)
	if err != nil {
		t.Fatalf("reconcileFailedJobs() failed: %v", err)
	}
	if len(result.Requeues) != 1 || result.Requeues[0].SkipReason != FailedJobRequeueSkipRecoveryMode {
		t.Fatalf("failed job requeues = %#v, want recovery-mode skip", result.Requeues)
	}
	assertFailedRequeueObservation(t, observation.FailedRequeues, ObservationOutcomeSkippedRecoveryMode, 1)
}

func TestReconcileFailedJobsDoesNotRepeatCompletedAttemptDecision(t *testing.T) {
	one := int32(1)
	snapshot := newControllerSnapshot(time.Now())
	addAccountingJob(&snapshot, failedJobWithRecovery(1, cluster.AutoResumeModeRequeue, &one))
	controller := Controller{
		ActionExecutor: newMockJobMutator(t),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	// APS policy is immutable for the lifetime of a job, so a completed decision
	// for an exact attempt can be safely deduplicated within this process.
	first, err := controller.reconcileFailedJobs(context.Background(), snapshot, nil)
	if err != nil {
		t.Fatalf("first reconcileFailedJobs() failed: %v", err)
	}
	if len(first.Requeues) != 1 || first.Requeues[0].SkipReason != FailedJobRequeueSkipLimitReached {
		t.Fatalf("first failed-job decision = %#v, want retry-limit skip", first.Requeues)
	}

	second, err := controller.reconcileFailedJobs(context.Background(), snapshot, nil)
	if err != nil {
		t.Fatalf("second reconcileFailedJobs() failed: %v", err)
	}
	if len(second.Requeues) != 0 {
		t.Fatalf("second failed-job decision = %#v, want exact attempt to be deduplicated", second.Requeues)
	}
}

func TestReconcileFailedJobsAllowsNewAttemptAfterGivingUp(t *testing.T) {
	executor := newMockJobMutator(t)
	gomock.InOrder(
		executor.EXPECT().JobRequeueHold(gomock.Any(), "123").Return(cluster.ErrJobRequeuePermanent),
		executor.EXPECT().JobRequeueHold(gomock.Any(), "123").Return(nil),
	)
	controller := Controller{
		ActionExecutor: executor,
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	firstSnapshot := newControllerSnapshot(time.Now())
	addAccountingJob(&firstSnapshot, managedJob(cluster.JobExecutionRecord{JobID: "123", State: cluster.JobStateFailed}))
	if _, err := controller.reconcileFailedJobs(context.Background(), firstSnapshot, nil); !errors.Is(err, cluster.ErrJobRequeuePermanent) {
		t.Fatalf("first reconcileFailedJobs() error = %v, want permanent failure", err)
	}

	secondSnapshot := newControllerSnapshot(time.Now())
	addAccountingJob(&secondSnapshot, managedJob(cluster.JobExecutionRecord{JobID: "123", AttemptIndex: 1, State: cluster.JobStateFailed}))
	second, err := controller.reconcileFailedJobs(context.Background(), secondSnapshot, nil)
	if err != nil {
		t.Fatalf("second reconcileFailedJobs() failed: %v", err)
	}
	if len(second.Requeues) != 1 || !second.Requeues[0].Applied || second.Requeues[0].AttemptIndex != 1 {
		t.Fatalf("second failed-job decision = %#v, want distinct attempt one action", second.Requeues)
	}
}

func TestReconcileFailedJobsContinuesAfterActionFailure(t *testing.T) {
	firstErr := errors.Wrap(cluster.ErrJobRequeuePermanent, "first requeue failed")
	completedAt := time.Date(2026, time.September, 2, 18, 0, 0, 0, time.UTC)
	auditObserver := &recordingAuditObserver{}
	executor := newMockJobMutator(t)
	gomock.InOrder(
		executor.EXPECT().JobRequeueHold(gomock.Any(), "100").Return(firstErr),
		executor.EXPECT().JobRequeueHold(gomock.Any(), "200").Return(nil),
	)
	snapshot := newControllerSnapshot(time.Now())
	addAccountingJob(&snapshot, managedJob(cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateFailed}))
	addAccountingJob(&snapshot, managedJob(cluster.JobExecutionRecord{JobID: "200", State: cluster.JobStateFailed}))
	controller := Controller{
		ActionExecutor: executor,
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		AuditObserver:  auditObserver,
		Now:            func() time.Time { return completedAt },
	}

	result, err := controller.reconcileFailedJobs(context.Background(), snapshot, nil)
	if !errors.Is(err, firstErr) {
		t.Fatalf("reconcileFailedJobs() error = %v, want first action failure", err)
	}
	if len(result.Requeues) != 2 || result.Requeues[0].Applied || !result.Requeues[1].Applied {
		t.Fatalf("failed job requeues = %#v, want failed first action and applied second action", result.Requeues)
	}
	if len(auditObserver.failedJobActions) != 2 ||
		auditObserver.failedJobActions[0].Outcome != SchedulerActionOutcomeError ||
		!errors.Is(auditObserver.failedJobActions[0].Err, firstErr) ||
		auditObserver.failedJobActions[1].Outcome != SchedulerActionOutcomeSuccess ||
		!auditObserver.failedJobActions[1].CompletedAt.Equal(completedAt) {
		t.Fatalf("audit observations = %#v, want failed then successful returned commands", auditObserver.failedJobActions)
	}

	second, err := controller.reconcileFailedJobs(context.Background(), snapshot, nil)
	if err != nil {
		t.Fatalf("second reconcileFailedJobs() failed: %v", err)
	}
	if len(second.Requeues) != 0 || len(auditObserver.failedJobActions) != 2 {
		t.Fatalf("second decisions = %#v, audit = %#v, want completed attempts not repeated", second.Requeues, auditObserver.failedJobActions)
	}
}

func TestReconcileFailedJobsIgnoresNonFailedAndStaleAttempts(t *testing.T) {
	snapshot := newControllerSnapshot(time.Now())
	addAccountingJob(&snapshot, managedJob(cluster.JobExecutionRecord{
		JobID:        "stale",
		AttemptIndex: 0,
		State:        cluster.JobStateFailed,
	}))
	addActiveJob(&snapshot, managedJob(cluster.JobExecutionRecord{
		JobID:        "stale",
		AttemptIndex: 1,
		State:        cluster.JobStatePending,
	}))
	addAccountingJob(&snapshot, managedJob(cluster.JobExecutionRecord{
		JobID: "requeued",
		State: cluster.JobStateRequeued,
	}))
	controller := Controller{
		ActionExecutor: newMockJobMutator(t),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.reconcileFailedJobs(context.Background(), snapshot, nil)
	if err != nil {
		t.Fatalf("reconcileFailedJobs() failed: %v", err)
	}
	if len(result.Requeues) != 0 {
		t.Fatalf("failed job requeues = %#v, want none", result.Requeues)
	}
}

func TestReconcileFailedJobsRequeuesDuringCompletingEpilog(t *testing.T) {
	for name, reason := range map[string]string{
		"non-zero exit": slurmReasonNonZeroExitCode,
		"raised signal": slurmReasonRaisedSignal,
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := newControllerSnapshot(time.Now())
			addActiveJob(&snapshot, managedJob(cluster.JobExecutionRecord{
				JobID:          "epilog",
				State:          cluster.JobStateRunning,
				RawState:       slurmStateCompleting,
				Reason:         reason,
				RecoveryConfig: &cluster.RecoveryConfig{AutoResumeMode: cluster.AutoResumeModeRequeue},
			}))
			executor := newMockJobMutator(t)
			executor.EXPECT().JobRequeueHold(gomock.Any(), "epilog").Return(nil)
			controller := Controller{
				ActionExecutor: executor,
				ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			}

			result, err := controller.reconcileFailedJobs(context.Background(), snapshot, &SyncObservation{})
			if err != nil {
				t.Fatalf("reconcileFailedJobs() failed: %v", err)
			}
			if len(result.Requeues) != 1 || !result.Requeues[0].Applied || result.Requeues[0].JobID != "epilog" {
				t.Fatalf("failed job requeues = %#v, want one applied requeue for the completing job", result.Requeues)
			}
		})
	}
}

func TestReconcileFailedJobsIgnoresHealthyCompletingEpilog(t *testing.T) {
	snapshot := newControllerSnapshot(time.Now())
	addActiveJob(&snapshot, managedJob(cluster.JobExecutionRecord{
		JobID:          "healthy",
		State:          cluster.JobStateRunning,
		RawState:       slurmStateCompleting,
		Reason:         "None",
		RecoveryConfig: &cluster.RecoveryConfig{AutoResumeMode: cluster.AutoResumeModeRequeue},
	}))
	controller := Controller{
		ActionExecutor: newMockJobMutator(t),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.reconcileFailedJobs(context.Background(), snapshot, &SyncObservation{})
	if err != nil {
		t.Fatalf("reconcileFailedJobs() failed: %v", err)
	}
	if len(result.Requeues) != 0 {
		t.Fatalf("failed job requeues = %#v, want none for a healthy completing job", result.Requeues)
	}
}

func TestReconcileFailedJobsDeduplicatesCompletingThenFailed(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "successful requeue"},
		{name: "permanent failure", err: cluster.ErrJobRequeuePermanent},
	} {
		t.Run(tt.name, func(t *testing.T) {
			key := cluster.JobAttemptRef{JobID: "epilog"}
			executor := newMockJobMutator(t)
			executor.EXPECT().JobRequeueHold(gomock.Any(), key.JobID).Return(tt.err)
			seen := state.NewMemorySeenJobStore(key)
			controller := Controller{
				ActionExecutor: executor,
				SeenJobs:       seen,
				ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			}

			// A decision while COMPLETING must preserve final accounting while
			// suppressing a second action for the same attempt after it fails.
			job := managedJob(cluster.JobExecutionRecord{
				JobID: key.JobID, State: cluster.JobStateRunning,
				RawState: slurmStateCompleting, Reason: slurmReasonNonZeroExitCode,
				RecoveryConfig: &cluster.RecoveryConfig{AutoResumeMode: cluster.AutoResumeModeRequeue},
			})
			completing := newControllerSnapshot(time.Now())
			addActiveJob(&completing, job)
			if _, err := controller.reconcileFailedJobs(context.Background(), completing, nil); !errors.Is(err, tt.err) {
				t.Fatalf("first reconcileFailedJobs() error = %v, want %v", err, tt.err)
			}
			assertSeenRecords(t, seen, []state.SeenJobRecord{key})

			failed := newControllerSnapshot(time.Now())
			job.State = cluster.JobStateFailed
			job.RawState = "FAILED"
			addAccountingJob(&failed, job)
			second, err := controller.reconcileFailedJobs(context.Background(), failed, nil)
			if err != nil {
				t.Fatalf("second reconcileFailedJobs() failed: %v", err)
			}
			if len(second.Requeues) != 0 {
				t.Fatalf("second failed-job decision = %#v, want completed decision deduplicated", second.Requeues)
			}
			assertSeenRecords(t, seen, nil)
		})
	}
}

func TestReconcileFailedJobsCancellationPreservesRecovery(t *testing.T) {
	for _, duringBackoff := range []bool{false, true} {
		name := "scheduler command"
		if duringBackoff {
			name = "retry backoff"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			key := cluster.JobAttemptRef{JobID: "123"}
			seen := state.NewMemorySeenJobStore(key)
			snapshot := newControllerSnapshot(time.Now())
			addAccountingJob(&snapshot, managedJob(cluster.JobExecutionRecord{JobID: key.JobID, State: cluster.JobStateFailed}))
			executor := newMockJobMutator(t)
			executor.EXPECT().JobRequeueHold(gomock.Any(), key.JobID).DoAndReturn(func(context.Context, string) error {
				if duringBackoff {
					// Cancellation while the retry timer is waiting must stop the
					// next command without consuming the recovery decision.
					timer := time.AfterFunc(20*time.Millisecond, cancel)
					t.Cleanup(func() { timer.Stop() })
					return errors.New("temporary Slurm failure")
				}
				cancel()
				return context.Canceled
			})
			controller := Controller{
				ActionExecutor: executor, SeenJobs: seen,
				ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			}
			if _, err := controller.reconcileFailedJobs(ctx, snapshot, nil); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled reconciliation error = %v, want cancellation", err)
			}
			assertSeenRecords(t, seen, []state.SeenJobRecord{key})

			executor.EXPECT().JobRequeueHold(gomock.Any(), key.JobID).Return(nil)
			result, err := controller.reconcileFailedJobs(context.Background(), snapshot, nil)
			if err != nil || len(result.Requeues) != 1 || !result.Requeues[0].Applied {
				t.Fatalf("resumed reconciliation = %#v, %v, want successful recovery", result, err)
			}
			assertSeenRecords(t, seen, nil)
		})
	}
}

func TestReconcileFailedJobsRetriesCleanupWithoutRepeatingAction(t *testing.T) {
	key := cluster.JobAttemptRef{JobID: "123"}
	cleanupErr := errors.New("seen store unavailable")
	seen := &failOnceForgetSeenStore{SeenJobStore: state.NewMemorySeenJobStore(key), err: cleanupErr}
	executor := newMockJobMutator(t)
	executor.EXPECT().JobRequeueHold(gomock.Any(), key.JobID).Return(cluster.ErrJobRequeuePermanent)
	controller := Controller{
		ActionExecutor: executor, SeenJobs: seen,
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}
	snapshot := newControllerSnapshot(time.Now())
	addAccountingJob(&snapshot, managedJob(cluster.JobExecutionRecord{JobID: key.JobID, State: cluster.JobStateFailed}))
	if _, err := controller.reconcileFailedJobs(context.Background(), snapshot, nil); !errors.Is(err, cleanupErr) || !errors.Is(err, cluster.ErrJobRequeuePermanent) {
		t.Fatalf("first reconciliation error = %v, want action and cleanup failures", err)
	}
	assertSeenRecords(t, seen, []state.SeenJobRecord{key})
	result, err := controller.reconcileFailedJobs(context.Background(), snapshot, nil)
	if err != nil || len(result.Requeues) != 0 {
		t.Fatalf("cleanup retry = %#v, %v, want cleanup without another action", result, err)
	}
	assertSeenRecords(t, seen, nil)
}

type failOnceForgetSeenStore struct {
	state.SeenJobStore
	err error
}

func (s *failOnceForgetSeenStore) Forget(ctx context.Context, record state.SeenJobRecord) error {
	if s.err != nil {
		err := s.err
		s.err = nil
		return err
	}
	return s.SeenJobStore.Forget(ctx, record)
}

func failedJobWithRecovery(attempt int32, mode cluster.AutoResumeMode, limit *int32) cluster.JobExecutionRecord {
	return managedJob(cluster.JobExecutionRecord{
		JobID:        fmt.Sprintf("job-%d", attempt),
		AttemptIndex: attempt,
		State:        cluster.JobStateFailed,
		RecoveryConfig: &cluster.RecoveryConfig{
			AutoResumeMode:  mode,
			MaxRequeueTimes: limit,
		},
	})
}

func assertFailedRequeueObservation(t *testing.T, observations []OutcomeCountObservation, outcome ObservationOutcome, count int) {
	t.Helper()

	for _, observation := range observations {
		if observation.Outcome == outcome && observation.Count == count {
			return
		}
	}
	t.Fatalf("failed requeue observation %s/%d not found in %#v", outcome, count, observations)
}
