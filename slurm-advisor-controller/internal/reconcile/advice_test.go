// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	zapobserver "go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAdviceIsStale(t *testing.T) {
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	maxAge := 5 * time.Minute

	testCases := []struct {
		name                  string
		createdAt             *timestamppb.Timestamp
		desiredStateChangedAt *timestamppb.Timestamp
		maxAge                time.Duration
		wantStale             bool
	}{
		{name: "recent generation on old record", createdAt: timestamppb.New(now.Add(-time.Hour)), desiredStateChangedAt: timestamppb.New(now.Add(-maxAge + time.Second)), maxAge: maxAge},
		{name: "at cutoff", createdAt: timestamppb.New(now.Add(-maxAge)), maxAge: maxAge, wantStale: true},
		{name: "future", createdAt: timestamppb.New(now.Add(time.Minute)), maxAge: maxAge},
		{name: "missing timestamp", maxAge: maxAge, wantStale: true},
		{name: "invalid timestamp", createdAt: &timestamppb.Timestamp{Seconds: 253402300800}, maxAge: maxAge, wantStale: true},
		{name: "disabled", maxAge: 0},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			advice := &pb.Advice{
				CreatedAt:             testCase.createdAt,
				DesiredStateChangedAt: testCase.desiredStateChangedAt,
			}
			if got := adviceIsStale(advice, now, testCase.maxAge); got != testCase.wantStale {
				t.Fatalf("adviceIsStale() = %t, want %t", got, testCase.wantStale)
			}
		})
	}
}

func TestReleasedAdviceDoesNotExpire(t *testing.T) {
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	state := pb.AdviceState_ADVICE_STATE_RELEASED
	advice := &pb.Advice{
		CreatedAt: timestamppb.New(now.Add(-24 * time.Hour)),
		Advice:    &pb.AdviceRecommendation{State: &state},
	}

	if adviceIsStale(advice, now, 5*time.Minute) {
		t.Fatal("adviceIsStale() = true for RELEASED, want false")
	}
}

func TestTerminatedAdviceDoesNotExpire(t *testing.T) {
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	state := pb.AdviceState_ADVICE_STATE_TERMINATED
	advice := &pb.Advice{
		CreatedAt: timestamppb.New(now.Add(-24 * time.Hour)),
		Advice:    &pb.AdviceRecommendation{State: &state},
	}

	if adviceIsStale(advice, now, 5*time.Minute) {
		t.Fatal("adviceIsStale() = true for TERMINATED, want false")
	}
}

func TestReconcileAdvicePublishesExpiredGenerationAsAbandoned(t *testing.T) {
	now := time.Date(2026, time.September, 4, 12, 0, 0, 0, time.UTC)
	desiredStateChangedAt := now.Add(-6 * time.Minute)
	advice := newAdvice(t, adviceFixture{
		adviceID:  "advice-restart",
		jobID:     "12345",
		state:     adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:  map[string]any{"scheduler": cluster.SchedulerSlurm},
		createdAt: &now,
	})
	advice.Generation = 2
	advice.DesiredStateChangedAt = timestamppb.New(desiredStateChangedAt)
	snapshot := newControllerSnapshot(now)
	addAdvice(&snapshot, advice)
	client := &fakeControllerClient{}
	auditObserver := &recordingAuditObserver{}
	controller := Controller{
		HeimdallClient: client,
		AuditObserver:  auditObserver,
		Options: ControllerOptions{
			AdviceLoopTimeout: time.Hour,
			AdviceMaxAge:      5 * time.Minute,
			Environment:       "prod",
		},
	}

	result, err := controller.reconcileAdvice(context.Background(), snapshot, nil)
	if err != nil {
		t.Fatalf("reconcileAdvice() failed: %v", err)
	}
	if len(result.Skips) != 1 {
		t.Fatalf("reconcileAdvice() skips = %#v, want one", result.Skips)
	}
	skip := result.Skips[0]
	if skip.AdviceID != "advice-restart" || skip.JobID != "12345" || skip.Reason != AdviceSkipStaleAdvice {
		t.Fatalf("reconcileAdvice() skip = %#v, want expired advice-restart for job 12345", skip)
	}
	if len(result.TerminalOutcomes) != 1 {
		t.Fatalf("terminal outcomes = %#v, want one", result.TerminalOutcomes)
	}
	if len(client.statusUpdates) != 1 {
		t.Fatalf("status updates = %#v, want one", client.statusUpdates)
	}
	update := client.statusUpdates[0]
	if update.AdviceID != "advice-restart" {
		t.Fatalf("status advice ID = %q, want advice-restart", update.AdviceID)
	}
	if update.Status.GetObservedGeneration() != 2 {
		t.Fatalf("observed generation = %d, want 2", update.Status.GetObservedGeneration())
	}
	if update.Status.GetDisposition() != pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED {
		t.Fatalf("disposition = %s, want ABANDONED", update.Status.GetDisposition())
	}
	if update.Status.GetReason() != pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_ADVICE_EXPIRED {
		t.Fatalf("reason = %s, want ADVICE_EXPIRED", update.Status.GetReason())
	}
	if !update.Status.GetObservedAt().AsTime().Equal(now) {
		t.Fatalf("observed_at = %s, want %s", update.Status.GetObservedAt().AsTime(), now)
	}
	if len(auditObserver.terminalStatuses) != 1 ||
		auditObserver.terminalStatuses[0].Disposition != pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED ||
		auditObserver.terminalStatuses[0].Reason != pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_ADVICE_EXPIRED {
		t.Fatalf("terminal audit observations = %#v, want persisted abandonment", auditObserver.terminalStatuses)
	}
}

func TestReconcileAdviceAppliesExpiredTermination(t *testing.T) {
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	snapshot := terminateAdviceSnapshot(t, "100")
	snapshot.ObservedAt = now
	snapshot.Advice[cluster.JobAttemptRef{JobID: "100"}][0].CreatedAt = timestamppb.New(now.Add(-24 * time.Hour))

	executor := newMockJobMutator(t)
	executor.EXPECT().JobKill(gomock.Any(), "100").Return(nil)
	controller := Controller{
		ActionExecutor: executor,
		Options: ControllerOptions{
			AdviceLoopTimeout: time.Hour,
			AdviceMaxAge:      5 * time.Minute,
		},
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.reconcileAdvice(context.Background(), snapshot, nil)
	if err != nil {
		t.Fatalf("reconcileAdvice() failed: %v", err)
	}
	if len(result.Skips) != 0 {
		t.Fatalf("reconcileAdvice() skips = %#v, want none", result.Skips)
	}
	if len(result.TerminalOutcomes) != 0 {
		t.Fatalf("terminal outcomes = %#v after command success, want none until a fresh snapshot", result.TerminalOutcomes)
	}
}

func TestReconcileAdvicePublishesConvergenceFromFreshFinalState(t *testing.T) {
	observedAt := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	snapshot := terminateAdviceSnapshot(t, "100")
	snapshot.ObservedAt = observedAt
	key := cluster.JobAttemptRef{JobID: "100", AttemptIndex: 0}
	job := snapshot.ActiveJobs[key]
	job.State = cluster.JobStateCompleted
	snapshot.ActiveJobs[key] = job
	client := &fakeControllerClient{}
	auditObserver := &recordingAuditObserver{}
	controller := Controller{
		HeimdallClient: client,
		AuditObserver:  auditObserver,
		Options: ControllerOptions{
			AdviceLoopTimeout: time.Hour,
			Environment:       "prod",
		},
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.reconcileAdvice(context.Background(), snapshot, nil)
	if err != nil {
		t.Fatalf("reconcileAdvice() failed: %v", err)
	}
	if len(result.TerminalOutcomes) != 1 {
		t.Fatalf("terminal outcomes = %#v, want one", result.TerminalOutcomes)
	}
	if len(client.statusUpdates) != 1 {
		t.Fatalf("status updates = %#v, want one", client.statusUpdates)
	}
	update := client.statusUpdates[0]
	if update.AdviceID != "terminate-100" {
		t.Fatalf("status advice ID = %q, want terminate-100", update.AdviceID)
	}
	if update.Status.GetObservedGeneration() != 1 {
		t.Fatalf("observed generation = %d, want 1", update.Status.GetObservedGeneration())
	}
	if update.Status.GetDisposition() != pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED {
		t.Fatalf("disposition = %s, want CONVERGED", update.Status.GetDisposition())
	}
	if update.Status.GetReason() != pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED {
		t.Fatalf("reason = %s, want DESIRED_STATE_OBSERVED", update.Status.GetReason())
	}
	if !update.Status.GetObservedAt().AsTime().Equal(observedAt) {
		t.Fatalf("observed_at = %s, want %s", update.Status.GetObservedAt().AsTime(), observedAt)
	}
	if len(auditObserver.terminalStatuses) != 1 {
		t.Fatalf("terminal audit observations = %#v, want one", auditObserver.terminalStatuses)
	}
	terminalStatus := auditObserver.terminalStatuses[0]
	if terminalStatus.AdviceID != "terminate-100" ||
		terminalStatus.JobID != "100" ||
		terminalStatus.AttemptIndex != 0 ||
		terminalStatus.Generation != 1 ||
		terminalStatus.Disposition != pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED ||
		terminalStatus.Reason != pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED ||
		!terminalStatus.ObservedAt.Equal(observedAt) {
		t.Fatalf("terminal audit observation = %#v, want persisted convergence", terminalStatus)
	}
}

func TestReconcileAdviceReturnsStatusPublicationFailure(t *testing.T) {
	statusErr := errors.New("status unavailable")
	snapshot := terminateAdviceSnapshot(t, "100")
	key := cluster.JobAttemptRef{JobID: "100", AttemptIndex: 0}
	job := snapshot.ActiveJobs[key]
	job.State = cluster.JobStateCompleted
	snapshot.ActiveJobs[key] = job
	client := &fakeControllerClient{statusUpdateErr: statusErr}
	auditObserver := &recordingAuditObserver{}
	controller := Controller{
		HeimdallClient: client,
		AuditObserver:  auditObserver,
		Options:        ControllerOptions{AdviceLoopTimeout: time.Hour},
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.reconcileAdvice(context.Background(), snapshot, nil)

	if !errors.Is(err, statusErr) {
		t.Fatalf("reconcileAdvice() error = %v, want status publication failure", err)
	}
	if len(result.TerminalOutcomes) != 1 {
		t.Fatalf("terminal outcomes = %#v, want retained outcome for retry", result.TerminalOutcomes)
	}
	if len(auditObserver.terminalStatuses) != 0 {
		t.Fatalf("terminal audit observations = %#v after failed persistence, want none", auditObserver.terminalStatuses)
	}
}

func TestReconcileAdviceContinuesAfterActionFailure(t *testing.T) {
	firstErr := errors.New("first kill failed")
	executor := newMockJobMutator(t)
	gomock.InOrder(
		executor.EXPECT().JobKill(gomock.Any(), "100").Return(firstErr),
		executor.EXPECT().JobKill(gomock.Any(), "200").Return(nil),
	)
	snapshot := terminateAdviceSnapshot(t, "100", "200")
	controller := Controller{
		ActionExecutor: executor,
		Options:        ControllerOptions{AdviceLoopTimeout: time.Hour},
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}
	observation := SyncObservation{}

	_, err := controller.reconcileAdvice(context.Background(), snapshot, &observation)
	if !errors.Is(err, firstErr) {
		t.Fatalf("reconcileAdvice() error = %v, want first failure", err)
	}
	if observation.AttemptedAdviceReconciliations != 2 {
		t.Fatalf("AttemptedAdviceReconciliations = %d, want 2", observation.AttemptedAdviceReconciliations)
	}
}

func TestReconcileAdviceItemTimeoutDoesNotStarveLaterAdvice(t *testing.T) {
	executor := newMockJobMutator(t)
	gomock.InOrder(
		executor.EXPECT().JobKill(gomock.Any(), "100").DoAndReturn(func(ctx context.Context, _ string) error {
			<-ctx.Done()
			return ctx.Err()
		}),
		executor.EXPECT().JobKill(gomock.Any(), "200").Return(nil),
	)
	snapshot := terminateAdviceSnapshot(t, "100", "200")
	controller := Controller{
		ActionExecutor: executor,
		Options: ControllerOptions{
			AdviceItemTimeout: 10 * time.Millisecond,
			AdviceLoopTimeout: time.Hour,
		},
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}
	observation := SyncObservation{}

	_, err := controller.reconcileAdvice(context.Background(), snapshot, &observation)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reconcileAdvice() error = %v, want item deadline", err)
	}
	if observation.AttemptedAdviceReconciliations != 2 {
		t.Fatalf("AttemptedAdviceReconciliations = %d, want 2", observation.AttemptedAdviceReconciliations)
	}
}

func TestReconcileAdviceDoesNotCountItemsAfterLoopCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	executor := newMockJobMutator(t)
	executor.EXPECT().JobKill(gomock.Any(), "100").DoAndReturn(func(context.Context, string) error {
		cancel()
		return context.Canceled
	})
	controller := Controller{
		ActionExecutor: executor,
		Options:        ControllerOptions{AdviceLoopTimeout: time.Hour},
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}
	observation := SyncObservation{}

	_, err := controller.reconcileAdvice(ctx, terminateAdviceSnapshot(t, "100", "200"), &observation)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("reconcileAdvice() error = %v, want context canceled", err)
	}
	if observation.AttemptedAdviceReconciliations != 1 {
		t.Fatalf("AttemptedAdviceReconciliations = %d, want 1", observation.AttemptedAdviceReconciliations)
	}
}

func TestReconcileAdviceDoesNotCountEnvironmentBlockedAdvice(t *testing.T) {
	snapshot := terminateAdviceSnapshot(t, "100", "200")
	snapshot.LatestRejectedEnvironmentAttempt["100"] = 0
	executor := newMockJobMutator(t)
	executor.EXPECT().JobKill(gomock.Any(), "200").Return(nil)
	controller := Controller{
		ActionExecutor: executor,
		Options:        ControllerOptions{AdviceLoopTimeout: time.Hour},
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}
	observation := SyncObservation{}

	_, err := controller.reconcileAdvice(context.Background(), snapshot, &observation)
	if err != nil {
		t.Fatalf("reconcileAdvice() failed: %v", err)
	}
	if observation.AttemptedAdviceReconciliations != 1 {
		t.Fatalf("AttemptedAdviceReconciliations = %d, want 1", observation.AttemptedAdviceReconciliations)
	}
}

func TestReconcileAdviceForJobStopsAfterFirstActionFailure(t *testing.T) {
	firstErr := errors.New("requeue failed")
	executor := newMockJobMutator(t)
	executor.EXPECT().JobRequeueHold(gomock.Any(), "100").Return(firstErr)
	controller := Controller{ActionExecutor: executor}
	advice := newAdvice(t, adviceFixture{
		adviceID: "restart-100",
		jobID:    "100",
		state:    adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata: map[string]any{
			"scheduler":  cluster.SchedulerSlurm,
			"managed_by": cluster.ManagedBySlurmAdvisor,
		},
		constraints: &pb.NodeConstraints{Exclusions: []string{"c3"}},
	})

	_, err := controller.reconcileAdviceForJob(
		context.Background(),
		advice,
		cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateRunning},
		cluster.JobModeAct,
		nil,
	)
	if !errors.Is(err, firstErr) {
		t.Fatalf("reconcileAdviceForJob() error = %v, want first action failure", err)
	}
}

func TestReconcileAdviceAbandonsPermanentRequeueRejectionByGeneration(t *testing.T) {
	ctx := context.Background()
	advice := newAdvice(t, adviceFixture{
		adviceID: "restart-100", jobID: "100",
		state:    adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	advice.Generation = 2
	snapshot := newControllerSnapshot(time.Now())
	addAdvice(&snapshot, advice)
	addActiveJob(&snapshot, managedJob(cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateFailed}))
	rejection := errors.Join(cluster.ErrJobRequeuePermanent, errors.New("scheduler rejected requeue"))
	publicationErr := errors.New("status unavailable")
	client := &fakeControllerClient{statusUpdateErr: publicationErr}
	audit := &recordingAuditObserver{}
	executor := newMockJobMutator(t)
	gomock.InOrder(
		executor.EXPECT().JobRequeueHold(gomock.Any(), "100").Return(rejection),
		executor.EXPECT().JobRequeueHold(gomock.Any(), "100").Return(rejection),
		executor.EXPECT().JobRequeueHold(gomock.Any(), "100").Return(nil),
		executor.EXPECT().JobRelease(gomock.Any(), "100").Return(nil),
	)
	controller := Controller{
		HeimdallClient: client, ActionExecutor: executor, AuditObserver: audit,
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	// A rejected requeue never falls through to release. Failed publication is
	// visible and the next fresh evaluation can retry the durable status write.
	result, err := controller.reconcileAdvice(ctx, snapshot, nil)
	if !errors.Is(err, publicationErr) || len(result.TerminalOutcomes) != 1 || len(audit.terminalStatuses) != 0 {
		t.Fatalf("first result = %#v, error = %v, terminal audits = %#v", result, err, audit.terminalStatuses)
	}
	client.statusUpdateErr = nil
	if _, err := controller.reconcileAdvice(ctx, snapshot, nil); err != nil {
		t.Fatal(err)
	}
	if len(client.statusUpdates) != 2 || len(audit.terminalStatuses) != 1 {
		t.Fatalf("status updates = %#v, terminal audits = %#v", client.statusUpdates, audit.terminalStatuses)
	}
	for _, update := range client.statusUpdates {
		status := update.Status
		if status.GetObservedGeneration() != 2 ||
			status.GetDisposition() != pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED ||
			status.GetReason() != pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_UNSUPPORTED_RECOMMENDATION {
			t.Fatalf("status = %v, want generation 2 unsupported abandonment", status)
		}
	}
	if len(audit.adviceActions) != 2 || len(audit.adviceActionApplications) != 2 {
		t.Fatalf("action audit = %#v, want the two rejected commands", audit.adviceActions)
	}
	for i, action := range audit.adviceActions {
		if action.Action != SchedulerActionRequeueHold || action.Outcome != SchedulerActionOutcomeError ||
			!errors.Is(action.Err, cluster.ErrJobRequeuePermanent) || audit.adviceActionApplications[i].Generation != 2 {
			t.Fatalf("action audit = %#v, want generation 2 rejection detail", action)
		}
	}

	// A later generation can act independently; command success still does not
	// publish convergence until a later scheduler observation confirms it.
	advice.Generation = 3
	if _, err := controller.reconcileAdvice(ctx, snapshot, nil); err != nil {
		t.Fatal(err)
	}
	if len(client.statusUpdates) != 2 {
		t.Fatalf("status updates = %#v, command success is not convergence", client.statusUpdates)
	}
}

func TestReconcileAdviceRetriesUnclassifiedRequeueFailuresFromFreshState(t *testing.T) {
	for _, commandErr := range []error{errors.New("temporary scheduler failure"), context.DeadlineExceeded} {
		t.Run(commandErr.Error(), func(t *testing.T) {
			advice := newAdvice(t, adviceFixture{
				adviceID: "restart-100", jobID: "100",
				state:    adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
				metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
			})
			snapshot := newControllerSnapshot(time.Now())
			addAdvice(&snapshot, advice)
			job := managedJob(cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateFailed})
			addActiveJob(&snapshot, job)
			client := &fakeControllerClient{}
			executor := newMockJobMutator(t)
			gomock.InOrder(
				executor.EXPECT().JobRequeueHold(gomock.Any(), "100").Return(commandErr),
				executor.EXPECT().JobRequeueHold(gomock.Any(), "100").Return(commandErr),
				executor.EXPECT().JobRelease(gomock.Any(), "100").Return(nil),
			)
			controller := Controller{
				HeimdallClient: client, ActionExecutor: executor,
				ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			}
			for range 2 {
				result, err := controller.reconcileAdvice(context.Background(), snapshot, nil)
				if !errors.Is(err, commandErr) || len(result.TerminalOutcomes) != 0 {
					t.Fatalf("result = %#v, error = %v, want retryable failure", result, err)
				}
			}
			// A command whose result was uncertain may have reached Slurm. Fresh
			// successor state completes the hold without requeueing again.
			job.AttemptIndex++
			job.State, job.Held = cluster.JobStatePending, true
			addActiveJob(&snapshot, job)
			if _, err := controller.reconcileAdvice(context.Background(), snapshot, nil); err != nil {
				t.Fatal(err)
			}
			if len(client.statusUpdates) != 0 {
				t.Fatalf("status updates = %#v, want no terminal conclusion yet", client.statusUpdates)
			}
		})
	}
}

func TestReconcileAdviceForRequeuedAttemptReleasesWithoutAnotherRequeue(t *testing.T) {
	executor := newMockJobMutator(t)
	executor.EXPECT().JobRelease(gomock.Any(), "100").Return(nil)
	controller := Controller{ActionExecutor: executor}
	advice := newAdvice(t, adviceFixture{
		adviceID: "restart-100",
		jobID:    "100",
		state:    adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata: map[string]any{
			"scheduler":  cluster.SchedulerSlurm,
			"managed_by": cluster.ManagedBySlurmAdvisor,
		},
	})

	result, err := controller.reconcileAdviceForJob(
		context.Background(),
		advice,
		cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateRequeued},
		cluster.JobModeAct,
		nil,
	)
	if err != nil {
		t.Fatalf("reconcileAdviceForJob() failed: %v", err)
	}
	if len(result.Skips) != 0 {
		t.Fatalf("skips = %#v, want none", result.Skips)
	}
}

func TestReconcileReleasedAdviceForExactAttempt(t *testing.T) {
	tests := []struct {
		name        string
		advice      adviceFixture
		job         cluster.JobExecutionRecord
		wantRelease bool
		wantSkip    AdviceSkipReason
	}{
		{
			name: "held pending releases",
			advice: adviceFixture{adviceID: "release-100", jobID: "100", state: adviceState(
				pb.AdviceState_ADVICE_STATE_RELEASED,
			)},
			job:         cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStatePending, Held: true},
			wantRelease: true,
		},
		{
			name:     "unheld pending satisfied",
			advice:   adviceFixture{adviceID: "release-100", jobID: "100", state: adviceState(pb.AdviceState_ADVICE_STATE_RELEASED)},
			job:      cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStatePending},
			wantSkip: AdviceSkipAlreadySatisfied,
		},
		{
			name:     "running satisfied",
			advice:   adviceFixture{adviceID: "release-100", jobID: "100", state: adviceState(pb.AdviceState_ADVICE_STATE_RELEASED)},
			job:      cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateRunning},
			wantSkip: AdviceSkipAlreadySatisfied,
		},
		{
			name:     "final satisfied",
			advice:   adviceFixture{adviceID: "release-100", jobID: "100", state: adviceState(pb.AdviceState_ADVICE_STATE_RELEASED)},
			job:      cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateCompleted},
			wantSkip: AdviceSkipAlreadySatisfied,
		},
		{
			name: "stale attempt",
			advice: adviceFixture{
				adviceID: "release-100", jobID: "100", attemptIndex: 1,
				state: adviceState(pb.AdviceState_ADVICE_STATE_RELEASED),
			},
			job:      cluster.JobExecutionRecord{JobID: "100", AttemptIndex: 2, State: cluster.JobStatePending, Held: true},
			wantSkip: AdviceSkipStaleAttempt,
		},
		{
			name: "future attempt",
			advice: adviceFixture{
				adviceID: "release-100", jobID: "100", attemptIndex: 2,
				state: adviceState(pb.AdviceState_ADVICE_STATE_RELEASED),
			},
			job:      cluster.JobExecutionRecord{JobID: "100", AttemptIndex: 1, State: cluster.JobStatePending, Held: true},
			wantSkip: AdviceSkipFutureAttempt,
		},
		{
			name: "constraints rejected",
			advice: adviceFixture{
				adviceID: "release-100", jobID: "100", state: adviceState(pb.AdviceState_ADVICE_STATE_RELEASED),
				constraints: &pb.NodeConstraints{Exclusions: []string{"c1"}},
			},
			job:      cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStatePending, Held: true},
			wantSkip: AdviceSkipUnsupportedRecommendation,
		},
		{
			name: "missing fresh scheduler state",
			advice: adviceFixture{
				adviceID: "release-100", jobID: "100", state: adviceState(pb.AdviceState_ADVICE_STATE_RELEASED),
			},
			job:      cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateUnspecified},
			wantSkip: AdviceSkipMissingFreshSchedulerState,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := newMockJobMutator(t)
			if tt.wantRelease {
				executor.EXPECT().JobRelease(gomock.Any(), tt.job.JobID).Return(nil)
			}
			controller := Controller{ActionExecutor: executor}
			result, err := controller.reconcileAdviceForJob(
				context.Background(), newAdvice(t, tt.advice), tt.job, cluster.JobModeAct, nil,
			)
			if err != nil {
				t.Fatalf("reconcileAdviceForJob() failed: %v", err)
			}
			if tt.wantSkip == "" {
				if len(result.Skips) != 0 {
					t.Fatalf("Skips = %#v, want none", result.Skips)
				}
				return
			}
			if len(result.Skips) != 1 || result.Skips[0].Reason != tt.wantSkip {
				t.Fatalf("Skips = %#v, want one %q skip", result.Skips, tt.wantSkip)
			}
		})
	}
}

func TestReconcileTerminatedAdviceForExactAttempt(t *testing.T) {
	tests := []struct {
		name     string
		advice   adviceFixture
		job      cluster.JobExecutionRecord
		wantKill bool
		wantSkip AdviceSkipReason
	}{
		{
			name: "exact held pending attempt is killed",
			advice: adviceFixture{
				adviceID: "terminate-100", jobID: "100", attemptIndex: 0,
				state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
			},
			job:      cluster.JobExecutionRecord{JobID: "100", AttemptIndex: 0, State: cluster.JobStatePending, Held: true},
			wantKill: true,
		},
		{
			name: "final exact attempt is already satisfied",
			advice: adviceFixture{
				adviceID: "terminate-100", jobID: "100", attemptIndex: 0,
				state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
			},
			job:      cluster.JobExecutionRecord{JobID: "100", AttemptIndex: 0, State: cluster.JobStateCancelled},
			wantSkip: AdviceSkipAlreadySatisfied,
		},
		{
			name: "stale attempt does not kill latest attempt",
			advice: adviceFixture{
				adviceID: "terminate-100", jobID: "100", attemptIndex: 0,
				state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
			},
			job:      cluster.JobExecutionRecord{JobID: "100", AttemptIndex: 1, State: cluster.JobStatePending, Held: true},
			wantSkip: AdviceSkipStaleAttempt,
		},
		{
			name: "future attempt does not kill observed attempt",
			advice: adviceFixture{
				adviceID: "terminate-100", jobID: "100", attemptIndex: 1,
				state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
			},
			job:      cluster.JobExecutionRecord{JobID: "100", AttemptIndex: 0, State: cluster.JobStatePending, Held: true},
			wantSkip: AdviceSkipFutureAttempt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := newMockJobMutator(t)
			if tt.wantKill {
				executor.EXPECT().JobKill(gomock.Any(), tt.job.JobID).Return(nil)
			}
			controller := Controller{ActionExecutor: executor}

			result, err := controller.reconcileAdviceForJob(
				context.Background(), newAdvice(t, tt.advice), tt.job, cluster.JobModeAct, nil,
			)
			if err != nil {
				t.Fatalf("reconcileAdviceForJob() failed: %v", err)
			}
			if tt.wantSkip == "" {
				if len(result.Skips) != 0 {
					t.Fatalf("Skips = %#v, want none", result.Skips)
				}
				return
			}
			if len(result.Skips) != 1 || result.Skips[0].Reason != tt.wantSkip {
				t.Fatalf("Skips = %#v, want one %q skip", result.Skips, tt.wantSkip)
			}
		})
	}
}

func TestReconcileAdviceForJobRetriesFailedActionUntilSatisfied(t *testing.T) {
	tests := []struct {
		name      string
		state     pb.AdviceState
		satisfied cluster.JobExecutionRecord
	}{
		{
			name:      "release",
			state:     pb.AdviceState_ADVICE_STATE_RELEASED,
			satisfied: cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateRunning},
		},
		{
			name:      "kill",
			state:     pb.AdviceState_ADVICE_STATE_TERMINATED,
			satisfied: cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateCancelled},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actionErr := errors.New("scheduler unavailable")
			executor := newMockJobMutator(t)
			if tt.state == pb.AdviceState_ADVICE_STATE_RELEASED {
				gomock.InOrder(
					executor.EXPECT().JobRelease(gomock.Any(), "100").Return(actionErr),
					executor.EXPECT().JobRelease(gomock.Any(), "100").Return(nil),
				)
			} else {
				gomock.InOrder(
					executor.EXPECT().JobKill(gomock.Any(), "100").Return(actionErr),
					executor.EXPECT().JobKill(gomock.Any(), "100").Return(nil),
				)
			}
			controller := Controller{ActionExecutor: executor}
			advice := newAdvice(t, adviceFixture{adviceID: "advice-100", jobID: "100", state: adviceState(tt.state)})
			held := cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStatePending, Held: true}

			result, err := controller.reconcileAdviceForJob(context.Background(), advice, held, cluster.JobModeAct, nil)
			if !errors.Is(err, actionErr) || len(result.TerminalOutcomes) != 0 {
				t.Fatalf("first pass result = %#v, error = %v, want retryable failure", result, err)
			}
			if _, err := controller.reconcileAdviceForJob(
				context.Background(), advice, held, cluster.JobModeAct, nil,
			); err != nil {
				t.Fatalf("retry pass failed: %v", err)
			}
			// The mock rejects a third scheduler call once Slurm shows the desired state.
			result, err = controller.reconcileAdviceForJob(
				context.Background(), advice, tt.satisfied, cluster.JobModeAct, nil,
			)
			if err != nil {
				t.Fatalf("satisfied pass failed: %v", err)
			}
			if len(result.Skips) != 1 || result.Skips[0].Reason != AdviceSkipAlreadySatisfied {
				t.Fatalf("Skips = %#v, want one %q skip", result.Skips, AdviceSkipAlreadySatisfied)
			}
		})
	}
}

func TestReconcileReleasedAdviceLogsSuccessfulRelease(t *testing.T) {
	executor := newMockJobMutator(t)
	executor.EXPECT().JobRelease(gomock.Any(), "100").Return(nil)
	core, logs := zapobserver.New(zap.InfoLevel)
	controller := Controller{
		ActionExecutor: executor,
		Logger:         zap.New(core),
	}
	advice := newAdvice(t, adviceFixture{
		adviceID:     "release-100",
		jobID:        "100",
		attemptIndex: 2,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RELEASED),
	})

	_, err := controller.reconcileAdviceForJob(
		context.Background(),
		advice,
		cluster.JobExecutionRecord{JobID: "100", AttemptIndex: 2, State: cluster.JobStatePending, Held: true},
		cluster.JobModeAct,
		nil,
	)
	if err != nil {
		t.Fatalf("reconcileAdviceForJob() failed: %v", err)
	}

	entries := logs.FilterMessage("applied advice job release").All()
	if len(entries) != 1 {
		t.Fatalf("release log entries = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["advice_id"] != "release-100" || fields["job_id"] != "100" || fields["attempt_index"] != int32(2) {
		t.Fatalf("release log fields = %#v, want advice and exact job attempt identifiers", fields)
	}
}

func TestReconcileAdviceForJobObserveModePlansEveryMutatorPath(t *testing.T) {
	tests := []struct {
		name        string
		advice      adviceFixture
		job         cluster.JobExecutionRecord
		wantActions []SchedulerAction
	}{
		{
			name: "restart plans requeue update and release",
			advice: adviceFixture{
				adviceID:    "restart-100",
				jobID:       "100",
				state:       adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
				constraints: &pb.NodeConstraints{Exclusions: []string{"c2"}},
			},
			job: cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateRunning},
			wantActions: []SchedulerAction{
				ActionAdviceRequeueHold,
				ActionAdviceNodeUpdate,
				ActionAdviceRelease,
			},
		},
		{
			name: "terminate plans kill",
			advice: adviceFixture{
				adviceID: "terminate-100",
				jobID:    "100",
				state:    adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
			},
			job:         cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateRunning},
			wantActions: []SchedulerAction{ActionAdviceKill},
		},
		{
			name: "release plans exact release",
			advice: adviceFixture{
				adviceID: "release-100",
				jobID:    "100",
				state:    adviceState(pb.AdviceState_ADVICE_STATE_RELEASED),
			},
			job:         cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStatePending, Held: true},
			wantActions: []SchedulerAction{ActionAdviceRelease},
		},
		{
			name: "constraint-only plans node update",
			advice: adviceFixture{
				adviceID:    "constraints-100",
				jobID:       "100",
				constraints: &pb.NodeConstraints{Inclusions: []string{"c1"}},
			},
			job:         cluster.JobExecutionRecord{JobID: "100", State: cluster.JobStateRunning},
			wantActions: []SchedulerAction{ActionAdviceNodeUpdate},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observation := &SyncObservation{}
			controller := Controller{}
			result, err := controller.reconcileAdviceForJob(
				context.Background(),
				newAdvice(t, tt.advice),
				tt.job,
				cluster.JobModeObserve,
				observation,
			)
			if err != nil {
				t.Fatalf("reconcileAdviceForJob() failed: %v", err)
			}
			if len(result.Skips) != 0 {
				t.Fatalf("skips = %#v, want none", result.Skips)
			}
			if len(observation.AdviceActions) != len(tt.wantActions) {
				t.Fatalf("actions = %#v, want %v", observation.AdviceActions, tt.wantActions)
			}
			for _, action := range tt.wantActions {
				assertActionObservation(t, observation.AdviceActions, action, cluster.JobModeObserve, ObservationOutcomePlanned, 1)
			}
		})
	}
}

func TestReconcileAdviceForJobObservesEveryApplication(t *testing.T) {
	tests := []struct {
		name                string
		advice              adviceFixture
		wantCorrelationUUID string
		wantActions         []SchedulerActionName
	}{
		{
			name: "constraint-only Advice",
			advice: adviceFixture{
				adviceID: "constraints-100", jobID: "100", attemptIndex: 2,
				recoverySequenceID: "123e4567-e89b-42d3-a456-426614174000",
				constraints:        &pb.NodeConstraints{Inclusions: []string{"c1"}},
			},
			wantCorrelationUUID: "123e4567-e89b-42d3-a456-426614174000",
			wantActions:         []SchedulerActionName{SchedulerActionUpdateConstraints},
		},
		{
			name: "mapped restart Advice without recovery context",
			advice: adviceFixture{
				adviceID: "restart-100", jobID: "100", attemptIndex: 2,
				state: adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
			},
			wantCorrelationUUID: "restart-100",
			wantActions: []SchedulerActionName{
				SchedulerActionRequeueHold,
				SchedulerActionRelease,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observer := &recordingAuditObserver{}
			controller := Controller{AuditObserver: observer}
			_, err := controller.reconcileAdviceForJob(
				context.Background(),
				newAdvice(t, test.advice),
				cluster.JobExecutionRecord{JobID: "100", AttemptIndex: 2, State: cluster.JobStateRunning},
				cluster.JobModeObserve,
				nil,
			)
			if err != nil {
				t.Fatalf("reconcileAdviceForJob() failed: %v", err)
			}
			if len(observer.advice) != 1 {
				t.Fatalf("observations = %#v, want one", observer.advice)
			}

			observation := observer.advice[0]
			if observation.AdviceID != test.advice.adviceID ||
				observation.JobID != "100" ||
				observation.AttemptIndex != 2 {
				t.Fatalf("observation = %#v, want Advice identity", observation)
			}
			if observation.CorrelationUUID != test.wantCorrelationUUID {
				t.Fatalf("correlation UUID = %q, want %q", observation.CorrelationUUID, test.wantCorrelationUUID)
			}
			if observation.Outcome != AdviceApplicationOutcomePlanned {
				t.Fatalf("outcome = %q, want %q", observation.Outcome, AdviceApplicationOutcomePlanned)
			}
			if len(observation.Actions) != len(test.wantActions) {
				t.Fatalf("actions = %#v, want %v", observation.Actions, test.wantActions)
			}
			for index, action := range observation.Actions {
				if action.Action != test.wantActions[index] ||
					action.Outcome != SchedulerActionOutcomePlanned {
					t.Fatalf("actions = %#v, want planned %v", observation.Actions, test.wantActions)
				}
			}
		})
	}
}

func TestReconcileConstraintAdviceObservesCompletedAction(t *testing.T) {
	completedAt := time.Date(2026, time.September, 1, 20, 0, 0, 0, time.UTC)
	executor := newMockJobMutator(t)
	executor.EXPECT().JobNodeUpdate(gomock.Any(), "100", gomock.Any(), []string{"c1"}, 0).Return(nil)
	observer := &recordingAuditObserver{}
	controller := Controller{
		ActionExecutor: executor,
		AuditObserver:  observer,
		Now:            func() time.Time { return completedAt },
	}

	result, err := controller.reconcileAdviceForJob(
		context.Background(),
		newAdvice(t, adviceFixture{
			adviceID: "constraints-100", jobID: "100", attemptIndex: 2,
			recoverySequenceID: "123e4567-e89b-42d3-a456-426614174002",
			constraints:        &pb.NodeConstraints{Inclusions: []string{"c1"}},
		}),
		cluster.JobExecutionRecord{JobID: "100", AttemptIndex: 2, State: cluster.JobStateRunning},
		cluster.JobModeAct,
		nil,
	)
	if err != nil {
		t.Fatalf("reconcileAdviceForJob() failed: %v", err)
	}
	if len(result.Skips) != 0 {
		t.Fatalf("skips = %#v, want none", result.Skips)
	}
	if len(observer.advice) != 1 {
		t.Fatalf("observations = %#v, want one", observer.advice)
	}
	if len(observer.adviceActions) != 1 ||
		observer.adviceActions[0].Action != SchedulerActionUpdateConstraints ||
		observer.adviceActions[0].Outcome != SchedulerActionOutcomeSuccess {
		t.Fatalf("action observations = %#v, want one completed update", observer.adviceActions)
	}
	if len(observer.adviceActionApplications) != 1 {
		t.Fatalf("action applications = %#v, want one", observer.adviceActionApplications)
	}
	if observer.adviceActionApplications[0].CorrelationUUID != "123e4567-e89b-42d3-a456-426614174002" {
		t.Fatalf("action correlation UUID = %q, want recovery sequence ID", observer.adviceActionApplications[0].CorrelationUUID)
	}

	observation := observer.advice[0]
	if observation.Outcome != AdviceApplicationOutcomeApplied {
		t.Fatalf("outcome = %q, want %q", observation.Outcome, AdviceApplicationOutcomeApplied)
	}
	if len(observation.Actions) != 1 ||
		observation.Actions[0].Action != SchedulerActionUpdateConstraints ||
		observation.Actions[0].Outcome != SchedulerActionOutcomeSuccess ||
		!observation.Actions[0].CompletedAt.Equal(completedAt) {
		t.Fatalf("actions = %#v, want completed update constraints", observation.Actions)
	}
}

type recordingAuditObserver struct {
	NopAuditObserver
	advice                   []AdviceApplicationObservation
	adviceActions            []SchedulerActionObservation
	adviceActionApplications []AdviceApplicationObservation
	terminalStatuses         []AdviceTerminalStatusObservation
	failedJobActions         []FailedJobRequeueHoldObservation
}

func (observer *recordingAuditObserver) ObserveAdviceSchedulerAction(
	application AdviceApplicationObservation,
	action SchedulerActionObservation,
) {
	observer.adviceActionApplications = append(observer.adviceActionApplications, application)
	observer.adviceActions = append(observer.adviceActions, action)
}

func (observer *recordingAuditObserver) ObserveAdviceApplication(
	observation AdviceApplicationObservation,
) {
	observer.advice = append(observer.advice, observation)
}

func (observer *recordingAuditObserver) ObserveAdviceTerminalStatus(
	observation AdviceTerminalStatusObservation,
) {
	observer.terminalStatuses = append(observer.terminalStatuses, observation)
}

func (observer *recordingAuditObserver) ObserveFailedJobRequeueHold(
	observation FailedJobRequeueHoldObservation,
) {
	observer.failedJobActions = append(observer.failedJobActions, observation)
}

func terminateAdviceSnapshot(t *testing.T, jobIDs ...string) ControllerSnapshot {
	t.Helper()

	snapshot := newControllerSnapshot(time.Now())
	for _, jobID := range jobIDs {
		addActiveJob(&snapshot, managedJob(cluster.JobExecutionRecord{
			JobID: jobID,
			State: cluster.JobStateRunning,
		}))
		addAdvice(&snapshot, newAdvice(t, adviceFixture{
			adviceID: "terminate-" + jobID,
			jobID:    jobID,
			state:    adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
			metadata: map[string]any{
				"scheduler":  cluster.SchedulerSlurm,
				"managed_by": cluster.ManagedBySlurmAdvisor,
			},
		}))
	}

	return snapshot
}

func TestReconcileAdviceForJobRetriesReleaseAfterAttemptAdvances(t *testing.T) {
	testCases := []struct {
		name        string
		constraints *pb.NodeConstraints
		observed    cluster.NodeConstraints
	}{
		{name: "restart only"},
		{
			name:        "restart with satisfied constraints",
			constraints: &pb.NodeConstraints{Exclusions: []string{"c2", "c1"}},
			observed:    cluster.NodeConstraints{Exclusions: []string{"c1", "c2"}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			releaseErr := errors.New("release failed")
			advice := newAdvice(t, adviceFixture{
				adviceID:     "advice-restart",
				jobID:        "12345",
				attemptIndex: 1,
				state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
				metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
				constraints:  testCase.constraints,
			})
			executor := newMockJobMutator(t)
			gomock.InOrder(
				executor.EXPECT().JobRequeueHold(gomock.Any(), "12345").Return(nil),
				executor.EXPECT().JobRelease(gomock.Any(), "12345").Return(releaseErr),
				executor.EXPECT().JobRelease(gomock.Any(), "12345").Return(nil),
			)
			controller := Controller{
				ActionExecutor: executor,
				Options:        ControllerOptions{AdviceRequeueHoldDelay: 0},
			}

			_, err := controller.reconcileAdviceForJob(ctx, advice, cluster.JobExecutionRecord{
				JobID:           "12345",
				AttemptIndex:    1,
				State:           cluster.JobStateRunning,
				NodeConstraints: testCase.observed,
			}, cluster.JobModeAct, nil)
			if !errors.Is(err, releaseErr) {
				t.Fatalf("first reconcile error = %v, want release failure", err)
			}

			result, err := controller.reconcileAdviceForJob(ctx, advice, cluster.JobExecutionRecord{
				JobID:           "12345",
				AttemptIndex:    2,
				State:           cluster.JobStatePending,
				Reason:          "Dependency",
				Held:            true,
				NodeConstraints: testCase.observed,
			}, cluster.JobModeAct, nil)
			if err != nil {
				t.Fatalf("retry reconcile failed: %v", err)
			}
			if len(result.Skips) != 0 {
				t.Fatalf("retry reconcile skips = %v, want release retry", result.Skips)
			}
		})
	}
}

func TestReconcileAdviceForJobRejectsUnsafeRestartContinuation(t *testing.T) {
	testCases := []struct {
		name       string
		advice     adviceFixture
		job        cluster.JobExecutionRecord
		wantReason AdviceSkipReason
	}{
		{
			name: "held attempt after immediate successor",
			advice: adviceFixture{
				adviceID:     "advice-restart",
				jobID:        "12345",
				attemptIndex: 1,
				state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
				metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
			},
			job: cluster.JobExecutionRecord{
				JobID:        "12345",
				AttemptIndex: 3,
				State:        cluster.JobStatePending,
				Held:         true,
			},
			wantReason: AdviceSkipStaleAttempt,
		},
		{
			name: "unheld successor missing constraints",
			advice: adviceFixture{
				adviceID:     "advice-restart",
				jobID:        "12345",
				attemptIndex: 1,
				state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
				metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
				constraints:  &pb.NodeConstraints{Exclusions: []string{"c1"}},
			},
			job: cluster.JobExecutionRecord{
				JobID:        "12345",
				AttemptIndex: 2,
				State:        cluster.JobStatePending,
			},
			wantReason: AdviceSkipStaleAttempt,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			controller := Controller{ActionExecutor: newMockJobMutator(t)}
			result, err := controller.reconcileAdviceForJob(
				context.Background(), newAdvice(t, testCase.advice), testCase.job, cluster.JobModeAct, nil,
			)
			if err != nil {
				t.Fatalf("reconcileAdviceForJob() failed: %v", err)
			}
			if len(result.Skips) != 1 || result.Skips[0].Reason != testCase.wantReason {
				t.Fatalf("Skips = %#v, want one %q skip", result.Skips, testCase.wantReason)
			}
		})
	}
}
