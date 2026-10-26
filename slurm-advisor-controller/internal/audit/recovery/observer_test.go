// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package recovery

import (
	"errors"
	"strconv"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobidentity"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	"github.com/google/go-cmp/cmp"
)

func TestObserverEmitsPlannedAdviceApplication(t *testing.T) {
	t.Parallel()
	producedAt := time.Date(2026, time.September, 2, 17, 0, 0, 0, time.UTC)
	sink := &recordingSink{}
	observer := NewObserver(sink)
	observer.now = func() time.Time { return producedAt }
	observer.newID = func() string { return "application-event" }

	observer.ObserveAdviceApplication(reconcile.AdviceApplicationObservation{
		OccurrenceID: "application-event", CompletedAt: producedAt,
		AdviceID:        "advice-123",
		JobID:           "123",
		AttemptIndex:    2,
		CorrelationUUID: "sequence-123",
		Outcome:         reconcile.AdviceApplicationOutcomePlanned,
		Actions: []reconcile.SchedulerActionObservation{
			{Action: reconcile.SchedulerActionRequeueHold, Outcome: reconcile.SchedulerActionOutcomePlanned},
			{Action: reconcile.SchedulerActionRelease, Outcome: reconcile.SchedulerActionOutcomePlanned},
		},
	})

	want := []audit.ProducerEvent{audit.AdviceApplicationEvaluatedEvent{
		HeimdallJobID:   "advice-123",
		JobID:           "123",
		AttemptIndex:    2,
		CorrelationUUID: "sequence-123",
		Outcome:         "planned",
		PlannedActions:  []string{"slurm.requeue_hold", "slurm.release"},
		ProducedAt:      producedAt,
		IdempotencyKey:  "application-event",
	}.ToProducerEvent()}
	if diff := cmp.Diff(want, sink.events); diff != "" {
		t.Fatalf("emitted events mismatch (-want +got):\n%s", diff)
	}
}

func TestObserverEmitsChangedPlannedEvaluations(t *testing.T) {
	t.Parallel()
	baseline := reconcile.AdviceApplicationObservation{
		AdviceID: "advice-123", JobID: "123", AttemptIndex: 2,
		Generation: 1, CorrelationUUID: "sequence-123",
		Outcome: reconcile.AdviceApplicationOutcomePlanned,
		Actions: []reconcile.SchedulerActionObservation{{
			Action: reconcile.SchedulerActionRelease, Outcome: reconcile.SchedulerActionOutcomePlanned,
		}},
	}
	changes := map[string]func(*reconcile.AdviceApplicationObservation){
		"advice":      func(o *reconcile.AdviceApplicationObservation) { o.AdviceID = "advice-456" },
		"job":         func(o *reconcile.AdviceApplicationObservation) { o.JobID = "456" },
		"attempt":     func(o *reconcile.AdviceApplicationObservation) { o.AttemptIndex++ },
		"correlation": func(o *reconcile.AdviceApplicationObservation) { o.CorrelationUUID = "sequence-456" },
		"generation":  func(o *reconcile.AdviceApplicationObservation) { o.Generation++ },
		"actions": func(o *reconcile.AdviceApplicationObservation) {
			o.Actions = []reconcile.SchedulerActionObservation{{
				Action: reconcile.SchedulerActionCancel, Outcome: reconcile.SchedulerActionOutcomePlanned,
			}}
		},
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sink := &recordingSink{}
			observer := NewObserver(sink)
			observer.ObserveAdviceApplication(baseline)
			observer.ObserveAdviceApplication(baseline)
			if len(sink.events) != 1 {
				t.Fatalf("unchanged evaluation emitted %d events, want 1", len(sink.events))
			}
			changed := baseline
			change(&changed)
			observer.ObserveAdviceApplication(changed)
			observer.ObserveAdviceApplication(changed)
			if len(sink.events) != 2 {
				t.Fatalf("changed evaluation emitted %d total events, want 2", len(sink.events))
			}
			if name == "generation" || name == "actions" {
				observer.ObserveAdviceApplication(baseline)
				if len(sink.events) != 3 {
					t.Fatalf("return to previous evaluation emitted %d total events, want 3", len(sink.events))
				}
			}
		})
	}
}

func TestObserverReannouncesPlanAfterOutcomeTransition(t *testing.T) {
	t.Parallel()
	for _, outcome := range []reconcile.AdviceApplicationOutcome{
		reconcile.AdviceApplicationOutcomeApplied,
		reconcile.AdviceApplicationOutcomeFailed,
		reconcile.AdviceApplicationOutcomeSkipped,
	} {
		t.Run(string(outcome), func(t *testing.T) {
			t.Parallel()
			sink := &recordingSink{}
			observer := NewObserver(sink)
			observation := reconcile.AdviceApplicationObservation{
				AdviceID: "advice-123", JobID: "123", Generation: 1,
				Outcome: reconcile.AdviceApplicationOutcomePlanned,
			}
			observer.ObserveAdviceApplication(observation)
			observation.Outcome = outcome
			observer.ObserveAdviceApplication(observation)
			observer.ObserveAdviceApplication(observation)
			want := 3
			if outcome == reconcile.AdviceApplicationOutcomeSkipped {
				want = 1
			}
			if len(sink.events) != want {
				t.Fatalf("non-planned events were suppressed: got %d events, want %d", len(sink.events), want)
			}
			observation.Outcome = reconcile.AdviceApplicationOutcomePlanned
			observer.ObserveAdviceApplication(observation)
			observer.ObserveAdviceApplication(observation)
			if len(sink.events) != want+1 {
				t.Fatalf("return to observe emitted %d events, want %d", len(sink.events), want+1)
			}
		})
	}
}

func TestObserverReannouncesPlansAfterEvictionAndRestart(t *testing.T) {
	t.Parallel()
	sink := &recordingSink{}
	observer := NewObserver(sink)
	observation := reconcile.AdviceApplicationObservation{
		AdviceID: "0", Generation: 1, Outcome: reconcile.AdviceApplicationOutcomePlanned,
	}
	for i := range maxPlannedEvaluations {
		observation.AdviceID = strconv.Itoa(i)
		observer.ObserveAdviceApplication(observation)
	}
	// Refresh the first identity, then make space for one new identity. The
	// least recently observed identity should be eligible to reannounce.
	observation.AdviceID = "0"
	observer.ObserveAdviceApplication(observation)
	observation.AdviceID = "new"
	observer.ObserveAdviceApplication(observation)
	observation.AdviceID = "0"
	observer.ObserveAdviceApplication(observation)
	if len(sink.events) != maxPlannedEvaluations+1 {
		t.Fatalf("recent evaluation was not retained: got %d events", len(sink.events))
	}
	observation.AdviceID = "1"
	observer.ObserveAdviceApplication(observation)
	observer.ObserveAdviceApplication(observation)
	if len(sink.events) != maxPlannedEvaluations+2 {
		t.Fatalf("evicted evaluation did not reannounce once: got %d events", len(sink.events))
	}
	restarted := NewObserver(sink)
	restarted.ObserveAdviceApplication(observation)
	restarted.ObserveAdviceApplication(observation)
	if len(sink.events) != maxPlannedEvaluations+3 {
		t.Fatalf("restarted observer did not reannounce once: got %d events", len(sink.events))
	}
}

func TestObserverEmitsCompletedActionBeforeAppliedAdvice(t *testing.T) {
	t.Parallel()
	actionAt := time.Date(2026, time.September, 2, 17, 1, 0, 0, time.UTC)
	applicationAt := actionAt.Add(time.Second)
	sink := &recordingSink{}
	observer := NewObserver(sink)
	observer.now = func() time.Time { return applicationAt }
	ids := []string{"action-event", "application-event"}
	observer.newID = func() string {
		id := ids[0]
		ids = ids[1:]
		return id
	}

	application := reconcile.AdviceApplicationObservation{
		OccurrenceID: "application-event", CompletedAt: applicationAt,
		Generation: 7, RecoverySequenceID: "sequence-456", TraceID: "trace-456", SpanID: "application-span",
		AdviceID:     "advice-456",
		JobID:        "456",
		AttemptIndex: 3,
		Outcome:      reconcile.AdviceApplicationOutcomeApplied,
		Actions: []reconcile.SchedulerActionObservation{{
			OccurrenceID: "action-event", TraceID: "trace-456", SpanID: "action-span",
			Action:      reconcile.SchedulerActionCancel,
			Outcome:     reconcile.SchedulerActionOutcomeSuccess,
			CompletedAt: actionAt,
		}},
	}
	observer.ObserveAdviceSchedulerAction(application, application.Actions[0])
	observer.ObserveAdviceApplication(application)

	want := []audit.ProducerEvent{
		audit.AdviceSchedulerActionCompletedEvent{
			HeimdallJobID: "advice-456", JobID: "456", AttemptIndex: 3,
			Generation: 7, RecoverySequenceID: "sequence-456", TraceID: "trace-456", SpanID: "action-span",
			Action: "slurm.cancel", Success: true, ProducedAt: actionAt,
			IdempotencyKey: "action-event",
		}.ToProducerEvent(),
		audit.AdviceApplicationAppliedEvent{
			HeimdallJobID: "advice-456", JobID: "456", AttemptIndex: 3,
			Generation: 7, RecoverySequenceID: "sequence-456", TraceID: "trace-456", SpanID: "application-span",
			ProducedAt: applicationAt, IdempotencyKey: "application-event",
		}.ToProducerEvent(),
	}
	if diff := cmp.Diff(want, sink.events); diff != "" {
		t.Fatalf("emitted events mismatch (-want +got):\n%s", diff)
	}
}

func TestObserverDoesNotEmitSkippedAdviceApplication(t *testing.T) {
	t.Parallel()
	sink := &recordingSink{}
	observer := NewObserver(sink)

	observer.ObserveAdviceApplication(reconcile.AdviceApplicationObservation{
		AdviceID: "skipped", JobID: "123", AttemptIndex: 1,
		Outcome:    reconcile.AdviceApplicationOutcomeSkipped,
		SkipReason: reconcile.AdviceSkipStaleAttempt,
	})

	if diff := cmp.Diff([]audit.ProducerEvent(nil), sink.events); diff != "" {
		t.Fatalf("emitted events mismatch (-want +got):\n%s", diff)
	}
}

func TestObserverEmitsPersistedTerminalAdviceStatus(t *testing.T) {
	t.Parallel()
	producedAt := time.Date(2026, time.September, 4, 19, 0, 0, 123_456_000, time.UTC)
	tests := map[string]struct {
		disposition pb.AdviceReconciliationDisposition
		reason      pb.AdviceReconciliationReason
		want        audit.ProducerEvent
	}{
		"converged": {
			disposition: pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED,
			reason:      pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED,
			want: audit.AdviceReconciliationConvergedEvent{
				HeimdallJobID: "advice-terminal", JobID: "123", AttemptIndex: 2,
				Generation: 7, Reason: "DESIRED_STATE_OBSERVED", ProducedAt: producedAt,
				IdempotencyKey: "terminal-event",
			}.ToProducerEvent(),
		},
		"abandoned": {
			disposition: pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED,
			reason:      pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_ADVICE_EXPIRED,
			want: audit.AdviceReconciliationAbandonedEvent{
				HeimdallJobID: "advice-terminal", JobID: "123", AttemptIndex: 2,
				Generation: 7, Reason: "ADVICE_EXPIRED", ProducedAt: producedAt,
				IdempotencyKey: "terminal-event",
			}.ToProducerEvent(),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sink := &recordingSink{}
			observer := NewObserver(sink)
			observer.newID = func() string { return "terminal-event" }
			observer.ObserveAdviceTerminalStatus(reconcile.AdviceTerminalStatusObservation{
				AdviceID: "advice-terminal", JobID: "123", AttemptIndex: 2,
				Generation: 7, Disposition: test.disposition, Reason: test.reason,
				ObservedAt: producedAt,
			})

			if diff := cmp.Diff([]audit.ProducerEvent{test.want}, sink.events); diff != "" {
				t.Fatalf("emitted events mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestObserverIgnoresNonTerminalAdviceStatus(t *testing.T) {
	t.Parallel()
	sink := &recordingSink{}
	observer := NewObserver(sink)

	observer.ObserveAdviceTerminalStatus(reconcile.AdviceTerminalStatusObservation{
		AdviceID: "pending", JobID: "123", AttemptIndex: 1, Generation: 2,
		Disposition: pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_PENDING,
	})

	if diff := cmp.Diff([]audit.ProducerEvent(nil), sink.events); diff != "" {
		t.Fatalf("emitted events mismatch (-want +got):\n%s", diff)
	}
}

func TestObserverEmitsFailedAdviceApplication(t *testing.T) {
	t.Parallel()
	producedAt := time.Date(2026, time.September, 2, 17, 2, 0, 0, time.UTC)
	sink := &recordingSink{}
	observer := NewObserver(sink)
	observer.now = func() time.Time { return producedAt }
	observer.newID = func() string { return "event-id" }

	observer.ObserveAdviceApplication(reconcile.AdviceApplicationObservation{
		OccurrenceID: "event-id", CompletedAt: producedAt,
		AdviceID: "failed", JobID: "456", AttemptIndex: 2,
		Outcome: reconcile.AdviceApplicationOutcomeFailed,
		Err:     errors.New("scheduler failed"),
	})

	want := []audit.ProducerEvent{
		audit.AdviceApplicationFailedEvent{
			HeimdallJobID: "failed", JobID: "456", AttemptIndex: 2,
			Error: "scheduler failed", ProducedAt: producedAt, IdempotencyKey: "event-id",
		}.ToProducerEvent(),
	}
	if diff := cmp.Diff(want, sink.events); diff != "" {
		t.Fatalf("emitted events mismatch (-want +got):\n%s", diff)
	}
}

func TestObserverEmitsFailedJobRequeueHoldCompletion(t *testing.T) {
	t.Parallel()
	completedAt := time.Date(2026, time.September, 2, 17, 3, 0, 0, time.UTC)
	commandErr := errors.New("requeue failed")
	sink := &recordingSink{}
	observer := NewObserver(sink)
	observer.newID = func() string { return "failed-job-event" }

	observer.ObserveFailedJobRequeueHold(reconcile.FailedJobRequeueHoldObservation{
		JobID: "789", AttemptIndex: 4,
		Outcome:     reconcile.SchedulerActionOutcomeError,
		CompletedAt: completedAt,
		Err:         commandErr,
	})

	want := []audit.ProducerEvent{audit.JobAutoRequeueHoldCompletedEvent{
		HeimdallJobID: jobidentity.HeimdallJobID("789", 4),
		JobID:         "789", AttemptIndex: 4, Success: false,
		Error: "requeue failed", ProducedAt: completedAt,
		IdempotencyKey: "failed-job-event",
	}.ToProducerEvent()}
	if diff := cmp.Diff(want, sink.events); diff != "" {
		t.Fatalf("emitted events mismatch (-want +got):\n%s", diff)
	}
}

type recordingSink struct {
	events []audit.ProducerEvent
}

func (sink *recordingSink) Add(events ...audit.ProducerEvent) {
	sink.events = append(sink.events, events...)
}
