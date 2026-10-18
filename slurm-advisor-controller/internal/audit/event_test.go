// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package audit

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestTypedEventsToProducerEvent(t *testing.T) {
	t.Parallel()

	producedAt := time.Date(2026, time.September, 1, 12, 30, 0, 123_456_789, time.UTC)
	tests := map[string]struct {
		event ToProducerEvent
		want  ProducerEvent
	}{
		"Advice scheduler action": {
			event: AdviceSchedulerActionCompletedEvent{
				HeimdallJobID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
				CorrelationUUID: "correlation-789", Action: "slurm.cancel", Success: false,
				Error: "scheduler unavailable", ProducedAt: producedAt, IdempotencyKey: "action-event",
			},
			want: ProducerEvent{
				HeimdallJobID: "heimdall-job-123", EventProducer: ProducerSlurmAdvisorController,
				EventType: AdviceSchedulerActionCompleted, EventLevel: LevelRecovery,
				Payload: AdviceSchedulerActionCompletedPayload{
					AdviceID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
					CorrelationUUID: "correlation-789", Action: "slurm.cancel",
					Success: false, Error: "scheduler unavailable",
				},
				EventProducedAt: producedAt.UnixMicro(), IdempotencyKey: "action-event",
			},
		},
		"Advice applied": {
			event: AdviceApplicationAppliedEvent{
				HeimdallJobID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
				CorrelationUUID: "correlation-789", ProducedAt: producedAt,
				IdempotencyKey: "applied-event",
			},
			want: ProducerEvent{
				HeimdallJobID: "heimdall-job-123", EventProducer: ProducerSlurmAdvisorController,
				EventType: AdviceApplicationApplied, EventLevel: LevelRecovery,
				Payload: AdviceApplicationAppliedPayload{
					AdviceID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
					CorrelationUUID: "correlation-789",
				},
				EventProducedAt: producedAt.UnixMicro(), IdempotencyKey: "applied-event",
			},
		},
		"Advice evaluated": {
			event: AdviceApplicationEvaluatedEvent{
				HeimdallJobID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
				CorrelationUUID: "correlation-789", Outcome: "planned",
				PlannedActions: []string{"slurm.requeue_hold", "slurm.release"},
				ProducedAt:     producedAt, IdempotencyKey: "evaluation-event",
			},
			want: ProducerEvent{
				HeimdallJobID: "heimdall-job-123", EventProducer: ProducerSlurmAdvisorController,
				EventType: AdviceApplicationEvaluated, EventLevel: LevelRecovery,
				Payload: AdviceApplicationEvaluatedPayload{
					AdviceID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
					CorrelationUUID: "correlation-789", Outcome: "planned",
					PlannedActions: []string{"slurm.requeue_hold", "slurm.release"},
				},
				EventProducedAt: producedAt.UnixMicro(), IdempotencyKey: "evaluation-event",
			},
		},
		"Advice failed": {
			event: AdviceApplicationFailedEvent{
				HeimdallJobID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
				CorrelationUUID: "correlation-789", Error: "application failed",
				ProducedAt: producedAt, IdempotencyKey: "failed-event",
			},
			want: ProducerEvent{
				HeimdallJobID: "heimdall-job-123", EventProducer: ProducerSlurmAdvisorController,
				EventType: AdviceApplicationFailed, EventLevel: LevelRecovery,
				Payload: AdviceApplicationFailedPayload{
					AdviceID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
					CorrelationUUID: "correlation-789", Error: "application failed",
				},
				EventProducedAt: producedAt.UnixMicro(), IdempotencyKey: "failed-event",
			},
		},
		"Advice reconciliation converged": {
			event: AdviceReconciliationConvergedEvent{
				HeimdallJobID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
				Generation: 7, Reason: "DESIRED_STATE_OBSERVED", ProducedAt: producedAt,
				IdempotencyKey: "converged-event",
			},
			want: ProducerEvent{
				HeimdallJobID: "heimdall-job-123", EventProducer: ProducerSlurmAdvisorController,
				EventType: AdviceReconciliationConverged, EventLevel: LevelRecovery,
				Payload: AdviceReconciliationConvergedPayload{
					AdviceID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
					Generation: 7, Reason: "DESIRED_STATE_OBSERVED",
				},
				EventProducedAt: producedAt.UnixMicro(), IdempotencyKey: "converged-event",
			},
		},
		"Advice reconciliation abandoned": {
			event: AdviceReconciliationAbandonedEvent{
				HeimdallJobID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
				Generation: 7, Reason: "ADVICE_EXPIRED", ProducedAt: producedAt,
				IdempotencyKey: "abandoned-event",
			},
			want: ProducerEvent{
				HeimdallJobID: "heimdall-job-123", EventProducer: ProducerSlurmAdvisorController,
				EventType: AdviceReconciliationAbandoned, EventLevel: LevelRecovery,
				Payload: AdviceReconciliationAbandonedPayload{
					AdviceID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
					Generation: 7, Reason: "ADVICE_EXPIRED",
				},
				EventProducedAt: producedAt.UnixMicro(), IdempotencyKey: "abandoned-event",
			},
		},
		"job auto requeue hold completed": {
			event: JobAutoRequeueHoldCompletedEvent{
				HeimdallJobID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
				CorrelationUUID: "correlation-789", Success: true, ProducedAt: producedAt,
				IdempotencyKey: "direct-event",
			},
			want: ProducerEvent{
				HeimdallJobID: "heimdall-job-123", EventProducer: ProducerSlurmAdvisorController,
				EventType: JobAutoRequeueHoldCompleted, EventLevel: LevelRecovery,
				Payload: JobAutoRequeueHoldCompletedPayload{
					JobID: "456", AttemptIndex: 2, CorrelationUUID: "correlation-789", Success: true,
				},
				EventProducedAt: producedAt.UnixMicro(), IdempotencyKey: "direct-event",
			},
		},
		"watchdog release": {
			event: WatchdogReleaseCompletedEvent{
				HeimdallJobID: "heimdall-job-123", JobID: "456", AttemptIndex: 2,
				Success: true, ProducedAt: producedAt, IdempotencyKey: "watchdog-event",
			},
			want: ProducerEvent{
				HeimdallJobID: "heimdall-job-123", EventProducer: ProducerSlurmWatchdog,
				EventType: WatchdogReleaseCompleted, EventLevel: LevelRecovery,
				Payload:         WatchdogReleaseCompletedPayload{JobID: "456", AttemptIndex: 2, Success: true},
				EventProducedAt: producedAt.UnixMicro(), IdempotencyKey: "watchdog-event",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if diff := cmp.Diff(test.want, test.event.ToProducerEvent()); diff != "" {
				t.Fatalf("producer event mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestAdviceApplicationEvaluatedEventCopiesPlannedActions(t *testing.T) {
	t.Parallel()
	actions := []string{"slurm.release"}
	event := AdviceApplicationEvaluatedEvent{PlannedActions: actions}.ToProducerEvent()
	actions[0] = "mutated"

	payload := event.Payload.(AdviceApplicationEvaluatedPayload)
	if diff := cmp.Diff([]string{"slurm.release"}, payload.PlannedActions); diff != "" {
		t.Fatalf("planned actions mismatch (-want +got):\n%s", diff)
	}
}
