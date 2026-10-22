// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package recovery projects controller recovery observations into audit events.
package recovery

import (
	"container/list"
	"strings"
	"sync"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobidentity"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	"github.com/google/uuid"
)

// maxPlannedEvaluations bounds process-local audit deduplication. Eviction or a
// controller restart may reannounce an otherwise unchanged observe evaluation.
const maxPlannedEvaluations = 4096

type adviceIdentity struct {
	adviceID        string
	jobID           string
	attemptIndex    int32
	correlationUUID string
}

type plannedEvaluation struct {
	identity       adviceIdentity
	generation     int64
	plannedActions string
}

// Observer emits audit events from completed controller recovery observations.
type Observer struct {
	sink  audit.Sink
	now   func() time.Time
	newID func() string

	evaluationsMu sync.Mutex
	evaluations   map[adviceIdentity]*list.Element
	evaluationLRU list.List
}

// NewObserver returns an audit observer backed by sink.
func NewObserver(sink audit.Sink) *Observer {
	if sink == nil {
		sink = audit.NopSink{}
	}
	return &Observer{
		sink:        sink,
		now:         time.Now,
		newID:       uuid.NewString,
		evaluations: make(map[adviceIdentity]*list.Element),
	}
}

// ObserveAdviceSchedulerAction emits one event for a returned Advice-driven
// scheduler command.
func (observer *Observer) ObserveAdviceSchedulerAction(
	application reconcile.AdviceApplicationObservation,
	action reconcile.SchedulerActionObservation,
) {
	if action.Outcome != reconcile.SchedulerActionOutcomeSuccess &&
		action.Outcome != reconcile.SchedulerActionOutcomeError {
		return
	}
	producedAt := action.CompletedAt
	if producedAt.IsZero() {
		producedAt = observer.now()
	}
	observer.add(audit.AdviceSchedulerActionCompletedEvent{
		HeimdallJobID:   application.AdviceID,
		JobID:           application.JobID,
		AttemptIndex:    application.AttemptIndex,
		CorrelationUUID: application.CorrelationUUID,
		Generation:      application.Generation, RecoverySequenceID: application.RecoverySequenceID, TraceID: action.TraceID, SpanID: action.SpanID,
		Action:         string(action.Action),
		Success:        action.Outcome == reconcile.SchedulerActionOutcomeSuccess,
		Error:          errorString(action.Err),
		ProducedAt:     producedAt,
		IdempotencyKey: action.OccurrenceID,
	})
}

// ObserveAdviceApplication emits one application event for a completed Advice
// observation. Returned scheduler commands are emitted separately at command
// completion; only changed planned evaluations are emitted for observe mode.
func (observer *Observer) ObserveAdviceApplication(
	observation reconcile.AdviceApplicationObservation,
) {
	plannedActions := make([]string, 0, len(observation.Actions))
	for _, action := range observation.Actions {
		if action.Outcome == reconcile.SchedulerActionOutcomePlanned {
			plannedActions = append(plannedActions, string(action.Action))
		}
	}

	if observer.unchangedEvaluation(observation, plannedActions) {
		return
	}

	switch observation.Outcome {
	case reconcile.AdviceApplicationOutcomeApplied:
		observer.add(audit.AdviceApplicationAppliedEvent{
			HeimdallJobID:   observation.AdviceID,
			JobID:           observation.JobID,
			AttemptIndex:    observation.AttemptIndex,
			CorrelationUUID: observation.CorrelationUUID,
			Generation:      observation.Generation, RecoverySequenceID: observation.RecoverySequenceID, TraceID: observation.TraceID, SpanID: observation.SpanID,
			ProducedAt:     observation.CompletedAt,
			IdempotencyKey: observation.OccurrenceID,
		})
	case reconcile.AdviceApplicationOutcomeSkipped:
		// Skips reflect level-triggered reconciliation state and can recur without
		// a workload recovery transition, so they are not audit events.
		return
	case reconcile.AdviceApplicationOutcomePlanned:
		observer.add(audit.AdviceApplicationEvaluatedEvent{
			HeimdallJobID:   observation.AdviceID,
			JobID:           observation.JobID,
			AttemptIndex:    observation.AttemptIndex,
			CorrelationUUID: observation.CorrelationUUID,
			Generation:      observation.Generation, RecoverySequenceID: observation.RecoverySequenceID, TraceID: observation.TraceID, SpanID: observation.SpanID,
			Outcome:        string(observation.Outcome),
			PlannedActions: plannedActions,
			ProducedAt:     observation.CompletedAt,
			IdempotencyKey: observation.OccurrenceID,
		})
	case reconcile.AdviceApplicationOutcomeFailed:
		observer.add(audit.AdviceApplicationFailedEvent{
			HeimdallJobID:   observation.AdviceID,
			JobID:           observation.JobID,
			AttemptIndex:    observation.AttemptIndex,
			CorrelationUUID: observation.CorrelationUUID,
			Generation:      observation.Generation, RecoverySequenceID: observation.RecoverySequenceID, TraceID: observation.TraceID, SpanID: observation.SpanID,
			Error:          errorString(observation.Err),
			ProducedAt:     observation.CompletedAt,
			IdempotencyKey: observation.OccurrenceID,
		})
	}
}

// unchangedEvaluation only suppresses audit emission after reconciliation. Any
// non-planned outcome invalidates the prior plan so a later return to observe
// mode or an earlier plan is visible. The lock protects the LRU, never scheduler
// work or sink delivery; action and terminal-status events bypass this cache.
func (observer *Observer) unchangedEvaluation(
	observation reconcile.AdviceApplicationObservation,
	plannedActions []string,
) bool {
	identity := adviceIdentity{
		adviceID:        observation.AdviceID,
		jobID:           observation.JobID,
		attemptIndex:    observation.AttemptIndex,
		correlationUUID: observation.CorrelationUUID,
	}
	observer.evaluationsMu.Lock()
	defer observer.evaluationsMu.Unlock()

	previous := observer.evaluations[identity]
	if observation.Outcome != reconcile.AdviceApplicationOutcomePlanned {
		if previous != nil {
			observer.evaluationLRU.Remove(previous)
			delete(observer.evaluations, identity)
		}
		return false
	}

	current := plannedEvaluation{
		identity:       identity,
		generation:     observation.Generation,
		plannedActions: strings.Join(plannedActions, "\x00"),
	}
	if previous != nil {
		observer.evaluationLRU.MoveToFront(previous)
		unchanged := previous.Value.(plannedEvaluation) == current
		previous.Value = current
		return unchanged
	}
	observer.evaluations[identity] = observer.evaluationLRU.PushFront(current)
	if len(observer.evaluations) > maxPlannedEvaluations {
		oldest := observer.evaluationLRU.Back()
		delete(observer.evaluations, oldest.Value.(plannedEvaluation).identity)
		observer.evaluationLRU.Remove(oldest)
	}
	return false
}

// ObserveAdviceTerminalStatus emits one event after Job Manager durably accepts
// a terminal Advice generation.
func (observer *Observer) ObserveAdviceTerminalStatus(
	observation reconcile.AdviceTerminalStatusObservation,
) {
	producedAt := observation.ObservedAt
	if producedAt.IsZero() {
		producedAt = observer.now()
	}
	reason := strings.TrimPrefix(
		observation.Reason.String(),
		"ADVICE_RECONCILIATION_REASON_",
	)

	switch observation.Disposition {
	case pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED:
		observer.add(audit.AdviceReconciliationConvergedEvent{
			HeimdallJobID:  observation.AdviceID,
			JobID:          observation.JobID,
			AttemptIndex:   observation.AttemptIndex,
			Generation:     observation.Generation,
			Reason:         reason,
			ProducedAt:     producedAt,
			IdempotencyKey: observer.newID(),
		})
	case pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED:
		observer.add(audit.AdviceReconciliationAbandonedEvent{
			HeimdallJobID:  observation.AdviceID,
			JobID:          observation.JobID,
			AttemptIndex:   observation.AttemptIndex,
			Generation:     observation.Generation,
			Reason:         reason,
			ProducedAt:     producedAt,
			IdempotencyKey: observer.newID(),
		})
	}
}

// ObserveFailedJobRequeueHold emits one event for a returned direct failed-job
// scheduler command. Planned decisions do not represent returned commands and
// are intentionally ignored.
func (observer *Observer) ObserveFailedJobRequeueHold(
	observation reconcile.FailedJobRequeueHoldObservation,
) {
	if observation.Outcome != reconcile.SchedulerActionOutcomeSuccess &&
		observation.Outcome != reconcile.SchedulerActionOutcomeError {
		return
	}
	producedAt := observation.CompletedAt
	if producedAt.IsZero() {
		producedAt = observer.now()
	}
	observer.add(audit.JobAutoRequeueHoldCompletedEvent{
		HeimdallJobID: jobidentity.HeimdallJobID(
			observation.JobID,
			observation.AttemptIndex,
		),
		JobID:           observation.JobID,
		AttemptIndex:    observation.AttemptIndex,
		CorrelationUUID: observation.CorrelationUUID,
		Success:         observation.Outcome == reconcile.SchedulerActionOutcomeSuccess,
		Error:           errorString(observation.Err),
		ProducedAt:      producedAt,
		IdempotencyKey:  observer.newID(),
	})
}

func (observer *Observer) add(event audit.ToProducerEvent) {
	observer.sink.Add(event.ToProducerEvent())
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

var _ reconcile.AuditObserver = (*Observer)(nil)
