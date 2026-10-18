// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package audit defines workload audit events produced by the Slurm Advisor Controller.
package audit

import (
	"slices"
	"time"
)

// EventLevel classifies the audience and operational importance of an audit event.
type EventLevel string

const (
	// LevelUnknown is reserved for events without a recognized level.
	LevelUnknown EventLevel = ""
	// LevelRecovery identifies events on the recovery path.
	LevelRecovery EventLevel = "recovery"
	// LevelOperational identifies events about Heimdall operations.
	LevelOperational EventLevel = "operational"
)

// EventType identifies the schema and meaning of an audit event payload.
type EventType string

// EventProducer identifies the process that emitted an audit event.
type EventProducer string

const (
	// AdviceSchedulerActionCompleted records a returned Advice-driven scheduler command.
	AdviceSchedulerActionCompleted EventType = "advice_scheduler_action_completed"
	// AdviceApplicationApplied records Advice that was applied successfully.
	AdviceApplicationApplied EventType = "advice_application_applied"
	// AdviceApplicationEvaluated records actions planned in observe mode.
	AdviceApplicationEvaluated EventType = "advice_application_evaluated"
	// AdviceApplicationFailed records an Advice application that failed.
	AdviceApplicationFailed EventType = "advice_application_failed"
	// AdviceReconciliationConverged records a durably persisted successful terminal outcome.
	AdviceReconciliationConverged EventType = "advice_reconciliation_converged"
	// AdviceReconciliationAbandoned records a durably persisted unsuccessful terminal outcome.
	AdviceReconciliationAbandoned EventType = "advice_reconciliation_abandoned"
	// JobAutoRequeueHoldCompleted records a returned direct failed-job requeue-hold command.
	JobAutoRequeueHoldCompleted EventType = "job_auto_requeue_hold_completed"
	// WatchdogReleaseCompleted records a returned watchdog release command.
	WatchdogReleaseCompleted EventType = "watchdog_release_completed"
)

const (
	// ProducerSlurmAdvisorController identifies the Slurm Advisor Controller audit producer.
	ProducerSlurmAdvisorController EventProducer = "slurm-advisor-controller"
	// ProducerSlurmWatchdog identifies the Slurm watchdog audit producer.
	ProducerSlurmWatchdog EventProducer = "slurm-watchdog"
)

// ProducerEvent is an immutable audit event accepted by the delivery buffer.
type ProducerEvent struct {
	// HeimdallJobID is the workload query key associated with the event.
	HeimdallJobID string `json:"heimdall_job_id"`
	// EventProducer identifies the component that produced this event.
	EventProducer EventProducer `json:"event_producer"`
	// EventType identifies the payload schema and event meaning.
	EventType EventType `json:"event_type"`
	// EventLevel classifies the event's audience and operational importance.
	EventLevel EventLevel `json:"event_level"`
	// Payload contains event-specific structured data.
	Payload any `json:"payload"`
	// EventProducedAt is the producer emission time as Unix microseconds.
	EventProducedAt int64 `json:"event_produced_at"`
	// IdempotencyKey identifies this producer event across delivery retries.
	IdempotencyKey string `json:"idempotency_key"`
}

// ToProducerEvent converts a typed audit event into its delivery envelope.
type ToProducerEvent interface {
	// ToProducerEvent returns the immutable producer event sent to the audit sink.
	ToProducerEvent() ProducerEvent
}

// AdviceSchedulerActionCompletedEvent records a returned Advice-driven scheduler command.
type AdviceSchedulerActionCompletedEvent struct {
	// HeimdallJobID identifies the Advice record and its Heimdall job.
	HeimdallJobID string
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32
	// CorrelationUUID identifies the durable recovery sequence, or the Advice ID
	// when recovery context is unavailable.
	CorrelationUUID string
	// Action identifies the scheduler command.
	Action string
	// Success reports whether the scheduler command returned successfully.
	Success bool
	// Error contains the scheduler command error when present.
	Error string
	// ProducedAt is when the scheduler command returned.
	ProducedAt time.Time
	// IdempotencyKey identifies this producer event across delivery retries.
	IdempotencyKey string
}

// ToProducerEvent converts the typed scheduler-action event into its delivery envelope.
func (event AdviceSchedulerActionCompletedEvent) ToProducerEvent() ProducerEvent {
	return newProducerEvent(
		event.HeimdallJobID,
		ProducerSlurmAdvisorController,
		AdviceSchedulerActionCompleted,
		AdviceSchedulerActionCompletedPayload{
			AdviceID:        event.HeimdallJobID,
			JobID:           event.JobID,
			AttemptIndex:    event.AttemptIndex,
			CorrelationUUID: event.CorrelationUUID,
			Action:          event.Action,
			Success:         event.Success,
			Error:           event.Error,
		},
		event.ProducedAt,
		event.IdempotencyKey,
	)
}

// AdviceApplicationAppliedEvent records Advice that was applied successfully.
type AdviceApplicationAppliedEvent struct {
	// HeimdallJobID identifies the Advice record and its Heimdall job.
	HeimdallJobID string
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32
	// CorrelationUUID identifies the durable recovery sequence, or the Advice ID
	// when recovery context is unavailable.
	CorrelationUUID string
	// ProducedAt is when the controller completed the Advice application.
	ProducedAt time.Time
	// IdempotencyKey identifies this producer event across delivery retries.
	IdempotencyKey string
}

// ToProducerEvent converts the typed applied-application event into its delivery envelope.
func (event AdviceApplicationAppliedEvent) ToProducerEvent() ProducerEvent {
	return newProducerEvent(
		event.HeimdallJobID,
		ProducerSlurmAdvisorController,
		AdviceApplicationApplied,
		AdviceApplicationAppliedPayload{
			AdviceID:        event.HeimdallJobID,
			JobID:           event.JobID,
			AttemptIndex:    event.AttemptIndex,
			CorrelationUUID: event.CorrelationUUID,
		},
		event.ProducedAt,
		event.IdempotencyKey,
	)
}

// AdviceApplicationEvaluatedEvent records actions planned in observe mode.
type AdviceApplicationEvaluatedEvent struct {
	// HeimdallJobID identifies the Advice record and its Heimdall job.
	HeimdallJobID string
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32
	// CorrelationUUID identifies the durable recovery sequence, or the Advice ID
	// when recovery context is unavailable.
	CorrelationUUID string
	// Outcome describes the observe-mode evaluation result.
	Outcome string
	// PlannedActions lists scheduler actions in evaluation order.
	PlannedActions []string
	// ProducedAt is when the controller completed the evaluation.
	ProducedAt time.Time
	// IdempotencyKey identifies this producer event across delivery retries.
	IdempotencyKey string
}

// ToProducerEvent converts the typed evaluated-application event into its delivery envelope.
func (event AdviceApplicationEvaluatedEvent) ToProducerEvent() ProducerEvent {
	return newProducerEvent(
		event.HeimdallJobID,
		ProducerSlurmAdvisorController,
		AdviceApplicationEvaluated,
		AdviceApplicationEvaluatedPayload{
			AdviceID:        event.HeimdallJobID,
			JobID:           event.JobID,
			AttemptIndex:    event.AttemptIndex,
			CorrelationUUID: event.CorrelationUUID,
			Outcome:         event.Outcome,
			PlannedActions:  slices.Clone(event.PlannedActions),
		},
		event.ProducedAt,
		event.IdempotencyKey,
	)
}

// AdviceApplicationFailedEvent records an Advice application that failed.
type AdviceApplicationFailedEvent struct {
	// HeimdallJobID identifies the Advice record and its Heimdall job.
	HeimdallJobID string
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32
	// CorrelationUUID identifies the durable recovery sequence, or the Advice ID
	// when recovery context is unavailable.
	CorrelationUUID string
	// Error contains the final Advice application error when present.
	Error string
	// ProducedAt is when the controller completed the Advice application.
	ProducedAt time.Time
	// IdempotencyKey identifies this producer event across delivery retries.
	IdempotencyKey string
}

// ToProducerEvent converts the typed failed-application event into its delivery envelope.
func (event AdviceApplicationFailedEvent) ToProducerEvent() ProducerEvent {
	return newProducerEvent(
		event.HeimdallJobID,
		ProducerSlurmAdvisorController,
		AdviceApplicationFailed,
		AdviceApplicationFailedPayload{
			AdviceID:        event.HeimdallJobID,
			JobID:           event.JobID,
			AttemptIndex:    event.AttemptIndex,
			CorrelationUUID: event.CorrelationUUID,
			Error:           event.Error,
		},
		event.ProducedAt,
		event.IdempotencyKey,
	)
}

// AdviceReconciliationConvergedEvent records a durably persisted converged Advice generation.
type AdviceReconciliationConvergedEvent struct {
	// HeimdallJobID identifies the Advice record and its Heimdall job.
	HeimdallJobID string
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32
	// Generation identifies the terminal Advice generation.
	Generation int64
	// Reason explains why fresh scheduler state proved convergence.
	Reason string
	// ProducedAt is when the terminal scheduler observation was made.
	ProducedAt time.Time
	// IdempotencyKey identifies this terminal transition across delivery retries.
	IdempotencyKey string
}

// ToProducerEvent converts the typed converged event into its delivery envelope.
func (event AdviceReconciliationConvergedEvent) ToProducerEvent() ProducerEvent {
	return newProducerEvent(
		event.HeimdallJobID,
		ProducerSlurmAdvisorController,
		AdviceReconciliationConverged,
		AdviceReconciliationConvergedPayload{
			AdviceID:     event.HeimdallJobID,
			JobID:        event.JobID,
			AttemptIndex: event.AttemptIndex,
			Generation:   event.Generation,
			Reason:       event.Reason,
		},
		event.ProducedAt,
		event.IdempotencyKey,
	)
}

// AdviceReconciliationAbandonedEvent records a durably persisted abandoned Advice generation.
type AdviceReconciliationAbandonedEvent struct {
	// HeimdallJobID identifies the Advice record and its Heimdall job.
	HeimdallJobID string
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32
	// Generation identifies the terminal Advice generation.
	Generation int64
	// Reason explains why the Advice generation can no longer converge.
	Reason string
	// ProducedAt is when the terminal scheduler or policy observation was made.
	ProducedAt time.Time
	// IdempotencyKey identifies this terminal transition across delivery retries.
	IdempotencyKey string
}

// ToProducerEvent converts the typed abandoned event into its delivery envelope.
func (event AdviceReconciliationAbandonedEvent) ToProducerEvent() ProducerEvent {
	return newProducerEvent(
		event.HeimdallJobID,
		ProducerSlurmAdvisorController,
		AdviceReconciliationAbandoned,
		AdviceReconciliationAbandonedPayload{
			AdviceID:     event.HeimdallJobID,
			JobID:        event.JobID,
			AttemptIndex: event.AttemptIndex,
			Generation:   event.Generation,
			Reason:       event.Reason,
		},
		event.ProducedAt,
		event.IdempotencyKey,
	)
}

// JobAutoRequeueHoldCompletedEvent records a returned direct failed-job command.
type JobAutoRequeueHoldCompletedEvent struct {
	// HeimdallJobID identifies the failed workload attempt.
	HeimdallJobID string
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the failed scheduler attempt.
	AttemptIndex int32
	// CorrelationUUID identifies the durable recovery sequence when available.
	CorrelationUUID string
	// Success reports whether the scheduler command returned successfully.
	Success bool
	// Error contains the scheduler command error when present.
	Error string
	// ProducedAt is when the scheduler command returned.
	ProducedAt time.Time
	// IdempotencyKey identifies this producer event across delivery retries.
	IdempotencyKey string
}

// ToProducerEvent converts the direct failed-job event into its delivery envelope.
func (event JobAutoRequeueHoldCompletedEvent) ToProducerEvent() ProducerEvent {
	return newProducerEvent(
		event.HeimdallJobID,
		ProducerSlurmAdvisorController,
		JobAutoRequeueHoldCompleted,
		JobAutoRequeueHoldCompletedPayload{
			JobID:           event.JobID,
			AttemptIndex:    event.AttemptIndex,
			CorrelationUUID: event.CorrelationUUID,
			Success:         event.Success,
			Error:           event.Error,
		},
		event.ProducedAt,
		event.IdempotencyKey,
	)
}

// WatchdogReleaseCompletedEvent records a returned watchdog release command.
type WatchdogReleaseCompletedEvent struct {
	// HeimdallJobID identifies the held workload attempt.
	HeimdallJobID string
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the held scheduler attempt.
	AttemptIndex int32
	// Success reports whether the scheduler command returned successfully.
	Success bool
	// Error contains the scheduler command error when present.
	Error string
	// ProducedAt is when the scheduler command returned.
	ProducedAt time.Time
	// IdempotencyKey identifies this producer event across delivery retries.
	IdempotencyKey string
}

// ToProducerEvent converts the watchdog release event into its delivery envelope.
func (event WatchdogReleaseCompletedEvent) ToProducerEvent() ProducerEvent {
	return newProducerEvent(
		event.HeimdallJobID,
		ProducerSlurmWatchdog,
		WatchdogReleaseCompleted,
		WatchdogReleaseCompletedPayload{
			JobID:        event.JobID,
			AttemptIndex: event.AttemptIndex,
			Success:      event.Success,
			Error:        event.Error,
		},
		event.ProducedAt,
		event.IdempotencyKey,
	)
}

func newProducerEvent(
	heimdallJobID string,
	producer EventProducer,
	eventType EventType,
	payload any,
	producedAt time.Time,
	idempotencyKey string,
) ProducerEvent {
	return ProducerEvent{
		HeimdallJobID:   heimdallJobID,
		EventProducer:   producer,
		EventType:       eventType,
		EventLevel:      LevelRecovery,
		Payload:         payload,
		EventProducedAt: producedAt.UnixMicro(),
		IdempotencyKey:  idempotencyKey,
	}
}

var (
	_ ToProducerEvent = AdviceSchedulerActionCompletedEvent{}
	_ ToProducerEvent = AdviceApplicationAppliedEvent{}
	_ ToProducerEvent = AdviceApplicationEvaluatedEvent{}
	_ ToProducerEvent = AdviceApplicationFailedEvent{}
	_ ToProducerEvent = AdviceReconciliationConvergedEvent{}
	_ ToProducerEvent = AdviceReconciliationAbandonedEvent{}
	_ ToProducerEvent = JobAutoRequeueHoldCompletedEvent{}
	_ ToProducerEvent = WatchdogReleaseCompletedEvent{}
)

// AdviceSchedulerActionCompletedPayload describes a completed scheduler command.
type AdviceSchedulerActionCompletedPayload struct {
	// AdviceID identifies the Advice record.
	AdviceID string `json:"advice_id"`
	// JobID identifies the scheduler job.
	JobID string `json:"job_id"`
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32 `json:"attempt_index"`
	// CorrelationUUID identifies the durable recovery sequence, or the Advice ID
	// when recovery context is unavailable.
	CorrelationUUID string `json:"correlation_uuid,omitempty"`
	// Action identifies the scheduler command.
	Action string `json:"action"`
	// Success reports whether the scheduler command returned successfully.
	Success bool `json:"success"`
	// Error contains the scheduler command error when present.
	Error string `json:"error,omitempty"`
}

// AdviceApplicationAppliedPayload describes a successfully applied Advice record.
type AdviceApplicationAppliedPayload struct {
	// AdviceID identifies the Advice record.
	AdviceID string `json:"advice_id"`
	// JobID identifies the scheduler job.
	JobID string `json:"job_id"`
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32 `json:"attempt_index"`
	// CorrelationUUID identifies the durable recovery sequence, or the Advice ID
	// when recovery context is unavailable.
	CorrelationUUID string `json:"correlation_uuid,omitempty"`
}

// AdviceApplicationEvaluatedPayload describes actions planned in observe mode.
type AdviceApplicationEvaluatedPayload struct {
	// AdviceID identifies the Advice record.
	AdviceID string `json:"advice_id"`
	// JobID identifies the scheduler job.
	JobID string `json:"job_id"`
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32 `json:"attempt_index"`
	// CorrelationUUID identifies the durable recovery sequence, or the Advice ID
	// when recovery context is unavailable.
	CorrelationUUID string `json:"correlation_uuid,omitempty"`
	// Outcome describes the observe-mode evaluation result.
	Outcome string `json:"outcome"`
	// PlannedActions lists scheduler actions in evaluation order.
	PlannedActions []string `json:"planned_actions"`
}

// AdviceApplicationFailedPayload describes a failed Advice application.
type AdviceApplicationFailedPayload struct {
	// AdviceID identifies the Advice record.
	AdviceID string `json:"advice_id"`
	// JobID identifies the scheduler job.
	JobID string `json:"job_id"`
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32 `json:"attempt_index"`
	// CorrelationUUID identifies the durable recovery sequence, or the Advice ID
	// when recovery context is unavailable.
	CorrelationUUID string `json:"correlation_uuid,omitempty"`
	// Error contains the final Advice application error when present.
	Error string `json:"error,omitempty"`
}

// AdviceReconciliationConvergedPayload describes a durably persisted converged Advice generation.
type AdviceReconciliationConvergedPayload struct {
	// AdviceID identifies the Advice record.
	AdviceID string `json:"advice_id"`
	// JobID identifies the scheduler job.
	JobID string `json:"job_id"`
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32 `json:"attempt_index"`
	// Generation identifies the terminal Advice generation.
	Generation int64 `json:"generation"`
	// Reason is the stable machine-readable convergence reason.
	Reason string `json:"reason"`
}

// AdviceReconciliationAbandonedPayload describes a durably persisted abandoned Advice generation.
type AdviceReconciliationAbandonedPayload struct {
	// AdviceID identifies the Advice record.
	AdviceID string `json:"advice_id"`
	// JobID identifies the scheduler job.
	JobID string `json:"job_id"`
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32 `json:"attempt_index"`
	// Generation identifies the terminal Advice generation.
	Generation int64 `json:"generation"`
	// Reason is the stable machine-readable abandonment reason.
	Reason string `json:"reason"`
}

// JobAutoRequeueHoldCompletedPayload describes a direct requeue-hold result.
type JobAutoRequeueHoldCompletedPayload struct {
	// JobID identifies the scheduler job.
	JobID string `json:"job_id"`
	// AttemptIndex identifies the failed scheduler attempt.
	AttemptIndex int32 `json:"attempt_index"`
	// CorrelationUUID identifies the durable recovery sequence when available.
	CorrelationUUID string `json:"correlation_uuid,omitempty"`
	// Success reports whether the scheduler command returned successfully.
	Success bool `json:"success"`
	// Error contains the scheduler command error when present.
	Error string `json:"error,omitempty"`
}

// WatchdogReleaseCompletedPayload describes a watchdog release result.
type WatchdogReleaseCompletedPayload struct {
	// JobID identifies the scheduler job.
	JobID string `json:"job_id"`
	// AttemptIndex identifies the held scheduler attempt.
	AttemptIndex int32 `json:"attempt_index"`
	// Success reports whether the scheduler command returned successfully.
	Success bool `json:"success"`
	// Error contains the scheduler command error when present.
	Error string `json:"error,omitempty"`
}
