// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"time"
)

// RecoveryOperation identifies the backend recovery contract being observed.
type RecoveryOperation string

const (
	// RecoveryOperationRestart identifies failed-job restart recovery.
	RecoveryOperationRestart RecoveryOperation = "restart"
	// RecoveryOperationTerminate identifies termination work.
	RecoveryOperationTerminate RecoveryOperation = "terminate"
	// RecoveryOperationDAGRelease identifies dependency-driven release.
	RecoveryOperationDAGRelease RecoveryOperation = "dag_release"
)

// RecoveryOutcome describes one Advice application without asserting sequence completion.
type RecoveryOutcome string

const (
	// RecoveryOutcomeApplied means applicable work returned successfully.
	RecoveryOutcomeApplied RecoveryOutcome = "applied"
	// RecoveryOutcomeSkipped means no application was required or currently possible.
	RecoveryOutcomeSkipped RecoveryOutcome = "skipped"
	// RecoveryOutcomeFailed means application failed and may be retried.
	RecoveryOutcomeFailed RecoveryOutcome = "failed"
	// RecoveryOutcomePlanned means the mutation policy allowed evaluation only.
	RecoveryOutcomePlanned RecoveryOutcome = "planned"
)

// RecoveryContext contains immutable correlation supplied by backend Advice.
type RecoveryContext struct {
	// SequenceID identifies the backend-owned recovery sequence.
	SequenceID string
	// Traceparent is its immutable W3C parent.
	Traceparent string
	// Tracestate carries validated W3C vendor state.
	Tracestate string
	// TriggeredAt is the first committed recovery trigger timestamp.
	TriggeredAt time.Time
	// Operation names the recovery contract.
	Operation RecoveryOperation
	// SourceAttempt identifies the originating workload attempt.
	SourceAttempt int32
}

// ReconcileSpanAttributes describe an Advice application before its work starts.
type ReconcileSpanAttributes struct {
	// Recovery carries backend-owned durable correlation.
	Recovery RecoveryContext
	// JobID identifies the scheduler job.
	JobID string
	// TargetAttempt identifies the workload attempt affected by the operation.
	TargetAttempt int32
	// AdviceID identifies the Advice record.
	AdviceID string
	// Generation and OccurrenceID retain historical operation identity.
	Generation   int64
	OccurrenceID string
	// AdviceState names the desired Advice state.
	AdviceState string
	// SLOEligible marks acting restart operations eligible for recovery measurement.
	SLOEligible bool
}

// ReconcileSpanResult describes the application result and existing generation classification.
type ReconcileSpanResult struct {
	// CompletedAt is the captured application return timestamp.
	CompletedAt time.Time
	// Outcome describes the captured operation result.
	Outcome RecoveryOutcome
	// Terminal reflects existing generation convergence or abandonment.
	Terminal bool
	// SkipReason explains a skipped application.
	SkipReason AdviceSkipReason
	// Err retains the causal operation error.
	Err error
}

// SchedulerActionSpanAttributes describe one scheduler action before execution.
type SchedulerActionSpanAttributes struct {
	// StartedAt is the action lifecycle start shared with other projections.
	StartedAt time.Time
	// Mode is the effective scheduler mutation policy.
	Mode cluster.JobMode
	// Recovery carries backend-owned durable correlation.
	Recovery RecoveryContext
	// JobID identifies the scheduler job.
	JobID string
	// TargetAttempt identifies the workload attempt affected by the operation.
	TargetAttempt int32
	// AdviceID identifies the Advice record.
	AdviceID string
	// Generation and OccurrenceID retain historical operation identity.
	Generation   int64
	OccurrenceID string
	// Action names the scheduler command.
	Action SchedulerActionName
	// SLOEligible marks acting restart operations eligible for recovery measurement.
	SLOEligible bool
}

// SchedulerActionSpanResult contains the captured command result shared by all projections.
type SchedulerActionSpanResult struct {
	// CompletesSequence retains explicit adapter completion intent separately from outcome.
	CompletesSequence bool
	// Duration is measured on the command execution monotonic clock.
	Duration time.Duration
	// Outcome describes the captured operation result.
	Outcome     SchedulerActionOutcome
	CompletedAt time.Time
	// Err retains the causal operation error.
	Err error
}

// RecoverySpan owns local tracing state for one real operation lifecycle.
type RecoverySpan interface {
	// SetMode records effective execution policy once admission has resolved it.
	SetMode(cluster.JobMode)
	// StartSchedulerAction starts a child using tracing-owned context.
	StartSchedulerAction(SchedulerActionSpanAttributes) RecoverySpan
	// Correlation returns valid IDs for this actual operation, or empty strings.
	Correlation() (traceID, spanID string)
	// EndReconcile ends the application span using the captured result.
	EndReconcile(ReconcileSpanResult)
	// EndSchedulerAction ends the action span using the captured command result.
	EndSchedulerAction(SchedulerActionSpanResult)
}

// RecoveryTelemetry projects domain lifecycles into traces and bounded metrics. Returned
// contexts carry instrumentation state only and must not control scheduler execution.
type RecoveryTelemetry interface {
	// WithoutParent removes unrelated polling trace ancestry.
	WithoutParent(context.Context) context.Context
	// RestoreParent validates and restores backend-owned W3C ancestry.
	RestoreParent(context.Context, RecoveryContext) (context.Context, error)
	// StartReconcile starts an Advice operation before application work.
	StartReconcile(context.Context, ReconcileSpanAttributes) (context.Context, RecoverySpan)
	// StartSchedulerAction starts a scheduler operation before its command.
	StartSchedulerAction(context.Context, SchedulerActionSpanAttributes) (context.Context, RecoverySpan)
	// ObserveRecoveryContext counts bounded context coverage outcomes.
	ObserveRecoveryContext(source, outcome string)
	// ObserveMilestoneReport counts independent report delivery outcomes.
	ObserveMilestoneReport(operation RecoveryOperation, action SchedulerActionName, result string)
}

// NopRecoveryTelemetry ignores tracing while preserving scheduler behavior.
type NopRecoveryTelemetry struct{}

// WithoutParent removes unrelated polling trace ancestry.
func (NopRecoveryTelemetry) WithoutParent(ctx context.Context) context.Context { return ctx }

// RestoreParent validates and restores backend-owned W3C ancestry.
func (NopRecoveryTelemetry) RestoreParent(ctx context.Context, _ RecoveryContext) (context.Context, error) {
	return ctx, nil
}

// StartReconcile starts an Advice operation before application work.
func (NopRecoveryTelemetry) StartReconcile(ctx context.Context, _ ReconcileSpanAttributes) (context.Context, RecoverySpan) {
	return ctx, nopRecoverySpan{}
}

// StartSchedulerAction starts a scheduler operation before its command.
func (NopRecoveryTelemetry) StartSchedulerAction(ctx context.Context, _ SchedulerActionSpanAttributes) (context.Context, RecoverySpan) {
	return ctx, nopRecoverySpan{}
}

// ObserveRecoveryContext counts bounded context coverage outcomes.
func (NopRecoveryTelemetry) ObserveRecoveryContext(string, string) {}

// ObserveMilestoneReport counts independent report delivery outcomes.
func (NopRecoveryTelemetry) ObserveMilestoneReport(RecoveryOperation, SchedulerActionName, string) {}

type nopRecoverySpan struct{}

func (nopRecoverySpan) EndReconcile(ReconcileSpanResult) {}

func (nopRecoverySpan) EndSchedulerAction(SchedulerActionSpanResult) {}

func (nopRecoverySpan) Correlation() (string, string) { return "", "" }

func (nopRecoverySpan) StartSchedulerAction(SchedulerActionSpanAttributes) RecoverySpan {
	return nopRecoverySpan{}
}

func (nopRecoverySpan) SetMode(cluster.JobMode) {}
