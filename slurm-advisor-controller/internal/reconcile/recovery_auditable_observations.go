// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"time"

	"github.com/google/uuid"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

// AdviceApplicationOutcome describes the final result of an Advice application.
type AdviceApplicationOutcome string

const (
	// AdviceApplicationOutcomeApplied means the requested changes were applied.
	AdviceApplicationOutcomeApplied AdviceApplicationOutcome = "applied"
	// AdviceApplicationOutcomeSkipped means the Advice was not applicable.
	AdviceApplicationOutcomeSkipped AdviceApplicationOutcome = "skipped"
	// AdviceApplicationOutcomeFailed means application returned an error.
	AdviceApplicationOutcomeFailed AdviceApplicationOutcome = "failed"
	// AdviceApplicationOutcomePlanned means the changes were evaluated without mutation.
	AdviceApplicationOutcomePlanned AdviceApplicationOutcome = "planned"
)

// SchedulerActionName identifies an auditable scheduler command.
type SchedulerActionName string

const (
	// SchedulerActionRequeueHold requeues a job directly into hold.
	SchedulerActionRequeueHold SchedulerActionName = "slurm.requeue_hold"
	// SchedulerActionUpdateConstraints updates a job's node constraints.
	SchedulerActionUpdateConstraints SchedulerActionName = "slurm.update_constraints"
	// SchedulerActionRelease releases a held job.
	SchedulerActionRelease SchedulerActionName = "slurm.release"
	// SchedulerActionCancel cancels a job.
	SchedulerActionCancel SchedulerActionName = "slurm.cancel"
)

// SchedulerActionOutcome describes the result of an auditable scheduler command.
type SchedulerActionOutcome string

const (
	// SchedulerActionOutcomePlanned means the command was evaluated without mutation.
	SchedulerActionOutcomePlanned SchedulerActionOutcome = "planned"
	// SchedulerActionOutcomeSuccess means the scheduler command succeeded.
	SchedulerActionOutcomeSuccess SchedulerActionOutcome = "success"
	// SchedulerActionOutcomeError means the scheduler command returned an error.
	SchedulerActionOutcomeError SchedulerActionOutcome = "error"
)

// AdviceApplicationObservation describes one completed Advice application.
type AdviceApplicationObservation struct {
	// OccurrenceID identifies this application independently from command attempts.
	OccurrenceID string
	// TargetAttempt is the scheduler attempt targeted by this application.
	TargetAttempt int32
	// RecoverySequenceID explicitly identifies the backend-owned recovery.
	RecoverySequenceID string
	// TraceID and SpanID identify the actual application span when available.
	TraceID string
	SpanID  string
	// CompletedAt records application return, before projection or delivery.
	CompletedAt time.Time
	// recoverySpan owns instrumentation state without exposing execution context.
	recoverySpan RecoverySpan
	recovery     *RecoveryContext

	// AdviceID identifies the Advice record.
	AdviceID string
	// Generation identifies the evaluated Advice generation.
	Generation int64
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32
	// CorrelationUUID identifies the durable recovery sequence, falling back to
	// the Advice ID when recovery context is unavailable.
	CorrelationUUID string
	// Outcome describes the completed application.
	Outcome AdviceApplicationOutcome
	// SkipReason explains why application was skipped.
	SkipReason AdviceSkipReason
	// Err is the application error when present.
	Err error
	// Actions contains scheduler actions planned or completed by the application.
	Actions []SchedulerActionObservation
}

// SchedulerActionObservation describes one Advice-driven scheduler command.
type SchedulerActionObservation struct {
	// OccurrenceID is shared by audit and milestone retries for one command attempt.
	OccurrenceID string
	// TraceID and SpanID identify this action's actual operation span.
	TraceID string
	SpanID  string
	// StartedAt records command start; Duration retains the monotonic interval.
	StartedAt time.Time
	Duration  time.Duration
	// Mode is the effective scheduler mutation policy for this attempt.
	Mode cluster.JobMode
	// CompletesSequence is explicit adapter intent after a successful final action.
	CompletesSequence bool

	// Action identifies the scheduler command.
	Action SchedulerActionName
	// Outcome describes the planned or completed command.
	Outcome SchedulerActionOutcome
	// CompletedAt records when the scheduler command returned.
	CompletedAt time.Time
	// Err is the scheduler command error when present.
	Err error
}

// FailedJobRequeueHoldObservation describes a returned direct failed-job command.
type FailedJobRequeueHoldObservation struct {
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the failed scheduler attempt.
	AttemptIndex int32
	// CorrelationUUID identifies a durable recovery sequence when available.
	CorrelationUUID string
	// Outcome describes the returned scheduler command.
	Outcome SchedulerActionOutcome
	// CompletedAt records when the scheduler command returned.
	CompletedAt time.Time
	// Err is the scheduler command error when present.
	Err error
}

// AdviceTerminalStatusObservation describes a terminal status accepted by the
// durable Advice status boundary.
type AdviceTerminalStatusObservation struct {
	// AdviceID identifies the Advice record.
	AdviceID string
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32
	// Generation identifies the terminal Advice generation.
	Generation int64
	// Disposition identifies whether the generation converged or was abandoned.
	Disposition pb.AdviceReconciliationDisposition
	// Reason explains the terminal disposition.
	Reason pb.AdviceReconciliationReason
	// ObservedAt records when the terminal scheduler or policy observation was made.
	ObservedAt time.Time
}

// AuditObserver receives completed recovery observations independently of
// scheduler decisions and audit delivery.
type AuditObserver interface {
	// ObserveAdviceSchedulerAction records one returned Advice-driven command.
	ObserveAdviceSchedulerAction(AdviceApplicationObservation, SchedulerActionObservation)
	// ObserveAdviceApplication records one completed Advice application.
	ObserveAdviceApplication(AdviceApplicationObservation)
	// ObserveAdviceTerminalStatus records a durably accepted terminal status.
	ObserveAdviceTerminalStatus(AdviceTerminalStatusObservation)
	// ObserveFailedJobRequeueHold records one returned direct failed-job command.
	ObserveFailedJobRequeueHold(FailedJobRequeueHoldObservation)
}

// NopAuditObserver ignores recovery observations.
type NopAuditObserver struct{}

// ObserveAdviceSchedulerAction records nothing.
func (NopAuditObserver) ObserveAdviceSchedulerAction(
	AdviceApplicationObservation,
	SchedulerActionObservation,
) {
}

// ObserveAdviceApplication records nothing.
func (NopAuditObserver) ObserveAdviceApplication(AdviceApplicationObservation) {}

// ObserveAdviceTerminalStatus records nothing.
func (NopAuditObserver) ObserveAdviceTerminalStatus(AdviceTerminalStatusObservation) {}

// ObserveFailedJobRequeueHold records nothing.
func (NopAuditObserver) ObserveFailedJobRequeueHold(FailedJobRequeueHoldObservation) {}

func (c *Controller) auditObserver() AuditObserver {
	if c.AuditObserver != nil {
		return c.AuditObserver
	}
	return NopAuditObserver{}
}

// startAdviceApplication begins before policy classification. Only the tracing
// projection retains its context; callers retain execution and timeout ownership.
func (c *Controller) startAdviceApplication(ctx context.Context, advice *pb.Advice) AdviceApplicationObservation {
	application := newAdviceApplicationObservation(advice)
	application.recoverySpan = nopRecoverySpan{}
	recovery, parent := c.adviceRecoveryContext(ctx, advice)
	application.recovery = recovery
	if recovery == nil {
		return application
	}
	application.RecoverySequenceID = recovery.SequenceID
	application.TargetAttempt = recovery.SourceAttempt
	if recovery.Operation == RecoveryOperationRestart {
		application.TargetAttempt++
	}
	_, application.recoverySpan = c.recoveryTelemetry().StartReconcile(parent, ReconcileSpanAttributes{
		Recovery: *recovery, JobID: advice.GetJobId(), TargetAttempt: application.TargetAttempt,
		AdviceID: application.AdviceID, Generation: application.Generation,
		OccurrenceID: application.OccurrenceID, AdviceState: advice.GetAdvice().GetState().String(),
	})
	application.TraceID, application.SpanID = application.recoverySpan.Correlation()
	return application
}

func (c *Controller) finishAdviceApplication(application AdviceApplicationObservation, result AdviceSyncResult, err error, mode cluster.JobMode) {
	application.CompletedAt = c.observedAt().UTC().Truncate(time.Microsecond)
	spanResult := classifyReconcileResult(result, err, mode)
	spanResult.CompletedAt = application.CompletedAt
	application.recoverySpan.EndReconcile(spanResult)
	c.observeAdviceApplication(application, result, err)
}

func (c *Controller) observeAdviceApplication(
	observation AdviceApplicationObservation,
	result AdviceSyncResult,
	err error,
) {
	observation.Outcome = AdviceApplicationOutcomeApplied
	switch {
	case err != nil:
		observation.Outcome = AdviceApplicationOutcomeFailed
		observation.Err = err
	case len(result.Skips) > 0:
		observation.Outcome = AdviceApplicationOutcomeSkipped
		observation.SkipReason = result.Skips[0].Reason
	case hasPlannedSchedulerAction(observation.Actions):
		observation.Outcome = AdviceApplicationOutcomePlanned
	}

	c.auditObserver().ObserveAdviceApplication(observation)
}

func newAdviceApplicationObservation(advice *pb.Advice) AdviceApplicationObservation {
	correlationUUID := advice.GetRecoveryContext().GetSequenceId()
	if correlationUUID == "" {
		// Older Advice does not carry RecoveryContext, so retain a stable join key
		// rather than emitting an uncorrelated recovery event.
		correlationUUID = advice.GetHeimdallJobId()
	}
	return AdviceApplicationObservation{
		OccurrenceID:    uuid.NewString(),
		TargetAttempt:   advice.GetAttemptIndex(),
		AdviceID:        advice.GetHeimdallJobId(),
		Generation:      advice.GetGeneration(),
		JobID:           advice.GetJobId(),
		AttemptIndex:    advice.GetAttemptIndex(),
		CorrelationUUID: correlationUUID,
	}
}

func hasPlannedSchedulerAction(actions []SchedulerActionObservation) bool {
	for _, action := range actions {
		if action.Outcome == SchedulerActionOutcomePlanned {
			return true
		}
	}
	return false
}

// runAdviceSchedulerAction is the sole Advice command boundary. It captures a
// returned command once, then projects the immutable occurrence independently.
// Remote delivery is owned by bounded workers and cannot delay the next command.
func (c *Controller) runAdviceSchedulerAction(
	ctx context.Context, application *AdviceApplicationObservation,
	action SchedulerActionName, completesSequenceOnSuccess bool,
	mode cluster.JobMode, command func(context.Context) error,
) error {
	started := time.Now()
	observation := SchedulerActionObservation{
		StartedAt:    started.UTC().Truncate(time.Microsecond),
		OccurrenceID: uuid.NewString(), Action: action, Mode: mode,
		Outcome: SchedulerActionOutcomePlanned,
	}
	var span RecoverySpan = nopRecoverySpan{}
	if recovery := application.recovery; recovery != nil {
		span = application.recoverySpan.StartSchedulerAction(SchedulerActionSpanAttributes{
			Recovery: *recovery, JobID: application.JobID, TargetAttempt: application.TargetAttempt,
			AdviceID: application.AdviceID, Generation: application.Generation,
			OccurrenceID: observation.OccurrenceID, Action: action, StartedAt: observation.StartedAt, Mode: mode,
			SLOEligible: recovery.Operation == RecoveryOperationRestart && mode == cluster.JobModeAct,
		})
		observation.TraceID, observation.SpanID = span.Correlation()
	}
	if mode == cluster.JobModeAct {
		observation.Err = command(ctx)
		// Capture both clocks before any observer or remote reporting can run.
		observation.Duration = time.Since(started)
		observation.CompletedAt = c.observedAt().UTC().Truncate(time.Microsecond)
		observation.Outcome = SchedulerActionOutcomeSuccess
		if observation.Err != nil {
			observation.Outcome = SchedulerActionOutcomeError
		}
		observation.CompletesSequence = completesSequenceOnSuccess && observation.Err == nil
	}
	span.EndSchedulerAction(SchedulerActionSpanResult{
		Outcome: observation.Outcome, CompletedAt: observation.CompletedAt,
		Duration: observation.Duration, Err: observation.Err,
		CompletesSequence: observation.CompletesSequence,
	})
	application.Actions = append(application.Actions, observation)
	if mode == cluster.JobModeAct {
		c.auditObserver().ObserveAdviceSchedulerAction(*application, observation)
		if recovery := application.recovery; recovery != nil && recovery.SequenceID != "" {
			if c.MilestoneReporter != nil {
				c.MilestoneReporter.Enqueue(RecoveryMilestone{Recovery: *recovery, Action: observation})
			} else {
				c.recoveryTelemetry().ObserveMilestoneReport(recovery.Operation, action, "unavailable")
			}
		}
	}
	return observation.Err
}

func (c *Controller) observeFailedJobRequeueHold(
	job cluster.JobExecutionRecord,
	err error,
) {
	outcome := SchedulerActionOutcomeSuccess
	if err != nil {
		outcome = SchedulerActionOutcomeError
	}
	c.auditObserver().ObserveFailedJobRequeueHold(FailedJobRequeueHoldObservation{
		JobID:        job.JobID,
		AttemptIndex: job.AttemptIndex,
		Outcome:      outcome,
		CompletedAt:  c.observedAt().UTC(),
		Err:          err,
	})
}
