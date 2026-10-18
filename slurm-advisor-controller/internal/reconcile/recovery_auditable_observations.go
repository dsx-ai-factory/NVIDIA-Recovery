// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"time"

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

func (c *Controller) observeAdviceSkip(advice *pb.Advice, reason AdviceSkipReason) {
	observation := newAdviceApplicationObservation(advice)
	observation.Outcome = AdviceApplicationOutcomeSkipped
	observation.SkipReason = reason
	c.auditObserver().ObserveAdviceApplication(observation)
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

func (c *Controller) runAdviceSchedulerAction(
	ctx context.Context,
	observation *AdviceApplicationObservation,
	action SchedulerActionName,
	mode cluster.JobMode,
	command func(context.Context) error,
) error {
	if mode != cluster.JobModeAct {
		observation.Actions = append(observation.Actions, SchedulerActionObservation{
			Action:  action,
			Outcome: SchedulerActionOutcomePlanned,
		})
		return nil
	}

	actionErr := command(ctx)
	outcome := SchedulerActionOutcomeSuccess
	if actionErr != nil {
		outcome = SchedulerActionOutcomeError
	}
	actionObservation := SchedulerActionObservation{
		Action:      action,
		Outcome:     outcome,
		CompletedAt: c.observedAt().UTC(),
		Err:         actionErr,
	}
	observation.Actions = append(observation.Actions, actionObservation)
	c.auditObserver().ObserveAdviceSchedulerAction(*observation, actionObservation)
	return actionErr
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
