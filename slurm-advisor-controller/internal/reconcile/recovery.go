// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"go.uber.org/zap"
)

const recoveryContextSourceAdvice = "advice"

func (c *Controller) adviceRecoveryContext(ctx context.Context, advice *pb.Advice) (*RecoveryContext, context.Context) {
	fallback := func() (*RecoveryContext, context.Context) {
		operation := recoveryOperationForAdvice(advice)
		if operation == "" {
			return nil, c.recoveryTelemetry().WithoutParent(ctx)
		}
		return &RecoveryContext{
			Operation:     operation,
			SourceAttempt: advice.GetAttemptIndex(),
		}, c.recoveryTelemetry().WithoutParent(ctx)
	}
	stored := advice.GetRecoveryContext()
	if stored == nil {
		c.recoveryTelemetry().ObserveRecoveryContext(recoveryContextSourceAdvice, "missing")
		return fallback()
	}
	recovery, err := recoveryContextFromProto(stored, advice.GetAttemptIndex())
	if err != nil {
		c.observeRecoveryContextFailure(recoveryContextSourceAdvice, "invalid", "validate Advice recovery context", advice.GetJobId(), err)
		return fallback()
	}
	if expected := recoveryOperationForAdvice(advice); expected != "" && recovery.Operation != expected {
		c.observeRecoveryContextFailure(
			recoveryContextSourceAdvice,
			"invalid",
			"validate Advice recovery operation",
			advice.GetJobId(),
			errors.Newf("stored operation %q does not match Advice operation %q", recovery.Operation, expected),
		)
		return fallback()
	}
	parent, err := c.recoveryTelemetry().RestoreParent(ctx, recovery)
	if err != nil {
		c.observeRecoveryContextFailure(recoveryContextSourceAdvice, "invalid", "restore Advice recovery context", advice.GetJobId(), err)
		return fallback()
	}
	c.recoveryTelemetry().ObserveRecoveryContext(recoveryContextSourceAdvice, "valid")
	return &recovery, parent
}

func recoveryOperationForAdvice(advice *pb.Advice) RecoveryOperation {
	switch advice.GetAdvice().GetState() {
	case pb.AdviceState_ADVICE_STATE_RESTARTED:
		return RecoveryOperationRestart
	case pb.AdviceState_ADVICE_STATE_TERMINATED:
		return RecoveryOperationTerminate
	case pb.AdviceState_ADVICE_STATE_RELEASED:
		return RecoveryOperationDAGRelease
	default:
		return ""
	}
}

func classifyReconcileResult(result AdviceSyncResult, err error, mode cluster.JobMode) ReconcileSpanResult {
	if err != nil {
		return ReconcileSpanResult{Outcome: RecoveryOutcomeFailed, Terminal: false, Err: err}
	}
	if len(result.Skips) > 0 {
		reason := result.Skips[0].Reason
		return ReconcileSpanResult{
			Outcome:    RecoveryOutcomeSkipped,
			Terminal:   len(result.TerminalOutcomes) > 0,
			SkipReason: reason,
		}
	}
	if mode != cluster.JobModeAct {
		return ReconcileSpanResult{Outcome: RecoveryOutcomePlanned, Terminal: false}
	}
	return ReconcileSpanResult{Outcome: RecoveryOutcomeApplied, Terminal: len(result.TerminalOutcomes) > 0}
}

func recoveryContextFromProto(stored *pb.RecoveryContext, sourceAttempt int32) (RecoveryContext, error) {
	if stored == nil {
		return RecoveryContext{}, errors.New("recovery context is missing")
	}
	if stored.GetTriggeredAt() == nil || stored.GetTriggeredAt().CheckValid() != nil {
		return RecoveryContext{}, errors.New("recovery trigger timestamp is invalid")
	}
	operation, err := recoveryOperationFromProto(stored.GetOperation())
	if err != nil {
		return RecoveryContext{}, err
	}
	return RecoveryContext{
		SequenceID:    stored.GetSequenceId(),
		Traceparent:   stored.GetTraceparent(),
		Tracestate:    stored.GetTracestate(),
		TriggeredAt:   stored.GetTriggeredAt().AsTime(),
		Operation:     operation,
		SourceAttempt: sourceAttempt,
	}, nil
}

func recoveryOperationFromProto(operation pb.RecoveryOperation) (RecoveryOperation, error) {
	switch operation {
	case pb.RecoveryOperation_RECOVERY_OPERATION_RESTART:
		return RecoveryOperationRestart, nil
	case pb.RecoveryOperation_RECOVERY_OPERATION_TERMINATE:
		return RecoveryOperationTerminate, nil
	case pb.RecoveryOperation_RECOVERY_OPERATION_DAG_RELEASE:
		return RecoveryOperationDAGRelease, nil
	default:
		return "", errors.Newf("unsupported recovery operation %s", operation.String())
	}
}

func recoveryOperationToProto(operation RecoveryOperation) pb.RecoveryOperation {
	switch operation {
	case RecoveryOperationRestart:
		return pb.RecoveryOperation_RECOVERY_OPERATION_RESTART
	case RecoveryOperationTerminate:
		return pb.RecoveryOperation_RECOVERY_OPERATION_TERMINATE
	case RecoveryOperationDAGRelease:
		return pb.RecoveryOperation_RECOVERY_OPERATION_DAG_RELEASE
	default:
		return pb.RecoveryOperation_RECOVERY_OPERATION_UNSPECIFIED
	}
}

func schedulerActionToProto(action SchedulerActionName) pb.RecoverySchedulerAction {
	switch action {
	case SchedulerActionRequeueHold:
		return pb.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_REQUEUE_HOLD
	case SchedulerActionUpdateConstraints:
		return pb.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_UPDATE_CONSTRAINTS
	case SchedulerActionRelease:
		return pb.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_RELEASE
	case SchedulerActionCancel:
		return pb.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_CANCEL
	default:
		return pb.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_UNSPECIFIED
	}
}

func schedulerActionOutcomeToProto(outcome SchedulerActionOutcome) pb.RecoverySchedulerActionOutcome {
	switch outcome {
	case SchedulerActionOutcomePlanned:
		return pb.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_PLANNED
	case SchedulerActionOutcomeSuccess:
		return pb.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_SUCCESS
	case SchedulerActionOutcomeError:
		return pb.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_ERROR
	default:
		return pb.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_UNSPECIFIED
	}
}

func (c *Controller) observeRecoveryContextFailure(source, outcome, message, jobID string, err error) {
	c.recoveryTelemetry().ObserveRecoveryContext(source, outcome)
	if c.Logger != nil {
		c.Logger.Warn(message, zap.String("job_id", jobID), zap.Error(err))
	}
}
