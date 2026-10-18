// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AdviceSyncResult struct {
	Skips            []AdviceSkip
	TerminalOutcomes []AdviceTerminalOutcome
}

// AdviceTerminalOutcome is a generation-scoped conclusion derived from fresh
// scheduler state. The outer reconciliation loop publishes it only after local
// ownership, routing, partition, and mode policy checks have run.
type AdviceTerminalOutcome struct {
	// AdviceID identifies the Advice record.
	AdviceID string
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the Advice's source attempt.
	AttemptIndex int32
	// Generation identifies the terminal Advice generation.
	Generation int64
	// Disposition identifies the terminal lifecycle outcome.
	Disposition pb.AdviceReconciliationDisposition
	// Reason explains why the generation reached its terminal outcome.
	Reason pb.AdviceReconciliationReason
}

type AdviceSkip struct {
	AdviceID string
	JobID    string
	Reason   AdviceSkipReason
}

type AdviceSkipReason string

const (
	AdviceSkipNotOwned                   AdviceSkipReason = "not_owned"
	AdviceSkipEnvironmentMismatch        AdviceSkipReason = "environment_mismatch"
	AdviceSkipMissingSnapshotJob         AdviceSkipReason = "missing_observed_job"
	AdviceSkipMissingFreshSchedulerState AdviceSkipReason = "missing_fresh_scheduler_state"
	AdviceSkipPartitionFiltered          AdviceSkipReason = "partition_filtered"
	AdviceSkipStaleAdvice                AdviceSkipReason = "stale_advice"
	AdviceSkipStaleAttempt               AdviceSkipReason = "stale_attempt"
	AdviceSkipFutureAttempt              AdviceSkipReason = "future_attempt"
	AdviceSkipAlreadySatisfied           AdviceSkipReason = "already_satisfied"
	AdviceSkipUnsupportedRecommendation  AdviceSkipReason = "unsupported_recommendation"
	AdviceSkipEmptyRecommendation        AdviceSkipReason = "empty_recommendation"
	AdviceSkipIgnoredJob                 AdviceSkipReason = "ignored_job"
)

func (c *Controller) reconcileAdvice(ctx context.Context, snapshot ControllerSnapshot, observation *SyncObservation) (AdviceSyncResult, error) {
	if snapshotAdviceCount(snapshot) == 0 {
		return AdviceSyncResult{}, nil
	}

	jobsByID := latestJobsByIDFromSnapshot(snapshot)
	result := AdviceSyncResult{}

	ctx, cancel := timeoutContext(ctx, c.Options.AdviceLoopTimeout)
	defer cancel()

	var errs []error
	for _, advice := range sortedSnapshotAdvice(snapshot) {
		if err := ctx.Err(); err != nil {
			errs = append(errs, errors.Wrap(err, "reconcile advice loop"))
			break
		}
		if environmentAdmissionBlocks(snapshot, jobAttemptRefFromAdvice(advice)) {
			continue
		}
		observeAdviceReconciliationAttempt(observation)

		if reason, ok := c.ownershipSkip(advice); ok {
			skip := adviceSkip(advice, reason)
			c.observeAdviceSkip(advice, skip.Reason)
			result.Skips = append(result.Skips, skip)
			observeAdviceSkip(observation, skip.Reason)
			continue
		}
		if adviceIsStale(advice, snapshot.ObservedAt, c.Options.AdviceMaxAge) {
			itemResult, _ := terminalAdviceResult(
				advice,
				AdviceSkipStaleAdvice,
				pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED,
				pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_ADVICE_EXPIRED,
			)
			result.Skips = append(result.Skips, itemResult.Skips...)
			result.TerminalOutcomes = append(result.TerminalOutcomes, itemResult.TerminalOutcomes...)
			skip := itemResult.Skips[0]
			c.observeAdviceSkip(advice, skip.Reason)
			observeAdviceSkip(observation, skip.Reason)
			if err := c.publishAdviceTerminalOutcomes(ctx, snapshot.ObservedAt, itemResult.TerminalOutcomes, observation); err != nil {
				errs = append(errs, err)
			}
			continue
		}

		job, ok := jobsByID[advice.GetJobId()]
		if !ok {
			skip := adviceSkip(advice, AdviceSkipMissingSnapshotJob)
			c.observeAdviceSkip(advice, skip.Reason)
			result.Skips = append(result.Skips, skip)
			observeAdviceSkip(observation, skip.Reason)
			continue
		}
		if len(c.Options.PartitionAllowlist) > 0 && !slices.Contains(c.Options.PartitionAllowlist, job.Partition) {
			skip := adviceSkip(advice, AdviceSkipPartitionFiltered)
			c.observeAdviceSkip(advice, skip.Reason)
			result.Skips = append(result.Skips, skip)
			observeAdviceSkip(observation, skip.Reason)
			continue
		}
		decision := modeDecisionForJob(snapshot, c.modePolicy(), job)
		if decision.Mode == cluster.JobModeIgnore {
			skip := adviceSkip(advice, AdviceSkipIgnoredJob)
			c.observeAdviceSkip(advice, skip.Reason)
			result.Skips = append(result.Skips, skip)
			observeAdviceSkip(observation, skip.Reason)
			continue
		}

		itemCtx, cancelItem := timeoutContext(ctx, c.Options.AdviceItemTimeout)
		itemResult, err := c.reconcileAdviceForJob(itemCtx, advice, job, decision.Mode, observation)
		result.Skips = append(result.Skips, itemResult.Skips...)
		result.TerminalOutcomes = append(result.TerminalOutcomes, itemResult.TerminalOutcomes...)
		for _, skip := range itemResult.Skips {
			observeAdviceSkip(observation, skip.Reason)
		}
		if err != nil {
			cancelItem()
			errs = append(errs, err)
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if err := c.publishAdviceTerminalOutcomes(itemCtx, snapshot.ObservedAt, itemResult.TerminalOutcomes, observation); err != nil {
			errs = append(errs, err)
		}
		cancelItem()
	}

	return result, errors.Join(errs...)
}

func (c *Controller) reconcileAdviceForJob(ctx context.Context, advice *pb.Advice, job cluster.JobExecutionRecord, mode cluster.JobMode, observation *SyncObservation) (result AdviceSyncResult, err error) {
	// Defer audit emission so every return path reports the final named result and error.
	auditObservation := newAdviceApplicationObservation(advice)
	defer func() {
		c.observeAdviceApplication(auditObservation, result, err)
	}()

	desiredState := advice.GetAdvice().GetState()

	adviceAttempt := advice.GetAttemptIndex()
	attemptDelta := int64(job.AttemptIndex) - int64(adviceAttempt)
	if attemptDelta < 0 {
		return skippedAdviceResult(advice, AdviceSkipFutureAttempt)
	}

	recommendation := advice.GetAdvice()
	desiredConstraints := recommendation.GetNodeConstraints()
	inclusions := sortedUniqueStrings(desiredConstraints.GetInclusions())
	exclusions := sortedUniqueStrings(desiredConstraints.GetExclusions())
	hasConstraintUpdate := len(inclusions) > 0 || len(exclusions) > 0

	if attemptDelta > 0 && desiredState != pb.AdviceState_ADVICE_STATE_RESTARTED {
		return terminalAdviceResult(advice, AdviceSkipStaleAttempt,
			pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED,
			pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_STALE_ATTEMPT)
	}
	if desiredState == pb.AdviceState_ADVICE_STATE_UNSPECIFIED && !hasConstraintUpdate {
		return terminalAdviceResult(advice, AdviceSkipEmptyRecommendation,
			pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED,
			pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_INVALID_RECOMMENDATION)
	}
	if job.State == cluster.JobStateUnspecified {
		return skippedAdviceResult(advice, AdviceSkipMissingFreshSchedulerState)
	}

	constraintsSatisfied := hasConstraintUpdate && desiredConstraintsSatisfied(job.NodeConstraints, inclusions, exclusions)

	switch desiredState {
	case pb.AdviceState_ADVICE_STATE_RESTARTED:
		if attemptDelta > 1 {
			return terminalAdviceResult(advice, AdviceSkipStaleAttempt,
				pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED,
				pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_STALE_ATTEMPT)
		}

		requeueCompleted := attemptDelta == 0 && job.State == cluster.JobStateRequeued
		needsRequeue := attemptDelta == 0 && !requeueCompleted
		needsNodeUpdate := hasConstraintUpdate && !constraintsSatisfied
		if attemptDelta == 1 && !job.Held {
			if needsNodeUpdate {
				return terminalAdviceResult(advice, AdviceSkipStaleAttempt,
					pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED,
					pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_SUCCESSOR_ALREADY_RUNNING)
			}
			return terminalAdviceResult(advice, AdviceSkipAlreadySatisfied,
				pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED,
				pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED)
		}
		needsRelease := needsRequeue || requeueCompleted || (attemptDelta == 1 && job.Held)
		if !needsRequeue && !requeueCompleted && !isMutableJobState(job.State) {
			return terminalAdviceResult(advice, AdviceSkipStaleAttempt,
				pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED,
				pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_TARGET_STATE_FINAL)
		}

		if needsRequeue {
			if mode != cluster.JobModeAct {
				observeAdviceAction(observation, ActionAdviceRequeueHold, mode, ObservationOutcomePlanned)
			}
			if err := c.runAdviceSchedulerAction(
				ctx, &auditObservation, SchedulerActionRequeueHold, mode,
				func(actionCtx context.Context) error { return c.ActionExecutor.JobRequeueHold(actionCtx, job.JobID) },
			); err != nil {
				if mode == cluster.JobModeAct {
					observeAdviceAction(observation, ActionAdviceRequeueHold, mode, ObservationOutcomeError)
				}
				if errors.Is(err, cluster.ErrJobRequeuePermanent) {
					// The action audit retains the scheduler rejection. Retire only
					// this generation through the normal durable status boundary.
					return terminalAdviceResult(advice, AdviceSkipUnsupportedRecommendation,
						pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED,
						pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_UNSUPPORTED_RECOMMENDATION)
				}
				return result, errors.Wrapf(err, "run advice action %s for job %s: requeuehold", advice.GetHeimdallJobId(), advice.GetJobId())
			}
			if mode == cluster.JobModeAct {
				observeAdviceAction(observation, ActionAdviceRequeueHold, mode, ObservationOutcomeSuccess)

				if err := c.waitAfterAdviceRequeueHold(ctx); err != nil {
					return result, errors.Wrapf(err, "wait after advice requeuehold %s for job %s", advice.GetHeimdallJobId(), advice.GetJobId())
				}
			}
		}

		if needsNodeUpdate {
			constraints, err := desiredNodeConstraints(job, inclusions, exclusions)
			if err != nil {
				return skippedAdviceResult(advice, AdviceSkipUnsupportedRecommendation)
			}

			if mode != cluster.JobModeAct {
				observeAdviceAction(observation, ActionAdviceNodeUpdate, mode, ObservationOutcomePlanned)
			}
			if err := c.runAdviceSchedulerAction(
				ctx, &auditObservation, SchedulerActionUpdateConstraints, mode,
				func(actionCtx context.Context) error {
					return c.ActionExecutor.JobNodeUpdate(actionCtx, job.JobID, constraints.Exclusions, constraints.Inclusions, 0)
				},
			); err != nil {
				observeAdviceAction(observation, ActionAdviceNodeUpdate, mode, ObservationOutcomeError)
				return result, errors.Wrapf(err, "run advice action %s for job %s: node update", advice.GetHeimdallJobId(), advice.GetJobId())
			} else if mode == cluster.JobModeAct {
				observeAdviceAction(observation, ActionAdviceNodeUpdate, mode, ObservationOutcomeSuccess)
			}
		}

		if needsRelease {
			if mode != cluster.JobModeAct {
				observeAdviceAction(observation, ActionAdviceRelease, mode, ObservationOutcomePlanned)
			}
			if err := c.runAdviceSchedulerAction(
				ctx, &auditObservation, SchedulerActionRelease, mode,
				func(actionCtx context.Context) error { return c.ActionExecutor.JobRelease(actionCtx, job.JobID) },
			); err != nil {
				observeAdviceAction(observation, ActionAdviceRelease, mode, ObservationOutcomeError)
				return result, errors.Wrapf(err, "run advice action %s for job %s: release", advice.GetHeimdallJobId(), advice.GetJobId())
			} else if mode == cluster.JobModeAct {
				observeAdviceAction(observation, ActionAdviceRelease, mode, ObservationOutcomeSuccess)
				c.logAdviceRelease(advice, job)
			}
		}
		return result, nil
	case pb.AdviceState_ADVICE_STATE_TERMINATED:
		if isFinalJobState(job.State) {
			return terminalAdviceResult(advice, AdviceSkipAlreadySatisfied,
				pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED,
				pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED)
		}
		if mode != cluster.JobModeAct {
			observeAdviceAction(observation, ActionAdviceKill, mode, ObservationOutcomePlanned)
			_ = c.runAdviceSchedulerAction(
				ctx, &auditObservation, SchedulerActionCancel, mode,
				func(context.Context) error { return nil },
			)
			return result, nil
		}
		if err := c.runAdviceSchedulerAction(
			ctx, &auditObservation, SchedulerActionCancel, mode,
			func(actionCtx context.Context) error { return c.ActionExecutor.JobKill(actionCtx, job.JobID) },
		); err != nil {
			observeAdviceAction(observation, ActionAdviceKill, mode, ObservationOutcomeError)
			return result, errors.Wrapf(err, "run advice action %s for job %s: kill", advice.GetHeimdallJobId(), advice.GetJobId())
		}
		observeAdviceAction(observation, ActionAdviceKill, mode, ObservationOutcomeSuccess)
		return result, nil
	case pb.AdviceState_ADVICE_STATE_RELEASED:
		if hasConstraintUpdate {
			return skippedAdviceResult(advice, AdviceSkipUnsupportedRecommendation)
		}
		switch {
		case job.State == cluster.JobStatePending && job.Held:
			if mode != cluster.JobModeAct {
				observeAdviceAction(observation, ActionAdviceRelease, mode, ObservationOutcomePlanned)
				_ = c.runAdviceSchedulerAction(
					ctx, &auditObservation, SchedulerActionRelease, mode,
					func(context.Context) error { return nil },
				)
				return result, nil
			}
			if err := c.runAdviceSchedulerAction(
				ctx, &auditObservation, SchedulerActionRelease, mode,
				func(actionCtx context.Context) error { return c.ActionExecutor.JobRelease(actionCtx, job.JobID) },
			); err != nil {
				observeAdviceAction(observation, ActionAdviceRelease, mode, ObservationOutcomeError)
				return result, errors.Wrapf(err, "run advice action %s for job %s: release", advice.GetHeimdallJobId(), advice.GetJobId())
			}
			observeAdviceAction(observation, ActionAdviceRelease, mode, ObservationOutcomeSuccess)
			c.logAdviceRelease(advice, job)
			return result, nil
		case job.State == cluster.JobStatePending,
			job.State == cluster.JobStateRunning,
			job.State == cluster.JobStateRequeued,
			isFinalJobState(job.State):
			return terminalAdviceResult(advice, AdviceSkipAlreadySatisfied,
				pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED,
				pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED)
		default:
			return skippedAdviceResult(advice, AdviceSkipUnsupportedRecommendation)
		}
	case pb.AdviceState_ADVICE_STATE_UNSPECIFIED:
		if constraintsSatisfied {
			return terminalAdviceResult(advice, AdviceSkipAlreadySatisfied,
				pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED,
				pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED)
		}
		if !isMutableJobState(job.State) {
			return terminalAdviceResult(advice, AdviceSkipStaleAttempt,
				pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_ABANDONED,
				pb.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_TARGET_STATE_FINAL)
		}

		constraints, err := desiredNodeConstraints(job, inclusions, exclusions)
		if err != nil {
			return skippedAdviceResult(advice, AdviceSkipUnsupportedRecommendation)
		}
		if mode != cluster.JobModeAct {
			observeAdviceAction(observation, ActionAdviceNodeUpdate, mode, ObservationOutcomePlanned)
			_ = c.runAdviceSchedulerAction(
				ctx, &auditObservation, SchedulerActionUpdateConstraints, mode,
				func(context.Context) error { return nil },
			)
			return result, nil
		}
		actionErr := c.runAdviceSchedulerAction(
			ctx, &auditObservation, SchedulerActionUpdateConstraints, mode,
			func(actionCtx context.Context) error {
				return c.ActionExecutor.JobNodeUpdate(actionCtx, job.JobID, constraints.Exclusions, constraints.Inclusions, 0)
			},
		)
		if actionErr != nil {
			observeAdviceAction(observation, ActionAdviceNodeUpdate, mode, ObservationOutcomeError)
			err = errors.Wrapf(actionErr, "run advice action %s for job %s: node update", advice.GetHeimdallJobId(), advice.GetJobId())
			auditObservation.Actions[len(auditObservation.Actions)-1].Err = err
			return result, err
		}
		observeAdviceAction(observation, ActionAdviceNodeUpdate, mode, ObservationOutcomeSuccess)
		return result, nil
	default:
		return skippedAdviceResult(advice, AdviceSkipUnsupportedRecommendation)
	}
}

func (c *Controller) logAdviceRelease(advice *pb.Advice, job cluster.JobExecutionRecord) {
	if c.Logger == nil {
		return
	}

	c.Logger.Info(
		"applied advice job release",
		zap.String("advice_id", advice.GetHeimdallJobId()),
		zap.String("job_id", job.JobID),
		zap.Int32("attempt_index", job.AttemptIndex),
	)
}

func (c *Controller) ownershipSkip(advice *pb.Advice) (AdviceSkipReason, bool) {
	metadata := advice.GetSchedulerMetadata()
	if metadata.GetFields()["scheduler"].GetStringValue() != cluster.SchedulerSlurm {
		return AdviceSkipNotOwned, true
	}
	if managedBy := metadata.GetFields()["managed_by"].GetStringValue(); managedBy != "" && managedBy != cluster.ManagedBySlurmAdvisor {
		return AdviceSkipNotOwned, true
	}
	if environment := metadata.GetFields()["environment"].GetStringValue(); c.Options.Environment != "" && environment != "" && environment != c.Options.Environment {
		return AdviceSkipEnvironmentMismatch, true
	}

	return "", false
}

func (c *Controller) waitAfterAdviceRequeueHold(ctx context.Context) error {
	delay := c.Options.AdviceRequeueHoldDelay
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func timeoutContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func adviceIsStale(advice *pb.Advice, observedAt time.Time, maxAge time.Duration) bool {
	// Released and terminated are durable, level-triggered desired states. Keep
	// reconciling the exact attempt until fresh Slurm state satisfies them;
	// otherwise controller downtime or a reconciliation backlog could strand a
	// held DAG job after the generic advice age limit passes.
	state := advice.GetAdvice().GetState()
	if state == pb.AdviceState_ADVICE_STATE_RELEASED || state == pb.AdviceState_ADVICE_STATE_TERMINATED {
		return false
	}
	if maxAge <= 0 {
		return false
	}

	desiredStateChangedAt := advice.GetDesiredStateChangedAt()
	if desiredStateChangedAt == nil {
		// Fall back while Job Manager and API Gateway roll out the
		// generation-scoped timestamp ahead of SAC.
		desiredStateChangedAt = advice.GetCreatedAt()
	}
	if desiredStateChangedAt == nil || desiredStateChangedAt.CheckValid() != nil {
		return true
	}

	return !observedAt.Before(desiredStateChangedAt.AsTime().Add(maxAge))
}

func adviceHasRecommendation(advice *pb.Advice) bool {
	recommendation := advice.GetAdvice()
	constraints := recommendation.GetNodeConstraints()
	return recommendation.GetState() != pb.AdviceState_ADVICE_STATE_UNSPECIFIED ||
		len(constraints.GetInclusions()) > 0 ||
		len(constraints.GetExclusions()) > 0
}

func desiredNodeConstraints(job cluster.JobExecutionRecord, inclusions, exclusions []string) (cluster.NodeConstraints, error) {
	desiredInclusions := sortedUniqueStrings(job.NodeConstraints.Inclusions)
	if len(inclusions) > 0 {
		desiredInclusions = inclusions
	}
	desiredExclusions := sortedUniqueStrings(job.NodeConstraints.Exclusions)
	if len(exclusions) > 0 {
		desiredExclusions = exclusions
	}
	if overlap := overlapStrings(desiredInclusions, desiredExclusions); len(overlap) > 0 {
		return cluster.NodeConstraints{}, errors.Wrapf(cluster.ErrNodeUpdateOverlap, "overlapping nodes %v", overlap)
	}

	return cluster.NodeConstraints{
		Inclusions: desiredInclusions,
		Exclusions: desiredExclusions,
	}, nil
}

func adviceSkip(advice *pb.Advice, reason AdviceSkipReason) AdviceSkip {
	return AdviceSkip{
		AdviceID: advice.GetHeimdallJobId(),
		JobID:    advice.GetJobId(),
		Reason:   reason,
	}
}

func skippedAdviceResult(advice *pb.Advice, reason AdviceSkipReason) (AdviceSyncResult, error) {
	return AdviceSyncResult{Skips: []AdviceSkip{adviceSkip(advice, reason)}}, nil
}

func terminalAdviceResult(
	advice *pb.Advice,
	skipReason AdviceSkipReason,
	disposition pb.AdviceReconciliationDisposition,
	reason pb.AdviceReconciliationReason,
) (AdviceSyncResult, error) {
	generation := advice.GetGeneration()
	if generation <= 0 {
		generation = 1
	}

	return AdviceSyncResult{
		Skips: []AdviceSkip{adviceSkip(advice, skipReason)},
		TerminalOutcomes: []AdviceTerminalOutcome{{
			AdviceID:     advice.GetHeimdallJobId(),
			JobID:        advice.GetJobId(),
			AttemptIndex: advice.GetAttemptIndex(),
			Generation:   generation,
			Disposition:  disposition,
			Reason:       reason,
		}},
	}, nil
}

func (c *Controller) publishAdviceTerminalOutcomes(
	ctx context.Context,
	observedAt time.Time,
	outcomes []AdviceTerminalOutcome,
	observation *SyncObservation,
) error {
	var errs []error
	for _, outcome := range outcomes {
		status := &pb.AdviceReconciliationStatus{
			ObservedGeneration: outcome.Generation,
			Disposition:        outcome.Disposition,
			Reason:             outcome.Reason,
			ObservedAt:         timestamppb.New(observedAt),
			TransitionedAt:     timestamppb.New(observedAt),
		}
		if _, err := c.HeimdallClient.UpdateAdviceReconciliationStatus(ctx, outcome.AdviceID, status); err != nil {
			observeAdviceStatusPublication(observation, outcome.Disposition, outcome.Reason, ObservationOutcomeError)
			errs = append(errs, errors.Wrapf(err, "publish reconciliation status for advice %s", outcome.AdviceID))
		} else {
			observeAdviceStatusPublication(observation, outcome.Disposition, outcome.Reason, ObservationOutcomeSuccess)
			c.auditObserver().ObserveAdviceTerminalStatus(AdviceTerminalStatusObservation{
				AdviceID:     outcome.AdviceID,
				JobID:        outcome.JobID,
				AttemptIndex: outcome.AttemptIndex,
				Generation:   outcome.Generation,
				Disposition:  outcome.Disposition,
				Reason:       outcome.Reason,
				ObservedAt:   observedAt.UTC(),
			})
		}
	}

	return errors.Join(errs...)
}

func desiredConstraintsSatisfied(current cluster.NodeConstraints, inclusions, exclusions []string) bool {
	if len(inclusions) > 0 && !slices.Equal(sortedUniqueStrings(current.Inclusions), inclusions) {
		return false
	}
	if len(exclusions) > 0 && !slices.Equal(sortedUniqueStrings(current.Exclusions), exclusions) {
		return false
	}

	return true
}

func sortedUniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	slices.Sort(out)

	return out
}

func overlapStrings(left, right []string) []string {
	rightSet := map[string]struct{}{}
	for _, value := range right {
		rightSet[value] = struct{}{}
	}

	var overlap []string
	for _, value := range left {
		if _, ok := rightSet[value]; ok {
			overlap = append(overlap, value)
		}
	}

	return overlap
}
