// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

const failedJobRequeuePrefix = "failed-job-requeuehold"

// Bound command failures independently of the job's successful-requeue cap.
// Short local waits keep recovery inside Slurm's job-retention window.
const (
	failedJobRequeueAttempts   = 3
	failedJobRequeueRetryDelay = 250 * time.Millisecond
)

var errFailedJobRecoveryBudget = errors.New("failed-job recovery phase budget exhausted")

// Older Slurm reports a bare COMPLETING state while the epilog runs, masking
// the base final state, but the state reason already distinguishes a failure.
// Treat that window as failed so a requeue overlaps the epilog instead of
// waiting for the terminal transition. These reasons mirror the Job Manager's
// COMPLETING_STATE_REASON_TO_FINAL_STATE failure mappings.
const (
	slurmStateCompleting       = "COMPLETING"
	slurmReasonNonZeroExitCode = "NonZeroExitCode"
	slurmReasonRaisedSignal    = "RaisedSignal"
)

type FailedJobSyncResult struct {
	Requeues []FailedJobRequeue
}

type FailedJobRequeueSkipReason string

const (
	FailedJobRequeueSkipNone         FailedJobRequeueSkipReason = ""
	FailedJobRequeueSkipRecoveryMode FailedJobRequeueSkipReason = "auto_resume_mode_not_requeue"
	FailedJobRequeueSkipLimitReached FailedJobRequeueSkipReason = "max_requeue_times_reached"
)

type FailedJobRequeue struct {
	RequeueID       string
	JobID           string
	AttemptIndex    int32
	MaxRequeueTimes *int32
	Applied         bool
	Mode            cluster.JobMode
	SkipReason      FailedJobRequeueSkipReason
}

// reconcileFailedJobs limits serial scheduler work to one phase budget. It
// retains unfinished exact-attempt work for a fresh snapshot on the next tick;
// only success, policy skips, permanent rejection, or exhausted attempts finish
// recovery. Phase expiry leaves the caller's context available for Advice.
func (c *Controller) reconcileFailedJobs(ctx context.Context, snapshot ControllerSnapshot, observation *SyncObservation) (FailedJobSyncResult, error) {
	failedJobs := failedJobsFromSnapshot(snapshot, c.Options)
	if len(failedJobs) == 0 {
		return FailedJobSyncResult{}, nil
	}
	phaseCtx := ctx
	cancel := func() {}
	if c.Options.FailedJobRecoveryTimeout > 0 {
		phaseCtx, cancel = context.WithTimeoutCause(ctx, c.Options.FailedJobRecoveryTimeout, errFailedJobRecoveryBudget)
	}
	defer cancel()
	if c.failedJobRecoveryCursor != nil {
		for i, job := range failedJobs {
			if jobAttemptRef(job) == *c.failedJobRecoveryCursor {
				// Allocate independently: append must not overwrite the prefix
				// while rotating the source slice.
				failedJobs = append(slices.Clone(failedJobs[i+1:]), failedJobs[:i+1]...)
				break
			}
		}
	}

	result := FailedJobSyncResult{}
	var errs []error
	for _, job := range failedJobs {
		if err := phaseCtx.Err(); err != nil {
			errs = append(errs, errors.Wrap(context.Cause(phaseCtx), "reconcile failed-job requeues"))
			break
		}
		if c.failedJobRecoveryProcessed(job) {
			if err := c.forgetCompletedFailedJobRecovery(ctx, snapshot, job); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		decision := modeDecisionForJob(snapshot, c.modePolicy(), job)
		if decision.Mode == cluster.JobModeIgnore {
			continue
		}

		requeue := failedJobRequeue(job)
		requeue.Mode = decision.Mode
		requeue.SkipReason = failedJobRequeueSkipReason(job)
		result.Requeues = append(result.Requeues, requeue)
		index := len(result.Requeues) - 1
		if requeue.SkipReason != FailedJobRequeueSkipNone {
			observeFailedRequeue(observation, decision.Mode, failedJobRequeueSkipOutcome(requeue.SkipReason))
			if err := c.completeFailedJobRecovery(ctx, snapshot, job); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		observeFailedRequeue(observation, decision.Mode, ObservationOutcomePlanned)
		if decision.Mode != cluster.JobModeAct {
			// Planning must not consume an attempt: a later explicit act request
			// should still recover it from a fresh scheduler observation.
			if err := c.forgetCompletedFailedJobRecovery(ctx, snapshot, job); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		actionErr := c.applyFailedJobRequeue(phaseCtx, job, requeue)
		if actionErr != nil {
			observeFailedRequeue(observation, decision.Mode, ObservationOutcomeError)
			errs = append(errs, actionErr)
			if ctx.Err() != nil {
				break
			}
			if phaseCtx.Err() != nil && !errors.Is(actionErr, cluster.ErrJobRequeuePermanent) &&
				c.failedJobRequeueFailures[jobAttemptRef(job)] < failedJobRequeueAttempts {
				break
			}
			// TODO: Once Andrew's advice-driven recovery convergence is available,
			// persist the give-up decision in Job Manager and report the aborted
			// recovery in the audit trail. This marker is process-local: a SAC
			// restart can bootstrap the same failed attempt from Job Manager and
			// retry it, including after a cross-service restart.
			if err := c.completeFailedJobRecovery(ctx, snapshot, job); err != nil {
				errs = append(errs, err)
			}
			continue
		}
		result.Requeues[index].Applied = true
		observeFailedRequeue(observation, decision.Mode, ObservationOutcomeApplied)
		if err := c.completeFailedJobRecovery(ctx, snapshot, job); err != nil {
			errs = append(errs, err)
		}
	}

	return result, errors.Join(errs...)
}

// completeFailedJobRecovery records the decision before removing terminal
// accounting work from SeenJobs. If cleanup fails, the in-process decision
// marker prevents a completed recovery decision from being repeated while a
// later tick retries only the cleanup. Active COMPLETING failures stay seen so
// their eventual disappearance from squeue still triggers a final sacct read.
func (c *Controller) completeFailedJobRecovery(ctx context.Context, snapshot ControllerSnapshot, job cluster.JobExecutionRecord) error {
	c.markFailedJobRecoveryProcessed(job)
	delete(c.failedJobRequeueFailures, jobAttemptRef(job))
	return c.forgetCompletedFailedJobRecovery(ctx, snapshot, job)
}

func (c *Controller) forgetCompletedFailedJobRecovery(ctx context.Context, snapshot ControllerSnapshot, job cluster.JobExecutionRecord) error {
	key := jobAttemptRef(job)
	if _, active := snapshot.ActiveJobs[key]; active {
		return nil
	}
	// reconcileFailedJobs is also unit-tested independently of a fully
	// validated Controller. Production SyncOnce always supplies SeenJobs.
	if c.SeenJobs == nil {
		return nil
	}
	if err := c.SeenJobs.Forget(ctx, key); err != nil {
		return errors.Wrapf(err, "forget reconciled failed job %s attempt %d", key.JobID, key.AttemptIndex)
	}
	return nil
}

func (c *Controller) failedJobRecoveryProcessed(job cluster.JobExecutionRecord) bool {
	if c.processedFailedJobRecoveries == nil {
		return false
	}
	_, ok := c.processedFailedJobRecoveries[cluster.JobAttemptRef{
		JobID:        job.JobID,
		AttemptIndex: job.AttemptIndex,
	}]
	return ok
}

func (c *Controller) markFailedJobRecoveryProcessed(job cluster.JobExecutionRecord) {
	if c.processedFailedJobRecoveries == nil {
		c.processedFailedJobRecoveries = make(map[cluster.JobAttemptRef]struct{})
	}
	c.processedFailedJobRecoveries[cluster.JobAttemptRef{
		JobID:        job.JobID,
		AttemptIndex: job.AttemptIndex,
	}] = struct{}{}
}

func failedJobsFromSnapshot(snapshot ControllerSnapshot, opts ControllerOptions) []cluster.JobExecutionRecord {
	var out []cluster.JobExecutionRecord
	jobsByID := latestJobsByIDFromSnapshot(snapshot)
	for _, key := range sortedSnapshotKeys(snapshot) {
		if environmentAdmissionBlocks(snapshot, key) {
			continue
		}
		job, ok := snapshot.AccountingJobs[key]
		if !ok {
			job, ok = snapshot.ActiveJobs[key]
		}
		if !ok || !jobIsRecoverableFailure(job) {
			continue
		}
		if latest, ok := jobsByID[job.JobID]; ok && latest.AttemptIndex > job.AttemptIndex {
			continue
		}
		if !jobMatchesControllerOptions(job, opts) {
			continue
		}
		out = append(out, job)
	}

	return out
}

// jobIsRecoverableFailure reports whether a job should enter failed-job requeue
// reconciliation. A terminal failed state always qualifies. In addition, an
// older single-state Slurm job that is still COMPLETING (running its epilog)
// but already carries a non-zero exit or raised-signal reason is treated as
// failed so recovery overlaps the epilog window instead of waiting for the
// terminal transition. Newer Slurm reports the base failed state directly in
// the state array (resolved to a failed state at squeue conversion time), so
// this only widens the legacy bare-COMPLETING form.
func jobIsRecoverableFailure(job cluster.JobExecutionRecord) bool {
	if job.State == cluster.JobStateFailed {
		return true
	}
	return jobIsCompletingFailure(job)
}

func jobIsCompletingFailure(job cluster.JobExecutionRecord) bool {
	if !strings.EqualFold(job.RawState, slurmStateCompleting) {
		return false
	}
	switch job.Reason {
	case slurmReasonNonZeroExitCode, slurmReasonRaisedSignal:
		return true
	default:
		return false
	}
}

func jobMatchesControllerOptions(job cluster.JobExecutionRecord, opts ControllerOptions) bool {
	if len(opts.PartitionAllowlist) > 0 && !slices.Contains(opts.PartitionAllowlist, job.Partition) {
		return false
	}
	if opts.Environment != "" && job.SchedulerMetadata != nil {
		environment, _ := job.SchedulerMetadata["environment"].(string)
		if environment != "" && environment != opts.Environment {
			return false
		}
	}

	return true
}

func failedJobRequeue(job cluster.JobExecutionRecord) FailedJobRequeue {
	requeue := FailedJobRequeue{
		RequeueID:    failedJobRequeueID(job),
		JobID:        job.JobID,
		AttemptIndex: job.AttemptIndex,
	}
	if job.RecoveryConfig != nil && job.RecoveryConfig.MaxRequeueTimes != nil {
		limit := *job.RecoveryConfig.MaxRequeueTimes
		requeue.MaxRequeueTimes = &limit
	}

	return requeue
}

func failedJobRequeueSkipReason(job cluster.JobExecutionRecord) FailedJobRequeueSkipReason {
	if job.RecoveryConfig == nil {
		return FailedJobRequeueSkipNone
	}
	// Job Manager defaults an omitted auto-resume mode to requeue. Preserve that
	// production default while rejecting an explicit non-requeue mode.
	if mode := job.RecoveryConfig.AutoResumeMode; mode != cluster.AutoResumeModeUnset && mode != cluster.AutoResumeModeRequeue {
		return FailedJobRequeueSkipRecoveryMode
	}
	if limit := job.RecoveryConfig.MaxRequeueTimes; limit != nil && job.AttemptIndex >= *limit {
		return FailedJobRequeueSkipLimitReached
	}

	return FailedJobRequeueSkipNone
}

func failedJobRequeueSkipOutcome(reason FailedJobRequeueSkipReason) ObservationOutcome {
	switch reason {
	case FailedJobRequeueSkipRecoveryMode:
		return ObservationOutcomeSkippedRecoveryMode
	case FailedJobRequeueSkipLimitReached:
		return ObservationOutcomeSkippedRequeueLimit
	default:
		return ObservationOutcomeError
	}
}

func failedJobRequeueID(job cluster.JobExecutionRecord) string {
	return fmt.Sprintf("%s:%s", failedJobRequeuePrefix, job.JobID)
}

// applyFailedJobRequeue spends the remaining exact-attempt retry allowance while
// the phase has time. Every returned command is audited. A command cut short by
// the phase deadline consumes an attempt, otherwise a stalled command could
// retry forever. Shutdown cancellation preserves the remaining allowance.
func (c *Controller) applyFailedJobRequeue(ctx context.Context, job cluster.JobExecutionRecord, requeue FailedJobRequeue) error {
	key := jobAttemptRef(job)
	if c.failedJobRequeueFailures[key] >= failedJobRequeueAttempts {
		// A caller cancellation can race the final error before completion is
		// recorded. Retain the cap even if this decision resumes on another tick.
		return errors.Newf("failed-job requeue %s exhausted its %d command attempts", requeue.RequeueID, failedJobRequeueAttempts)
	}
	for attempt := c.failedJobRequeueFailures[key] + 1; attempt <= failedJobRequeueAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(context.Cause(ctx), "cancel failed-job requeue")
		}
		c.failedJobRecoveryCursor = &key
		err := c.ActionExecutor.JobRequeueHold(ctx, requeue.JobID)
		c.observeFailedJobRequeueHold(job, err)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil && !errors.Is(context.Cause(ctx), errFailedJobRecoveryBudget) {
			return errors.Join(err, errors.Wrap(context.Cause(ctx), "cancel failed-job requeue"))
		}
		if c.failedJobRequeueFailures == nil {
			c.failedJobRequeueFailures = make(map[cluster.JobAttemptRef]int)
		}
		c.failedJobRequeueFailures[key] = attempt
		if ctx.Err() != nil {
			err = errors.Join(err, context.Cause(ctx))
		}
		if errors.Is(err, cluster.ErrJobRequeuePermanent) || attempt == failedJobRequeueAttempts {
			return errors.Wrapf(err, "give up failed-job requeue %s for job %s after %d command attempts", requeue.RequeueID, requeue.JobID, attempt)
		}
		if ctx.Err() != nil {
			return err
		}
		timer := time.NewTimer(failedJobRequeueRetryDelay * time.Duration(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, errors.Wrap(context.Cause(ctx), "cancel failed-job requeue backoff"))
		case <-timer.C:
		}
	}
	return nil
}
