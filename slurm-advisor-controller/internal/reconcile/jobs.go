// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/heimdall"
)

type jobUpsertContext struct {
	index     int
	key       cluster.JobAttemptRef
	job       cluster.JobExecutionRecord
	source    JobUpsertSource
	finalized bool
}

type jobUpsertResult struct {
	upsert jobUpsertContext
	err    error
}

func (c *Controller) reconcileJobs(ctx context.Context, snapshot ControllerSnapshot, observation *SyncObservation) error {
	keys := sortedSnapshotKeys(snapshot)
	workerCount := min(c.jobUpsertWorkerPoolSize(), len(keys))
	upserts := make(chan jobUpsertContext, workerCount)
	results := make(chan jobUpsertResult, len(keys))

	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for upsert := range upserts {
				result := jobUpsertResult{upsert: upsert}
				if ctxErr := ctx.Err(); ctxErr != nil {
					result.err = errors.Wrapf(
						ctxErr,
						"upsert job %s attempt %d",
						upsert.key.JobID,
						upsert.key.AttemptIndex,
					)
				} else {
					result.err = c.upsertJob(ctx, upsert.job)
				}
				results <- result
			}
		}()
	}

	submitted, submissionErr := c.submitJobUpserts(ctx, snapshot, keys, upserts)
	close(upserts)
	workers.Wait()
	close(results)

	orderedResults := make([]jobUpsertResult, submitted)
	for result := range results {
		orderedResults[result.upsert.index] = result
	}

	var errs []error
	pendingFailedRecoveries := failedJobRecoveryKeys(snapshot, c.Options)
	for _, result := range orderedResults {
		outcome := ObservationOutcomeSuccess
		if result.err != nil {
			outcome = ObservationOutcomeError
		}
		observeJobUpsert(observation, result.upsert.source, outcome)
		if result.err != nil {
			errs = append(errs, result.err)
			continue
		}
		if result.upsert.finalized {
			// With eager recovery enabled, a failed attempt remains discoverable
			// until failed-job reconciliation reaches a decision. A later tick can
			// retry a transient Slurm mutation failure without another full GetJobs
			// read from Job Manager.
			if _, pending := pendingFailedRecoveries[result.upsert.key]; pending {
				continue
			}
			if err := c.SeenJobs.Forget(ctx, result.upsert.key); err != nil {
				errs = append(errs, errors.Wrapf(
					err,
					"forget finalized job %s attempt %d",
					result.upsert.key.JobID,
					result.upsert.key.AttemptIndex,
				))
			}
		}
	}
	if submissionErr != nil {
		errs = append(errs, submissionErr)
	}
	return errors.Join(errs...)
}

func (c *Controller) submitJobUpserts(
	ctx context.Context,
	snapshot ControllerSnapshot,
	keys []cluster.JobAttemptRef,
	upserts chan<- jobUpsertContext,
) (int, error) {
	submitted := 0
	for _, key := range keys {
		upsert, ok, err := c.prepareJobUpsert(ctx, snapshot, key, submitted)
		if err != nil {
			return submitted, err
		}
		if !ok {
			continue
		}
		if err := enqueueJobUpsert(ctx, upserts, upsert); err != nil {
			return submitted, err
		}
		submitted++
	}
	return submitted, nil
}

func (c *Controller) prepareJobUpsert(
	ctx context.Context,
	snapshot ControllerSnapshot,
	key cluster.JobAttemptRef,
	index int,
) (jobUpsertContext, bool, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return jobUpsertContext{}, false, errors.Wrap(ctxErr, "submit Heimdall job upserts")
	}
	if environmentAdmissionBlocks(snapshot, key) {
		return jobUpsertContext{}, false, nil
	}
	job, ok := jobForModePolicy(snapshot, key)
	if ok && modeDecisionForJob(snapshot, c.modePolicy(), job).Mode == cluster.JobModeIgnore {
		if err := c.SeenJobs.Forget(ctx, key); err != nil {
			return jobUpsertContext{}, false, errors.Wrapf(
				err,
				"forget ignored job %s attempt %d",
				key.JobID,
				key.AttemptIndex,
			)
		}
		return jobUpsertContext{}, false, nil
	}
	if job, ok := snapshot.ActiveJobs[key]; ok {
		if err := c.SeenJobs.MarkSeen(ctx, key); err != nil {
			return jobUpsertContext{}, false, errors.Wrapf(
				err,
				"mark seen job %s attempt %d",
				key.JobID,
				key.AttemptIndex,
			)
		}
		return jobUpsertContext{
			index:  index,
			key:    key,
			job:    job,
			source: JobUpsertSourceSlurmActive,
		}, true, nil
	}
	if job, ok := snapshot.AccountingJobs[key]; ok {
		return jobUpsertContext{
			index:     index,
			key:       key,
			job:       job,
			source:    JobUpsertSourceSlurmAccounting,
			finalized: true,
		}, true, nil
	}
	return jobUpsertContext{}, false, nil
}

func enqueueJobUpsert(ctx context.Context, upserts chan<- jobUpsertContext, upsert jobUpsertContext) error {
	select {
	case upserts <- upsert:
		return nil
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "submit Heimdall job upserts")
	}
}

func (c *Controller) jobUpsertWorkerPoolSize() int {
	if c.Options.JobUpsertWorkerPoolSize <= 0 {
		return 1
	}
	return c.Options.JobUpsertWorkerPoolSize
}

func failedJobRecoveryKeys(snapshot ControllerSnapshot, opts ControllerOptions) map[cluster.JobAttemptRef]struct{} {
	// Advice independently rediscovers finalized attempts after publication.
	// Retain them for direct mutation retries only when eager recovery is on.
	if !opts.EnableEagerRequeue {
		return nil
	}
	keys := make(map[cluster.JobAttemptRef]struct{})
	for _, job := range failedJobsFromSnapshot(snapshot, opts) {
		keys[jobAttemptRef(job)] = struct{}{}
	}
	return keys
}

func (c *Controller) forgetRejectedJobs(ctx context.Context, snapshot ControllerSnapshot) error {
	keys := make(map[cluster.JobAttemptRef]struct{}, len(snapshot.EnvironmentAdmissions)+len(snapshot.JobKeys))
	for key, decision := range snapshot.EnvironmentAdmissions {
		if !decision.Admitted {
			keys[key] = struct{}{}
		}
	}
	for key := range snapshot.JobKeys {
		if environmentAdmissionBlocks(snapshot, key) {
			keys[key] = struct{}{}
		}
	}
	for key := range keys {
		if err := c.SeenJobs.Forget(ctx, key); err != nil {
			return errors.Wrapf(err, "forget admission-rejected job %s attempt %d", key.JobID, key.AttemptIndex)
		}
	}
	return nil
}

func (c *Controller) upsertJob(ctx context.Context, job cluster.JobExecutionRecord) error {
	req, err := heimdall.BuildCreateOrUpdateJobRequest(job, heimdall.UpsertRequestOptions{Environment: c.Environment})
	if err != nil {
		return errors.Wrapf(err, "build upsert request for job %s attempt %d", job.JobID, job.AttemptIndex)
	}
	if _, err := c.HeimdallClient.CreateOrUpdateJob(ctx, req); err != nil {
		return errors.Wrapf(err, "upsert job %s attempt %d", job.JobID, job.AttemptIndex)
	}

	return nil
}

func isMutableJobState(status cluster.JobState) bool {
	switch status {
	case cluster.JobStatePending, cluster.JobStateRunning:
		return true
	default:
		return false
	}
}

func isFinalJobState(status cluster.JobState) bool {
	switch status {
	case cluster.JobStateCompleted,
		cluster.JobStateFailed,
		cluster.JobStateCancelled,
		cluster.JobStateDeleted,
		cluster.JobStateStopped:
		return true
	default:
		return false
	}
}
