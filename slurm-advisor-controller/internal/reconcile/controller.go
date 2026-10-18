// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/heimdall"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type ControllerOptions struct {
	AdvicePageLimit                int32
	JobPageLimit                   int32
	JobUpsertWorkerPoolSize        int
	AdviceLoopTimeout              time.Duration
	AdviceItemTimeout              time.Duration
	AdviceMaxAge                   time.Duration
	AdviceRequeueHoldDelay         time.Duration
	Environment                    string
	PartitionAllowlist             []string
	UpdateEligibleTimeAfterRequeue bool
	// FailedJobRecoveryTimeout bounds direct failed-job recovery within a tick.
	// Zero keeps the caller's deadline; production configuration requires a
	// positive duration and owns the default.
	FailedJobRecoveryTimeout time.Duration

	// EnableEagerRequeue permits recovery before Advice. Disabled by default so
	// Job Manager can apply retry and checkpoint policies before requeue.
	EnableEagerRequeue bool
	// BootstrapFailureLookback bounds finished failure discovery when eager
	// requeue is enabled. Production configuration supplies a positive duration.
	BootstrapFailureLookback time.Duration

	// UserAllowlist temporarily selects act for listed users and observe for others.
	// Explicit job mode wins. Empty uses the default; removable without notice.
	UserAllowlist []string
}

type Controller struct {
	JobReader           cluster.JobReader
	ActiveJobEnricher   cluster.ActiveJobEnricher
	HeimdallClient      heimdall.HeimdallClient
	SeenJobs            state.SeenJobStore
	ActionExecutor      cluster.JobMutator
	LogLinkMaterializer LogLinkMaterializer
	Environment         string
	Options             ControllerOptions
	Observer            Observer
	AuditObserver       AuditObserver
	ModePolicy          ModePolicy
	Logger              *zap.Logger
	Now                 func() time.Time

	// heimdallJobsBootstrapped records that the controller has successfully
	// loaded the reconciliation job set from Job Manager and recorded those job
	// attempts in the process-local SeenJobs store. Later ticks use that local
	// discovery set plus fresh Slurm and Advice reads instead of reloading every
	// persisted job.
	heimdallJobsBootstrapped bool

	// processedFailedJobRecoveries suppresses duplicate decisions for the exact
	// same Slurm attempt while this controller process is alive. Passive planning
	// is not completion: a later job-mode change can still authorize recovery.
	processedFailedJobRecoveries map[cluster.JobAttemptRef]struct{}

	// failedJobRequeueFailures carries consumed command attempts across phase
	// deadlines. The serial reconciliation loop exclusively owns this state.
	failedJobRequeueFailures map[cluster.JobAttemptRef]int
	// failedJobRecoveryCursor rotates the next tick past the last attempted job
	// so a slow command cannot repeatedly take the budget ahead of other jobs.
	failedJobRecoveryCursor *cluster.JobAttemptRef
}

type ControllerSyncResult struct {
	Snapshot   ControllerSnapshot
	FailedJobs FailedJobSyncResult
	Advice     AdviceSyncResult
	LogLinks   LogLinkObservation
}

func (c *Controller) SyncOnce(ctx context.Context) (result ControllerSyncResult, err error) {
	startedAt := time.Now()
	observation := SyncObservation{
		Mode: c.modePolicy().DefaultMode,
	}
	observer := c.observer()
	if progressObserver, ok := observer.(ProgressObserver); ok {
		progressObserver.ObserveSyncStarted(observation.Mode)
		defer progressObserver.ObserveSyncCompleted()
	}
	defer func() {
		if c.SeenJobs != nil {
			observeJobAttemptCache(&observation, c.SeenJobs.CountSeenJobs())
		}
		observation.Duration = time.Since(startedAt)
		if err != nil {
			observation.Outcome = ObservationOutcomeError
		} else {
			observation.Outcome = ObservationOutcomeSuccess
		}
		observer.ObserveSync(observation)
	}()

	// Each tick builds one consistent view of Slurm, Heimdall jobs, advice, and
	// local seen state. The controller then augments that view with targeted
	// accounting reads before running job-state upserts, optional eager
	// failed-job requeues, and advice reconciliation. Keeping those phases
	// explicit avoids mixing discovery, final-state enrichment, API upserts,
	// and Slurm mutations in one pass.
	if err := c.validate(); err != nil {
		return ControllerSyncResult{}, err
	}

	observeStageStarted(observer, StageJobStatusPolling)
	stageStartedAt := time.Now()
	snapshot, err := c.buildBaseSnapshot(ctx, &observation)
	if err != nil {
		observeStage(&observation, StageJobStatusPolling, err, time.Since(stageStartedAt))
		observeSnapshot(&observation, snapshot)
		return ControllerSyncResult{Snapshot: snapshot}, err
	}
	if err := c.completeHeimdallJobBootstrap(ctx, snapshot); err != nil {
		phaseErr := errors.Wrap(err, "persist Heimdall reconciliation job bootstrap")
		observeStage(&observation, StageJobStatusPolling, phaseErr, time.Since(stageStartedAt))
		observeSnapshot(&observation, snapshot)
		return ControllerSyncResult{Snapshot: snapshot}, phaseErr
	}

	accountingRequests := accountingRequestKeys(snapshot)
	var accountingErr error
	if len(accountingRequests) > 0 {
		sourceStartedAt := time.Now()
		finalizedJobs, err := c.JobReader.FinalizedJobs(ctx, accountingRequests)
		observeSource(
			&observation,
			SourceSlurmAccounting,
			err,
			len(finalizedJobs),
			time.Since(sourceStartedAt),
		)
		if err != nil {
			accountingErr = errors.Wrap(err, "read targeted finalized Slurm jobs")
		}
		if ctx.Err() != nil {
			phaseErr := errors.Join(accountingErr, ctx.Err())
			observeStage(&observation, StageJobStatusPolling, phaseErr, time.Since(stageStartedAt))
			return ControllerSyncResult{Snapshot: snapshot}, phaseErr
		}
		admittedAccountingJobs := make([]cluster.JobExecutionRecord, 0, len(finalizedJobs))
		for _, job := range finalizedJobs {
			key := jobAttemptRef(job)
			if _, active := snapshot.ActiveJobs[key]; active {
				if environmentAdmission(c.controllerEnvironment(), job).Admitted {
					admittedAccountingJobs = append(admittedAccountingJobs, job)
				}
				continue
			}
			if c.admitJob(&snapshot, job) {
				admittedAccountingJobs = append(admittedAccountingJobs, job)
			}
		}
		for _, job := range admittedAccountingJobs {
			if !environmentAdmissionBlocks(snapshot, jobAttemptRef(job)) {
				addAccountingJob(&snapshot, job)
			}
		}
	}
	observeEnvironmentAdmissions(&observation, snapshot)
	if err := c.forgetRejectedJobs(ctx, snapshot); err != nil {
		return ControllerSyncResult{Snapshot: snapshot}, errors.Join(accountingErr, err)
	}
	observeStage(&observation, StageJobStatusPolling, accountingErr, time.Since(stageStartedAt))
	resolveSnapshotModes(&snapshot, c.modePolicy(), &observation)
	observeSnapshot(&observation, snapshot)

	result.Snapshot = snapshot
	if c.LogLinkMaterializer != nil {
		result.LogLinks = c.LogLinkMaterializer.Drain()
		observation.LogLinks = result.LogLinks
		c.LogLinkMaterializer.Enqueue(
			snapshot.ObservedAt,
			logLinkTargets(snapshot),
		)
	}

	observeStageStarted(observer, StageJobReconciliation)
	stageStartedAt = time.Now()
	phaseErr := c.reconcileJobs(ctx, snapshot, &observation)
	observeStage(&observation, StageJobReconciliation, phaseErr, time.Since(stageStartedAt))
	if phaseErr != nil || accountingErr != nil {
		// Publish fresh observations even after a partial accounting failure,
		// but preserve the complete-snapshot requirement for scheduler actions
		// and Advice dispositions. Unresolved attempts remain seen for retry.
		return result, errors.Join(accountingErr, phaseErr)
	}

	var actionErrs []error

	if c.Options.EnableEagerRequeue {
		observeStageStarted(observer, StageFailedJobReconciliation)
		stageStartedAt = time.Now()
		failedJobResult, phaseErr := c.reconcileFailedJobs(ctx, snapshot, &observation)
		observeStage(&observation, StageFailedJobReconciliation, phaseErr, time.Since(stageStartedAt))
		result.FailedJobs = failedJobResult
		actionErrs = append(actionErrs, phaseErr)
	}
	if ctx.Err() != nil {
		return result, joinActionErrors(ctx, actionErrs)
	}

	observeStageStarted(observer, StageAdviceReconciliation)
	stageStartedAt = time.Now()
	adviceResult, phaseErr := c.reconcileAdvice(ctx, snapshot, &observation)
	observeStage(&observation, StageAdviceReconciliation, phaseErr, time.Since(stageStartedAt))
	result.Advice = adviceResult
	actionErrs = append(actionErrs, phaseErr)
	return result, joinActionErrors(ctx, actionErrs)
}

func observeStageStarted(observer Observer, stage ReconcileStage) {
	if progressObserver, ok := observer.(ProgressObserver); ok {
		progressObserver.ObserveStageStarted(stage)
	}
}

func joinActionErrors(ctx context.Context, errs []error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		found := false
		for _, err := range errs {
			if errors.Is(err, ctxErr) {
				found = true
				break
			}
		}
		if !found {
			errs = append(errs, errors.Wrap(ctxErr, "reconcile scheduler action phases"))
		}
	}

	return errors.Join(errs...)
}

func (c *Controller) validate() error {
	if c.JobReader == nil {
		return errors.New("controller missing job reader")
	}
	if c.HeimdallClient == nil {
		return errors.New("controller missing Heimdall client")
	}
	if c.SeenJobs == nil {
		return errors.New("controller missing seen-job store")
	}
	if c.Options.JobUpsertWorkerPoolSize < 0 {
		return errors.Newf(
			"controller job upsert worker pool size must not be negative: %d",
			c.Options.JobUpsertWorkerPoolSize,
		)
	}
	policy := c.modePolicy()
	switch policy.DefaultMode {
	case cluster.JobModeIgnore, cluster.JobModeObserve, cluster.JobModeAct:
	default:
		return errors.Newf("controller has invalid default mode %q", policy.DefaultMode)
	}
	if policy.AllowAct && c.ActionExecutor == nil {
		return errors.New("controller action allowance missing action executor")
	}
	return nil
}

func (c *Controller) observer() Observer {
	if c.Observer != nil {
		return c.Observer
	}

	return NopObserver{}
}

func (c *Controller) modePolicy() ModePolicy {
	policy := c.ModePolicy
	policy.UserAllowlist = c.Options.UserAllowlist
	if policy.DefaultMode == "" {
		policy.DefaultMode = cluster.JobModeObserve
	}
	return policy
}

func (c *Controller) observedAt() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c *Controller) buildBaseSnapshot(ctx context.Context, observation *SyncObservation) (ControllerSnapshot, error) {
	snapshot := newControllerSnapshot(c.observedAt())

	var activeJobs []cluster.JobExecutionRecord
	var activeErr error
	var activeDuration time.Duration
	var heimdallJobs []*pb.Job
	var heimdallErr error
	var heimdallDuration time.Duration
	readHeimdallJobs := !c.heimdallJobsBootstrapped
	var advice []*pb.Advice
	var adviceErr error
	var adviceDuration time.Duration
	var seenJobs []state.SeenJobRecord
	var seenErr error
	var seenDuration time.Duration

	var g errgroup.Group
	g.Go(func() error {
		startedAt := time.Now()
		activeJobs, activeErr = c.JobReader.ActiveJobs(ctx)
		activeDuration = time.Since(startedAt)

		return nil
	})
	if readHeimdallJobs {
		g.Go(func() error {
			startedAt := time.Now()
			opts := []heimdall.PaginationOption{heimdall.WithPageLimit(c.Options.JobPageLimit)}
			if c.Options.EnableEagerRequeue {
				opts = append(opts, heimdall.WithRecentFailuresSince(snapshot.ObservedAt.Add(-c.Options.BootstrapFailureLookback)))
			}
			resp, err := c.HeimdallClient.GetJobs(ctx, opts...)
			heimdallDuration = time.Since(startedAt)
			if err != nil {
				heimdallErr = errors.Wrap(err, "get Heimdall jobs")
				return nil
			}
			heimdallJobs = resp

			return nil
		})
	}
	g.Go(func() error {
		startedAt := time.Now()
		advice, adviceErr = c.HeimdallClient.ListAdvice(
			ctx,
			heimdall.WithPageLimit(c.Options.AdvicePageLimit),
			heimdall.WithOnlyActiveAdvice(),
		)
		adviceDuration = time.Since(startedAt)

		return nil
	})
	g.Go(func() error {
		startedAt := time.Now()
		seenJobs, seenErr = c.SeenJobs.ListSeenJobs(ctx)
		seenDuration = time.Since(startedAt)

		return nil
	})
	if err := g.Wait(); err != nil {
		return snapshot, err
	}

	observeSource(observation, SourceSlurmActive, activeErr, len(activeJobs), activeDuration)
	if readHeimdallJobs {
		observeSource(observation, SourceHeimdallJobs, heimdallErr, len(heimdallJobs), heimdallDuration)
	}
	observeSource(observation, SourceHeimdallAdvice, adviceErr, len(advice), adviceDuration)
	observeSource(observation, SourceSeenStore, seenErr, len(seenJobs), seenDuration)

	var errs []error
	if activeErr != nil {
		errs = append(errs, errors.Wrap(activeErr, "read active Slurm jobs"))
	}
	if heimdallErr != nil {
		errs = append(errs, heimdallErr)
	}
	if adviceErr != nil {
		errs = append(errs, adviceErr)
	}
	if seenErr != nil {
		errs = append(errs, errors.Wrap(seenErr, "list seen jobs"))
	}
	if len(errs) > 0 {
		return snapshot, errors.Join(errs...)
	}

	// Admission must be complete before filtering because a newer rejected
	// attempt blocks older attempts regardless of squeue response order.
	for _, job := range activeJobs {
		c.admitJob(&snapshot, job)
	}
	admittedActiveJobs := make([]cluster.JobExecutionRecord, 0, len(activeJobs))
	for _, job := range activeJobs {
		key := jobAttemptRef(job)
		if snapshot.EnvironmentAdmissions[key].Admitted && !environmentAdmissionBlocks(snapshot, key) {
			admittedActiveJobs = append(admittedActiveJobs, job)
		}
	}
	if c.ActiveJobEnricher != nil {
		sourceStartedAt := time.Now()
		admittedActiveJobs = c.ActiveJobEnricher.EnrichActiveJobs(ctx, admittedActiveJobs)
		observeSource(
			observation,
			SourceSlurmLogMetadata,
			nil,
			len(admittedActiveJobs),
			time.Since(sourceStartedAt),
		)
	}
	for _, job := range admittedActiveJobs {
		addActiveJob(&snapshot, job)
	}
	for _, job := range heimdallJobs {
		// Reject historical candidates before bootstrap retains their keys or
		// accounting queries Slurm. Do not record a scheduler rejection here:
		// a stale backend value must not block a fresh, independently admitted job.
		if heimdallJobEnvironmentAdmission(c.controllerEnvironment(), job).Admitted &&
			!environmentAdmissionBlocks(snapshot, jobAttemptRefFromHeimdall(job)) {
			addHeimdallJob(&snapshot, job)
		}
	}
	for _, item := range advice {
		addAdvice(&snapshot, item)
		// Keep skipped Advice observable without letting it seed accounting.
		if _, skip := c.ownershipSkip(item); !skip && adviceHasRecommendation(item) {
			addJobKey(&snapshot, jobAttemptRefFromAdvice(item))
		}
	}
	for _, record := range seenJobs {
		addJobKey(&snapshot, record)
	}
	snapshot.SeenJobCount = len(seenJobs)

	return snapshot, nil
}

// completeHeimdallJobBootstrap retains the initial Job Manager discovery for
// the lifetime of this controller process. The bootstrap is complete only
// after every admitted backend job attempt is in SeenJobs; a source or store
// failure therefore causes the next tick to retry GetJobs instead of continuing
// with a partial discovery set.
func (c *Controller) completeHeimdallJobBootstrap(ctx context.Context, snapshot ControllerSnapshot) error {
	if c.heimdallJobsBootstrapped {
		return nil
	}

	for key := range snapshot.HeimdallJobs {
		if err := c.SeenJobs.MarkSeen(ctx, key); err != nil {
			return errors.Wrapf(err, "mark bootstrapped job %s attempt %d as seen", key.JobID, key.AttemptIndex)
		}
	}
	c.heimdallJobsBootstrapped = true

	return nil
}
