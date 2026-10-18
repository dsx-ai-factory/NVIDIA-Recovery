// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package watchdog

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	admission "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/environment"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobpolicy"
)

type Mode string

const (
	ModeObserve Mode = "observe"
	ModeAct     Mode = "act"
)

type ReleaseOutcome string

const (
	ReleasePlanned ReleaseOutcome = "planned"
	ReleaseApplied ReleaseOutcome = "applied"
	ReleaseError   ReleaseOutcome = "error"
)

type JobObserver interface {
	ActiveJobs(context.Context) ([]cluster.JobExecutionRecord, error)
}

type JobReleaser interface {
	JobRelease(context.Context, string) error
}

type Options struct {
	// JobPolicy selects the SAC action cohort independently of watchdog release mode.
	JobPolicy jobpolicy.Policy

	Mode                        Mode
	AcknowledgeSchedulerActions bool
	ReleaseTimeout              time.Duration
	CleanupGracePeriod          time.Duration
	EnvironmentPattern          string
	PartitionAllowlist          []string
	Now                         func() time.Time
	ReleaseLifecycleObserver    ReleaseLifecycleObserver
}

type Watchdog struct {
	jobPolicy                jobpolicy.Policy
	observer                 JobObserver
	releaser                 JobReleaser
	mode                     Mode
	releaseTimeout           time.Duration
	cleanupGracePeriod       time.Duration
	partitionAllowlist       map[string]struct{}
	environmentMatcher       admission.Matcher
	now                      func() time.Time
	releaseLifecycleObserver ReleaseLifecycleObserver
	jobs                     map[string]*jobState
}

type jobState struct {
	lastSeen    time.Time
	heldAttempt int32
	heldSince   time.Time
}

type SyncResult struct {
	ObservedJobs          int
	ManagedJobs           int
	TrackedJobs           int
	Releases              []Release
	EnvironmentAdmissions map[admission.AdmissionReason]int
}

type Release struct {
	JobID        string
	AttemptIndex int32
	Partition    string
	HeldSince    time.Time
	Outcome      ReleaseOutcome
}

func New(observer JobObserver, releaser JobReleaser, options Options) (*Watchdog, error) {
	if observer == nil {
		return nil, errors.New("watchdog missing Slurm job observer")
	}
	if options.ReleaseTimeout <= 0 {
		return nil, errors.Newf("watchdog release timeout must be positive: %s", options.ReleaseTimeout)
	}
	if options.CleanupGracePeriod <= 0 {
		return nil, errors.Newf("watchdog cleanup grace period must be positive: %s", options.CleanupGracePeriod)
	}
	switch options.Mode {
	case ModeObserve:
	case ModeAct:
		if !options.AcknowledgeSchedulerActions {
			return nil, errors.New("watchdog act mode requires explicit scheduler action acknowledgement")
		}
		if releaser == nil {
			return nil, errors.New("watchdog act mode missing Slurm job releaser")
		}
	default:
		return nil, errors.Newf("unsupported watchdog mode %q", options.Mode)
	}

	if err := validateJobDefaultMode(options.JobPolicy.DefaultMode); err != nil {
		return nil, err
	}
	options.JobPolicy.UserAllowlist = slices.Clone(options.JobPolicy.UserAllowlist)

	allowlist := map[string]struct{}{}
	for _, partition := range options.PartitionAllowlist {
		partition = strings.TrimSpace(partition)
		if partition != "" {
			allowlist[partition] = struct{}{}
		}
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	pattern := options.EnvironmentPattern
	if pattern == "" {
		pattern = admission.Unspecified
	}
	environmentMatcher, err := admission.NewMatcher(pattern)
	if err != nil {
		return nil, errors.Wrap(err, "create watchdog environment matcher")
	}
	releaseLifecycleObserver := options.ReleaseLifecycleObserver
	if releaseLifecycleObserver == nil {
		releaseLifecycleObserver = NopReleaseLifecycleObserver{}
	}

	return &Watchdog{
		jobPolicy:                options.JobPolicy,
		observer:                 observer,
		releaser:                 releaser,
		mode:                     options.Mode,
		releaseTimeout:           options.ReleaseTimeout,
		cleanupGracePeriod:       options.CleanupGracePeriod,
		partitionAllowlist:       allowlist,
		environmentMatcher:       environmentMatcher,
		now:                      now,
		releaseLifecycleObserver: releaseLifecycleObserver,
		jobs:                     map[string]*jobState{},
	}, nil
}

func (w *Watchdog) SyncOnce(ctx context.Context) (SyncResult, error) {
	jobs, err := w.observer.ActiveJobs(ctx)
	if err != nil {
		return SyncResult{}, errors.Wrap(err, "observe active Slurm jobs")
	}

	now := w.now()
	result := SyncResult{ObservedJobs: len(jobs)}
	result.Releases, result.ManagedJobs, result.EnvironmentAdmissions = w.observe(jobs, now)
	result.TrackedJobs = len(w.jobs)
	if w.mode == ModeObserve {
		for i := range result.Releases {
			result.Releases[i].Outcome = ReleasePlanned
		}
		return result, nil
	}

	var releaseErrors []error
	for i := range result.Releases {
		release := &result.Releases[i]
		releaseCtx, scope := w.releaseLifecycleObserver.StartReleaseScope(ctx, ReleaseAttributes{
			JobID:        release.JobID,
			AttemptIndex: release.AttemptIndex,
		})
		err := w.releaser.JobRelease(releaseCtx, release.JobID)
		scope.EndReleaseScope(ReleaseResult{CompletedAt: w.now(), Err: err})
		if err != nil {
			release.Outcome = ReleaseError
			releaseErrors = append(releaseErrors, errors.Wrapf(err, "release watchdog candidate job %s", release.JobID))
			continue
		}

		release.Outcome = ReleaseApplied
		if state := w.jobs[release.JobID]; state != nil && state.heldAttempt == release.AttemptIndex {
			state.heldSince = now
		}
	}

	if len(releaseErrors) > 0 {
		return result, errors.Join(releaseErrors...)
	}
	return result, nil
}

func (w *Watchdog) observe(observations []cluster.JobExecutionRecord, now time.Time) ([]Release, int, map[admission.AdmissionReason]int) {
	for jobID, state := range w.jobs {
		if !now.Before(state.lastSeen.Add(w.cleanupGracePeriod)) {
			delete(w.jobs, jobID)
		}
	}

	sort.SliceStable(observations, func(i, j int) bool {
		return observations[i].JobID < observations[j].JobID
	})
	seen := map[string]struct{}{}
	releases := []Release{}
	managedJobs := 0
	environmentAdmissions := map[admission.AdmissionReason]int{}
	for _, job := range observations {
		if job.JobID == "" {
			continue
		}
		if _, duplicate := seen[job.JobID]; duplicate {
			continue
		}
		seen[job.JobID] = struct{}{}

		decision := w.environmentMatcher.Admit(job)
		environmentAdmissions[decision.Reason]++
		if !decision.Admitted || jobpolicy.Resolve(w.jobPolicy, job).Mode != cluster.JobModeAct {
			delete(w.jobs, job.JobID)
			continue
		}
		if !w.partitionAllowed(job.Partition) {
			delete(w.jobs, job.JobID)
			continue
		}
		managedJobs++
		state := w.jobs[job.JobID]
		if state == nil {
			state = &jobState{}
			w.jobs[job.JobID] = state
		}
		state.lastSeen = now

		switch {
		case job.State == cluster.JobStatePending && job.Held && holdReleaseEligible(job):
			if state.heldSince.IsZero() || state.heldAttempt != job.AttemptIndex {
				state.heldAttempt = job.AttemptIndex
				state.heldSince = now
			}
			if !now.Before(state.heldSince.Add(w.releaseTimeout)) {
				releases = append(releases, Release{
					JobID:        job.JobID,
					AttemptIndex: job.AttemptIndex,
					Partition:    job.Partition,
					HeldSince:    state.heldSince,
				})
			}
		case job.State == cluster.JobStatePending || job.State == cluster.JobStateRunning:
			state.clearHeld()
		default:
			state.disarm()
		}
	}

	return releases, managedJobs, environmentAdmissions
}

func (result SyncResult) EnvironmentAdmissionCount(reason admission.AdmissionReason) int {
	return result.EnvironmentAdmissions[reason]
}

func (w *Watchdog) partitionAllowed(partition string) bool {
	if len(w.partitionAllowlist) == 0 {
		return true
	}
	_, ok := w.partitionAllowlist[partition]
	return ok
}

func holdReleaseEligible(job cluster.JobExecutionRecord) bool {
	return !job.DAGChild || job.AttemptIndex >= 1
}

func (s *jobState) clearHeld() {
	s.heldAttempt = 0
	s.heldSince = time.Time{}
}

func (s *jobState) disarm() {
	s.clearHeld()
}
