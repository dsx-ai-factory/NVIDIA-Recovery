// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package watchdog

import (
	"context"
	stderrors "errors"
	"slices"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	admission "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/environment"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobpolicy"
)

func TestWatchdogProtectsInitialHeldDAGAttempt(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	watchdog, slurm, clock := newTestWatchdog(t, ModeAct, time.Minute, 10*time.Minute)
	initial := withDAGChild(heldJob("100", 0))

	syncJobs(t, watchdog, slurm, clock, start, initial)
	result := syncJobs(t, watchdog, slurm, clock, start.Add(10*time.Minute), initial)

	if len(result.Releases) != 0 || len(slurm.releaseCalls) != 0 {
		t.Fatalf("initial DAG hold produced releases=%#v calls=%v, want none", result.Releases, slurm.releaseCalls)
	}
}

func TestWatchdogReleasesFirstObservedDAGRetryAtTimeout(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	watchdog, slurm, clock := newTestWatchdog(t, ModeAct, 5*time.Minute, 10*time.Minute)
	retry := withDAGChild(heldJob("100_2", 1))

	syncJobs(t, watchdog, slurm, clock, start, retry)
	before := syncJobs(t, watchdog, slurm, clock, start.Add(5*time.Minute-time.Nanosecond), retry)
	if len(before.Releases) != 0 {
		t.Fatalf("Releases before boundary = %#v, want none", before.Releases)
	}

	atBoundary := syncJobs(t, watchdog, slurm, clock, start.Add(5*time.Minute), retry)
	assertRelease(t, atBoundary.Releases, "100_2", 1, ReleaseApplied)
	if !slices.Equal(slurm.releaseCalls, []string{"100_2"}) {
		t.Fatalf("release calls = %v, want exact array element 100_2", slurm.releaseCalls)
	}
}

func TestWatchdogAlwaysProtectsManagedNonDAGJob(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	watchdog, slurm, clock := newTestWatchdog(t, ModeAct, time.Minute, 10*time.Minute)
	initialHold := heldJob("100", 0)

	syncJobs(t, watchdog, slurm, clock, start, initialHold)
	result := syncJobs(t, watchdog, slurm, clock, start.Add(time.Minute), initialHold)

	assertRelease(t, result.Releases, "100", 0, ReleaseApplied)
	if !slices.Equal(slurm.releaseCalls, []string{"100"}) {
		t.Fatalf("release calls = %v, want managed non-DAG job 100", slurm.releaseCalls)
	}
}

func TestWatchdogDoesNotProtectUnmanagedJob(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	watchdog, slurm, clock := newTestWatchdog(t, ModeAct, time.Minute, 10*time.Minute)
	unmanaged := withManaged(heldJob("100", 1), false)

	syncJobs(t, watchdog, slurm, clock, start, unmanaged)
	result := syncJobs(t, watchdog, slurm, clock, start.Add(10*time.Minute), unmanaged)

	if len(result.Releases) != 0 || len(slurm.releaseCalls) != 0 {
		t.Fatalf("unmanaged hold produced releases=%#v calls=%v, want none", result.Releases, slurm.releaseCalls)
	}
}

// TestWatchdogPartitionChangeRestartsTimer proves that returning to an allowed
// partition cannot reuse a held timer retained within the cleanup grace period.
func TestWatchdogPartitionChangeRestartsTimer(t *testing.T) {
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	slurm := newFakeSlurm()
	clock := &fakeClock{now: start}
	engine, err := New(slurm, slurm, Options{
		JobPolicy:                   jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Mode:                        ModeAct,
		AcknowledgeSchedulerActions: true,
		ReleaseTimeout:              time.Minute,
		CleanupGracePeriod:          10 * time.Minute,
		PartitionAllowlist:          []string{"gpu"},
		Now:                         clock.Now,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	job := heldJob("job", 1)
	job.Partition = "gpu"
	syncJobs(t, engine, slurm, clock, start, job)

	job.Partition = "cpu"
	rejected := syncJobs(t, engine, slurm, clock, start.Add(30*time.Second), job)
	if rejected.TrackedJobs != 0 || len(rejected.Releases) != 0 || len(slurm.releaseCalls) != 0 {
		t.Fatalf("partition rejection retained state or released: result=%#v calls=%v", rejected, slurm.releaseCalls)
	}

	job.Partition = "gpu"
	returnedAt := start.Add(2 * time.Minute)
	for _, elapsed := range []time.Duration{0, time.Minute - time.Nanosecond} {
		result := syncJobs(t, engine, slurm, clock, returnedAt.Add(elapsed), job)
		if result.TrackedJobs != 1 || len(result.Releases) != 0 || len(slurm.releaseCalls) != 0 {
			t.Fatalf("before fresh timeout at %s: result=%#v calls=%v", elapsed, result, slurm.releaseCalls)
		}
	}
	result := syncJobs(t, engine, slurm, clock, returnedAt.Add(time.Minute), job)
	assertRelease(t, result.Releases, job.JobID, job.AttemptIndex, ReleaseApplied)
	if !result.Releases[0].HeldSince.Equal(returnedAt) || !slices.Equal(slurm.releaseCalls, []string{job.JobID}) {
		t.Fatalf("release = %#v calls=%v, want one release timed from return", result.Releases[0], slurm.releaseCalls)
	}
}

func TestWatchdogEnvironmentPatternGatesTrackingAndRelease(t *testing.T) {
	start := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	slurm := newFakeSlurm()
	clock := &fakeClock{now: start}
	engine, err := New(slurm, slurm, Options{
		JobPolicy:                   jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Mode:                        ModeAct,
		AcknowledgeSchedulerActions: true,
		ReleaseTimeout:              time.Minute,
		CleanupGracePeriod:          10 * time.Minute,
		EnvironmentPattern:          "blue",
		Now:                         clock.Now,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	blue := withEnvironment(heldJob("blue-job", 1), "blue")
	green := withEnvironment(heldJob("green-job", 1), "green")

	first := syncJobs(t, engine, slurm, clock, start, blue, green)
	if first.ManagedJobs != 1 || first.TrackedJobs != 1 {
		t.Fatalf("first counts = managed:%d tracked:%d, want 1/1", first.ManagedJobs, first.TrackedJobs)
	}
	if got := first.EnvironmentAdmissionCount(admission.AdmissionMatched); got != 1 {
		t.Fatalf("matched admissions = %d, want 1", got)
	}
	if got := first.EnvironmentAdmissionCount(admission.AdmissionMismatch); got != 1 {
		t.Fatalf("mismatch admissions = %d, want 1", got)
	}

	result := syncJobs(t, engine, slurm, clock, start.Add(time.Minute), blue, green)
	assertRelease(t, result.Releases, blue.JobID, 1, ReleaseApplied)
	if !slices.Equal(slurm.releaseCalls, []string{blue.JobID}) {
		t.Fatalf("release calls = %v, want only %s", slurm.releaseCalls, blue.JobID)
	}
}

func TestWatchdogLiveEnvironmentChangeDisarmsAndRestartsTimer(t *testing.T) {
	start := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	slurm := newFakeSlurm()
	clock := &fakeClock{now: start}
	engine, err := New(slurm, slurm, Options{
		JobPolicy:          jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Mode:               ModeObserve,
		ReleaseTimeout:     time.Minute,
		CleanupGracePeriod: 10 * time.Minute,
		EnvironmentPattern: "dev-*",
		Now:                clock.Now,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	matching := withEnvironment(heldJob("job-100", 1), "dev-blue")
	routedAway := withEnvironment(heldJob("job-100", 1), "prod")

	syncJobs(t, engine, slurm, clock, start, matching)
	rejected := syncJobs(t, engine, slurm, clock, start.Add(30*time.Second), routedAway)
	if rejected.TrackedJobs != 0 || len(rejected.Releases) != 0 {
		t.Fatalf("routed-away result = tracked:%d releases:%#v, want no state", rejected.TrackedJobs, rejected.Releases)
	}
	syncJobs(t, engine, slurm, clock, start.Add(time.Minute), matching)
	beforeResetBoundary := syncJobs(t, engine, slurm, clock, start.Add(90*time.Second), matching)
	if len(beforeResetBoundary.Releases) != 0 {
		t.Fatalf("release before restarted timeout = %#v, want none", beforeResetBoundary.Releases)
	}
	atResetBoundary := syncJobs(t, engine, slurm, clock, start.Add(2*time.Minute), matching)
	assertRelease(t, atResetBoundary.Releases, matching.JobID, 1, ReleasePlanned)
}

func TestWatchdogEnvironmentAdmissionPrecedesPartitionFiltering(t *testing.T) {
	start := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	slurm := newFakeSlurm()
	clock := &fakeClock{now: start}
	engine, err := New(slurm, slurm, Options{
		JobPolicy:          jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Mode:               ModeObserve,
		ReleaseTimeout:     time.Minute,
		CleanupGracePeriod: 10 * time.Minute,
		EnvironmentPattern: "dev-*",
		PartitionAllowlist: []string{"gpu"},
		Now:                clock.Now,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	matching := withEnvironment(heldJob("job-100", 1), "dev-blue")
	routedAway := withEnvironment(heldJob("job-100", 1), "prod")
	routedAway.Partition = "unmanaged"

	syncJobs(t, engine, slurm, clock, start, matching)
	rejected := syncJobs(t, engine, slurm, clock, start.Add(30*time.Second), routedAway)
	if rejected.TrackedJobs != 0 || len(rejected.Releases) != 0 {
		t.Fatalf("partition-filtered routed-away result = tracked:%d releases:%#v, want no state", rejected.TrackedJobs, rejected.Releases)
	}
	if got := rejected.EnvironmentAdmissionCount(admission.AdmissionMismatch); got != 1 {
		t.Fatalf("partition-filtered mismatch admissions = %d, want 1", got)
	}
	syncJobs(t, engine, slurm, clock, start.Add(time.Minute), matching)
	beforeResetBoundary := syncJobs(t, engine, slurm, clock, start.Add(90*time.Second), matching)
	if len(beforeResetBoundary.Releases) != 0 {
		t.Fatalf("release before restarted timeout = %#v, want none", beforeResetBoundary.Releases)
	}
	atResetBoundary := syncJobs(t, engine, slurm, clock, start.Add(2*time.Minute), matching)
	assertRelease(t, atResetBoundary.Releases, matching.JobID, 1, ReleasePlanned)
}

func TestWatchdogWildcardNeverAdmitsInvalidEnvironment(t *testing.T) {
	start := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	slurm := newFakeSlurm()
	clock := &fakeClock{now: start}
	engine, err := New(slurm, slurm, Options{
		JobPolicy:          jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Mode:               ModeObserve,
		ReleaseTimeout:     time.Minute,
		CleanupGracePeriod: 10 * time.Minute,
		EnvironmentPattern: "*",
		Now:                clock.Now,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	invalid := heldJob("job-100", 1)
	invalid.EnvironmentRequest = cluster.JobEnvironmentRequest{Specified: true}

	result := syncJobs(t, engine, slurm, clock, start, invalid)
	if result.ManagedJobs != 0 || result.TrackedJobs != 0 || len(result.Releases) != 0 {
		t.Fatalf("invalid result = %#v, want entirely untracked", result)
	}
	if got := result.EnvironmentAdmissionCount(admission.AdmissionInvalid); got != 1 {
		t.Fatalf("invalid admissions = %d, want 1", got)
	}
}

func TestWatchdogUsesSlurmHoldBitInsteadOfHeldReasonText(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	watchdog, slurm, clock := newTestWatchdog(t, ModeAct, time.Minute, 10*time.Minute)

	syncJobs(t, watchdog, slurm, clock, start, runningJob("100", 0))
	notHeld := heldJob("100", 1)
	notHeld.Held = false
	notHeld.Reason = "JobHeldUser"
	syncJobs(t, watchdog, slurm, clock, start.Add(time.Minute), notHeld)
	result := syncJobs(t, watchdog, slurm, clock, start.Add(3*time.Minute), notHeld)

	if len(result.Releases) != 0 || len(slurm.releaseCalls) != 0 {
		t.Fatalf("reason-only hold produced releases=%#v calls=%v, want none", result.Releases, slurm.releaseCalls)
	}
}

func TestWatchdogTracksExactJobIdentityWithoutSameNameGrouping(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	watchdog, slurm, clock := newTestWatchdog(t, ModeAct, time.Minute, 10*time.Minute)
	first := runningJob("100", 0)
	second := runningJob("101", 0)
	first.JobName = "same-name"
	second.JobName = "same-name"

	syncJobs(t, watchdog, slurm, clock, start, first, second)
	first = heldJob("100", 1)
	first.JobName = "same-name"
	second = runningJob("101", 0)
	second.JobName = "same-name"
	syncJobs(t, watchdog, slurm, clock, start.Add(time.Minute), first, second)
	result := syncJobs(t, watchdog, slurm, clock, start.Add(2*time.Minute), first, second)

	assertRelease(t, result.Releases, "100", 1, ReleaseApplied)
	if !slices.Equal(slurm.releaseCalls, []string{"100"}) {
		t.Fatalf("release calls = %v, want only exact transitioned job 100", slurm.releaseCalls)
	}
}

func TestWatchdogRetainsStateAcrossBriefObservationGap(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	watchdog, slurm, clock := newTestWatchdog(t, ModeObserve, time.Minute, 10*time.Minute)

	syncJobs(t, watchdog, slurm, clock, start, runningJob("100", 0))
	gap := syncJobs(t, watchdog, slurm, clock, start.Add(9*time.Minute))
	if gap.TrackedJobs != 1 {
		t.Fatalf("TrackedJobs during grace = %d, want 1", gap.TrackedJobs)
	}
	syncJobs(t, watchdog, slurm, clock, start.Add(9*time.Minute+time.Second), heldJob("100", 1))
	result := syncJobs(t, watchdog, slurm, clock, start.Add(10*time.Minute+time.Second), heldJob("100", 1))

	assertRelease(t, result.Releases, "100", 1, ReleasePlanned)
}

func TestWatchdogClearsStateAtCleanupGraceBoundary(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	watchdog, slurm, clock := newTestWatchdog(t, ModeObserve, time.Minute, 10*time.Minute)

	syncJobs(t, watchdog, slurm, clock, start, runningJob("100", 0))
	cleared := syncJobs(t, watchdog, slurm, clock, start.Add(10*time.Minute))
	if cleared.TrackedJobs != 0 {
		t.Fatalf("TrackedJobs at cleanup boundary = %d, want 0", cleared.TrackedJobs)
	}
	syncJobs(t, watchdog, slurm, clock, start.Add(10*time.Minute+time.Second), heldJob("100", 1))
	before := syncJobs(t, watchdog, slurm, clock, start.Add(11*time.Minute), heldJob("100", 1))
	if len(before.Releases) != 0 {
		t.Fatalf("Releases before restarted timer boundary = %#v, want none", before.Releases)
	}
	result := syncJobs(t, watchdog, slurm, clock, start.Add(11*time.Minute+time.Second), heldJob("100", 1))
	assertRelease(t, result.Releases, "100", 1, ReleasePlanned)
}

func TestWatchdogObserveModeReportsCandidateWithoutMutation(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	watchdog, slurm, clock := newTestWatchdog(t, ModeObserve, time.Minute, 10*time.Minute)

	syncJobs(t, watchdog, slurm, clock, start, runningJob("100", 0))
	syncJobs(t, watchdog, slurm, clock, start.Add(time.Minute), heldJob("100", 1))
	result := syncJobs(t, watchdog, slurm, clock, start.Add(2*time.Minute), heldJob("100", 1))

	assertRelease(t, result.Releases, "100", 1, ReleasePlanned)
	if len(slurm.releaseCalls) != 0 {
		t.Fatalf("release calls = %v, want none in observe mode", slurm.releaseCalls)
	}
}

func TestWatchdogRetriesFailedReleaseOnLaterPoll(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	releaseErr := stderrors.New("release failed")
	watchdog, slurm, clock := newTestWatchdog(t, ModeAct, time.Minute, 10*time.Minute)
	slurm.releaseErrors["100"] = []error{releaseErr, nil}

	syncJobs(t, watchdog, slurm, clock, start, runningJob("100", 0))
	syncJobs(t, watchdog, slurm, clock, start.Add(time.Minute), heldJob("100", 1))
	clock.now = start.Add(2 * time.Minute)
	slurm.jobs = []cluster.JobExecutionRecord{heldJob("100", 1)}
	failed, err := watchdog.SyncOnce(context.Background())
	if !stderrors.Is(err, releaseErr) {
		t.Fatalf("SyncOnce() error = %v, want release error", err)
	}
	assertRelease(t, failed.Releases, "100", 1, ReleaseError)

	retried := syncJobs(t, watchdog, slurm, clock, start.Add(2*time.Minute+time.Second), heldJob("100", 1))
	assertRelease(t, retried.Releases, "100", 1, ReleaseApplied)
	if !slices.Equal(slurm.releaseCalls, []string{"100", "100"}) {
		t.Fatalf("release calls = %v, want failed call and later retry", slurm.releaseCalls)
	}
}

func TestWatchdogAggregatesIndependentReleaseErrors(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	firstErr := stderrors.New("first release failed")
	secondErr := stderrors.New("second release failed")
	watchdog, slurm, clock := newTestWatchdog(t, ModeAct, time.Minute, 10*time.Minute)
	slurm.releaseErrors["100"] = []error{firstErr}
	slurm.releaseErrors["101"] = []error{secondErr}

	syncJobs(t, watchdog, slurm, clock, start, runningJob("100", 0), runningJob("101", 0))
	syncJobs(t, watchdog, slurm, clock, start.Add(time.Minute), heldJob("100", 1), heldJob("101", 1))
	clock.now = start.Add(2 * time.Minute)
	slurm.jobs = []cluster.JobExecutionRecord{heldJob("100", 1), heldJob("101", 1)}
	result, err := watchdog.SyncOnce(context.Background())
	if !stderrors.Is(err, firstErr) || !stderrors.Is(err, secondErr) {
		t.Fatalf("SyncOnce() error = %v, want both release errors", err)
	}
	if !slices.Equal(slurm.releaseCalls, []string{"100", "101"}) {
		t.Fatalf("release calls = %v, want both independent attempts", slurm.releaseCalls)
	}
	if len(result.Releases) != 2 || result.Releases[0].Outcome != ReleaseError || result.Releases[1].Outcome != ReleaseError {
		t.Fatalf("Releases = %#v, want two error outcomes", result.Releases)
	}
}

func TestWatchdogFiltersPartitionAllowlist(t *testing.T) {
	start := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	slurm := newFakeSlurm()
	clock := &fakeClock{now: start}
	watchdog, err := New(slurm, slurm, Options{
		JobPolicy:                   jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Mode:                        ModeAct,
		AcknowledgeSchedulerActions: true,
		ReleaseTimeout:              time.Minute,
		CleanupGracePeriod:          10 * time.Minute,
		PartitionAllowlist:          []string{"allowed"},
		Now:                         clock.Now,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	allowed := runningJob("100", 0)
	allowed.Partition = "allowed"
	blocked := runningJob("101", 0)
	blocked.Partition = "blocked"
	syncJobs(t, watchdog, slurm, clock, start, allowed, blocked)
	allowed = heldJob("100", 1)
	allowed.Partition = "allowed"
	blocked = heldJob("101", 1)
	blocked.Partition = "blocked"
	syncJobs(t, watchdog, slurm, clock, start.Add(time.Minute), allowed, blocked)
	result := syncJobs(t, watchdog, slurm, clock, start.Add(2*time.Minute), allowed, blocked)

	assertRelease(t, result.Releases, "100", 1, ReleaseApplied)
	if !slices.Equal(slurm.releaseCalls, []string{"100"}) {
		t.Fatalf("release calls = %v, want only allowlisted partition", slurm.releaseCalls)
	}
}

func TestNewRejectsActionModeWithoutAcknowledgement(t *testing.T) {
	slurm := newFakeSlurm()
	_, err := New(slurm, slurm, Options{
		JobPolicy:          jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Mode:               ModeAct,
		ReleaseTimeout:     time.Minute,
		CleanupGracePeriod: time.Minute,
	})
	if err == nil {
		t.Fatal("New() error = nil, want action acknowledgement error")
	}
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

type fakeSlurm struct {
	jobs          []cluster.JobExecutionRecord
	observeErr    error
	releaseCalls  []string
	releaseErrors map[string][]error
}

func newFakeSlurm() *fakeSlurm {
	return &fakeSlurm{releaseErrors: map[string][]error{}}
}

func (s *fakeSlurm) ActiveJobs(context.Context) ([]cluster.JobExecutionRecord, error) {
	return slices.Clone(s.jobs), s.observeErr
}

func (s *fakeSlurm) JobRelease(_ context.Context, jobID string) error {
	s.releaseCalls = append(s.releaseCalls, jobID)
	errs := s.releaseErrors[jobID]
	if len(errs) == 0 {
		return nil
	}
	s.releaseErrors[jobID] = errs[1:]
	return errs[0]
}

func newTestWatchdog(t *testing.T, mode Mode, releaseTimeout, cleanupGrace time.Duration) (*Watchdog, *fakeSlurm, *fakeClock) {
	t.Helper()
	slurm := newFakeSlurm()
	clock := &fakeClock{}
	watchdog, err := New(slurm, slurm, Options{
		JobPolicy:                   jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Mode:                        mode,
		AcknowledgeSchedulerActions: mode == ModeAct,
		ReleaseTimeout:              releaseTimeout,
		CleanupGracePeriod:          cleanupGrace,
		Now:                         clock.Now,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	return watchdog, slurm, clock
}

func syncJobs(
	t *testing.T,
	watchdog *Watchdog,
	slurm *fakeSlurm,
	clock *fakeClock,
	at time.Time,
	jobs ...cluster.JobExecutionRecord,
) SyncResult {
	t.Helper()
	clock.now = at
	slurm.jobs = jobs
	result, err := watchdog.SyncOnce(context.Background())
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	return result
}

func runningJob(jobID string, attempt int32) cluster.JobExecutionRecord {
	return cluster.JobExecutionRecord{
		JobID:           jobID,
		AttemptIndex:    attempt,
		JobName:         "train",
		Partition:       "gpu",
		State:           cluster.JobStateRunning,
		RawState:        "RUNNING",
		HeimdallManaged: true,
	}
}

func heldJob(jobID string, attempt int32) cluster.JobExecutionRecord {
	return cluster.JobExecutionRecord{
		JobID:           jobID,
		AttemptIndex:    attempt,
		JobName:         "train",
		Partition:       "gpu",
		State:           cluster.JobStatePending,
		RawState:        "PENDING",
		Held:            true,
		HeimdallManaged: true,
	}
}

func withManaged(job cluster.JobExecutionRecord, managed bool) cluster.JobExecutionRecord {
	job.HeimdallManaged = managed
	return job
}

func withDAGChild(job cluster.JobExecutionRecord) cluster.JobExecutionRecord {
	job.DAGChild = true
	return job
}

func withEnvironment(job cluster.JobExecutionRecord, value string) cluster.JobExecutionRecord {
	job.EnvironmentRequest = cluster.JobEnvironmentRequest{Environment: value, Specified: true, Valid: true}
	return job
}

func assertRelease(t *testing.T, releases []Release, jobID string, attempt int32, outcome ReleaseOutcome) {
	t.Helper()
	if len(releases) != 1 {
		t.Fatalf("len(Releases) = %d, want 1: %#v", len(releases), releases)
	}
	if releases[0].JobID != jobID || releases[0].AttemptIndex != attempt || releases[0].Outcome != outcome {
		t.Fatalf("Release = %#v, want job=%s attempt=%d outcome=%s", releases[0], jobID, attempt, outcome)
	}
}
