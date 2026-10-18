// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package watchdog

import (
	"slices"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobpolicy"
)

func TestWatchdogUserAdmissionGatesTrackingAndRelease(t *testing.T) {
	start := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name        string
		users       []string
		user        string
		environment string
		partition   string
		initialDAG  bool
		wantTracked int
		wantRelease bool
	}{
		{name: "first allowed user", users: []string{"alice", "bob"}, user: "alice", environment: "blue", partition: "gpu", wantTracked: 1, wantRelease: true},
		{name: "second allowed user", users: []string{"alice", "bob"}, user: "bob", environment: "blue", partition: "gpu", wantTracked: 1, wantRelease: true},
		{name: "other user", users: []string{"alice"}, user: "carol", environment: "blue", partition: "gpu"},
		{name: "unknown user", users: []string{"alice"}, environment: "blue", partition: "gpu"},
		{name: "case sensitive", users: []string{"alice"}, user: "Alice", environment: "blue", partition: "gpu"},
		{name: "no username trimming", users: []string{"alice"}, user: " alice ", environment: "blue", partition: "gpu"},
		{name: "empty list uses act default", environment: "blue", partition: "gpu", wantTracked: 1, wantRelease: true},
		{name: "environment still required", users: []string{"alice"}, user: "alice", environment: "green", partition: "gpu"},
		{name: "partition still required", users: []string{"alice"}, user: "alice", environment: "blue", partition: "cpu"},
		{name: "initial DAG remains protected", users: []string{"alice"}, user: "alice", environment: "blue", partition: "gpu", initialDAG: true, wantTracked: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			slurm := newFakeSlurm()
			clock := &fakeClock{now: start}
			engine, err := New(slurm, slurm, Options{
				Mode: ModeAct, AcknowledgeSchedulerActions: true,
				ReleaseTimeout: time.Minute, CleanupGracePeriod: 10 * time.Minute,
				EnvironmentPattern: "blue", PartitionAllowlist: []string{"gpu"},
				JobPolicy: jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true, UserAllowlist: tt.users}, Now: clock.Now,
			})
			if err != nil {
				t.Fatalf("New() failed: %v", err)
			}
			job := withEnvironment(heldJob("job", 1), tt.environment)
			job.User, job.Partition = tt.user, tt.partition
			if tt.initialDAG {
				job.DAGChild, job.AttemptIndex = true, 0
			}
			first := syncJobs(t, engine, slurm, clock, start, job)
			if first.TrackedJobs != tt.wantTracked || first.ManagedJobs != tt.wantTracked || len(first.Releases) != 0 {
				t.Fatalf("initial result = %#v, want managed/tracked=%d and no release", first, tt.wantTracked)
			}
			result := syncJobs(t, engine, slurm, clock, start.Add(time.Minute), job)
			if result.TrackedJobs != tt.wantTracked || result.ManagedJobs != tt.wantTracked {
				t.Fatalf("timeout result = %#v, want managed/tracked=%d", result, tt.wantTracked)
			}
			if tt.wantRelease {
				assertRelease(t, result.Releases, job.JobID, job.AttemptIndex, ReleaseApplied)
				if !slices.Equal(slurm.releaseCalls, []string{job.JobID}) {
					t.Fatalf("release calls = %v, want one admitted job", slurm.releaseCalls)
				}
			} else if len(result.Releases) != 0 || len(slurm.releaseCalls) != 0 {
				t.Fatalf("ineligible job produced releases=%v calls=%v", result.Releases, slurm.releaseCalls)
			}
		})
	}
}

func TestWatchdogUserChangeDisarmsAndRestartsTimer(t *testing.T) {
	start := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	slurm := newFakeSlurm()
	clock := &fakeClock{now: start}
	engine, err := New(slurm, slurm, Options{
		Mode: ModeAct, AcknowledgeSchedulerActions: true,
		ReleaseTimeout: time.Minute, CleanupGracePeriod: 10 * time.Minute,
		EnvironmentPattern: "blue", PartitionAllowlist: []string{"gpu"},
		JobPolicy: jobpolicy.Policy{DefaultMode: cluster.JobModeObserve, AllowAct: true, UserAllowlist: []string{"alice"}}, Now: clock.Now,
	})
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}
	allowed := withEnvironment(heldJob("job", 1), "blue")
	allowed.User = "alice"
	rejected := allowed
	rejected.User = "carol"
	// User rejection must clear a running timer even when partition filtering
	// also rejects the fresh record; cleanup grace must not retain that timer.
	rejected.Partition = "cpu"

	first := syncJobs(t, engine, slurm, clock, start, allowed)
	if first.TrackedJobs != 1 {
		t.Fatalf("initial tracked jobs = %d, want 1", first.TrackedJobs)
	}
	for _, elapsed := range []time.Duration{30 * time.Second, time.Minute} {
		result := syncJobs(t, engine, slurm, clock, start.Add(elapsed), rejected)
		if result.TrackedJobs != 0 || len(result.Releases) != 0 || len(slurm.releaseCalls) != 0 {
			t.Fatalf("rejected at %s: result=%#v calls=%v, want no state or release", elapsed, result, slurm.releaseCalls)
		}
	}
	readmittedAt := start.Add(2 * time.Minute)
	readmitted := syncJobs(t, engine, slurm, clock, readmittedAt, allowed)
	if readmitted.TrackedJobs != 1 || len(readmitted.Releases) != 0 {
		t.Fatalf("readmitted result = %#v, want a fresh timer without release", readmitted)
	}
	before := syncJobs(t, engine, slurm, clock, readmittedAt.Add(time.Minute-time.Nanosecond), allowed)
	if len(before.Releases) != 0 || len(slurm.releaseCalls) != 0 {
		t.Fatalf("release before fresh timeout: releases=%v calls=%v", before.Releases, slurm.releaseCalls)
	}
	result := syncJobs(t, engine, slurm, clock, readmittedAt.Add(time.Minute), allowed)
	assertRelease(t, result.Releases, allowed.JobID, allowed.AttemptIndex, ReleaseApplied)
	if !result.Releases[0].HeldSince.Equal(readmittedAt) || !slices.Equal(slurm.releaseCalls, []string{allowed.JobID}) {
		t.Fatalf("release = %#v calls=%v, want one release timed from readmission", result.Releases[0], slurm.releaseCalls)
	}
}

func TestWatchdogJobPolicyOverridesAndActionCeilings(t *testing.T) {
	start := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name         string
		policy       jobpolicy.Policy
		request      cluster.JobModeRequest
		watchdogMode Mode
		watchdogAck  bool
		wantTracked  int
		wantOutcome  ReleaseOutcome
		wantCalls    int
		wantError    bool
	}{
		{name: "listed user overrides observe default", policy: jobpolicy.Policy{DefaultMode: cluster.JobModeObserve, AllowAct: true, UserAllowlist: []string{"alice"}}, watchdogMode: ModeAct, watchdogAck: true, wantTracked: 1, wantOutcome: ReleaseApplied, wantCalls: 1},
		{name: "unlisted user overrides act default", policy: jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true, UserAllowlist: []string{"bob"}}, watchdogMode: ModeAct, watchdogAck: true},
		{name: "explicit act overrides unlisted user", policy: jobpolicy.Policy{DefaultMode: cluster.JobModeObserve, AllowAct: true, UserAllowlist: []string{"bob"}}, request: cluster.JobModeRequest{Mode: cluster.JobModeAct, Specified: true, Valid: true}, watchdogMode: ModeAct, watchdogAck: true, wantTracked: 1, wantOutcome: ReleaseApplied, wantCalls: 1},
		{name: "explicit observe overrides listed user", policy: jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true, UserAllowlist: []string{"alice"}}, request: cluster.JobModeRequest{Mode: cluster.JobModeObserve, Specified: true, Valid: true}, watchdogMode: ModeAct, watchdogAck: true},
		{name: "explicit ignore overrides listed user", policy: jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true, UserAllowlist: []string{"alice"}}, request: cluster.JobModeRequest{Mode: cluster.JobModeIgnore, Specified: true, Valid: true}, watchdogMode: ModeAct, watchdogAck: true},
		{name: "invalid job mode cannot act", policy: jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true, UserAllowlist: []string{"alice"}}, request: cluster.JobModeRequest{Mode: "automatic", Specified: true}, watchdogMode: ModeAct, watchdogAck: true},
		{name: "SAC acknowledgement caps explicit act", policy: jobpolicy.Policy{DefaultMode: cluster.JobModeObserve, UserAllowlist: []string{"alice"}}, request: cluster.JobModeRequest{Mode: cluster.JobModeAct, Specified: true, Valid: true}, watchdogMode: ModeAct, watchdogAck: true},
		{name: "empty list uses observe default", policy: jobpolicy.Policy{DefaultMode: cluster.JobModeObserve, AllowAct: true}, watchdogMode: ModeAct, watchdogAck: true},
		{name: "watchdog observe only plans actionable jobs", policy: jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true}, watchdogMode: ModeObserve, wantTracked: 1, wantOutcome: ReleasePlanned},
		{name: "watchdog acknowledgement remains required", policy: jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true}, watchdogMode: ModeAct, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			slurm := newFakeSlurm()
			clock := &fakeClock{now: start}
			engine, err := New(slurm, slurm, Options{
				Mode: tt.watchdogMode, AcknowledgeSchedulerActions: tt.watchdogAck,
				JobPolicy: tt.policy, ReleaseTimeout: time.Minute, CleanupGracePeriod: 10 * time.Minute, Now: clock.Now,
			})
			if tt.wantError {
				if err == nil {
					t.Fatal("New() succeeded without watchdog action acknowledgement")
				}
				return
			}
			if err != nil {
				t.Fatalf("New() failed: %v", err)
			}
			job := heldJob("job", 1)
			job.User, job.ModeRequest = "alice", tt.request
			first := syncJobs(t, engine, slurm, clock, start, job)
			if first.TrackedJobs != tt.wantTracked {
				t.Fatalf("tracked jobs = %d, want %d", first.TrackedJobs, tt.wantTracked)
			}
			result := syncJobs(t, engine, slurm, clock, start.Add(time.Minute), job)
			if tt.wantOutcome != "" {
				assertRelease(t, result.Releases, job.JobID, job.AttemptIndex, tt.wantOutcome)
			} else if len(result.Releases) != 0 {
				t.Fatalf("non-actionable job produced releases: %#v", result.Releases)
			}
			if len(slurm.releaseCalls) != tt.wantCalls {
				t.Fatalf("release calls = %v, want %d", slurm.releaseCalls, tt.wantCalls)
			}
		})
	}
}

func TestWatchdogJobModeChangeRestartsTimer(t *testing.T) {
	for _, mode := range []cluster.JobMode{cluster.JobModeObserve, cluster.JobModeIgnore} {
		t.Run(string(mode), func(t *testing.T) {
			engine, slurm, clock := newTestWatchdog(t, ModeAct, time.Minute, 10*time.Minute)
			start := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
			job := heldJob("job", 1)
			syncJobs(t, engine, slurm, clock, start, job)
			job.ModeRequest = cluster.JobModeRequest{Mode: mode, Specified: true, Valid: true}
			disarmed := syncJobs(t, engine, slurm, clock, start.Add(time.Minute), job)
			if disarmed.TrackedJobs != 0 || len(disarmed.Releases) != 0 || len(slurm.releaseCalls) != 0 {
				t.Fatalf("non-actionable job retained timer or released: %#v calls=%v", disarmed, slurm.releaseCalls)
			}
			job.ModeRequest = cluster.JobModeRequest{Mode: cluster.JobModeAct, Specified: true, Valid: true}
			reenabled := syncJobs(t, engine, slurm, clock, start.Add(2*time.Minute), job)
			if len(reenabled.Releases) != 0 {
				t.Fatalf("re-enabled job used stale timer: %#v", reenabled.Releases)
			}
			result := syncJobs(t, engine, slurm, clock, start.Add(3*time.Minute), job)
			assertRelease(t, result.Releases, job.JobID, job.AttemptIndex, ReleaseApplied)
			if !result.Releases[0].HeldSince.Equal(start.Add(2 * time.Minute)) {
				t.Fatalf("held since = %s, want fresh timer", result.Releases[0].HeldSince)
			}
		})
	}
}
