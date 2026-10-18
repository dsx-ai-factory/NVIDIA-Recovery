// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package watchdog

import (
	"context"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobpolicy"
)

func TestWatchdogReleaseLifecyclePropagatesContextAndCompletion(t *testing.T) {
	t.Parallel()
	observer := newFakeSlurm()
	clock := &fakeClock{}
	lifecycle := &recordingReleaseLifecycle{}
	releaser := contextCheckingReleaser{t: t}
	engine, err := New(observer, releaser, Options{
		JobPolicy: jobpolicy.Policy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Mode:      ModeAct, AcknowledgeSchedulerActions: true,
		ReleaseTimeout: time.Minute, CleanupGracePeriod: 10 * time.Minute,
		Now: clock.Now, ReleaseLifecycleObserver: lifecycle,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	start := time.Date(2026, time.September, 2, 16, 0, 0, 0, time.UTC)
	clock.now = start
	observer.jobs = []cluster.JobExecutionRecord{runningJob("123", 0)}
	if _, err := engine.SyncOnce(context.Background()); err != nil {
		t.Fatalf("initial SyncOnce() error = %v", err)
	}
	clock.now = start.Add(time.Minute)
	observer.jobs = []cluster.JobExecutionRecord{heldJob("123", 1)}
	if _, err := engine.SyncOnce(context.Background()); err != nil {
		t.Fatalf("held SyncOnce() error = %v", err)
	}
	clock.now = start.Add(2 * time.Minute)
	if _, err := engine.SyncOnce(context.Background()); err != nil {
		t.Fatalf("release SyncOnce() error = %v", err)
	}

	if len(lifecycle.attributes) != 1 || lifecycle.attributes[0] != (ReleaseAttributes{
		JobID: "123", AttemptIndex: 1,
	}) {
		t.Fatalf("release attributes = %#v, want job 123 attempt 1", lifecycle.attributes)
	}
	if len(lifecycle.results) != 1 {
		t.Fatalf("release result count = %d, want 1", len(lifecycle.results))
	}
	if lifecycle.results[0].Err != nil || !lifecycle.results[0].CompletedAt.Equal(clock.now) {
		t.Fatalf("release result = %#v, want successful completion at %s", lifecycle.results[0], clock.now)
	}
}

type releaseLifecycleContextKey struct{}

type recordingReleaseLifecycle struct {
	attributes []ReleaseAttributes
	results    []ReleaseResult
}

func (lifecycle *recordingReleaseLifecycle) StartReleaseScope(
	ctx context.Context,
	attributes ReleaseAttributes,
) (context.Context, ReleaseScope) {
	lifecycle.attributes = append(lifecycle.attributes, attributes)
	return context.WithValue(ctx, releaseLifecycleContextKey{}, "release"), &recordingReleaseScope{
		lifecycle: lifecycle,
	}
}

type recordingReleaseScope struct {
	lifecycle *recordingReleaseLifecycle
}

func (scope *recordingReleaseScope) EndReleaseScope(result ReleaseResult) {
	scope.lifecycle.results = append(scope.lifecycle.results, result)
}

type contextCheckingReleaser struct {
	t *testing.T
}

func (releaser contextCheckingReleaser) JobRelease(ctx context.Context, _ string) error {
	releaser.t.Helper()
	if got := ctx.Value(releaseLifecycleContextKey{}); got != "release" {
		releaser.t.Fatalf("scheduler release context value = %v, want release", got)
	}
	return nil
}
