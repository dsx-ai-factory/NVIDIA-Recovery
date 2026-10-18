// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/loglinks"
	"github.com/google/go-cmp/cmp"
)

func TestAsyncLogLinkMaterializerKeepsLatestPendingRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncer := newControlledLogLinkSyncer()
	materializer := NewAsyncLogLinkMaterializer(ctx, syncer)

	firstTargets := []loglinks.Target{{
		JobID:        "first",
		AttemptIndex: 1,
		Paths:        []string{"/logs/first.out"},
	}}
	firstObservedAt := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	materializer.Enqueue(firstObservedAt, firstTargets)
	firstTargets[0].JobID = "mutated"
	firstTargets[0].Paths[0] = "/logs/mutated.out"

	firstCall := receiveLogLinkSyncCall(t, syncer.started)
	if firstCall.observedAt != firstObservedAt {
		t.Fatalf("first observedAt = %s, want %s", firstCall.observedAt, firstObservedAt)
	}
	wantFirstTargets := []loglinks.Target{{
		JobID:        "first",
		AttemptIndex: 1,
		Paths:        []string{"/logs/first.out"},
	}}
	if diff := cmp.Diff(wantFirstTargets, firstCall.targets); diff != "" {
		t.Fatalf("copied targets mismatch (-want +got):\n%s", diff)
	}

	const lastAttempt = 500
	enqueued := make(chan struct{})
	go func() {
		defer close(enqueued)
		for attempt := int32(2); attempt <= lastAttempt; attempt++ {
			materializer.Enqueue(
				firstObservedAt.Add(time.Duration(attempt)*time.Second),
				[]loglinks.Target{{
					JobID:        fmt.Sprintf("job-%d", attempt),
					AttemptIndex: attempt,
				}},
			)
		}
	}()
	select {
	case <-enqueued:
	case <-time.After(time.Second):
		t.Fatal("Enqueue() blocked while the filesystem worker was stuck")
	}

	select {
	case call := <-syncer.started:
		t.Fatalf("delegate started concurrent work while first sync was blocked: %#v", call)
	case <-time.After(25 * time.Millisecond):
	}

	syncer.responses <- loglinks.Observation{
		Created:      1,
		Errors:       2,
		Owned:        10,
		Scans:        1,
		ScanDuration: 3 * time.Millisecond,
	}
	secondCall := receiveLogLinkSyncCall(t, syncer.started)
	if secondCall.observedAt != firstObservedAt.Add(lastAttempt*time.Second) {
		t.Fatalf(
			"second observedAt = %s, want latest %s",
			secondCall.observedAt,
			firstObservedAt.Add(lastAttempt*time.Second),
		)
	}
	if len(secondCall.targets) != 1 ||
		secondCall.targets[0].JobID != fmt.Sprintf("job-%d", lastAttempt) {
		t.Fatalf("second targets = %#v, want latest request", secondCall.targets)
	}

	syncer.responses <- loglinks.Observation{
		Created:      3,
		Existing:     4,
		Owned:        20,
		Scans:        1,
		ScanDuration: 5 * time.Millisecond,
	}

	got := drainCompletedLogLinks(t, materializer)
	want := LogLinkObservation{
		Completed: true,
		Observation: loglinks.Observation{
			Created:      4,
			Existing:     4,
			Errors:       2,
			Owned:        20,
			Scans:        2,
			ScanDuration: 8 * time.Millisecond,
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Drain() mismatch (-want +got):\n%s", diff)
	}
	if second := materializer.Drain(); second.Completed {
		t.Fatalf("second Drain() = %#v, want completed work surfaced exactly once", second)
	}

	select {
	case call := <-syncer.started:
		t.Fatalf("delegate ran an obsolete queued request: %#v", call)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestAsyncLogLinkMaterializerStopsBeforePendingWorkAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	syncer := newControlledLogLinkSyncer()
	materializer := NewAsyncLogLinkMaterializer(ctx, syncer)

	materializer.Enqueue(time.Now(), []loglinks.Target{{JobID: "in-flight"}})
	receiveLogLinkSyncCall(t, syncer.started)
	materializer.Enqueue(time.Now(), []loglinks.Target{{JobID: "pending"}})

	cancel()
	select {
	case call := <-syncer.started:
		t.Fatalf("delegate started pending work after cancellation: %#v", call)
	case <-time.After(25 * time.Millisecond):
	}
	if got := materializer.Drain(); got.Completed {
		t.Fatalf("Drain() after cancellation = %#v, want canceled work unpublished", got)
	}
}

type controlledLogLinkSyncer struct {
	started   chan logLinkMaterializerCall
	responses chan loglinks.Observation
}

func newControlledLogLinkSyncer() *controlledLogLinkSyncer {
	return &controlledLogLinkSyncer{
		started:   make(chan logLinkMaterializerCall, 2),
		responses: make(chan loglinks.Observation, 2),
	}
}

func (s *controlledLogLinkSyncer) Sync(
	ctx context.Context,
	observedAt time.Time,
	targets []loglinks.Target,
) loglinks.Observation {
	call := logLinkMaterializerCall{
		observedAt: observedAt,
		targets:    cloneTargetsForTest(targets),
	}
	select {
	case s.started <- call:
	case <-ctx.Done():
		return loglinks.Observation{}
	}

	select {
	case observation := <-s.responses:
		return observation
	case <-ctx.Done():
		return loglinks.Observation{}
	}
}

func receiveLogLinkSyncCall(
	t *testing.T,
	started <-chan logLinkMaterializerCall,
) logLinkMaterializerCall {
	t.Helper()
	select {
	case call := <-started:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for filesystem synchronization to start")
		return logLinkMaterializerCall{}
	}
}

func drainCompletedLogLinks(
	t *testing.T,
	materializer *AsyncLogLinkMaterializer,
) LogLinkObservation {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for completed filesystem synchronization")
			return LogLinkObservation{}
		case <-ticker.C:
			if observation := materializer.Drain(); observation.Completed {
				return observation
			}
		}
	}
}

func cloneTargetsForTest(targets []loglinks.Target) []loglinks.Target {
	copied := make([]loglinks.Target, len(targets))
	for index := range targets {
		copied[index] = targets[index]
		copied[index].Paths = slices.Clone(targets[index].Paths)
	}
	return copied
}
