// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package watchdog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobidentity"
	watchdogengine "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/watchdog"
	"github.com/google/go-cmp/cmp"
)

func TestObserverEmitsOneWatchdogReleaseCompletion(t *testing.T) {
	t.Parallel()
	completedAt := time.Date(2026, time.September, 2, 15, 0, 0, 0, time.UTC)
	commandErr := errors.New("release\nfailed")
	sink := &recordingSink{}
	observer := NewObserver(sink)
	observer.newID = func() string { return "watchdog-event" }
	_, scope := observer.StartReleaseScope(context.Background(), watchdogengine.ReleaseAttributes{
		JobID: "123", AttemptIndex: 2,
	})
	scope.EndReleaseScope(watchdogengine.ReleaseResult{CompletedAt: completedAt, Err: commandErr})
	scope.EndReleaseScope(watchdogengine.ReleaseResult{CompletedAt: completedAt})

	want := []audit.ProducerEvent{audit.WatchdogReleaseCompletedEvent{
		HeimdallJobID: jobidentity.HeimdallJobID("123", 2), JobID: "123", AttemptIndex: 2,
		Success: false, Error: "release\nfailed", ProducedAt: completedAt,
		IdempotencyKey: "watchdog-event",
	}.ToProducerEvent()}
	if diff := cmp.Diff(want, sink.events); diff != "" {
		t.Fatalf("emitted events mismatch (-want +got):\n%s", diff)
	}
}

type recordingSink struct {
	events []audit.ProducerEvent
}

func (sink *recordingSink) Add(events ...audit.ProducerEvent) {
	sink.events = append(sink.events, events...)
}
