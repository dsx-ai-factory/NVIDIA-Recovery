// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/watchdog"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRunPollsImmediatelyAndOnTicksUntilContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ticks := make(chan time.Time)
	polled := make(chan struct{}, 3)
	done := make(chan struct{})

	go func() {
		run(ctx, ticks, zap.NewNop(), func(context.Context) {
			polled <- struct{}{}
		})
		close(done)
	}()

	waitForPoll(t, polled, "immediate poll")
	ticks <- time.Now()
	waitForPoll(t, polled, "ticker poll")
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run() did not stop after context cancellation")
	}
}

func TestRunRecoversPollPanicAndContinuesOnNextTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	core, logs := observer.New(zap.ErrorLevel)
	logger := zap.New(core)
	polls := 0
	done := make(chan struct{})

	go func() {
		run(ctx, ticks, logger, func(context.Context) {
			polls++
			if polls == 1 {
				panic("poll boom")
			}
			cancel()
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run() did not continue after recovering the poll panic")
	}

	if polls != 2 {
		t.Fatalf("poll count = %d, want 2", polls)
	}
	entries := logs.FilterMessage("watchdog poll panicked").All()
	if len(entries) != 1 {
		t.Fatalf("panic log count = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["panic"] != "poll boom" {
		t.Fatalf("panic field = %#v, want %q", fields["panic"], "poll boom")
	}
	stacktrace, ok := fields["stacktrace"].(string)
	if !ok || !strings.Contains(stacktrace, "TestRunRecoversPollPanicAndContinuesOnNextTick") {
		t.Fatalf("stacktrace = %#v, want recovered poll stack", fields["stacktrace"])
	}
}

func TestNewLoggerUsesConfiguredLevel(t *testing.T) {
	logger, err := newLogger(watchdog.Config{Mode: watchdog.ModeObserve, LogLevel: "debug"})
	if err != nil {
		t.Fatalf("newLogger() failed: %v", err)
	}
	t.Cleanup(func() { _ = logger.Sync() })

	if !logger.Core().Enabled(zap.DebugLevel) {
		t.Fatal("debug logging is disabled, want configured debug level")
	}
}

func waitForPoll(t *testing.T, polled <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-polled:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}
