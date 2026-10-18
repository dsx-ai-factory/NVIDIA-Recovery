// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import "time"

// LogMetadataCacheSource identifies one bounded source within the exact-attempt
// log-metadata cache.
type LogMetadataCacheSource string

const (
	LogMetadataCacheSourceParent LogMetadataCacheSource = "parent"
	LogMetadataCacheSourceSteps  LogMetadataCacheSource = "steps"
)

// LogMetadataCacheResult classifies a cache read without exposing job identity.
type LogMetadataCacheResult string

const (
	LogMetadataCacheHit   LogMetadataCacheResult = "hit"
	LogMetadataCacheMiss  LogMetadataCacheResult = "miss"
	LogMetadataCacheStale LogMetadataCacheResult = "stale"
)

// LogMetadataWorkEvent identifies a bounded background-work transition.
type LogMetadataWorkEvent string

const (
	LogMetadataWorkOffered      LogMetadataWorkEvent = "offered"
	LogMetadataWorkDeduplicated LogMetadataWorkEvent = "deduplicated"
	LogMetadataWorkAdmitted     LogMetadataWorkEvent = "admitted"
	LogMetadataWorkQueueFull    LogMetadataWorkEvent = "queue_full"
	LogMetadataWorkStarted      LogMetadataWorkEvent = "started"
	LogMetadataWorkCompleted    LogMetadataWorkEvent = "completed"
	LogMetadataWorkFailed       LogMetadataWorkEvent = "failed"
	LogMetadataWorkTimedOut     LogMetadataWorkEvent = "timed_out"
	LogMetadataWorkCanceled     LogMetadataWorkEvent = "canceled"
	LogMetadataWorkStale        LogMetadataWorkEvent = "stale"
)

// LogMetadataCommand identifies one read-only supplemental Slurm command.
type LogMetadataCommand string

const (
	LogMetadataCommandSacct    LogMetadataCommand = "sacct"
	LogMetadataCommandScontrol LogMetadataCommand = "scontrol"
)

// LogMetadataCommandOutcome classifies a bounded command result.
type LogMetadataCommandOutcome string

const (
	LogMetadataCommandSuccess  LogMetadataCommandOutcome = "success"
	LogMetadataCommandError    LogMetadataCommandOutcome = "error"
	LogMetadataCommandTimeout  LogMetadataCommandOutcome = "timeout"
	LogMetadataCommandCanceled LogMetadataCommandOutcome = "canceled"
)

// LogMetadataEvictionReason identifies one closed cache-eviction cause.
type LogMetadataEvictionReason string

const (
	LogMetadataEvictionTTL          LogMetadataEvictionReason = "ttl"
	LogMetadataEvictionAttemptLimit LogMetadataEvictionReason = "attempt_limit"
	LogMetadataEvictionPathBytes    LogMetadataEvictionReason = "path_bytes"
)

// LogMetadataCacheObservation is a point-in-time view of bounded cache and
// worker state. It contains no job-derived labels or values.
type LogMetadataCacheObservation struct {
	PathBytes       int
	Attempts        int
	PathByteLimit   int
	AttemptLimit    int
	QueueDepth      int
	QueueCapacity   int
	WorkersInFlight int
	WorkerLimit     int
	OldestQueuedAge time.Duration
}

// LogMetadataObserver receives bounded asynchronous-enrichment telemetry.
// Implementations must be concurrency-safe because workers call it directly.
type LogMetadataObserver interface {
	ObserveLogMetadataCacheAccess(LogMetadataCacheSource, LogMetadataCacheResult)
	ObserveLogMetadataWork(LogMetadataWorkEvent)
	ObserveLogMetadataCommand(LogMetadataCommand, LogMetadataCommandOutcome, time.Duration)
	ObserveLogMetadataEviction(LogMetadataEvictionReason, int)
	ObserveLogMetadataPathsDiscovered(LogMetadataCacheSource, int)
	ObserveLogMetadataPublished(int, int)
	ObserveLogMetadataCache(LogMetadataCacheObservation)
}

// NopLogMetadataObserver keeps metrics optional without nil checks in workers.
type NopLogMetadataObserver struct{}

func (NopLogMetadataObserver) ObserveLogMetadataCacheAccess(LogMetadataCacheSource, LogMetadataCacheResult) {
}
func (NopLogMetadataObserver) ObserveLogMetadataWork(LogMetadataWorkEvent) {}
func (NopLogMetadataObserver) ObserveLogMetadataCommand(
	LogMetadataCommand,
	LogMetadataCommandOutcome,
	time.Duration,
) {
}
func (NopLogMetadataObserver) ObserveLogMetadataEviction(LogMetadataEvictionReason, int) {}
func (NopLogMetadataObserver) ObserveLogMetadataPathsDiscovered(LogMetadataCacheSource, int) {
}
func (NopLogMetadataObserver) ObserveLogMetadataPublished(int, int)                {}
func (NopLogMetadataObserver) ObserveLogMetadataCache(LogMetadataCacheObservation) {}
