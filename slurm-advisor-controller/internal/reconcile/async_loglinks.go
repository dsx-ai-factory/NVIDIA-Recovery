// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/loglinks"
)

type logLinkSyncRequest struct {
	observedAt time.Time
	targets    []loglinks.Target
}

// AsyncLogLinkMaterializer serializes filesystem synchronization behind a
// single worker. While that worker is busy, one pending request is retained and
// replaced by newer snapshots.
type AsyncLogLinkMaterializer struct {
	ctx      context.Context
	delegate LogLinkSyncer
	wake     chan struct{}

	mu             sync.Mutex
	pending        *logLinkSyncRequest
	completed      loglinks.Observation
	completedValid bool
}

// NewAsyncLogLinkMaterializer starts one process-scoped worker around the
// blocking filesystem syncer.
func NewAsyncLogLinkMaterializer(
	ctx context.Context,
	delegate LogLinkSyncer,
) *AsyncLogLinkMaterializer {
	if delegate == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	materializer := &AsyncLogLinkMaterializer{
		ctx:      ctx,
		delegate: delegate,
		wake:     make(chan struct{}, 1),
	}
	go materializer.run()
	return materializer
}

// Enqueue records the latest requested snapshot without waiting for filesystem
// synchronization. Targets are copied so callers may safely reuse their input.
func (m *AsyncLogLinkMaterializer) Enqueue(
	observedAt time.Time,
	targets []loglinks.Target,
) {
	if m == nil || m.ctx.Err() != nil {
		return
	}

	request := &logLinkSyncRequest{
		observedAt: observedAt,
		targets:    cloneLogLinkTargets(targets),
	}

	m.mu.Lock()
	if m.ctx.Err() != nil {
		m.mu.Unlock()
		return
	}
	m.pending = request
	m.mu.Unlock()

	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// Drain returns all completed work not previously observed. Counter values are
// merged when more than one synchronization completes between drains, while
// Owned reflects the latest completion.
func (m *AsyncLogLinkMaterializer) Drain() LogLinkObservation {
	if m == nil {
		return LogLinkObservation{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.completedValid {
		return LogLinkObservation{}
	}

	observation := m.completed
	m.completed = loglinks.Observation{}
	m.completedValid = false
	return LogLinkObservation{
		Completed:   true,
		Observation: observation,
	}
}

func (m *AsyncLogLinkMaterializer) run() {
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-m.wake:
		}

		request, ok := m.takePending()
		if !ok {
			continue
		}
		if m.ctx.Err() != nil {
			return
		}

		observation := m.delegate.Sync(m.ctx, request.observedAt, request.targets)
		if m.ctx.Err() != nil {
			return
		}
		m.publish(observation)
	}
}

func (m *AsyncLogLinkMaterializer) takePending() (logLinkSyncRequest, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending == nil {
		return logLinkSyncRequest{}, false
	}

	request := *m.pending
	m.pending = nil
	return request, true
}

func (m *AsyncLogLinkMaterializer) publish(observation loglinks.Observation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.completedValid {
		m.completed = mergeLogLinkObservations(m.completed, observation)
		return
	}
	m.completed = observation
	m.completedValid = true
}

func cloneLogLinkTargets(targets []loglinks.Target) []loglinks.Target {
	copied := make([]loglinks.Target, len(targets))
	for index := range targets {
		copied[index] = targets[index]
		copied[index].Paths = slices.Clone(targets[index].Paths)
	}
	return copied
}

func mergeLogLinkObservations(
	earlier loglinks.Observation,
	later loglinks.Observation,
) loglinks.Observation {
	return loglinks.Observation{
		Created:         saturatingAddInt(earlier.Created, later.Created),
		Existing:        saturatingAddInt(earlier.Existing, later.Existing),
		Collisions:      saturatingAddInt(earlier.Collisions, later.Collisions),
		Rejected:        saturatingAddInt(earlier.Rejected, later.Rejected),
		Errors:          saturatingAddInt(earlier.Errors, later.Errors),
		Removed:         saturatingAddInt(earlier.Removed, later.Removed),
		Owned:           later.Owned,
		Scans:           saturatingAddInt(earlier.Scans, later.Scans),
		ScanDirectories: saturatingAddInt(earlier.ScanDirectories, later.ScanDirectories),
		ScanEntries:     saturatingAddInt(earlier.ScanEntries, later.ScanEntries),
		ScanMatches:     saturatingAddInt(earlier.ScanMatches, later.ScanMatches),
		ScanDuration:    saturatingAddDuration(earlier.ScanDuration, later.ScanDuration),
		CleanupFailures: saturatingAddInt(earlier.CleanupFailures, later.CleanupFailures),
	}
}

func saturatingAddInt(value, addition int) int {
	if addition <= 0 {
		return value
	}
	maxInt := int(^uint(0) >> 1)
	if value > maxInt-addition {
		return maxInt
	}
	return value + addition
}

func saturatingAddDuration(value, addition time.Duration) time.Duration {
	if addition <= 0 {
		return value
	}
	const maxDuration = time.Duration(1<<63 - 1)
	if value > maxDuration-addition {
		return maxDuration
	}
	return value + addition
}
