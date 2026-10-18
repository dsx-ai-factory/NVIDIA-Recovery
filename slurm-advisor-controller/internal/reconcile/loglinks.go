// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"slices"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/loglinks"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/slurm"
)

// LogLinkMaterializer is the filesystem capability consumed by reconciliation.
// Enqueue and Drain must not wait for filesystem work, so link compatibility
// cannot interrupt job publication or scheduler recovery.
type LogLinkMaterializer interface {
	Enqueue(time.Time, []loglinks.Target)
	Drain() LogLinkObservation
}

// LogLinkSyncer is the blocking filesystem capability owned by the asynchronous
// materializer adapter.
type LogLinkSyncer interface {
	Sync(context.Context, time.Time, []loglinks.Target) loglinks.Observation
}

// LogLinkObservation distinguishes a completed filesystem synchronization from
// a reconciliation tick that has no new result to publish.
type LogLinkObservation struct {
	Completed   bool
	Observation loglinks.Observation
}

func logLinkTargets(snapshot ControllerSnapshot) []loglinks.Target {
	keys := make([]cluster.JobAttemptRef, 0, len(snapshot.ActiveJobs))
	for key := range snapshot.ActiveJobs {
		keys = append(keys, key)
	}
	sortJobKeys(keys)

	targets := make([]loglinks.Target, 0, len(keys))
	for _, key := range keys {
		job := snapshot.ActiveJobs[key]
		if !job.HeimdallManaged {
			continue
		}
		decision, ok := snapshot.EffectiveModes[key]
		if !ok || decision.Mode == cluster.JobModeIgnore {
			continue
		}

		paths := slurm.LogFilePaths(job)
		slices.Sort(paths)
		paths = slices.Compact(paths)
		targets = append(targets, loglinks.Target{
			JobID:        job.JobID,
			AttemptIndex: job.AttemptIndex,
			ArrayJobID:   job.ArrayJobID,
			Paths:        paths,
		})
	}

	return targets
}
