// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"maps"
	"path/filepath"
	"slices"
	"strings"
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
		target := logLinkTarget(job)
		target.Paths = paths
		targets = append(targets, target)
	}

	return targets
}

func logLinkTarget(job cluster.JobExecutionRecord) loglinks.Target {
	target := loglinks.Target{
		JobID: job.JobID, JobName: job.JobName, AttemptIndex: job.AttemptIndex,
		ArrayJobID: job.ArrayJobID, Paths: slurm.LogFilePaths(job),
	}
	if job.SubmitTime != nil {
		target.SubmitTime = *job.SubmitTime
	}
	return target
}

// withLogLinkPaths decorates a fresh publication record, never the scheduler
// snapshot or metadata cache. Names are planned without filesystem I/O and may
// precede asynchronous creation; a failed worker cannot block job publication.
// Final accounting uses the same plan, so it cannot revert a renamed active path
// to its source basename when the attempt finishes.
func (c *Controller) withLogLinkPaths(job cluster.JobExecutionRecord) cluster.JobExecutionRecord {
	cfg := c.LogLinkNaming
	if cfg == nil || !cfg.RenameEnabled || !job.HeimdallManaged {
		return job
	}
	target := logLinkTarget(job)
	if len(target.Paths) == 0 {
		return job
	}
	paths := make([]string, 0, len(target.Paths))
	mapping := make(map[string]any)
	maxPathLength := 4096 // Job Manager's per-path UTF-8 byte contract.
	if cfg.MaxPathLength > 0 {
		maxPathLength = min(maxPathLength, cfg.MaxPathLength)
	}
	for _, source := range target.Paths {
		name, renamed, err := loglinks.Name(target, source, true)
		link := filepath.Join(cfg.Destination, name)
		// Invalid or out-of-scope paths keep their scheduler meaning. Planning
		// does not grant access: the worker independently checks filesystem
		// ancestors and configured roots before creating any link.
		if err != nil || !renamed || !logLinkSourceAllowed(cfg, source) ||
			len(source) > maxPathLength || len(link) > maxPathLength {
			paths = append(paths, source)
			continue
		}
		paths = append(paths, link)
		mapping[link] = source
	}
	slices.Sort(paths)
	job.SchedulerMetadata = maps.Clone(job.SchedulerMetadata)
	job.SchedulerMetadata["slurm_log_file_paths"] = slices.Compact(paths)
	// Send an explicit empty map when all paths keep their own name. Backend
	// metadata merges must not retain mappings from an earlier publication.
	job.SchedulerMetadata["app_log_link_targets"] = mapping
	return job
}

func logLinkSourceAllowed(cfg *loglinks.Config, source string) bool {
	if !filepath.IsAbs(cfg.Destination) || filepath.Clean(cfg.Destination) != cfg.Destination {
		return false
	}
	if source == cfg.Destination || strings.HasPrefix(source, cfg.Destination+string(filepath.Separator)) {
		return false
	}
	for _, root := range cfg.SourceRoots {
		relative, err := filepath.Rel(root, source)
		if err == nil && relative != "." && relative != ".." &&
			!strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative) {
			return true
		}
	}
	return false
}
