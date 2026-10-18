// Package loglinks materializes validated Slurm log paths into a dedicated,
// flat directory of symbolic links.
// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package loglinks

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
)

const (
	manifestFilename = ".heimdall-log-links.json"
	internalPrefix   = ".heimdall-log-links-"
)

// Config controls filesystem access and bounds all work performed by a
// Materializer.
type Config struct {
	Destination            string
	SourceRoots            []string
	ApplicationPatterns    []string
	ScanInterval           time.Duration
	CleanupInterval        time.Duration
	Retention              time.Duration
	ScanTimeout            time.Duration
	MaxPathLength          int
	MaxPathsPerSync        int
	MaxPatterns            int
	MaxScanDirectories     int
	MaxEntriesPerDirectory int
	MaxMatchesPerScan      int
	MaxOwnedLinks          int
}

// Target associates exact-attempt Slurm log paths with their owning job.
type Target struct {
	JobID        string
	AttemptIndex int32
	ArrayJobID   string
	Paths        []string
}

// Observation summarizes one bounded synchronization cycle. It intentionally
// contains no filesystem paths or job identifiers.
type Observation struct {
	Created         int
	Existing        int
	Collisions      int
	Rejected        int
	Errors          int
	Removed         int
	Owned           int
	Scans           int
	ScanDirectories int
	ScanEntries     int
	ScanMatches     int
	CleanupFailures int
	ScanDuration    time.Duration
}

// RecoveryReport describes conservative manifest recovery at startup.
type RecoveryReport struct {
	Owned          int
	InvalidEntries int
	ManifestError  bool
}

// Materializer owns links that it both created and durably recorded.
type Materializer struct {
	mu sync.Mutex

	config   Config
	logger   *zap.Logger
	patterns []patternTemplate
	owned    map[string]manifestRecord

	lastScan             time.Time
	lastCleanup          time.Time
	manifestNeedsRewrite bool
}

type candidate struct {
	jobID        string
	attemptIndex int32
	arrayJobID   string
	target       string
	origin       linkOrigin
}

type jobAttemptKey struct {
	jobID        string
	attemptIndex int32
}

type syncState struct {
	observation     Observation
	owned           map[string]manifestRecord
	dirty           bool
	contextReported bool
}

// New validates all configured filesystem boundaries, probes destination
// capabilities, and conservatively recovers the ownership manifest.
func New(config Config, logger *zap.Logger) (*Materializer, RecoveryReport, error) {
	normalized, patterns, err := normalizeConfig(config)
	if err != nil {
		return nil, RecoveryReport{}, err
	}
	if err := validateManifestPath(normalized); err != nil {
		return nil, RecoveryReport{}, errors.Wrap(err, "validate log link manifest path")
	}
	if err := probeDestination(normalized.Destination); err != nil {
		return nil, RecoveryReport{}, errors.Wrap(err, "probe log link destination")
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	owned, report := recoverManifest(normalized)
	materializer := &Materializer{
		config:               normalized,
		logger:               logger,
		patterns:             patterns,
		owned:                owned,
		manifestNeedsRewrite: report.ManifestError || report.InvalidEntries > 0,
	}
	return materializer, report, nil
}

// Sync refreshes active ownership, optionally scans adjacent directories, and
// removes only expired links whose identity still matches the manifest.
func (m *Materializer) Sync(ctx context.Context, now time.Time, targets []Target) Observation {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	now = now.UTC()
	original := cloneRecords(m.owned)
	state := syncState{
		owned: cloneRecords(m.owned),
		dirty: m.manifestNeedsRewrite,
	}

	m.refreshActiveOwned(ctx, now, targets, &state)
	primaries := m.collectPrimaryCandidates(ctx, targets, &state)
	for _, primary := range primaries {
		if ctx.Err() != nil {
			state.reportContextError()
			break
		}
		m.materialize(primary, now, &state)
	}

	if len(m.patterns) > 0 && cadenceDue(now, m.lastScan, m.config.ScanInterval) {
		m.lastScan = now
		m.scan(ctx, now, primaries, &state)
	}
	if ctx.Err() == nil && cadenceDue(now, m.lastCleanup, m.config.CleanupInterval) {
		m.lastCleanup = now
		m.cleanup(ctx, now, &state)
	}

	if state.dirty {
		renamed, err := writeManifest(m.config, state.owned)
		if err != nil {
			state.observation.Errors = saturatingIncrement(state.observation.Errors)
			if renamed {
				m.owned = state.owned
				m.manifestNeedsRewrite = false
			} else {
				m.owned = original
				m.manifestNeedsRewrite = true
			}
		} else {
			m.owned = state.owned
			m.manifestNeedsRewrite = false
		}
	}

	state.observation.Owned = len(m.owned)
	m.logObservation(state.observation)
	return state.observation
}

func (m *Materializer) refreshActiveOwned(
	ctx context.Context,
	now time.Time,
	targets []Target,
	state *syncState,
) {
	if len(state.owned) == 0 {
		return
	}

	activeAttempts := make(map[jobAttemptKey]bool, len(state.owned))
	for _, record := range state.owned {
		if record.Origin != linkOriginApplication {
			continue
		}
		activeAttempts[jobAttemptKey{
			jobID:        record.JobID,
			attemptIndex: record.AttemptIndex,
		}] = false
	}
	if len(activeAttempts) == 0 {
		return
	}

	activeCount := 0
	for _, target := range targets {
		if ctx.Err() != nil {
			state.reportContextError()
			return
		}
		key := jobAttemptKey{
			jobID:        target.JobID,
			attemptIndex: target.AttemptIndex,
		}
		active, ownedAttempt := activeAttempts[key]
		if !ownedAttempt || active {
			continue
		}
		activeAttempts[key] = true
		activeCount++
		if activeCount == len(activeAttempts) {
			break
		}
	}

	for name, record := range state.owned {
		if ctx.Err() != nil {
			state.reportContextError()
			return
		}
		if record.Origin != linkOriginApplication {
			continue
		}
		if !activeAttempts[jobAttemptKey{
			jobID:        record.JobID,
			attemptIndex: record.AttemptIndex,
		}] {
			continue
		}
		if record.LastObservedAt.Equal(now) {
			continue
		}
		record.LastObservedAt = now
		state.owned[name] = record
		state.dirty = true
	}
}

func (m *Materializer) collectPrimaryCandidates(
	ctx context.Context,
	targets []Target,
	state *syncState,
) []candidate {
	primaries := make([]candidate, 0, min(m.config.MaxPathsPerSync, len(targets)))
	seen := make(map[string]struct{}, min(m.config.MaxPathsPerSync, len(targets)))
	processed := 0

	for targetIndex, target := range targets {
		if ctx.Err() != nil {
			state.reportContextError()
			break
		}
		if processed >= m.config.MaxPathsPerSync {
			state.observation.Rejected = saturatingAdd(
				state.observation.Rejected,
				remainingPathCount(targets, targetIndex),
			)
			break
		}
		if err := validateTargetIdentity(target, m.config.MaxPathLength); err != nil {
			rejected := min(len(target.Paths), m.config.MaxPathsPerSync-processed)
			state.observation.Rejected = saturatingAdd(state.observation.Rejected, rejected)
			processed += rejected
			if rejected < len(target.Paths) {
				state.observation.Rejected = saturatingAdd(
					state.observation.Rejected,
					remainingPathCount(targets, targetIndex)-rejected,
				)
				break
			}
			continue
		}

		for pathIndex, path := range target.Paths {
			if processed >= m.config.MaxPathsPerSync {
				state.observation.Rejected = saturatingAdd(
					state.observation.Rejected,
					len(target.Paths)-pathIndex+remainingPathCount(targets, targetIndex+1),
				)
				return primaries
			}
			processed++

			normalized, err := validateTargetPath(m.config, path)
			if err != nil {
				state.observation.Rejected = saturatingIncrement(state.observation.Rejected)
				continue
			}
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			primaries = append(primaries, candidate{
				jobID:        target.JobID,
				attemptIndex: target.AttemptIndex,
				arrayJobID:   target.ArrayJobID,
				target:       normalized,
				origin:       linkOriginPrimary,
			})
		}
	}
	return primaries
}

func (m *Materializer) materialize(value candidate, now time.Time, state *syncState) {
	name := filepath.Base(value.target)
	destination := filepath.Join(m.config.Destination, name)

	info, err := os.Lstat(destination)
	if err == nil {
		m.handleExisting(destination, info, value, now, state)
		return
	}
	if !errors.Is(err, fs.ErrNotExist) {
		state.observation.Errors = saturatingIncrement(state.observation.Errors)
		return
	}

	if _, alreadyOwned := state.owned[name]; !alreadyOwned && len(state.owned) >= m.config.MaxOwnedLinks {
		state.observation.Rejected = saturatingIncrement(state.observation.Rejected)
		return
	}
	if err := os.Symlink(value.target, destination); err != nil {
		if errors.Is(err, fs.ErrExist) {
			if racedInfo, lstatErr := os.Lstat(destination); lstatErr == nil {
				m.handleExisting(destination, racedInfo, value, now, state)
				return
			}
		}
		state.observation.Errors = saturatingIncrement(state.observation.Errors)
		return
	}

	state.observation.Created = saturatingIncrement(state.observation.Created)
	state.owned[name] = newManifestRecord(value, now)
	state.dirty = true
}

func (m *Materializer) handleExisting(
	destination string,
	info fs.FileInfo,
	value candidate,
	now time.Time,
	state *syncState,
) {
	name := filepath.Base(destination)
	if info.Mode()&os.ModeSymlink == 0 {
		state.observation.Collisions = saturatingIncrement(state.observation.Collisions)
		if _, ok := state.owned[name]; ok {
			delete(state.owned, name)
			state.dirty = true
		}
		return
	}

	rawTarget, err := os.Readlink(destination)
	if err != nil {
		state.observation.Errors = saturatingIncrement(state.observation.Errors)
		return
	}
	if normalizeLinkTarget(m.config.Destination, rawTarget) != value.target {
		state.observation.Collisions = saturatingIncrement(state.observation.Collisions)
		if record, ok := state.owned[name]; ok && rawTarget != record.Target {
			delete(state.owned, name)
			state.dirty = true
		}
		return
	}

	state.observation.Existing = saturatingIncrement(state.observation.Existing)
	record, owned := state.owned[name]
	if !owned {
		return
	}
	if record.Target != value.target || rawTarget != record.Target {
		delete(state.owned, name)
		state.dirty = true
		return
	}
	if (record.Origin == "" || record.Origin == linkOriginPrimary) &&
		value.origin == linkOriginApplication {
		return
	}
	refreshed := newManifestRecord(value, now)
	if record != refreshed {
		state.owned[name] = refreshed
		state.dirty = true
	}
}

func (m *Materializer) cleanup(
	ctx context.Context,
	now time.Time,
	state *syncState,
) {
	names := make([]string, 0, len(state.owned))
	for name := range state.owned {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		if ctx.Err() != nil {
			state.reportContextError()
			return
		}
		record := state.owned[name]
		if now.Before(record.LastObservedAt.Add(m.config.Retention)) {
			continue
		}

		path := filepath.Join(m.config.Destination, name)
		info, err := os.Lstat(path)
		if err != nil {
			state.observation.CleanupFailures = saturatingIncrement(state.observation.CleanupFailures)
			if errors.Is(err, fs.ErrNotExist) {
				delete(state.owned, name)
				state.dirty = true
			}
			continue
		}
		if info.Mode()&os.ModeSymlink == 0 {
			state.observation.CleanupFailures = saturatingIncrement(state.observation.CleanupFailures)
			delete(state.owned, name)
			state.dirty = true
			continue
		}
		target, err := os.Readlink(path)
		if err != nil {
			state.observation.CleanupFailures = saturatingIncrement(state.observation.CleanupFailures)
			continue
		}
		if target != record.Target {
			state.observation.CleanupFailures = saturatingIncrement(state.observation.CleanupFailures)
			delete(state.owned, name)
			state.dirty = true
			continue
		}
		if err := os.Remove(path); err != nil {
			state.observation.CleanupFailures = saturatingIncrement(state.observation.CleanupFailures)
			continue
		}
		delete(state.owned, name)
		state.dirty = true
		state.observation.Removed = saturatingIncrement(state.observation.Removed)
	}
}

func (m *Materializer) logObservation(observation Observation) {
	fields := []zap.Field{
		zap.Int("created", observation.Created),
		zap.Int("existing", observation.Existing),
		zap.Int("collisions", observation.Collisions),
		zap.Int("rejected", observation.Rejected),
		zap.Int("errors", observation.Errors),
		zap.Int("removed", observation.Removed),
		zap.Int("owned", observation.Owned),
		zap.Int("scans", observation.Scans),
		zap.Int("scan_directories", observation.ScanDirectories),
		zap.Int("scan_entries", observation.ScanEntries),
		zap.Int("scan_matches", observation.ScanMatches),
		zap.Duration("scan_duration", observation.ScanDuration),
		zap.Int("cleanup_failures", observation.CleanupFailures),
	}
	if observation.Errors > 0 || observation.Collisions > 0 ||
		observation.Rejected > 0 || observation.CleanupFailures > 0 {
		m.logger.Warn("log link materialization completed with issues", fields...)
		return
	}
	m.logger.Debug("log link materialization completed", fields...)
}

func (state *syncState) reportContextError() {
	if state.contextReported {
		return
	}
	state.contextReported = true
	state.observation.Errors = saturatingIncrement(state.observation.Errors)
}

func newManifestRecord(value candidate, now time.Time) manifestRecord {
	return manifestRecord{
		Name:           filepath.Base(value.target),
		Target:         value.target,
		JobID:          value.jobID,
		AttemptIndex:   value.attemptIndex,
		Origin:         value.origin,
		LastObservedAt: now,
	}
}

func normalizeLinkTarget(destination, target string) string {
	if filepath.IsAbs(target) {
		return filepath.Clean(target)
	}
	return filepath.Clean(filepath.Join(destination, target))
}

func cadenceDue(now, previous time.Time, interval time.Duration) bool {
	return previous.IsZero() || !now.Before(previous.Add(interval))
}

func cloneRecords(records map[string]manifestRecord) map[string]manifestRecord {
	cloned := make(map[string]manifestRecord, len(records))
	for name, record := range records {
		cloned[name] = record
	}
	return cloned
}

func validateTargetIdentity(target Target, maxLength int) error {
	if target.JobID == "" {
		return errors.New("job ID is empty")
	}
	if len(target.JobID) > maxLength || strings.IndexByte(target.JobID, 0) >= 0 {
		return errors.New("job ID is invalid")
	}
	if target.AttemptIndex < 0 {
		return errors.New("attempt index is negative")
	}
	if len(target.ArrayJobID) > maxLength || strings.IndexByte(target.ArrayJobID, 0) >= 0 {
		return errors.New("array job ID is invalid")
	}
	return nil
}

func remainingPathCount(targets []Target, start int) int {
	total := 0
	for index := start; index < len(targets); index++ {
		total = saturatingAdd(total, len(targets[index].Paths))
	}
	return total
}

func saturatingIncrement(value int) int {
	return saturatingAdd(value, 1)
}

func saturatingAdd(value, addition int) int {
	maxInt := int(^uint(0) >> 1)
	if addition <= 0 {
		return value
	}
	if value > maxInt-addition {
		return maxInt
	}
	return value + addition
}
