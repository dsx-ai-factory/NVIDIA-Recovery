// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package loglinks

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

type scanOwner struct {
	target   Target
	patterns []*regexp.Regexp
}

type scanDirectory struct {
	path   string
	owners []scanOwner
}

type scanOwnerKey struct {
	jobID        string
	arrayJobID   string
	attemptIndex int32
}

func (m *Materializer) scan(
	ctx context.Context,
	now time.Time,
	primaries []candidate,
	state *syncState,
) {
	started := time.Now()
	state.observation.Scans = saturatingIncrement(state.observation.Scans)
	defer func() {
		state.observation.ScanDuration += time.Since(started)
	}()

	scanCtx, cancel := context.WithTimeout(ctx, m.config.ScanTimeout)
	defer cancel()

	directories := m.scanDirectories(scanCtx, primaries, state)
	if scanCtx.Err() != nil {
		state.reportContextError()
		return
	}
	for _, directory := range directories {
		if scanCtx.Err() != nil {
			state.reportContextError()
			return
		}
		if state.observation.ScanMatches >= m.config.MaxMatchesPerScan {
			return
		}
		m.scanOneDirectory(scanCtx, now, directory, state)
	}
}

func (m *Materializer) scanDirectories(
	ctx context.Context,
	primaries []candidate,
	state *syncState,
) []scanDirectory {
	indexByPath := make(map[string]int)
	directories := make([]scanDirectory, 0, min(len(primaries), m.config.MaxScanDirectories))
	ownerKeys := make(map[string]map[scanOwnerKey]struct{})
	patternCache := make(map[scanOwnerKey][]*regexp.Regexp)

	for _, primary := range primaries {
		if ctx.Err() != nil {
			state.reportContextError()
			return nil
		}
		path := filepath.Dir(primary.target)
		index, exists := indexByPath[path]
		if !exists {
			if len(directories) >= m.config.MaxScanDirectories {
				state.observation.Rejected = saturatingIncrement(state.observation.Rejected)
				continue
			}
			index = len(directories)
			indexByPath[path] = index
			directories = append(directories, scanDirectory{path: path})
			ownerKeys[path] = make(map[scanOwnerKey]struct{})
		}
		key := scanOwnerKey{
			jobID:        primary.jobID,
			arrayJobID:   primary.arrayJobID,
			attemptIndex: primary.attemptIndex,
		}
		if _, duplicate := ownerKeys[path][key]; duplicate {
			continue
		}
		patterns, cached := patternCache[key]
		if !cached {
			var err error
			patterns, err = m.patternsForTarget(ctx, primary.jobID, primary.arrayJobID)
			if err != nil {
				if ctx.Err() != nil {
					state.reportContextError()
					return nil
				}
				state.observation.Errors = saturatingIncrement(state.observation.Errors)
				continue
			}
			patternCache[key] = patterns
		}
		ownerKeys[path][key] = struct{}{}
		directories[index].owners = append(directories[index].owners, scanOwner{
			target: Target{
				JobID:        primary.jobID,
				AttemptIndex: primary.attemptIndex,
				ArrayJobID:   primary.arrayJobID,
			},
			patterns: patterns,
		})
	}
	return directories
}

func (m *Materializer) patternsForTarget(
	ctx context.Context,
	jobID string,
	arrayJobID string,
) ([]*regexp.Regexp, error) {
	compiled := make([]*regexp.Regexp, 0, len(m.patterns)*2)
	for _, pattern := range m.patterns {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !pattern.placeholder {
			compiled = append(compiled, pattern.compiled)
			continue
		}
		identifiers := []string{jobID}
		if arrayJobID != "" && arrayJobID != jobID {
			identifiers = append(identifiers, arrayJobID)
		}
		for _, identifier := range identifiers {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			expanded := strings.ReplaceAll(pattern.raw, "{job_id}", regexp.QuoteMeta(identifier))
			expression, err := regexp.Compile(expanded)
			if err != nil {
				return nil, errors.Wrap(err, "compile expanded application pattern")
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			compiled = append(compiled, expression)
		}
	}
	return compiled, nil
}

func (m *Materializer) scanOneDirectory(
	ctx context.Context,
	now time.Time,
	directory scanDirectory,
	state *syncState,
) {
	if err := validateExistingDirectoryBoundary(directory.path); err != nil {
		state.observation.Errors = saturatingIncrement(state.observation.Errors)
		return
	}
	handle, err := os.Open(directory.path)
	if err != nil {
		state.observation.Errors = saturatingIncrement(state.observation.Errors)
		return
	}
	defer func() { _ = handle.Close() }()
	state.observation.ScanDirectories = saturatingIncrement(state.observation.ScanDirectories)

	entries, readErr := handle.ReadDir(m.config.MaxEntriesPerDirectory + 1)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		state.observation.Errors = saturatingIncrement(state.observation.Errors)
		return
	}
	if len(entries) > m.config.MaxEntriesPerDirectory {
		entries = entries[:m.config.MaxEntriesPerDirectory]
		state.observation.Rejected = saturatingIncrement(state.observation.Rejected)
	}

	for _, entry := range entries {
		if ctx.Err() != nil {
			state.reportContextError()
			return
		}
		if state.observation.ScanMatches >= m.config.MaxMatchesPerScan {
			return
		}
		state.observation.ScanEntries = saturatingIncrement(state.observation.ScanEntries)
		if isReservedName(entry.Name()) {
			continue
		}

		entryPath := filepath.Join(directory.path, entry.Name())
		info, err := os.Lstat(entryPath)
		if err != nil {
			state.observation.Errors = saturatingIncrement(state.observation.Errors)
			continue
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			continue
		}

		owner, matched, err := matchingOwner(ctx, entry.Name(), directory.owners)
		if err != nil {
			state.reportContextError()
			return
		}
		if !matched {
			continue
		}
		normalized, err := validateTargetPath(m.config, entryPath)
		if err != nil {
			state.observation.Rejected = saturatingIncrement(state.observation.Rejected)
			continue
		}
		state.observation.ScanMatches = saturatingIncrement(state.observation.ScanMatches)
		m.materialize(candidate{
			jobID:        owner.JobID,
			attemptIndex: owner.AttemptIndex,
			arrayJobID:   owner.ArrayJobID,
			target:       normalized,
			origin:       linkOriginApplication,
		}, now, state)
	}
}

func matchingOwner(
	ctx context.Context,
	name string,
	owners []scanOwner,
) (Target, bool, error) {
	for _, owner := range owners {
		if err := ctx.Err(); err != nil {
			return Target{}, false, err
		}
		for _, pattern := range owner.patterns {
			if err := ctx.Err(); err != nil {
				return Target{}, false, err
			}
			matched := pattern.MatchString(name)
			if err := ctx.Err(); err != nil {
				return Target{}, false, err
			}
			if matched {
				return owner.target, true, nil
			}
		}
	}
	return Target{}, false, nil
}
