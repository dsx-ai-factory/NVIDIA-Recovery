// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package loglinks

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewValidatesConfigurationAndFilesystemBoundaries(t *testing.T) {
	t.Parallel()

	base, source, destination := testDirectories(t)
	valid := testConfig(source, destination)
	if _, report, err := New(valid, zap.NewNop()); err != nil {
		t.Fatalf("New(valid) error = %v", err)
	} else if report != (RecoveryReport{}) {
		t.Fatalf("New(valid) report = %+v, want zero", report)
	}

	sourceLink := filepath.Join(base, "source-link")
	if err := os.Symlink(source, sourceLink); err != nil {
		t.Fatal(err)
	}
	destinationLink := filepath.Join(base, "destination-link")
	if err := os.Symlink(destination, destinationLink); err != nil {
		t.Fatal(err)
	}
	nestedSource := filepath.Join(destination, "nested-source")
	if err := os.Mkdir(nestedSource, 0o755); err != nil {
		t.Fatal(err)
	}
	nestedDestination := filepath.Join(source, "nested-destination")
	if err := os.Mkdir(nestedDestination, 0o755); err != nil {
		t.Fatal(err)
	}

	tests := map[string]func(Config) Config{
		"relative destination": func(config Config) Config {
			config.Destination = "relative"
			return config
		},
		"unnormalized destination": func(config Config) Config {
			config.Destination = destination + string(filepath.Separator) + ".." +
				string(filepath.Separator) + filepath.Base(destination)
			return config
		},
		"missing destination": func(config Config) Config {
			config.Destination = filepath.Join(base, "missing")
			return config
		},
		"symlink destination": func(config Config) Config {
			config.Destination = destinationLink
			return config
		},
		"no source roots": func(config Config) Config {
			config.SourceRoots = nil
			return config
		},
		"unnormalized source root": func(config Config) Config {
			config.SourceRoots = []string{
				source + string(filepath.Separator) + ".." +
					string(filepath.Separator) + filepath.Base(source),
			}
			return config
		},
		"symlink source root": func(config Config) Config {
			config.SourceRoots = []string{sourceLink}
			return config
		},
		"source beneath destination": func(config Config) Config {
			config.SourceRoots = []string{nestedSource}
			return config
		},
		"destination beneath source": func(config Config) Config {
			config.Destination = nestedDestination
			return config
		},
		"invalid regular expression": func(config Config) Config {
			config.ApplicationPatterns = []string{"["}
			return config
		},
		"too many patterns": func(config Config) Config {
			config.MaxPatterns = 1
			config.ApplicationPatterns = []string{"a", "b"}
			return config
		},
		"zero scan interval": func(config Config) Config {
			config.ScanInterval = 0
			return config
		},
		"zero cleanup interval": func(config Config) Config {
			config.CleanupInterval = 0
			return config
		},
		"zero retention": func(config Config) Config {
			config.Retention = 0
			return config
		},
		"zero timeout": func(config Config) Config {
			config.ScanTimeout = 0
			return config
		},
		"zero path limit": func(config Config) Config {
			config.MaxPathsPerSync = 0
			return config
		},
		"zero owned limit": func(config Config) Config {
			config.MaxOwnedLinks = 0
			return config
		},
		"unsupported count": func(config Config) Config {
			config.MaxEntriesPerDirectory = maxConfiguredCount + 1
			return config
		},
		"unsupported path length": func(config Config) Config {
			config.MaxPathLength = maxConfiguredPathLength + 1
			return config
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := New(mutate(valid), zap.NewNop()); err == nil {
				t.Fatal("New() error = nil, want configuration error")
			}
		})
	}
}

func TestNewProbesDestinationWriteAndSymlinkCapabilities(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	if err := os.Chmod(destination, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chmod(destination, 0o755); err != nil {
			t.Errorf("restore destination permissions: %v", err)
		}
	}()

	_, _, err := New(config, zap.NewNop())
	if err == nil {
		t.Skip("filesystem user can write a mode-0555 directory; permission probe is not testable")
	}
}

func TestSyncCreatesRefreshesRecoversAndExpiresOwnedLinks(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.Retention = time.Hour
	config.CleanupInterval = time.Minute
	materializer := mustNew(t, config)
	existingTarget := filepath.Join(source, "slurm-42.out")
	writeFile(t, existingTarget)
	missingTarget := filepath.Join(source, "slurm-42-step-1.out")
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	target := Target{
		JobID:        "42",
		AttemptIndex: 3,
		Paths:        []string{existingTarget, missingTarget},
	}

	observation := materializer.Sync(context.Background(), now, []Target{target})
	assertObservationCounts(t, observation, Observation{Created: 2, Owned: 2})
	assertSymlink(t, filepath.Join(destination, filepath.Base(existingTarget)), existingTarget)
	assertSymlink(t, filepath.Join(destination, filepath.Base(missingTarget)), missingTarget)
	assertManifestMode(t, destination, 0o600)

	records := readManifest(t, destination)
	if len(records) != 2 {
		t.Fatalf("manifest records = %d, want 2", len(records))
	}
	for _, record := range records {
		if record.JobID != "42" || record.AttemptIndex != 3 ||
			record.Origin != linkOriginPrimary || !record.LastObservedAt.Equal(now) {
			t.Fatalf("manifest record = %+v, want job 42 attempt 3 at %s", record, now)
		}
	}

	refreshTime := now.Add(30 * time.Minute)
	target.AttemptIndex = 4
	observation = materializer.Sync(context.Background(), refreshTime, []Target{target})
	assertObservationCounts(t, observation, Observation{Existing: 2, Owned: 2})
	for _, record := range readManifest(t, destination) {
		if record.AttemptIndex != 4 || !record.LastObservedAt.Equal(refreshTime) {
			t.Fatalf("refreshed manifest record = %+v", record)
		}
	}

	restarted, report, err := New(config, zap.NewNop())
	if err != nil {
		t.Fatalf("New(restart) error = %v", err)
	}
	if report != (RecoveryReport{Owned: 2}) {
		t.Fatalf("recovery report = %+v, want 2 owned", report)
	}

	observation = restarted.Sync(
		context.Background(),
		refreshTime.Add(59*time.Minute),
		nil,
	)
	assertObservationCounts(t, observation, Observation{Owned: 2})
	observation = restarted.Sync(
		context.Background(),
		refreshTime.Add(time.Hour),
		nil,
	)
	assertObservationCounts(t, observation, Observation{Removed: 2, Owned: 0})
	assertNotExist(t, filepath.Join(destination, filepath.Base(existingTarget)))
	assertNotExist(t, filepath.Join(destination, filepath.Base(missingTarget)))
}

func TestSyncRefusesCollisionsAndNeverAdoptsExistingLinks(t *testing.T) {
	t.Parallel()

	base, source, destination := testDirectories(t)
	otherSource := filepath.Join(base, "other-source")
	if err := os.Mkdir(otherSource, 0o755); err != nil {
		t.Fatal(err)
	}
	config := testConfig(source, destination)
	config.SourceRoots = append(config.SourceRoots, otherSource)
	config.Retention = time.Minute
	config.CleanupInterval = time.Second
	materializer := mustNew(t, config)

	unmanagedTarget := filepath.Join(source, "unmanaged.log")
	writeFile(t, unmanagedTarget)
	unmanagedLink := filepath.Join(destination, filepath.Base(unmanagedTarget))
	if err := os.Symlink(unmanagedTarget, unmanagedLink); err != nil {
		t.Fatal(err)
	}

	wrongTarget := filepath.Join(source, "wrong.log")
	writeFile(t, wrongTarget)
	differentTarget := filepath.Join(source, "different.log")
	writeFile(t, differentTarget)
	wrongLink := filepath.Join(destination, filepath.Base(wrongTarget))
	if err := os.Symlink(differentTarget, wrongLink); err != nil {
		t.Fatal(err)
	}

	regularTarget := filepath.Join(source, "regular.log")
	writeFile(t, regularTarget)
	regularLink := filepath.Join(destination, filepath.Base(regularTarget))
	writeFile(t, regularLink)

	firstFlat := filepath.Join(source, "shared.log")
	secondFlat := filepath.Join(otherSource, "shared.log")
	writeFile(t, firstFlat)
	writeFile(t, secondFlat)
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	observation := materializer.Sync(context.Background(), now, []Target{{
		JobID:        "42",
		AttemptIndex: 0,
		Paths: []string{
			unmanagedTarget,
			wrongTarget,
			regularTarget,
			firstFlat,
			secondFlat,
		},
	}})
	assertObservationCounts(t, observation, Observation{
		Created:    1,
		Existing:   1,
		Collisions: 3,
		Owned:      1,
	})
	assertSymlink(t, unmanagedLink, unmanagedTarget)
	assertSymlink(t, wrongLink, differentTarget)
	if contents, err := os.ReadFile(regularLink); err != nil || string(contents) != "log\n" {
		t.Fatalf("regular collision was changed: contents=%q err=%v", contents, err)
	}
	assertSymlink(t, filepath.Join(destination, "shared.log"), firstFlat)

	observation = materializer.Sync(context.Background(), now.Add(time.Minute), nil)
	assertObservationCounts(t, observation, Observation{Removed: 1, Owned: 0})
	assertSymlink(t, unmanagedLink, unmanagedTarget)
	assertSymlink(t, wrongLink, differentTarget)
	if _, err := os.Stat(regularLink); err != nil {
		t.Fatalf("regular collision removed: %v", err)
	}

	restarted, report, err := New(config, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if report.Owned != 0 {
		t.Fatalf("recovered unmanaged links as owned: %+v", report)
	}
	observation = restarted.Sync(context.Background(), now.Add(2*time.Minute), []Target{{
		JobID: "42", Paths: []string{unmanagedTarget},
	}})
	assertObservationCounts(t, observation, Observation{Existing: 1, Owned: 0})
}

func TestCleanupDoesNotRemoveReplacedOwnedEntry(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.Retention = time.Minute
	config.CleanupInterval = time.Second
	materializer := mustNew(t, config)
	target := filepath.Join(source, "replaced.log")
	writeFile(t, target)
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	observation := materializer.Sync(context.Background(), now, []Target{{
		JobID: "42", Paths: []string{target},
	}})
	assertObservationCounts(t, observation, Observation{Created: 1, Owned: 1})

	link := filepath.Join(destination, filepath.Base(target))
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	writeFile(t, link)
	observation = materializer.Sync(context.Background(), now.Add(time.Minute), nil)
	assertObservationCounts(t, observation, Observation{Owned: 0, CleanupFailures: 1})
	if info, err := os.Lstat(link); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("replacement changed: info=%v err=%v", info, err)
	}
}

func TestSyncDropsOwnershipWithoutAdoptingAReplacementSymlink(t *testing.T) {
	t.Parallel()

	base, source, destination := testDirectories(t)
	secondRoot := filepath.Join(base, "second-source")
	if err := os.Mkdir(secondRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	config := testConfig(source, destination)
	config.SourceRoots = append(config.SourceRoots, secondRoot)
	materializer := mustNew(t, config)
	original := filepath.Join(source, "replaceable.log")
	replacement := filepath.Join(secondRoot, filepath.Base(original))
	writeFile(t, original)
	writeFile(t, replacement)
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)

	observation := materializer.Sync(context.Background(), now, []Target{{
		JobID: "42", Paths: []string{original},
	}})
	assertObservationCounts(t, observation, Observation{Created: 1, Owned: 1})

	link := filepath.Join(destination, filepath.Base(original))
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(replacement, link); err != nil {
		t.Fatal(err)
	}
	observation = materializer.Sync(context.Background(), now.Add(time.Minute), []Target{{
		JobID: "43", Paths: []string{replacement},
	}})
	assertObservationCounts(t, observation, Observation{Existing: 1, Owned: 0})
	assertSymlink(t, link, replacement)
}

func TestActiveRefreshPrecedesExpiredCleanup(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.Retention = time.Minute
	config.CleanupInterval = time.Second
	materializer := mustNew(t, config)
	target := filepath.Join(source, "active.log")
	writeFile(t, target)
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	job := Target{JobID: "42", Paths: []string{target}}
	materializer.Sync(context.Background(), now, []Target{job})

	observation := materializer.Sync(
		context.Background(),
		now.Add(24*time.Hour),
		[]Target{job},
	)
	assertObservationCounts(t, observation, Observation{Existing: 1, Owned: 1})
	assertSymlink(t, filepath.Join(destination, filepath.Base(target)), target)
}

func TestPrimaryRetentionTracksExactObservedPath(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		nextPaths       func(string) []string
		wantCreated     int
		wantReplacement bool
	}{
		"changed": {
			nextPaths: func(source string) []string {
				replacement := filepath.Join(source, "replacement.log")
				writeFile(t, replacement)
				return []string{replacement}
			},
			wantCreated:     1,
			wantReplacement: true,
		},
		"disappeared": {
			nextPaths: func(string) []string {
				return nil
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, source, destination := testDirectories(t)
			config := testConfig(source, destination)
			config.Retention = time.Minute
			config.CleanupInterval = time.Second
			materializer := mustNew(t, config)
			original := filepath.Join(source, "original.log")
			writeFile(t, original)
			now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
			activeAttempt := Target{
				JobID:        "42",
				AttemptIndex: 3,
				Paths:        []string{original},
			}

			observation := materializer.Sync(context.Background(), now, []Target{activeAttempt})
			assertObservationCounts(t, observation, Observation{Created: 1, Owned: 1})
			if err := os.Remove(original); err != nil {
				t.Fatal(err)
			}

			activeAttempt.Paths = test.nextPaths(source)
			observation = materializer.Sync(
				context.Background(),
				now.Add(config.Retention),
				[]Target{activeAttempt},
			)
			assertObservationCounts(t, observation, Observation{
				Created: test.wantCreated,
				Removed: 1,
				Owned:   test.wantCreated,
			})
			assertNotExist(t, filepath.Join(destination, filepath.Base(original)))
			if test.wantReplacement {
				replacement := activeAttempt.Paths[0]
				assertSymlink(t, filepath.Join(destination, filepath.Base(replacement)), replacement)
				records := readManifest(t, destination)
				if len(records) != 1 || records[0].Target != replacement ||
					records[0].Origin != linkOriginPrimary {
					t.Fatalf("manifest records = %+v, want replacement primary", records)
				}
			}
		})
	}
}

func TestUnprocessedPrimaryPathDoesNotRefreshByAttempt(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.MaxPathsPerSync = 1
	config.Retention = time.Minute
	config.CleanupInterval = time.Second
	materializer := mustNew(t, config)
	first := filepath.Join(source, "first-active.log")
	second := filepath.Join(source, "second-active.log")
	writeFile(t, first)
	writeFile(t, second)
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	firstJob := Target{JobID: "41", AttemptIndex: 1, Paths: []string{first}}
	secondJob := Target{JobID: "42", AttemptIndex: 2, Paths: []string{second}}

	materializer.Sync(context.Background(), now, []Target{firstJob})
	materializer.Sync(context.Background(), now, []Target{secondJob})
	observation := materializer.Sync(
		context.Background(),
		now.Add(2*time.Minute),
		[]Target{firstJob, secondJob},
	)
	assertObservationCounts(t, observation, Observation{
		Existing: 1,
		Rejected: 1,
		Removed:  1,
		Owned:    1,
	})
	assertSymlink(t, filepath.Join(destination, filepath.Base(first)), first)
	assertNotExist(t, filepath.Join(destination, filepath.Base(second)))
}

func TestActiveRefreshKeepsApplicationLinksBetweenScansAndPathCaps(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.ApplicationPatterns = []string{`^application-42\.log$`}
	config.ScanInterval = time.Hour
	config.Retention = time.Minute
	config.CleanupInterval = time.Second
	config.MaxPathsPerSync = 1
	materializer := mustNew(t, config)
	stdout := filepath.Join(source, "stdout.log")
	application := filepath.Join(source, "application-42.log")
	writeFile(t, stdout)
	writeFile(t, application)
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	job := Target{JobID: "42", AttemptIndex: 3, Paths: []string{stdout}}

	observation := materializer.Sync(context.Background(), now, []Target{job})
	assertObservationCounts(t, observation, Observation{
		Created:         2,
		Owned:           2,
		Scans:           1,
		ScanDirectories: 1,
		ScanEntries:     2,
		ScanMatches:     1,
	})

	first := filepath.Join(source, "first.log")
	writeFile(t, first)
	firstJob := Target{JobID: "41", AttemptIndex: 1, Paths: []string{first}}
	observation = materializer.Sync(context.Background(), now, []Target{firstJob})
	assertObservationCounts(t, observation, Observation{Created: 1, Owned: 3})

	observation = materializer.Sync(
		context.Background(),
		now.Add(2*time.Minute),
		[]Target{firstJob, job},
	)
	assertObservationCounts(t, observation, Observation{
		Existing: 1,
		Rejected: 1,
		Removed:  1,
		Owned:    2,
	})
	assertSymlink(t, filepath.Join(destination, filepath.Base(first)), first)
	assertNotExist(t, filepath.Join(destination, filepath.Base(stdout)))
	assertSymlink(t, filepath.Join(destination, filepath.Base(application)), application)
	records := readManifest(t, destination)
	for _, record := range records {
		if record.Target == application {
			if record.Origin != linkOriginApplication ||
				!record.LastObservedAt.Equal(now.Add(2*time.Minute)) {
				t.Fatalf("application manifest record = %+v", record)
			}
			return
		}
	}
	t.Fatalf("application manifest record missing: %+v", records)
}

func TestSyncValidatesAndBoundsSchedulerPaths(t *testing.T) {
	t.Parallel()

	base, source, destination := testDirectories(t)
	outside := filepath.Join(base, "outside.log")
	writeFile(t, outside)
	valid := filepath.Join(source, "valid.log")
	writeFile(t, valid)
	unnormalizedInput := source + string(filepath.Separator) + "subdir" +
		string(filepath.Separator) + ".." + string(filepath.Separator) + filepath.Base(valid)
	directoryTarget := filepath.Join(source, "directory")
	if err := os.Mkdir(directoryTarget, 0o755); err != nil {
		t.Fatal(err)
	}
	symlinkedDirectory := filepath.Join(source, "symlinked-directory")
	if err := os.Symlink(source, symlinkedDirectory); err != nil {
		t.Fatal(err)
	}
	finalSymlink := filepath.Join(source, "final-symlink.log")
	if err := os.Symlink(valid, finalSymlink); err != nil {
		t.Fatal(err)
	}
	underDestination := filepath.Join(destination, "source.log")

	config := testConfig(source, destination)
	config.MaxPathsPerSync = 9
	materializer := mustNew(t, config)
	observation := materializer.Sync(context.Background(), time.Now(), []Target{
		{
			JobID: "42",
			Paths: []string{
				valid,
				unnormalizedInput,
				"relative.log",
				outside,
				underDestination,
				directoryTarget,
				filepath.Join(symlinkedDirectory, "escaped.log"),
				finalSymlink,
				source + string([]byte{0}) + "bad.log",
				filepath.Join(source, "over-limit.log"),
			},
		},
	})
	assertObservationCounts(t, observation, Observation{Created: 1, Rejected: 9, Owned: 1})
	assertSymlink(t, filepath.Join(destination, filepath.Base(valid)), valid)

	invalidIdentity := materializer.Sync(context.Background(), time.Now(), []Target{
		{Paths: []string{valid}},
		{JobID: "43", AttemptIndex: -1, Paths: []string{valid}},
	})
	assertObservationCounts(t, invalidIdentity, Observation{Rejected: 2, Owned: 1})
}

func TestSyncIsFailureIsolatedAndContextBounded(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	materializer := mustNew(t, config)
	valid := filepath.Join(source, "valid.log")
	writeFile(t, valid)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	observation := materializer.Sync(cancelled, time.Now(), []Target{{
		JobID: "42", Paths: []string{valid},
	}})
	assertObservationCounts(t, observation, Observation{Errors: 1})
	assertNotExist(t, filepath.Join(destination, filepath.Base(valid)))

	observation = materializer.Sync(context.Background(), time.Now(), []Target{{
		JobID: "42", Paths: []string{"invalid", valid},
	}})
	assertObservationCounts(t, observation, Observation{Created: 1, Rejected: 1, Owned: 1})
}

func TestSyncReportsDestinationPermissionFailureWhereEnforced(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	materializer := mustNew(t, testConfig(source, destination))
	target := filepath.Join(source, "permission.log")
	writeFile(t, target)
	if err := os.Chmod(destination, 0o555); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chmod(destination, 0o755); err != nil {
			t.Errorf("restore destination permissions: %v", err)
		}
	}()

	observation := materializer.Sync(context.Background(), time.Now(), []Target{{
		JobID: "42", Paths: []string{target},
	}})
	if observation.Created > 0 {
		t.Skip("filesystem user can write a mode-0555 directory; permission failure is not testable")
	}
	assertObservationCounts(t, observation, Observation{Errors: 1})
	assertNotExist(t, filepath.Join(destination, filepath.Base(target)))
}

func TestManifestFailureLeavesCreatedLinkUnmanaged(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	materializer := mustNew(t, config)
	manifestPath := filepath.Join(destination, manifestFilename)
	if err := os.Mkdir(manifestPath, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(source, "created-before-manifest-failure.log")
	writeFile(t, target)

	observation := materializer.Sync(context.Background(), time.Now(), []Target{{
		JobID: "42", Paths: []string{target},
	}})
	assertObservationCounts(t, observation, Observation{Created: 1, Errors: 1, Owned: 0})
	assertSymlink(t, filepath.Join(destination, filepath.Base(target)), target)

	if err := os.Remove(manifestPath); err != nil {
		t.Fatal(err)
	}
	restarted, report, err := New(config, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if report.Owned != 0 {
		t.Fatalf("recovery report = %+v, want unmanaged link", report)
	}
	observation = restarted.Sync(context.Background(), time.Now(), []Target{{
		JobID: "42", Paths: []string{target},
	}})
	assertObservationCounts(t, observation, Observation{Existing: 1, Owned: 0})
}

func TestSyncSerializesConcurrentCycles(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	materializer := mustNew(t, testConfig(source, destination))
	target := filepath.Join(source, "concurrent.log")
	writeFile(t, target)
	now := time.Now()

	var wait sync.WaitGroup
	observations := make(chan Observation, 8)
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			observations <- materializer.Sync(context.Background(), now, []Target{{
				JobID: "42", Paths: []string{target},
			}})
		}()
	}
	wait.Wait()
	close(observations)

	created := 0
	existing := 0
	for observation := range observations {
		created += observation.Created
		existing += observation.Existing
	}
	if created != 1 || existing != 7 {
		t.Fatalf("concurrent observations: created=%d existing=%d, want 1 and 7", created, existing)
	}
	restarted, report, err := New(testConfig(source, destination), zap.NewNop())
	if err != nil || restarted == nil || report.Owned != 1 {
		t.Fatalf("restart: materializer=%v report=%+v err=%v", restarted, report, err)
	}
}

func TestSyncLoggingIsAggregatedAndPathFree(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	var output bytes.Buffer
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&output),
		zap.DebugLevel,
	)
	materializer := mustNewWithLogger(t, testConfig(source, destination), zap.New(core))
	jobID := "sensitive-job-identifier"
	target := filepath.Join(source, "sensitive-customer-path.log")
	writeFile(t, target)

	materializer.Sync(context.Background(), time.Now(), []Target{{
		JobID: jobID, Paths: []string{target},
	}})
	logged := output.String()
	if strings.Contains(logged, source) || strings.Contains(logged, target) ||
		strings.Contains(logged, jobID) {
		t.Fatalf("aggregate log contains path or job identity: %s", logged)
	}
	if !strings.Contains(logged, `"created":1`) || !strings.Contains(logged, `"owned":1`) {
		t.Fatalf("aggregate log omits observation counts: %s", logged)
	}
}

func testDirectories(t *testing.T) (string, string, string) {
	t.Helper()
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatalf("canonicalize temporary directory: %v", err)
	}
	source := filepath.Join(base, "source")
	destination := filepath.Join(base, "destination")
	if err := os.Mkdir(source, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(destination, 0o755); err != nil {
		t.Fatal(err)
	}
	return base, source, destination
}

func testConfig(source, destination string) Config {
	return Config{
		Destination:            destination,
		SourceRoots:            []string{source},
		ScanInterval:           time.Minute,
		CleanupInterval:        time.Hour,
		Retention:              24 * time.Hour,
		ScanTimeout:            time.Second,
		MaxPathLength:          4096,
		MaxPathsPerSync:        100,
		MaxPatterns:            10,
		MaxScanDirectories:     10,
		MaxEntriesPerDirectory: 100,
		MaxMatchesPerScan:      100,
		MaxOwnedLinks:          100,
	}
}

func mustNew(t *testing.T, config Config) *Materializer {
	t.Helper()
	return mustNewWithLogger(t, config, zap.NewNop())
}

func mustNewWithLogger(t *testing.T, config Config, logger *zap.Logger) *Materializer {
	t.Helper()
	materializer, report, err := New(config, logger)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if report != (RecoveryReport{}) {
		t.Fatalf("New() report = %+v, want zero", report)
	}
	return materializer
}

func writeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("log\n"), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func assertSymlink(t *testing.T, path, target string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("Lstat(%s): %v", path, err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("%s mode = %v, want symlink", path, info.Mode())
	}
	got, err := os.Readlink(path)
	if err != nil {
		t.Fatalf("Readlink(%s): %v", path, err)
	}
	if got != target {
		t.Fatalf("Readlink(%s) = %q, want %q", path, got, target)
	}
}

func assertNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("Lstat(%s) error = %v, want not exist", path, err)
	}
}

func assertManifestMode(t *testing.T, destination string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(filepath.Join(destination, manifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("manifest mode = %o, want %o", got, want)
	}
}

func readManifest(t *testing.T, destination string) []manifestRecord {
	t.Helper()
	contents, err := os.ReadFile(filepath.Join(destination, manifestFilename))
	if err != nil {
		t.Fatal(err)
	}
	var manifest manifestFile
	if err := json.Unmarshal(contents, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != manifestVersion {
		t.Fatalf("manifest version = %d, want %d", manifest.Version, manifestVersion)
	}
	return manifest.Links
}

func assertObservationCounts(t *testing.T, got, want Observation) {
	t.Helper()
	scanDuration := got.ScanDuration
	got.ScanDuration = 0
	want.ScanDuration = 0
	if got != want {
		t.Fatalf("observation = %+v, want %+v (scan duration %s)", got, want, scanDuration)
	}
}
