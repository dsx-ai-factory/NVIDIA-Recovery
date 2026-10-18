// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package loglinks

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestScannerExpandsConcreteAndArrayParentJobIDs(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	jobDirectory := filepath.Join(source, "job")
	if err := os.Mkdir(jobDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	stdout := filepath.Join(jobDirectory, "stdout.log")
	writeFile(t, stdout)
	concrete := filepath.Join(jobDirectory, "app-123_7.log")
	arrayParent := filepath.Join(jobDirectory, "app-123.log")
	generic := filepath.Join(jobDirectory, "generic.log")
	later := filepath.Join(jobDirectory, "later-123_7.log")
	writeFile(t, concrete)
	writeFile(t, arrayParent)
	writeFile(t, generic)

	nested := filepath.Join(jobDirectory, "nested")
	if err := os.Mkdir(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	nestedLog := filepath.Join(nested, "app-123_7.log")
	writeFile(t, nestedLog)
	symlinkedApplicationLog := filepath.Join(jobDirectory, "app-123_7-link.log")
	if err := os.Symlink(concrete, symlinkedApplicationLog); err != nil {
		t.Fatal(err)
	}

	config := testConfig(source, destination)
	config.ApplicationPatterns = []string{
		`^app-{job_id}\.log$`,
		`^generic\.log$`,
		`^later-{job_id}\.log$`,
	}
	config.ScanInterval = time.Minute
	materializer := mustNew(t, config)
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	target := Target{
		JobID:        "123_7",
		ArrayJobID:   "123",
		AttemptIndex: 2,
		Paths:        []string{stdout},
	}

	observation := materializer.Sync(context.Background(), now, []Target{target})
	assertObservationCounts(t, observation, Observation{
		Created:         4,
		Owned:           4,
		Scans:           1,
		ScanDirectories: 1,
		ScanEntries:     6,
		ScanMatches:     3,
	})
	assertSymlink(t, filepath.Join(destination, filepath.Base(concrete)), concrete)
	assertSymlink(t, filepath.Join(destination, filepath.Base(arrayParent)), arrayParent)
	assertSymlink(t, filepath.Join(destination, filepath.Base(generic)), generic)
	assertNotExist(t, filepath.Join(destination, filepath.Base(symlinkedApplicationLog)))
	if got, err := filepath.Glob(filepath.Join(destination, "nested*")); err != nil || len(got) != 0 {
		t.Fatalf("nested scan result = %v, err=%v", got, err)
	}
	for _, record := range readManifest(t, destination) {
		wantOrigin := linkOriginApplication
		if record.Target == stdout {
			wantOrigin = linkOriginPrimary
		}
		if record.Origin != wantOrigin {
			t.Fatalf("manifest record = %+v, want origin %q", record, wantOrigin)
		}
	}

	writeFile(t, later)
	observation = materializer.Sync(context.Background(), now.Add(30*time.Second), []Target{target})
	assertObservationCounts(t, observation, Observation{Existing: 1, Owned: 4})
	assertNotExist(t, filepath.Join(destination, filepath.Base(later)))

	observation = materializer.Sync(context.Background(), now.Add(time.Minute), []Target{target})
	assertObservationCounts(t, observation, Observation{
		Created:         1,
		Existing:        4,
		Owned:           5,
		Scans:           1,
		ScanDirectories: 1,
		ScanEntries:     7,
		ScanMatches:     4,
	})
	assertSymlink(t, filepath.Join(destination, filepath.Base(later)), later)
}

func TestScannerBoundsDirectoriesEntriesAndMatches(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	firstDirectory := filepath.Join(source, "a-directory")
	secondDirectory := filepath.Join(source, "b-directory")
	for _, directory := range []string{firstDirectory, secondDirectory} {
		if err := os.Mkdir(directory, 0o755); err != nil {
			t.Fatal(err)
		}
		// Every three-entry prefix contains a match regardless of filesystem order.
		writeFile(t, filepath.Join(directory, "a-match.log"))
		writeFile(t, filepath.Join(directory, "b-match.log"))
		writeFile(t, filepath.Join(directory, "c-other.log"))
		writeFile(t, filepath.Join(directory, "z-stdout.log"))
	}

	config := testConfig(source, destination)
	config.ApplicationPatterns = []string{`^[ab]-match\.log$`}
	config.MaxScanDirectories = 1
	config.MaxEntriesPerDirectory = 3
	config.MaxMatchesPerScan = 1
	materializer := mustNew(t, config)
	observation := materializer.Sync(context.Background(), time.Now(), []Target{{
		JobID: "42",
		Paths: []string{
			filepath.Join(firstDirectory, "z-stdout.log"),
			filepath.Join(secondDirectory, "z-stdout.log"),
		},
	}})

	// File.ReadDir returns filesystem order, so the first matching entry may
	// appear anywhere within the bounded prefix.
	scanEntries := observation.ScanEntries
	if scanEntries < 1 || scanEntries > config.MaxEntriesPerDirectory {
		t.Fatalf(
			"scan entries = %d, want between 1 and %d",
			scanEntries,
			config.MaxEntriesPerDirectory,
		)
	}
	assertObservationCounts(t, observation, Observation{
		Created:         2,
		Collisions:      1,
		Rejected:        2,
		Owned:           2,
		Scans:           1,
		ScanDirectories: 1,
		ScanEntries:     scanEntries,
		ScanMatches:     1,
	})
	links, err := filepath.Glob(filepath.Join(destination, "*-match.log"))
	if err != nil || len(links) != 1 {
		t.Fatalf("matched links = %v, err=%v; want exactly one", links, err)
	}
	assertSymlink(t, links[0], filepath.Join(firstDirectory, filepath.Base(links[0])))
}

func TestScannerTimeoutIsBoundedAndDoesNotUndoPrimaryLinks(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	stdout := filepath.Join(source, "stdout.log")
	writeFile(t, stdout)
	config := testConfig(source, destination)
	config.ApplicationPatterns = []string{`.*`}
	config.ScanTimeout = time.Nanosecond
	materializer := mustNew(t, config)

	observation := materializer.Sync(context.Background(), time.Now(), []Target{{
		JobID: "42", Paths: []string{stdout},
	}})
	assertObservationCounts(t, observation, Observation{
		Created: 1,
		Errors:  1,
		Owned:   1,
		Scans:   1,
	})
	assertSymlink(t, filepath.Join(destination, filepath.Base(stdout)), stdout)
}

func TestScanPreparationAndMatchingHonorCancellation(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.ApplicationPatterns = []string{`^app-{job_id}\.log$`}
	materializer := mustNew(t, config)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	state := syncState{owned: make(map[string]manifestRecord)}
	directories := materializer.scanDirectories(cancelled, []candidate{{
		jobID:        "42",
		attemptIndex: 1,
		target:       filepath.Join(source, "stdout.log"),
	}}, &state)
	if directories != nil {
		t.Fatalf("scanDirectories() = %+v, want no work after cancellation", directories)
	}
	if state.observation.Errors != 1 {
		t.Fatalf("scan preparation errors = %d, want 1", state.observation.Errors)
	}

	if _, err := materializer.patternsForTarget(cancelled, "42", "40"); err != context.Canceled {
		t.Fatalf("patternsForTarget() error = %v, want context.Canceled", err)
	}
	_, matched, err := matchingOwner(cancelled, "application.log", []scanOwner{{
		target: Target{JobID: "42", AttemptIndex: 1},
		patterns: []*regexp.Regexp{
			regexp.MustCompile(`does-not-match`),
			regexp.MustCompile(`application`),
		},
	}})
	if err != context.Canceled || matched {
		t.Fatalf("matchingOwner() = matched %t, error %v; want canceled", matched, err)
	}
}

func TestMaxOwnedLinksRefusesAdditionalLinks(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	first := filepath.Join(source, "first.log")
	second := filepath.Join(source, "second.log")
	writeFile(t, first)
	writeFile(t, second)
	config := testConfig(source, destination)
	config.MaxOwnedLinks = 1
	materializer := mustNew(t, config)

	observation := materializer.Sync(context.Background(), time.Now(), []Target{{
		JobID: "42", Paths: []string{first, second},
	}})
	assertObservationCounts(t, observation, Observation{
		Created:  1,
		Rejected: 1,
		Owned:    1,
	})
	assertSymlink(t, filepath.Join(destination, filepath.Base(first)), first)
	assertNotExist(t, filepath.Join(destination, filepath.Base(second)))
}

func TestRecoveryStreamsEntriesPastOwnedLimit(t *testing.T) {
	t.Parallel()

	const entryCount = 150_000
	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.MaxOwnedLinks = 2
	contents := `{"version":1,"links":[` +
		strings.Repeat("null,", entryCount-1) +
		`null]}`
	if int64(len(contents)) >= manifestSizeLimit(config) {
		t.Fatalf("test manifest size = %d, limit = %d", len(contents), manifestSizeLimit(config))
	}
	if err := os.WriteFile(
		filepath.Join(destination, manifestFilename),
		[]byte(contents),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	materializer, report, err := New(config, zap.NewNop())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if materializer == nil || report != (RecoveryReport{InvalidEntries: entryCount}) {
		t.Fatalf("materializer=%v report=%+v", materializer, report)
	}
}

func TestRecoveryRejectsNonStrictManifestJSON(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"unknown top-level field": `{"version":1,"links":[],"unknown":true}`,
		"duplicate field":         `{"version":1,"version":1,"links":[]}`,
		"missing links":           `{"version":1}`,
		"unknown link field":      `{"version":1,"links":[{"unknown":true}]}`,
		"trailing JSON":           `{"version":1,"links":[]} {}`,
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			_, source, destination := testDirectories(t)
			if err := os.WriteFile(
				filepath.Join(destination, manifestFilename),
				[]byte(contents),
				0o600,
			); err != nil {
				t.Fatal(err)
			}
			materializer, report, err := New(testConfig(source, destination), zap.NewNop())
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if materializer == nil || report != (RecoveryReport{ManifestError: true}) {
				t.Fatalf("materializer=%v report=%+v", materializer, report)
			}
		})
	}
}

func TestRecoveryDiscardsCorruptManifestWithoutAdoptingLinks(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	materializer := mustNew(t, config)
	target := filepath.Join(source, "owned-before-corruption.log")
	writeFile(t, target)
	materializer.Sync(context.Background(), time.Now(), []Target{{
		JobID: "42", Paths: []string{target},
	}})
	if err := os.WriteFile(
		filepath.Join(destination, manifestFilename),
		[]byte("{not-json"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	restarted, report, err := New(config, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if report != (RecoveryReport{ManifestError: true}) {
		t.Fatalf("recovery report = %+v, want manifest error", report)
	}
	observation := restarted.Sync(context.Background(), time.Now(), []Target{{
		JobID: "42", Paths: []string{target},
	}})
	assertObservationCounts(t, observation, Observation{Existing: 1, Owned: 0})
	assertSymlink(t, filepath.Join(destination, filepath.Base(target)), target)
	if records := readManifest(t, destination); len(records) != 0 {
		t.Fatalf("rewritten manifest records = %+v, want empty", records)
	}
}

func TestRecoveryRejectsInvalidManifestOrigin(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	target := filepath.Join(source, "invalid-origin.log")
	writeFile(t, target)
	if err := os.Symlink(target, filepath.Join(destination, filepath.Base(target))); err != nil {
		t.Fatal(err)
	}
	manifest := manifestFile{
		Version: manifestVersion,
		Links: []manifestRecord{{
			Name:           filepath.Base(target),
			Target:         target,
			JobID:          "42",
			Origin:         linkOrigin("invalid"),
			LastObservedAt: time.Now().UTC(),
		}},
	}
	contents, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination, manifestFilename), contents, 0o600); err != nil {
		t.Fatal(err)
	}

	materializer, report, err := New(config, zap.NewNop())
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if materializer == nil || report != (RecoveryReport{InvalidEntries: 1}) {
		t.Fatalf("materializer=%v report=%+v", materializer, report)
	}
}

func TestLegacyOriginlessPrimaryIsNotReclassifiedByApplicationScan(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.ApplicationPatterns = []string{`^application-42\.log$`}
	config.Retention = time.Minute
	config.CleanupInterval = time.Second

	stdout := filepath.Join(source, "stdout.log")
	legacyPrimary := filepath.Join(source, "application-42.log")
	writeFile(t, stdout)
	writeFile(t, legacyPrimary)
	if err := os.Symlink(
		legacyPrimary,
		filepath.Join(destination, filepath.Base(legacyPrimary)),
	); err != nil {
		t.Fatal(err)
	}

	lastObserved := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	manifest := manifestFile{
		Version: manifestVersion,
		Links: []manifestRecord{{
			Name:           filepath.Base(legacyPrimary),
			Target:         legacyPrimary,
			JobID:          "42",
			AttemptIndex:   3,
			LastObservedAt: lastObserved,
		}},
	}
	contents, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(destination, manifestFilename),
		contents,
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	materializer, report, err := New(config, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if report != (RecoveryReport{Owned: 1}) {
		t.Fatalf("recovery report = %+v, want one legacy owned link", report)
	}

	observation := materializer.Sync(
		context.Background(),
		lastObserved.Add(config.Retention),
		[]Target{{
			JobID:        "42",
			AttemptIndex: 3,
			Paths:        []string{stdout},
		}},
	)
	assertObservationCounts(t, observation, Observation{
		Created:         1,
		Existing:        1,
		Removed:         1,
		Owned:           1,
		Scans:           1,
		ScanDirectories: 1,
		ScanEntries:     2,
		ScanMatches:     1,
	})
	assertNotExist(t, filepath.Join(destination, filepath.Base(legacyPrimary)))
	assertSymlink(t, filepath.Join(destination, filepath.Base(stdout)), stdout)
}

func TestRecoveryKeepsOnlyEntriesWithProvenIdentity(t *testing.T) {
	t.Parallel()

	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	validTarget := filepath.Join(source, "valid.log")
	replacedTarget := filepath.Join(source, "replaced.log")
	otherTarget := filepath.Join(source, "other.log")
	for _, path := range []string{validTarget, replacedTarget, otherTarget} {
		writeFile(t, path)
	}
	if err := os.Symlink(validTarget, filepath.Join(destination, filepath.Base(validTarget))); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(otherTarget, filepath.Join(destination, filepath.Base(replacedTarget))); err != nil {
		t.Fatal(err)
	}
	observed := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	manifest := manifestFile{
		Version: manifestVersion,
		Links: []manifestRecord{
			{
				Name:           filepath.Base(validTarget),
				Target:         validTarget,
				JobID:          "42",
				AttemptIndex:   0,
				LastObservedAt: observed,
			},
			{
				Name:           filepath.Base(replacedTarget),
				Target:         replacedTarget,
				JobID:          "43",
				AttemptIndex:   1,
				LastObservedAt: observed,
			},
		},
	}
	contents, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination, manifestFilename), contents, 0o600); err != nil {
		t.Fatal(err)
	}

	materializer, report, err := New(config, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if report != (RecoveryReport{Owned: 1, InvalidEntries: 1}) {
		t.Fatalf("recovery report = %+v", report)
	}
	configuredNow := observed.Add(48 * time.Hour)
	observation := materializer.Sync(context.Background(), configuredNow, nil)
	assertObservationCounts(t, observation, Observation{Removed: 1, Owned: 0})
	assertNotExist(t, filepath.Join(destination, filepath.Base(validTarget)))
	assertSymlink(t, filepath.Join(destination, filepath.Base(replacedTarget)), otherTarget)
}

func TestNewRejectsNonRegularManifestPath(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*testing.T, string, string){
		"directory": func(t *testing.T, _ string, manifestPath string) {
			t.Helper()
			if err := os.Mkdir(manifestPath, 0o755); err != nil {
				t.Fatal(err)
			}
		},
		"symlink": func(t *testing.T, source string, manifestPath string) {
			t.Helper()
			target := filepath.Join(source, "manifest-target")
			writeFile(t, target)
			if err := os.Symlink(target, manifestPath); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, setup := range tests {
		t.Run(name, func(t *testing.T) {
			_, source, destination := testDirectories(t)
			setup(t, source, filepath.Join(destination, manifestFilename))
			materializer, report, err := New(testConfig(source, destination), zap.NewNop())
			if err == nil {
				t.Fatal("New() error = nil, want nonregular manifest startup error")
			}
			if materializer != nil || report != (RecoveryReport{}) {
				t.Fatalf("materializer=%v report=%+v on startup error", materializer, report)
			}
		})
	}
}
