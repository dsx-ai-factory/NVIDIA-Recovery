// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package loglinks

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Only the complete APS convention keeps its basename. A name that merely
// starts with the APS prefix would otherwise reach the collector unchanged, and
// the collector would take a later numeric suffix as the job ID, so renaming
// exists precisely to give such files an identity of their own.
func TestNameRenamesNamesThatOnlyResembleAPSNames(t *testing.T) {
	now := time.Now().UTC()
	for _, base := range []string{
		"train_42_datebook.log",       // word that merely begins with "date"
		"train_42_date_report_99.log", // collector would extract 99, another job's ID
		"train_42_date_26-11-17.log",  // date without the time component
	} {
		path := "/logs/" + base
		name, renamed, err := Name(Target{JobID: "42", JobName: "train", SubmitTime: now}, path, true)
		if err != nil || !renamed || name == base {
			t.Errorf("Name(%s) = %q, renamed=%v, err=%v; want a generated name", base, name, renamed, err)
		}
	}
}

// Real APS names may carry a trailing qualifier such as an NVRx cycle. They must
// keep their basenames, since the collector already identifies them correctly.
func TestNamePreservesAPSNamesWithTrailingQualifier(t *testing.T) {
	now := time.Now().UTC()
	for _, base := range []string{
		"train_42_date_26-11-17_time_01-02-03.log",
		"train_42_date_26-11-17_time_01-02-03_cycle1.log",
	} {
		name, renamed, err := Name(Target{JobID: "42", JobName: "train", SubmitTime: now}, "/logs/"+base, true)
		if err != nil || renamed || name != base {
			t.Errorf("Name(%s) = %q, renamed=%v, err=%v; want the basename kept", base, name, renamed, err)
		}
	}
}

// Losing the primary's submit time must not disable independent app discovery.
func TestNamingFailureKeepsAdjacentLogDiscovery(t *testing.T) {
	_, source, destination := testDirectories(t)
	stdout := filepath.Join(source, "stdout.log")
	app := filepath.Join(source, "application-42.log")
	writeFile(t, stdout)
	writeFile(t, app)
	cfg := testConfig(source, destination)
	cfg.RenameEnabled = true
	cfg.ApplicationPatterns = []string{`^application-{job_id}\.log$`}
	m := mustNew(t, cfg)
	observation := m.Sync(context.Background(), time.Now(), []Target{{JobID: "42", Paths: []string{stdout}}})
	if observation.Created != 1 || observation.Rejected != 1 {
		t.Fatalf("observation = %+v", observation)
	}
	assertSymlink(t, filepath.Join(destination, filepath.Base(app)), app)
	assertNotExist(t, filepath.Join(destination, filepath.Base(stdout)))
}

// Opt-in cannot remove support for an unchanged APS basename within old bounds.
func TestPreservedAPSLinkKeepsLegacyLengthBounds(t *testing.T) {
	root, source, _ := testDirectories(t)
	destination := filepath.Join(root, strings.Repeat("d", 55))
	if err := os.Mkdir(destination, 0o750); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(source, "train_42_date_26-11-17_time_12-00-00.log")
	writeFile(t, path)
	cfg := testConfig(source, destination)
	cfg.RenameEnabled = true
	cfg.MaxPathLength = max(len(path), len(destination))
	m := mustNew(t, cfg)
	observation := m.Sync(context.Background(), time.Now(), []Target{{JobID: "42", JobName: "train", Paths: []string{path}}})
	if observation.Created != 1 {
		t.Fatalf("observation = %+v", observation)
	}
	assertSymlink(t, filepath.Join(destination, filepath.Base(path)), path)
}

// With renaming off, adjacent-log discovery must behave exactly as before the
// option existed: jobs that share a stdout path seed that directory once. Seeding
// it for every job lets one job's match consume the scan's whole match budget on
// every pass, so a job in another directory never gets its application log.
func TestScanSeedsKeepPreFeatureDeduplicationWhenRenamingIsOff(t *testing.T) {
	_, source, destination := testDirectories(t)
	shared, other := filepath.Join(source, "w1"), filepath.Join(source, "w2")
	for _, directory := range []string{shared, other} {
		if err := os.Mkdir(directory, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	sharedStdout, ownStdout := filepath.Join(shared, "out.log"), filepath.Join(other, "out2.log")
	for _, path := range []string{
		sharedStdout, ownStdout, filepath.Join(shared, "app-43.log"), filepath.Join(other, "app-44.log"),
	} {
		writeFile(t, path)
	}
	cfg := testConfig(source, destination)
	cfg.MaxMatchesPerScan = 1
	cfg.ApplicationPatterns = []string{`^app-{job_id}\.log$`}
	m := mustNew(t, cfg)
	m.Sync(context.Background(), time.Now().UTC(), []Target{
		{JobID: "42", Paths: []string{sharedStdout}},
		{JobID: "43", Paths: []string{sharedStdout}},
		{JobID: "44", Paths: []string{ownStdout}},
	})
	assertSymlink(t, filepath.Join(destination, "app-44.log"), filepath.Join(other, "app-44.log"))
}

// A job's generated link outlives the job until retention. A later scan for
// another job must not expose that same file again under its plain basename,
// which would send one file to the collector under two names. A different
// adjacent application file must stay eligible.
func TestScanDoesNotRelinkTheSourceOfAnOwnedGeneratedLink(t *testing.T) {
	_, source, destination := testDirectories(t)
	finished := filepath.Join(source, "stdout-42.log")
	active := filepath.Join(source, "output43")
	adjacent := filepath.Join(source, "extra.log")
	for _, path := range []string{finished, active, adjacent} {
		writeFile(t, path)
	}
	cfg := testConfig(source, destination)
	cfg.RenameEnabled = true
	cfg.ScanInterval = time.Second
	cfg.ApplicationPatterns = []string{`\.log$`}
	m := mustNew(t, cfg)
	now := time.Now().UTC()
	m.Sync(context.Background(), now, []Target{{JobID: "42", SubmitTime: now, Paths: []string{finished}}})
	// Job 42 has ended; its generated link is still within retention.
	m.Sync(context.Background(), now.Add(time.Minute), []Target{{JobID: "43", SubmitTime: now, Paths: []string{active}}})

	assertNotExist(t, filepath.Join(destination, "stdout-42.log"))
	assertSymlink(t, filepath.Join(destination, "extra.log"), adjacent)
}

// A failed atomic replacement must not strand a link created by this process.
func TestGeneratedOwnershipRetriesAfterManifestFailure(t *testing.T) {
	_, source, destination := testDirectories(t)
	cfg := testConfig(source, destination)
	cfg.RenameEnabled = true
	m := mustNew(t, cfg)
	now := time.Now().UTC()
	path := filepath.Join(source, "output")
	writeFile(t, path)
	manifest := filepath.Join(destination, manifestFilename)
	if err := os.Mkdir(manifest, 0o750); err != nil {
		t.Fatal(err)
	}
	target := Target{JobID: "42", SubmitTime: now, Paths: []string{path}}
	first := m.Sync(context.Background(), now, []Target{target})
	if first.Created != 1 || first.Errors != 1 {
		t.Fatalf("first = %+v", first)
	}
	if err := os.Remove(manifest); err != nil {
		t.Fatal(err)
	}
	// Replay nothing: observing the target again would refresh its record and force
	// a write by itself, hiding a retry that is not scheduled when it should be.
	second := m.Sync(context.Background(), now.Add(time.Second), nil)
	if second.Owned != 1 || second.Errors != 0 {
		t.Fatalf("retry = %+v", second)
	}
	_, recovered, err := New(cfg, nil)
	if err != nil || recovered.Owned != 1 {
		t.Fatalf("recovery = %+v, %v", recovered, err)
	}
	retired := m.Sync(context.Background(), now.Add(cfg.Retention+cfg.CleanupInterval), nil)
	if retired.Removed != 1 {
		t.Fatalf("cleanup = %+v", retired)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("source changed: %v", err)
	}
}

// Corrupted envelopes and impossible record combinations must not claim links.
func TestManifestRejectsMislabeledGeneratedAndExtendedBasenameRecords(t *testing.T) {
	for _, test := range []struct {
		name     string
		version  int
		basename bool
	}{
		{"generated under v1", 1, false}, {"basename with submit time", 2, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, source, destination := testDirectories(t)
			cfg := testConfig(source, destination)
			now := time.Now().UTC()
			path := filepath.Join(source, "output")
			writeFile(t, path)
			name, _, err := Name(Target{JobID: "42", SubmitTime: now}, path, true)
			if err != nil {
				t.Fatal(err)
			}
			if test.basename {
				name = filepath.Base(path)
			}
			if err := os.Symlink(path, filepath.Join(destination, name)); err != nil {
				t.Fatal(err)
			}
			// Independent map fixture deliberately serializes links before version.
			data, err := json.Marshal(map[string]any{"version": test.version, "links": []map[string]any{{
				"name": name, "target": path, "job_id": "42", "attempt_index": 0,
				"origin": "primary", "submit_time": now, "last_observed_at": now,
			}}})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(destination, manifestFilename), data, 0o600); err != nil {
				t.Fatal(err)
			}
			_, report, err := New(cfg, nil)
			if err != nil || report.Owned != 0 || (!report.ManifestError && report.InvalidEntries == 0) {
				t.Fatalf("report = %+v, %v", report, err)
			}
		})
	}
}
