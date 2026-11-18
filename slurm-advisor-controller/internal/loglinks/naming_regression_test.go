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

// A filename containing a word beginning with date is not an APS identity.
func TestNameRenamesPartialDateToken(t *testing.T) {
	now := time.Now().UTC()
	path := "/logs/train_42_datebook.log"
	name, renamed, err := Name(Target{JobID: "42", JobName: "train", SubmitTime: now}, path, true)
	if err != nil || !renamed || name == filepath.Base(path) {
		t.Fatalf("Name = %q, %v, %v", name, renamed, err)
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
	second := m.Sync(context.Background(), now.Add(time.Second), []Target{target})
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
