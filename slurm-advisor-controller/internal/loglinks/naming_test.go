// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package loglinks

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestNameRetainsLegacyAndAPSNames proves existing deployments keep their
// basenames unless an explicitly enabled rename is necessary.
func TestNameRetainsLegacyAndAPSNames(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		target  Target
		path    string
		enabled bool
	}{
		{name: "disabled", target: Target{JobID: "42"}, path: "/logs/output.log"},
		{name: "APS", target: Target{JobID: "42", JobName: "train"}, path: "/logs/train_42_date_26-11-17_time_01-02-03.log", enabled: true},
		{name: "array APS", target: Target{JobID: "42_7", ArrayJobID: "42", JobName: "train"}, path: "/logs/train_42_date_26-11-17_time_01-02-03.log", enabled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, renamed, err := Name(test.target, test.path, test.enabled)
			if err != nil || renamed || got != filepath.Base(test.path) {
				t.Fatalf("Name = %q, renamed=%t, err=%v", got, renamed, err)
			}
		})
	}
}

// TestNameUsesStableExactAttemptIdentity proves repeated observations and
// timezone representations converge, while independent logs and attempts differ.
func TestNameUsesStableExactAttemptIdentity(t *testing.T) {
	t.Parallel()
	submit := time.Date(2026, 11, 17, 1, 2, 3, 0, time.UTC)
	target := Target{JobID: "42_7", AttemptIndex: 2, SubmitTime: submit}
	first, renamed, err := Name(target, "/logs/My output [1].out", true)
	if err != nil || !renamed {
		t.Fatalf("Name = %q, renamed=%t, err=%v", first, renamed, err)
	}
	if !regexp.MustCompile(`^My-output-1-[0-9a-f]{16}-a2_42_7_date_26-11-17_time_01-02-03\.log$`).MatchString(first) {
		t.Fatalf("generated name %q does not carry collector-compatible exact identity", first)
	}
	target.SubmitTime = submit.In(time.FixedZone("offset", -8*60*60))
	if got, _, err := Name(target, "/logs/My output [1].out", true); err != nil || got != first {
		t.Fatalf("timezone changed generated name: %q, %v", got, err)
	}
	names := map[string]bool{first: true}
	for _, variant := range []struct {
		target Target
		path   string
	}{
		{Target{JobID: "42_7", AttemptIndex: 3, SubmitTime: submit}, "/logs/My output [1].out"},
		{Target{JobID: "43", AttemptIndex: 2, SubmitTime: submit}, "/logs/My output [1].out"},
		{target, "/other/My output [1].out"},
	} {
		got, _, err := Name(variant.target, variant.path, true)
		if err != nil || names[got] {
			t.Fatalf("distinct identity got duplicate name %q, err=%v", got, err)
		}
		names[got] = true
	}
}

func TestNameRejectsUnprovableGeneratedIdentity(t *testing.T) {
	t.Parallel()
	submit := time.Date(2026, 11, 17, 1, 2, 3, 0, time.UTC)
	for _, test := range []struct {
		name   string
		target Target
		path   string
	}{
		{"missing submit", Target{JobID: "42"}, "/logs/output.log"},
		{"negative attempt", Target{JobID: "42", AttemptIndex: -1, SubmitTime: submit}, "/logs/output.log"},
		{"unsafe job", Target{JobID: "../42", SubmitTime: submit}, "/logs/output.log"},
		{"relative source", Target{JobID: "42", SubmitTime: submit}, "logs/output.log"},
		{"unnormalized source", Target{JobID: "42", SubmitTime: submit}, "/logs/../output.log"},
		{"NUL", Target{JobID: "42", SubmitTime: submit}, "/logs/a\x00.log"},
		{"UTF8", Target{JobID: "42", SubmitTime: submit}, "/logs/a\xff.log"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := Name(test.target, test.path, true); err == nil {
				t.Fatal("Name accepted unprovable identity")
			}
		})
	}
}

// TestRenamedLinksSeparateJobsAndRecoverOwnership exercises actual filesystem
// creation, idempotence, manifest recovery, and retention instead of only names.
func TestRenamedLinksSeparateJobsAndRecoverOwnership(t *testing.T) {
	t.Parallel()
	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.RenameEnabled = true
	config.Retention = time.Hour
	config.CleanupInterval = time.Minute
	for _, directory := range []string{"a", "b"} {
		if err := os.Mkdir(filepath.Join(source, directory), 0o755); err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(source, directory, "output.log"))
	}
	now := time.Date(2026, 11, 17, 1, 2, 3, 0, time.UTC)
	targets := []Target{
		{JobID: "42", SubmitTime: now, Paths: []string{filepath.Join(source, "a", "output.log"), filepath.Join(source, "b", "output.log")}},
		{JobID: "43", SubmitTime: now, Paths: []string{filepath.Join(source, "a", "output.log")}},
	}
	materializer := mustNew(t, config)
	assertObservationCounts(t, materializer.Sync(context.Background(), now, targets), Observation{Created: 3, Owned: 3})
	records := readManifest(t, destination)
	if len(records) != 3 {
		t.Fatalf("manifest has %d entries, want three distinct identities", len(records))
	}
	for _, record := range records {
		assertSymlink(t, filepath.Join(destination, record.Name), record.Target)
		if record.SubmitTime != now || !strings.Contains(record.Name, "_"+record.JobID+"_date_") {
			t.Fatalf("record loses generated identity: %+v", record)
		}
	}
	restarted, report, err := New(config, zap.NewNop())
	if err != nil || report != (RecoveryReport{Owned: 3}) {
		t.Fatalf("recover generated links: report=%+v, err=%v", report, err)
	}
	assertObservationCounts(t, restarted.Sync(context.Background(), now, targets), Observation{Existing: 3, Owned: 3})
	// Turning the flag off must not make previously proven ownership disappear.
	config.RenameEnabled = false
	restarted, report, err = New(config, zap.NewNop())
	if err != nil || report != (RecoveryReport{Owned: 3}) {
		t.Fatalf("recover with flag disabled: report=%+v, err=%v", report, err)
	}
	assertObservationCounts(t, restarted.Sync(context.Background(), now.Add(time.Hour), nil), Observation{Removed: 3})
	for _, record := range records {
		assertNotExist(t, filepath.Join(destination, record.Name))
		if _, err := os.Stat(record.Target); err != nil {
			t.Fatalf("cleanup touched source: %v", err)
		}
	}
}

func TestRenamedLinksRequireSubmitTimeWithoutLosingAPSLinks(t *testing.T) {
	t.Parallel()
	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.RenameEnabled = true
	aps := filepath.Join(source, "train_42_date_26-11-17_time_01-02-03.log")
	observation := mustNew(t, config).Sync(context.Background(), time.Now(), []Target{{
		JobID: "42", JobName: "train", Paths: []string{aps, filepath.Join(source, "output.log")},
	}})
	assertObservationCounts(t, observation, Observation{Created: 1, Owned: 1, Rejected: 1})
	assertSymlink(t, filepath.Join(destination, filepath.Base(aps)), aps)
	assertNotExist(t, filepath.Join(destination, "output.log"))
}

func TestRenamedPrimaryIsNotRelinkedByAdjacentScanner(t *testing.T) {
	t.Parallel()
	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.RenameEnabled = true
	config.ApplicationPatterns = []string{`\.log$`}
	primary := filepath.Join(source, "output.log")
	extra := filepath.Join(source, "extra-42.log")
	writeFile(t, primary)
	writeFile(t, extra)
	now := time.Date(2026, 11, 17, 1, 2, 3, 0, time.UTC)
	observation := mustNew(t, config).Sync(context.Background(), now, []Target{{
		JobID: "42", SubmitTime: now, Paths: []string{primary},
	}})
	if observation.Created != 2 || observation.Owned != 2 || observation.ScanMatches != 1 {
		t.Fatalf("scan re-exposed primary or lost extra log: %+v", observation)
	}
	assertNotExist(t, filepath.Join(destination, "output.log"))
	assertSymlink(t, filepath.Join(destination, filepath.Base(extra)), extra)
}

func TestRecoveryRefusesForgedGeneratedName(t *testing.T) {
	t.Parallel()
	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.RenameEnabled = true
	path := filepath.Join(source, "output.log")
	now := time.Date(2026, 11, 17, 1, 2, 3, 0, time.UTC)
	materializer := mustNew(t, config)
	materializer.Sync(context.Background(), now, []Target{{JobID: "42", SubmitTime: now, Paths: []string{path}}})
	records := make(map[string]manifestRecord)
	for _, record := range readManifest(t, destination) {
		// The link is unchanged, but the manifest now claims another attempt.
		// Recovery must not adopt ownership that no longer agrees with the name.
		record.AttemptIndex++
		records[record.Name] = record
	}
	if _, err := writeManifest(config, records); err != nil {
		t.Fatal(err)
	}
	_, report, err := New(config, zap.NewNop())
	if err != nil || report.Owned != 0 || report.InvalidEntries != 1 {
		t.Fatalf("forged name recovered: report=%+v, err=%v", report, err)
	}
	for name := range records {
		assertSymlink(t, filepath.Join(destination, name), path)
	}
}
