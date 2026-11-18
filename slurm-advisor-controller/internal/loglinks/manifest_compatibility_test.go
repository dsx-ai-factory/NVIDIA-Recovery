// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package loglinks

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestManifestUpgradeRetainsOwnershipAndRetiresExtendedFormat proves that an
// upgrade keeps legacy ownership, a mixed manifest survives restart, and retiring
// the generated link produces a file that the original strict schema can read.
func TestManifestUpgradeRetainsOwnershipAndRetiresExtendedFormat(t *testing.T) {
	t.Parallel()
	_, source, destination := testDirectories(t)
	config := testConfig(source, destination)
	config.RenameEnabled = true
	config.Retention = time.Hour
	config.CleanupInterval = time.Minute
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	legacy := filepath.Join(source, "train_42_date_26-11-17_time_12-00-00.log")
	arbitrary := filepath.Join(source, "output.txt")
	writeFile(t, legacy)
	writeFile(t, arbitrary)
	legacyLink := filepath.Join(destination, filepath.Base(legacy))
	if err := os.Symlink(legacy, legacyLink); err != nil {
		t.Fatal(err)
	}
	// This fixture specifies the pre-feature on-disk contract independently of
	// the current manifest structs and puts version after links deliberately.
	fixture := map[string]any{
		"version": 1,
		"links": []map[string]any{{
			"name": filepath.Base(legacy), "target": legacy, "job_id": "42",
			"attempt_index": 0, "origin": "primary", "last_observed_at": now,
		}},
	}
	contents, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(destination, manifestFilename)
	if err := os.WriteFile(manifestPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	materializer, report, err := New(config, nil)
	if err != nil || report != (RecoveryReport{Owned: 1}) {
		t.Fatalf("legacy ownership lost during upgrade: report=%+v, err=%v", report, err)
	}
	observation := materializer.Sync(context.Background(), now, []Target{{
		JobID: "42", JobName: "train", SubmitTime: now, Paths: []string{legacy, arbitrary},
	}})
	assertObservationCounts(t, observation, Observation{Created: 1, Existing: 1, Owned: 2})
	assertManifestFormatVersion(t, manifestPath, 2)

	config.RenameEnabled = false
	restarted, report, err := New(config, nil)
	if err != nil || report != (RecoveryReport{Owned: 2}) {
		t.Fatalf("mixed ownership lost on restart with renaming disabled: report=%+v, err=%v", report, err)
	}
	// Disabling the option alone cannot downgrade the file while both records
	// are still retained, even if that sync rewrites the legacy observation.
	restarted.Sync(context.Background(), now.Add(time.Minute), []Target{{JobID: "42", Paths: []string{legacy}}})
	assertManifestFormatVersion(t, manifestPath, 2)
	observation = restarted.Sync(context.Background(), now.Add(time.Hour), []Target{{JobID: "42", Paths: []string{legacy}}})
	assertObservationCounts(t, observation, Observation{Existing: 1, Removed: 1, Owned: 1})
	assertManifestFormatVersion(t, manifestPath, 1)
	assertSymlink(t, legacyLink, legacy)
	for _, path := range []string{legacy, arbitrary} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("retiring link changed source file: %v", err)
		}
	}

	// The old controller rejects unknown record fields before it can retain
	// any ownership. Its original record shape must accept the whole rewrite.
	var oldSchema struct {
		Version int `json:"version"`
		Links   []struct {
			Name           string    `json:"name"`
			Target         string    `json:"target"`
			JobID          string    `json:"job_id"`
			AttemptIndex   int32     `json:"attempt_index"`
			Origin         string    `json:"origin,omitempty"`
			LastObservedAt time.Time `json:"last_observed_at"`
		} `json:"links"`
	}
	contents, err = os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&oldSchema); err != nil {
		t.Fatalf("legacy reader rejects rewritten manifest: %v", err)
	}
	if len(oldSchema.Links) != 1 || oldSchema.Links[0].Target != legacy {
		t.Fatalf("legacy reader recovered wrong ownership: %+v", oldSchema)
	}
}

func assertManifestFormatVersion(t *testing.T, path string, want int) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(contents, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Version != want {
		t.Fatalf("on-disk format version = %d, want %d", envelope.Version, want)
	}
}
