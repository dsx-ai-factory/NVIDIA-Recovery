// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestRedactedSqueueCorpusParsesRealOutputShapes(t *testing.T) {
	t.Parallel()

	fixtures := fixtureCorpus(t, "testdata/redacted/squeue/*.json")
	var sawArrayJob, sawRecoveryConfig, sawNodeList bool

	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(filepath.Base(fixture), func(t *testing.T) {
			data := readFixture(t, fixture)
			assertRedactedFixturePayload(t, fixture, data)
			assertJSONNumberAtPath(t, fixture, data, "jobs", "0", "job_id")

			jobs, err := observedJobsFromSqueue(data)
			if err != nil {
				t.Fatalf("observedJobsFromSqueue(%s) failed: %v", fixture, err)
			}
			if len(jobs) == 0 {
				t.Fatalf("observedJobsFromSqueue(%s) returned no jobs", fixture)
			}

			for _, job := range jobs {
				if strings.TrimSpace(job.RawState) == "" {
					t.Fatalf("job %#v has empty raw state", job)
				}
				if containsSensitiveFixtureToken(job.Cluster, job.Partition, job.User, job.Command, job.Comment) {
					t.Fatalf("job %#v contains unredacted identifying token", job)
				}
				if job.ArrayJobID != "" && job.ArrayTaskID != "" {
					sawArrayJob = true
				}
				if job.RecoveryConfig != nil {
					sawRecoveryConfig = true
				}
				if len(job.AllocatedNodes) > 1 || len(job.NodeConstraints.Exclusions) > 1 {
					sawNodeList = true
				}
			}
		})
	}

	if !sawArrayJob {
		t.Fatal("redacted squeue corpus did not include an array job")
	}
	if !sawRecoveryConfig {
		t.Fatal("redacted squeue corpus did not include APS/HEIMDALL recovery config")
	}
	if !sawNodeList {
		t.Fatal("redacted squeue corpus did not include expanded node-list shape")
	}
}

func TestRedactedSacctCorpusParsesRealOutputShapes(t *testing.T) {
	t.Parallel()

	fixtures := fixtureCorpus(t, "testdata/redacted/sacct/*.json")
	var sawCompleted, sawFailed, sawArrayJob, sawFailedNode bool

	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(filepath.Base(fixture), func(t *testing.T) {
			data := readFixture(t, fixture)
			assertRedactedFixturePayload(t, fixture, data)
			assertJSONNumberAtPath(t, fixture, data, "jobs", "0", "job_id")

			jobs, err := observedJobsFromSacct(data)
			if err != nil {
				t.Fatalf("observedJobsFromSacct(%s) failed: %v", fixture, err)
			}
			if len(jobs) == 0 {
				t.Fatalf("observedJobsFromSacct(%s) returned no jobs", fixture)
			}

			for _, job := range jobs {
				switch job.RawState {
				case "COMPLETED":
					sawCompleted = true
				case "FAILED", "NODE_FAIL":
					sawFailed = true
				}
				if job.ArrayJobID != "" && job.ArrayTaskID != "" {
					sawArrayJob = true
				}
				if len(job.FailedNodes) > 0 {
					sawFailedNode = true
				}
				if containsSensitiveFixtureToken(job.Partition, job.Comment) {
					t.Fatalf("job %#v contains unredacted identifying token", job)
				}
			}
		})
	}

	if !sawCompleted {
		t.Fatal("redacted sacct corpus did not include a completed job")
	}
	if !sawFailed {
		t.Fatal("redacted sacct corpus did not include a failed job")
	}
	if !sawArrayJob {
		t.Fatal("redacted sacct corpus did not include an array job")
	}
	if !sawFailedNode {
		t.Fatal("redacted sacct corpus did not include a failed node")
	}
}

func TestRedactedPinnedSlurm2505FixturesDeclareStructuredVersion(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		fixture     string
		wantCommand []string
	}{
		{
			name:        "squeue",
			fixture:     "testdata/redacted/squeue/slurm_25_05_log_paths.json",
			wantCommand: []string{"squeue"},
		},
		{
			name:        "sacct all rows",
			fixture:     "testdata/redacted/sacct/slurm_25_05_all_rows_log_paths.json",
			wantCommand: []string{"sacct", "-j", "9401", "--duplicates"},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			data := readFixture(t, test.fixture)
			assertRedactedFixturePayload(t, test.fixture, data)

			var payload struct {
				Meta struct {
					Plugin struct {
						DataParser string `json:"data_parser"`
					} `json:"plugin"`
					Command []string `json:"command"`
					Slurm   struct {
						Version struct {
							Major string `json:"major"`
							Minor string `json:"minor"`
							Micro string `json:"micro"`
						} `json:"version"`
						Release string `json:"release"`
					} `json:"slurm"`
				} `json:"meta"`
			}
			if err := json.Unmarshal(data, &payload); err != nil {
				t.Fatalf("decode %s metadata: %v", test.fixture, err)
			}

			if payload.Meta.Plugin.DataParser != "data_parser/v0.0.43" {
				t.Fatalf(
					"%s data parser = %q, want data_parser/v0.0.43",
					test.fixture,
					payload.Meta.Plugin.DataParser,
				)
			}
			if payload.Meta.Slurm.Release != "25.05.8" {
				t.Fatalf(
					"%s Slurm release = %q, want 25.05.8",
					test.fixture,
					payload.Meta.Slurm.Release,
				)
			}
			if payload.Meta.Slurm.Version.Major != "25" ||
				payload.Meta.Slurm.Version.Minor != "05" ||
				payload.Meta.Slurm.Version.Micro != "8" {
				t.Fatalf(
					"%s Slurm version = %s.%s.%s, want 25.05.8",
					test.fixture,
					payload.Meta.Slurm.Version.Major,
					payload.Meta.Slurm.Version.Minor,
					payload.Meta.Slurm.Version.Micro,
				)
			}
			if !slices.Equal(payload.Meta.Command, test.wantCommand) {
				t.Fatalf(
					"%s command = %#v, want %#v",
					test.fixture,
					payload.Meta.Command,
					test.wantCommand,
				)
			}
		})
	}
}

func TestNoRawSlurmCapturesTracked(t *testing.T) {
	t.Parallel()

	cmd := exec.Command(
		"git", "-C", "../..", "ls-files", "-z", "--cached", "--others",
		"--exclude-standard", "--", "internal/slurm/testdata", "tools/slurm-fixture-redactor",
	)
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files failed: %v", err)
	}

	for _, rawPath := range bytes.Split(output, []byte{0}) {
		if len(rawPath) == 0 {
			continue
		}
		path := filepath.ToSlash(string(rawPath))
		if isRawSlurmCapturePath(path) {
			t.Fatalf("raw Slurm capture path is tracked: %s", path)
		}
	}
}

func fixtureCorpus(t *testing.T, pattern string) []string {
	t.Helper()

	fixtures, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("invalid fixture pattern %q: %v", pattern, err)
	}
	if len(fixtures) == 0 {
		t.Fatalf("no fixtures matched %q", pattern)
	}
	slices.Sort(fixtures)
	return fixtures
}

func readFixture(t *testing.T, path string) []byte {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func assertRedactedFixturePayload(t *testing.T, path string, data []byte) {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("%s is not valid JSON: %v", path, err)
	}
	if containsSensitiveFixtureToken(string(data)) {
		t.Fatalf("%s contains an unredacted identifying token", path)
	}
}

func assertJSONNumberAtPath(t *testing.T, fixture string, data []byte, path ...string) {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var current any
	if err := decoder.Decode(&current); err != nil {
		t.Fatalf("%s is not valid JSON: %v", fixture, err)
	}

	for _, element := range path {
		switch value := current.(type) {
		case map[string]any:
			current = value[element]
		case []any:
			if element != "0" {
				t.Fatalf("test helper only supports array index 0, got %q", element)
			}
			if len(value) == 0 {
				t.Fatalf("%s has empty array at %v", fixture, path)
			}
			current = value[0]
		default:
			t.Fatalf("%s has %T at %v before %q", fixture, current, path, element)
		}
	}
	if _, ok := current.(json.Number); !ok {
		t.Fatalf("%s path %v = %T, want json.Number", fixture, path, current)
	}
}

func containsSensitiveFixtureToken(values ...string) bool {
	forbidden := []string{
		"alice",
		"bob",
		"customer-a",
		"dgx",
		"example-lab",
		"prod-cluster",
		"priority-lab",
		"/home/",
		"/lustre/",
	}
	for _, value := range values {
		lower := strings.ToLower(value)
		for _, token := range forbidden {
			if strings.Contains(lower, token) {
				return true
			}
		}
	}
	return false
}

func isRawSlurmCapturePath(path string) bool {
	rawMarkers := []string{
		"/raw/",
		"/capture/",
		"/captures/",
		".raw.json",
		"raw-squeue",
		"raw-sacct",
		"squeue-raw",
		"sacct-raw",
	}
	for _, marker := range rawMarkers {
		if strings.Contains(path, marker) {
			return true
		}
	}
	return false
}
