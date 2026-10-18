// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The release boundary chooses a query that the cluster can populate, including
// maintenance/vendor builds and future release series.
func TestSlurmLogMetadataMode(t *testing.T) {
	for _, test := range []struct {
		version string
		legacy  bool
		invalid bool
	}{
		{"slurm 23.02.7\n", true, false},
		{"slurm 24.11.7", true, false},
		{"slurm 25.04.0", true, false},
		{"slurm 25.05.0", false, false},
		{"slurm 25.05.0rc1", false, false},
		{"slurm 25.11.7-1+vendor", false, false},
		{"slurm 26.02.0", false, false},
		{"slurm 26.05.0", false, false},
		{"", false, true},
		{"unknown 23.02.7", false, true},
		{"slurm 25.05", false, true},
		{"slurm 25.bad.1", false, true},
		{"slurm 25.05.bad", false, true},
	} {
		t.Run(test.version, func(t *testing.T) {
			version, err := parseSlurmVersion([]byte(test.version))
			if (err != nil) != test.invalid {
				t.Fatalf("parse error = %v; want invalid %v", err, test.invalid)
			}
			if test.invalid {
				return
			}
			legacy := !hostSlurmSupportsExpandedJSON(version)
			if legacy != test.legacy {
				t.Fatalf("legacy mode = %v; want %v", legacy, test.legacy)
			}
		})
	}
}

func TestDetectLogMetadataModeDetectsOnce(t *testing.T) {
	for _, test := range []struct {
		version string
		legacy  bool
	}{
		{"slurm 23.02.7\n", true},
		{"slurm 25.11.7", false},
		{"slurm 26.02.0", false},
	} {
		t.Run(test.version, func(t *testing.T) {
			executor := newScriptedLogExecutor(map[string][]commandResult{
				commandKey([]string{"sacct", "--version"}): {{output: []byte(test.version)}},
			})
			s := newTestLogMetadataCluster(t, executor, nil)
			for range 2 {
				if err := s.DetectLogMetadataMode(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if legacy := s.logMetadata.legacyMode.Load(); legacy != test.legacy {
				t.Fatalf("legacy mode = %v; want %v", legacy, test.legacy)
			}
			assertCommandArgs(t, executor.calls, [][]string{{"sacct", "--version"}})
		})
	}
}

func TestDetectLogMetadataModeFailureKeepsJSONDiscovery(t *testing.T) {
	probeErr := errors.New("version command unavailable")
	for _, test := range []struct {
		name   string
		result commandResult
	}{
		{"command failure", commandResult{err: probeErr}},
		{"unknown version", commandResult{output: []byte("unknown version")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := newScriptedLogExecutor(map[string][]commandResult{
				commandKey([]string{"sacct", "--version"}): {test.result},
				commandKey([]string{"squeue", "--json"}): {{output: []byte(`{"jobs":[
					{"job_id":12345,"job_state":"RUNNING","stdout_expanded":"/logs/parent.out"}
				]}`)}},
				logSacctKey("12345"): {{output: []byte(`{"jobs":[
					{"job_id":12345,"time":{"submission":10,"start":20},"steps":[
						{"step":{"id":"12345.0","stdout_expanded":"/logs/native.out"}}]}
				]}`)}},
			})
			s := newTestLogMetadataCluster(t, executor, nil)
			for range 2 {
				err := s.DetectLogMetadataMode(context.Background())
				if err == nil || (test.result.err != nil && !errors.Is(err, probeErr)) {
					t.Fatalf("initialization error = %v", err)
				}
			}
			jobs, err := s.ActiveJobs(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			s.EnrichActiveJobs(context.Background(), jobs)
			waitForLogMetadataIdle(t, s)
			jobs = s.EnrichActiveJobs(context.Background(), jobs)
			requireLogPaths(t, jobs, []string{"/logs/native.out", "/logs/parent.out"})
			assertCommandArgs(t, executor.calls, [][]string{
				{"sacct", "--version"}, {"squeue", "--json"},
				{"sacct", "-j", "12345", "--duplicates", "--json=v0.0.43"},
			})
		})
	}
}

func TestDetectLogMetadataModeHasDeadline(t *testing.T) {
	s := newSlurmCluster(Options{EnableLogMetadata: true, LogMetadataEnrichmentTimeout: 10 * time.Millisecond})
	s.roCommands = blockingLogExecutor{}
	t.Cleanup(s.Close)
	if err := s.DetectLogMetadataMode(context.Background()); err == nil {
		t.Fatal("version detection unexpectedly completed")
	}
	if s.logMetadata.legacyMode.Load() {
		t.Fatal("failed detection must retain JSON discovery")
	}
}

// A newer client uses only JSON, including when an expected path is missing.
// Incomplete results preserve known paths and retry without a text query.
func TestModernLogMetadataUsesOnlyJSON(t *testing.T) {
	for _, test := range []struct {
		name     string
		metadata string
		missing  bool
	}{
		{"complete JSON", `,"stdout_expanded":"/logs/step.out"`, false},
		{"missing expanded path", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := newScriptedLogExecutor(map[string][]commandResult{
				commandKey([]string{"sacct", "--version"}): {{output: []byte("slurm 25.11.7")}},
				commandKey([]string{"squeue", "--json"}): {{output: []byte(`{"jobs":[
					{"job_id":12345,"job_state":"RUNNING","stdout_expanded":"/logs/parent.out"}
				]}`)}},
				logSacctKey("12345"): {{output: []byte(`{"jobs":[{"job_id":12345,
					"time":{"submission":10,"start":20},"steps":[
					{"step":{"id":"12345.0"` + test.metadata + `}}]}]}`)}},
			})
			s := newTestLogMetadataCluster(t, executor, nil)
			if err := s.DetectLogMetadataMode(context.Background()); err != nil {
				t.Fatal(err)
			}
			jobs, err := s.ActiveJobs(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			s.EnrichActiveJobs(context.Background(), jobs)
			waitForLogMetadataIdle(t, s)
			jobs = s.EnrichActiveJobs(context.Background(), jobs)
			wantPaths := []string{"/logs/parent.out"}
			if !test.missing {
				wantPaths = append(wantPaths, "/logs/step.out")
			}
			requireLogPaths(t, jobs, wantPaths)
			entry := s.logMetadata.attempts[jobAttemptRef(jobs[0])]
			if (entry.steps.failures > 0) != test.missing || entry.parent.failures != 0 {
				t.Fatalf("retry state: parent = %#v, steps = %#v", entry.parent, entry.steps)
			}
			assertCommandArgs(t, executor.calls, [][]string{
				{"sacct", "--version"}, {"squeue", "--json"},
				{"sacct", "-j", "12345", "--duplicates", "--json=v0.0.43"},
			})
		})
	}
}
