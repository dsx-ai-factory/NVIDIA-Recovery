// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

// A current attempt's complete JSON does not inherit missing metadata from an
// earlier attempt, even when both attempts reuse the same numeric step ID.
func TestActiveJSONLogMetadataSelectsCurrentAttempt(t *testing.T) {
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"squeue", "--json"}): {{output: []byte(`{"jobs":[
			{"job_id":12345,"restart_cnt":1,"job_state":"RUNNING","stdout_expanded":"/logs/parent.out"}
		]}`)}},
		logSacctKey("12345"): {{output: []byte(`{"jobs":[
			{"job_id":12345,"time":{"submission":10,"start":20},"steps":[{"step":{"id":"12345.0"}}]},
			{"job_id":12345,"time":{"submission":40,"start":50},"steps":[
				{"step":{"id":"12345.0","stdout_expanded":"/logs/native.out"}}]}
		]}`)}},
	})
	s := newTestLogMetadataCluster(t, executor, nil)
	jobs, err := s.ActiveJobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s.EnrichActiveJobs(context.Background(), jobs)
	waitForLogMetadataIdle(t, s)
	jobs = s.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/native.out", "/logs/parent.out"})
	if jobs[0].AttemptIndex != 1 {
		t.Fatalf("active attempt = %d; want 1", jobs[0].AttemptIndex)
	}
	assertCommandArgs(t, executor.calls, [][]string{
		{"squeue", "--json"}, {"sacct", "-j", "12345", "--duplicates", "--json=v0.0.43"},
	})
}

// Partial JSON keeps cached and newly discovered paths while backing off. A
// complete later response replaces the stale step set without a text query.
func TestActiveJSONLogMetadataRetainsPathsAndRetries(t *testing.T) {
	currentTime := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"squeue", "--json"}): {{output: []byte(`{"jobs":[
			{"job_id":12345,"job_state":"RUNNING","stdout_expanded":"/logs/parent.out"}
		]}`)}},
		logSacctKey("12345"): {
			{output: sacctLogPayload("/logs/cached.out")},
			{output: []byte(`{"jobs":[{"job_id":12345,"time":{"submission":10,"start":20},"steps":[
				{"step":{"id":"12345.0"}},
				{"step":{"id":"12345.1","stdout_expanded":"/logs/new.out"}}
			]}]}`)},
			{output: sacctLogPayload("/logs/recovered.out")},
		},
	})
	s := newTestLogMetadataCluster(t, executor, func() time.Time { return currentTime })
	jobs, err := s.ActiveJobs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		advance  time.Duration
		paths    []string
		queries  int
		failures uint
	}{
		{0, []string{"/logs/cached.out", "/logs/parent.out"}, 1, 0},
		{time.Minute, []string{"/logs/cached.out", "/logs/new.out", "/logs/parent.out"}, 2, 1},
		{5 * time.Second, []string{"/logs/cached.out", "/logs/new.out", "/logs/parent.out"}, 2, 1},
		{5 * time.Second, []string{"/logs/parent.out", "/logs/recovered.out"}, 3, 0},
	} {
		currentTime = currentTime.Add(check.advance)
		s.EnrichActiveJobs(context.Background(), jobs)
		waitForLogMetadataIdle(t, s)
		published := s.EnrichActiveJobs(context.Background(), jobs)
		requireLogPaths(t, published, check.paths)
		entry := s.logMetadata.attempts[jobAttemptRef(published[0])]
		if entry.steps.failures != check.failures || entry.parent.failures != 0 {
			t.Fatalf("retry state: parent = %#v, steps = %#v", entry.parent, entry.steps)
		}
		if got := executor.callCount(logSacctKey("12345")); got != check.queries {
			t.Fatalf("JSON queries = %d; want %d", got, check.queries)
		}
	}
	assertCommandArgs(t, executor.calls, [][]string{
		{"squeue", "--json"},
		{"sacct", "-j", "12345", "--duplicates", "--json=v0.0.43"},
		{"sacct", "-j", "12345", "--duplicates", "--json=v0.0.43"},
		{"sacct", "-j", "12345", "--duplicates", "--json=v0.0.43"},
	})
}

// Known modern and unknown clients both use one JSON metadata query for all
// due attempts. Missing sources retry independently while valid paths publish.
func TestFinalizedJSONLogMetadataUsesOneQuery(t *testing.T) {
	status := []byte(`{"jobs":[
		{"job_id":12345,"state":{"current":"COMPLETED"},"time":{"submission":10,"start":20,"end":30}},
		{"job_id":12345,"state":{"current":"COMPLETED"},"time":{"submission":40,"start":50,"end":60}},
		{"job_id":12345,"state":{"current":"COMPLETED"},"time":{"submission":70,"start":80,"end":90}}
	]}`)
	metadata := []byte(`{"jobs":[
		{"job_id":12345,"time":{"submission":10,"start":20},"stdout_expanded":"/logs/parent-0.out",
		 "steps":[{"step":{"id":"12345.0"}},
		          {"step":{"id":"12345.1","stdout_expanded":"/logs/native-0.out"}}]},
		{"job_id":12345,"time":{"submission":40,"start":50},
		 "steps":[{"step":{"id":"12345.0","stdout_expanded":"/logs/native-1.out"}}]},
		{"job_id":12345,"time":{"submission":70,"start":80},"stdout_expanded":"/logs/parent-2.out",
		 "steps":[{"step":{"id":"12345.0","stdout_expanded":"/logs/native-2.out"}}]}
	]}`)
	refs := []cluster.JobAttemptRef{{JobID: "12345"}, {JobID: "12345", AttemptIndex: 1}, {JobID: "12345", AttemptIndex: 2}}
	wantPaths := [][]string{
		{"/logs/native-0.out", "/logs/parent-0.out"}, {"/logs/native-1.out"},
		{"/logs/native-2.out", "/logs/parent-2.out"},
	}
	for _, test := range []struct {
		version string
		invalid bool
	}{
		{"slurm 25.11.7", false}, {"unknown version", true},
	} {
		t.Run(test.version, func(t *testing.T) {
			executor := newScriptedLogExecutor(map[string][]commandResult{
				commandKey([]string{"sacct", "--version"}):                              {{output: []byte(test.version)}},
				commandKey([]string{"sacct", "-Xj", "12345", "--duplicates", "--json"}): {{output: status}},
				logSacctKey("12345"): {{output: metadata}},
			})
			s := newTestLogMetadataCluster(t, executor, nil)
			s.logMetadata.queryBudget = 1
			if err := s.DetectLogMetadataMode(context.Background()); (err != nil) != test.invalid {
				t.Fatalf("version detection error = %v; want invalid %v", err, test.invalid)
			}
			jobs, err := s.FinalizedJobs(context.Background(), refs)
			if err != nil || len(jobs) != len(refs) {
				t.Fatalf("jobs = %#v, error = %v", jobs, err)
			}
			for _, job := range jobs {
				if job.State != cluster.JobStateCompleted || !slices.Equal(LogFilePaths(job), wantPaths[job.AttemptIndex]) {
					t.Fatalf("attempt %d = %#v", job.AttemptIndex, job)
				}
				entry := s.logMetadata.attempts[jobAttemptRef(job)]
				if (entry.parent.failures > 0) != (job.AttemptIndex == 1) ||
					(entry.steps.failures > 0) != (job.AttemptIndex == 0) {
					t.Fatalf("attempt %d retry state: parent = %#v, steps = %#v", job.AttemptIndex, entry.parent, entry.steps)
				}
			}
			assertCommandArgs(t, executor.calls, [][]string{
				{"sacct", "--version"}, {"sacct", "-Xj", "12345", "--duplicates", "--json"},
				{"sacct", "-j", "12345", "--duplicates", "--json=v0.0.43"},
			})
		})
	}
}

func TestFinalizedJSONLogMetadataRetainsPathsAndRetries(t *testing.T) {
	currentTime := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	statusArgs := []string{"sacct", "-Xj", "12345", "--duplicates", "--json"}
	status := []byte(`{"jobs":[{"job_id":12345,"state":{"current":"COMPLETED"},
		"time":{"submission":10,"start":20,"end":30}}]}`)
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey(statusArgs): {{output: status}, {output: status}, {output: status}, {output: status}},
		logSacctKey("12345"): {
			{output: []byte(`{"jobs":[{"job_id":12345,"time":{"submission":10,"start":20},
				"stdout_expanded":"/logs/parent.out","steps":[{"step":{"id":"12345.0","stdout_expanded":"/logs/cached.out"}}]}]}`)},
			{output: []byte(`{"jobs":[{"job_id":12345,"time":{"submission":10,"start":20},
				"stdout_expanded":"/logs/parent.out","steps":[{"step":{"id":"12345.0"}},
				{"step":{"id":"12345.1","stdout_expanded":"/logs/new.out"}}]}]}`)},
			{output: []byte(`{"jobs":[{"job_id":12345,"time":{"submission":10,"start":20},
				"stdout_expanded":"/logs/parent.out","steps":[{"step":{"id":"12345.0","stdout_expanded":"/logs/recovered.out"}},
				{"step":{"id":"12345.1","stdout_expanded":"/logs/new.out"}}]}]}`)},
		},
	})
	s := newTestLogMetadataCluster(t, executor, func() time.Time { return currentTime })
	s.logMetadata.queryBudget = 1
	refs := []cluster.JobAttemptRef{{JobID: "12345"}}
	for _, check := range []struct {
		advance  time.Duration
		paths    []string
		queries  int
		failures uint
	}{
		{0, []string{"/logs/cached.out", "/logs/parent.out"}, 1, 0},
		{time.Minute, []string{"/logs/cached.out", "/logs/new.out", "/logs/parent.out"}, 2, 1},
		{5 * time.Second, []string{"/logs/cached.out", "/logs/new.out", "/logs/parent.out"}, 2, 1},
		{5 * time.Second, []string{"/logs/new.out", "/logs/parent.out", "/logs/recovered.out"}, 3, 0},
	} {
		currentTime = currentTime.Add(check.advance)
		jobs, err := s.FinalizedJobs(context.Background(), refs)
		if err != nil {
			t.Fatal(err)
		}
		requireLogPaths(t, jobs, check.paths)
		if got := executor.callCount(logSacctKey("12345")); got != check.queries {
			t.Fatalf("JSON queries = %d; want %d", got, check.queries)
		}
		entry := s.logMetadata.attempts[refs[0]]
		if entry.steps.failures != check.failures || entry.parent.failures != 0 {
			t.Fatalf("retry state: parent = %#v, steps = %#v", entry.parent, entry.steps)
		}
	}
	if len(executor.calls) != 7 {
		t.Fatalf("commands = %v; want four status queries and three metadata JSON queries", executor.calls)
	}
}
