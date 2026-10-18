// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"errors"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

func TestSlurmLogMetadataContract(t *testing.T) {
	if slurmLogFilePathsMetadataKey != "slurm_log_file_paths" {
		t.Fatalf("metadata key = %q, want %q", slurmLogFilePathsMetadataKey, "slurm_log_file_paths")
	}
	if maxSlurmLogFilePaths != 256 {
		t.Fatalf("max path count = %d, want 256", maxSlurmLogFilePaths)
	}
	if maxSlurmLogFilePathBytes != 4096 {
		t.Fatalf("max path bytes = %d, want 4096", maxSlurmLogFilePathBytes)
	}

	tooMany := make([]string, maxSlurmLogFilePaths+1)
	for index := range tooMany {
		tooMany[index] = "/logs/duplicate.out"
	}
	tests := []struct {
		name      string
		paths     []string
		want      []string
		wantError string
	}{
		{name: "empty input"},
		{
			name:  "opaque values are deduplicated and sorted",
			paths: []string{" relative.out ", "/logs/a.out", "/logs/a.out"},
			want:  []string{" relative.out ", "/logs/a.out"},
		},
		{name: "empty path", paths: []string{""}, wantError: "Slurm log file path is empty"},
		{
			name:      "invalid UTF-8",
			paths:     []string{string([]byte{0xff})},
			wantError: "Slurm log file path is not valid UTF-8",
		},
		{
			name:      "NUL",
			paths:     []string{"/logs/a.out\x00suffix"},
			wantError: "Slurm log file path contains NUL",
		},
		{
			name:      "too long",
			paths:     []string{strings.Repeat("x", maxSlurmLogFilePathBytes+1)},
			wantError: "Slurm log file path is too long",
		},
		{
			name:      "too many",
			paths:     tooMany,
			wantError: "too many Slurm log file paths",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, gotErr := normalizeSlurmLogFilePaths(test.paths)
			if test.wantError == "" {
				if gotErr != nil {
					t.Fatalf("normalizeSlurmLogFilePaths() error = %v", gotErr)
				}
			} else if gotErr == nil || gotErr.Error() != test.wantError {
				t.Fatalf("normalizeSlurmLogFilePaths() error = %v, want %q", gotErr, test.wantError)
			}
			if !slices.Equal(got, test.want) {
				t.Fatalf("normalizeSlurmLogFilePaths() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestLogFilePathsReturnsSafeCopy(t *testing.T) {
	metadataPaths := []string{"/work/123/stdout.log", "/work/123/0.log"}
	job := cluster.JobExecutionRecord{
		SchedulerMetadata: map[string]any{
			slurmLogFilePathsMetadataKey: metadataPaths,
		},
	}

	got := LogFilePaths(job)
	if !slices.Equal(got, metadataPaths) {
		t.Fatalf("LogFilePaths() = %#v, want %#v", got, metadataPaths)
	}
	got[0] = "/mutated"
	if metadataPaths[0] != "/work/123/stdout.log" {
		t.Fatalf("metadata paths mutated through accessor: %#v", metadataPaths)
	}
}

func TestLogFilePathsRejectsNonSlurmMetadataType(t *testing.T) {
	job := cluster.JobExecutionRecord{
		SchedulerMetadata: map[string]any{
			slurmLogFilePathsMetadataKey: []any{"/work/123/stdout.log"},
		},
	}

	if got := LogFilePaths(job); got != nil {
		t.Fatalf("LogFilePaths() = %#v, want nil", got)
	}
}

func TestLogMetadataEnrichmentTimeoutConfiguration(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		enricher := newLogMetadataEnricher(Options{})
		if enricher.enrichmentTimeout != defaultLogMetadataEnrichmentTimeout {
			t.Fatalf(
				"enrichment timeout = %s, want %s",
				enricher.enrichmentTimeout,
				defaultLogMetadataEnrichmentTimeout,
			)
		}
	})

	t.Run("configured", func(t *testing.T) {
		const timeout = 8 * time.Second
		enricher := newLogMetadataEnricher(Options{LogMetadataEnrichmentTimeout: timeout})
		if enricher.enrichmentTimeout != timeout {
			t.Fatalf("enrichment timeout = %s, want %s", enricher.enrichmentTimeout, timeout)
		}
	})
}

func TestLogMetadataQueryBudgetConfiguration(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		enricher := newLogMetadataEnricher(Options{})
		if enricher.queryBudget != defaultLogMetadataQueryBudget {
			t.Fatalf(
				"query budget = %d, want %d",
				enricher.queryBudget,
				defaultLogMetadataQueryBudget,
			)
		}
	})

	t.Run("configured", func(t *testing.T) {
		const queryBudget = 750
		enricher := newLogMetadataEnricher(Options{LogMetadataQueryBudget: queryBudget})
		if enricher.queryBudget != queryBudget {
			t.Fatalf("query budget = %d, want %d", enricher.queryBudget, queryBudget)
		}
	})
}

func TestLogMetadataParallelismConfiguration(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		enricher := newLogMetadataEnricher(Options{})
		if enricher.parallelism != defaultLogMetadataParallelism {
			t.Fatalf(
				"parallelism = %d, want %d",
				enricher.parallelism,
				defaultLogMetadataParallelism,
			)
		}
	})

	t.Run("configured", func(t *testing.T) {
		const parallelism = 3
		enricher := newLogMetadataEnricher(Options{LogMetadataParallelism: parallelism})
		if enricher.parallelism != parallelism {
			t.Fatalf("parallelism = %d, want %d", enricher.parallelism, parallelism)
		}
	})
}

func TestLogPathsFromSacctKeepsAttemptsSeparateAndMergesDuplicateRows(t *testing.T) {
	discovered, err := logPathsFromSacct([]byte(`{
		"jobs": [
			{
				"job_id": 12345,
				"time": {"submission": 10, "start": 20},
				"stdout_expanded": "/logs/attempt-0.out",
				"steps": [
					{"step": {"id": {"job_id": 12345, "step_id": "0"}, "stdout_expanded": "/logs/attempt-0-step-0.out"}},
					{"step": {"id": {"job_id": 12345, "step_id": "batch"}, "stdout_expanded": "/logs/batch.out"}},
					{"step": {"id": {"job_id": 12345, "step_id": "extern"}, "stdout_expanded": "/logs/extern.out"}}
				]
			},
			{
				"job_id": 12345,
				"time": {"submission": 10, "start": 20},
				"stdout_expanded": "/logs/attempt-0.out",
				"steps": [
					{"step": {"id": {"job_id": 12345, "step_id": 1}, "stdout_expanded": "/logs/attempt-0-step-1.out"}}
				]
			},
			{
				"job_id": 12345,
				"time": {"submission": 30, "start": 40},
				"stdout_expanded": "/logs/attempt-1.out",
				"steps": [
					{"step": {"id": {"job_id": 12345, "step_id": "0"}, "stdout_expanded": "/logs/attempt-1-step-0.out"}}
				]
			}
		]
	}`))
	if err != nil {
		t.Fatalf("logPathsFromSacct() failed: %v", err)
	}

	attempt0 := discovered[cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 0}]
	if want := []string{"/logs/attempt-0.out"}; !slices.Equal(attempt0.parent, want) {
		t.Fatalf("attempt 0 parent = %#v, want %#v", attempt0.parent, want)
	}
	if want := []string{"/logs/attempt-0-step-0.out", "/logs/attempt-0-step-1.out"}; !slices.Equal(attempt0.steps, want) {
		t.Fatalf("attempt 0 steps = %#v, want %#v", attempt0.steps, want)
	}

	attempt1 := discovered[cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 1}]
	if want := []string{"/logs/attempt-1.out"}; !slices.Equal(attempt1.parent, want) {
		t.Fatalf("attempt 1 parent = %#v, want %#v", attempt1.parent, want)
	}
	if want := []string{"/logs/attempt-1-step-0.out"}; !slices.Equal(attempt1.steps, want) {
		t.Fatalf("attempt 1 steps = %#v, want %#v", attempt1.steps, want)
	}
}

func TestLogPathsFromSacctUsesExistingArrayAttemptIdentity(t *testing.T) {
	discovered, err := logPathsFromSacct([]byte(`{
		"jobs": [{
			"job_id": 9002,
			"array": {"job_id": 9000, "task_id": {"set": true, "number": 2}},
			"time": {"submission": 10, "start": 20},
			"stdout_expanded": "/logs/array-9000_2.out",
			"steps": [{
				"step": {
					"id": {"job_id": 9002, "step_id": "0"},
					"stdout_expanded": "/logs/array-9000_2-step-0.out"
				}
			}]
		}]
	}`))
	if err != nil {
		t.Fatalf("logPathsFromSacct() failed: %v", err)
	}

	got := discovered[cluster.JobAttemptRef{JobID: "9000_2", AttemptIndex: 0}]
	if want := []string{"/logs/array-9000_2.out"}; !slices.Equal(got.parent, want) {
		t.Fatalf("parent = %#v, want %#v", got.parent, want)
	}
	if want := []string{"/logs/array-9000_2-step-0.out"}; !slices.Equal(got.steps, want) {
		t.Fatalf("steps = %#v, want %#v", got.steps, want)
	}
}

func TestLogPathsFromSacctOmitsMissingAndInvalidExpandedPaths(t *testing.T) {
	oversized := strings.Repeat("x", maxSlurmLogFilePathBytes+1)
	payload := `{
		"jobs": [{
			"job_id": 12345,
			"time": {"submission": 10, "start": 20},
			"stdout": "/logs/unexpanded-%j.out",
			"steps": [
				{"step": {"id": {"job_id": 12345, "step_id": "0"}, "stdout": "/logs/unexpanded-%j-%s.out"}},
				{"step": {"id": {"job_id": 12345, "step_id": "1"}, "stdout_expanded": "` + oversized + `"}},
				{"step": {"id": {"job_id": 12345, "step_id": "2"}, "stdout_expanded": "/logs/valid.out"}}
			]
		}]
	}`

	discovered, err := logPathsFromSacct([]byte(payload))
	if err != nil {
		t.Fatalf("logPathsFromSacct() failed: %v", err)
	}
	got := discovered[cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 0}]
	if len(got.parent) != 0 {
		t.Fatalf("parent = %#v, want missing unexpanded path omitted", got.parent)
	}
	if want := []string{"/logs/valid.out"}; !slices.Equal(got.steps, want) {
		t.Fatalf("steps = %#v, want %#v", got.steps, want)
	}
	if got.invalidSteps != 1 {
		t.Fatalf("invalidSteps = %d, want 1", got.invalidSteps)
	}
}

func TestPinnedSlurm2505SqueueFixturePreservesStructuredParentPath(t *testing.T) {
	data := readFixture(
		t,
		"testdata/redacted/squeue/slurm_25_05_log_paths.json",
	)

	observations, err := observedJobsWithSourceFromSqueue(data)
	if err != nil {
		t.Fatalf("observedJobsWithSourceFromSqueue() failed: %v", err)
	}
	if len(observations) != 1 {
		t.Fatalf("len(observations) = %d, want 1", len(observations))
	}

	observation := observations[0]
	if observation.Job.JobID != "9401" {
		t.Fatalf("job ID = %q, want 9401", observation.Job.JobID)
	}
	if observation.Source.StandardOutput != "/redacted/logs/parent-%j.out" {
		t.Fatalf(
			"source standard output = %q, want unexpanded scheduler pattern",
			observation.Source.StandardOutput,
		)
	}
	if observation.Source.StdoutExpanded != "/redacted/logs/parent-9401.out" {
		t.Fatalf(
			"source expanded stdout = %q, want /redacted/logs/parent-9401.out",
			observation.Source.StdoutExpanded,
		)
	}
}

func TestPinnedSlurm2505SacctFixtureDiscoversOnlyNumericStepPaths(t *testing.T) {
	data := readFixture(
		t,
		"testdata/redacted/sacct/slurm_25_05_all_rows_log_paths.json",
	)

	parsed, err := parseSacctOutput(data)
	if err != nil {
		t.Fatalf("parseSacctOutput() failed: %v", err)
	}
	if len(parsed.Jobs) != 1 {
		t.Fatalf("len(parsed.Jobs) = %d, want 1", len(parsed.Jobs))
	}
	if len(parsed.Jobs[0].Steps) != 3 {
		t.Fatalf("len(parsed.Jobs[0].Steps) = %d, want 3", len(parsed.Jobs[0].Steps))
	}

	stepIDs := make([]string, 0, len(parsed.Jobs[0].Steps))
	for _, step := range parsed.Jobs[0].Steps {
		if step.Step.ID.JobID != 9401 {
			t.Fatalf("step job ID = %d, want 9401", step.Step.ID.JobID)
		}
		stepIDs = append(stepIDs, step.Step.ID.StepID)
	}
	slices.Sort(stepIDs)
	if want := []string{"0", "batch", "extern"}; !slices.Equal(stepIDs, want) {
		t.Fatalf("step IDs = %#v, want %#v", stepIDs, want)
	}

	discovered, err := logPathsFromSacct(data)
	if err != nil {
		t.Fatalf("logPathsFromSacct() failed: %v", err)
	}
	got := discovered[cluster.JobAttemptRef{JobID: "9401", AttemptIndex: 0}]
	if want := []string{"/redacted/logs/parent-9401.out"}; !slices.Equal(got.parent, want) {
		t.Fatalf("parent paths = %#v, want %#v", got.parent, want)
	}
	if want := []string{"/redacted/logs/step-9401-0.out"}; !slices.Equal(got.steps, want) {
		t.Fatalf("step paths = %#v, want %#v", got.steps, want)
	}
}

func TestEnrichActiveJobsPublishesStructuredParentAndNumericSteps(t *testing.T) {
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"squeue", "--json"}): {{
			output: []byte(`{"jobs":[{
					"job_id":12345,
					"job_state":"RUNNING",
					"standard_output":"/logs/parent-%j.out",
					"stdout_expanded":"/logs/parent-12345.out"
				}]}`),
		}},
		logSacctKey("12345"): {{
			output: []byte(`{"jobs":[{
				"job_id":12345,
				"time":{"submission":10,"start":20},
				"stdout_expanded":"/logs/parent-12345.out",
				"steps":[
					{"step":{"id":{"job_id":12345,"step_id":"0"},"stdout_expanded":"/logs/step-12345-0.out"}},
					{"step":{"id":{"job_id":12345,"step_id":"batch"},"stdout_expanded":"/logs/batch.out"}},
					{"step":{"id":{"job_id":12345,"step_id":"extern"},"stdout_expanded":"/logs/extern.out"}}
				]
			}]}`),
		}},
	})
	slurmCluster := newTestLogMetadataCluster(t, executor, nil)

	jobs, err := slurmCluster.ActiveJobs(context.Background())
	if err != nil {
		t.Fatalf("ActiveJobs() failed: %v", err)
	}
	assertCommandArgs(t, executor.calls, [][]string{{"squeue", "--json"}})
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/parent-12345.out"})
	waitForLogMetadataIdle(t, slurmCluster)
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/parent-12345.out", "/logs/step-12345-0.out"})
	assertCommandArgs(t, executor.calls, [][]string{
		{"squeue", "--json"},
		{"sacct", "-j", "12345", "--duplicates", "--json=v0.0.43"},
	})
}

func TestActiveJobsFallsBackToScontrolForParentPath(t *testing.T) {
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"squeue", "--json"}): {{
			output: []byte(`{"jobs":[{"job_id":12345,"job_state":"RUNNING"}]}`),
		}},
		logSacctKey("12345"): {{
			output: []byte(`{"jobs":[{"job_id":12345,"time":{"submission":10,"start":20}}]}`),
		}},
		commandKey([]string{"scontrol", "show", "job", "-o", "12345"}): {{
			output: []byte("JobId=12345 JobState=RUNNING Restarts=0 StdOut=/logs/fallback-12345.out StdErr=/logs/fallback-12345.err"),
		}},
	})
	slurmCluster := newTestLogMetadataCluster(t, executor, nil)

	jobs, err := slurmCluster.ActiveJobs(context.Background())
	if err != nil {
		t.Fatalf("ActiveJobs() failed: %v", err)
	}
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	if got := LogFilePaths(jobs[0]); got != nil {
		t.Fatalf("first-pass log paths = %#v, want asynchronous miss", got)
	}
	waitForLogMetadataIdle(t, slurmCluster)
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/fallback-12345.out"})
	assertCommandArgs(t, executor.calls, [][]string{
		{"squeue", "--json"},
		{"sacct", "-j", "12345", "--duplicates", "--json=v0.0.43"},
		{"scontrol", "show", "job", "-o", "12345"},
	})
}

func TestActiveLogMetadataCacheRefreshAndFailureSemantics(t *testing.T) {
	currentTime := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	squeue := []byte(`{"jobs":[{
		"job_id":12345,
		"job_state":"RUNNING",
		"standard_output":"/logs/parent-%j.out",
		"stdout_expanded":"/logs/parent.out"
	}]}`)
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"squeue", "--json"}): {
			{output: squeue},
			{output: squeue},
			{output: squeue},
			{output: squeue},
			{output: squeue},
		},
		logSacctKey("12345"): {
			{output: sacctLogPayload("/logs/step-old.out")},
			{err: errors.New("temporary accounting failure")},
			{err: errors.New("temporary accounting failure")},
			{output: sacctLogPayload("")},
		},
	})
	slurmCluster := newTestLogMetadataCluster(t, executor, func() time.Time { return currentTime })

	jobs, err := slurmCluster.ActiveJobs(context.Background())
	if err != nil {
		t.Fatalf("first ActiveJobs() failed: %v", err)
	}
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	waitForLogMetadataIdle(t, slurmCluster)
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/parent.out", "/logs/step-old.out"})

	currentTime = currentTime.Add(30 * time.Second)
	jobs, err = slurmCluster.ActiveJobs(context.Background())
	if err != nil {
		t.Fatalf("cached ActiveJobs() failed: %v", err)
	}
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/parent.out", "/logs/step-old.out"})
	waitForLogMetadataIdle(t, slurmCluster)
	if got := executor.callCount(logSacctKey("12345")); got != 1 {
		t.Fatalf("sacct calls before expiration = %d, want 1", got)
	}

	currentTime = currentTime.Add(time.Minute)
	jobs, err = slurmCluster.ActiveJobs(context.Background())
	if err != nil {
		t.Fatalf("failed-refresh ActiveJobs() failed: %v", err)
	}
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/parent.out", "/logs/step-old.out"})
	waitForLogMetadataIdle(t, slurmCluster)
	if got := executor.callCount(logSacctKey("12345")); got != 3 {
		t.Fatalf("sacct calls after immediate retry = %d, want 3", got)
	}

	currentTime = currentTime.Add(5 * time.Second)
	jobs, err = slurmCluster.ActiveJobs(context.Background())
	if err != nil {
		t.Fatalf("backoff ActiveJobs() failed: %v", err)
	}
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/parent.out", "/logs/step-old.out"})
	if got := executor.callCount(logSacctKey("12345")); got != 3 {
		t.Fatalf("sacct calls during backoff = %d, want 3", got)
	}

	currentTime = currentTime.Add(5 * time.Second)
	jobs, err = slurmCluster.ActiveJobs(context.Background())
	if err != nil {
		t.Fatalf("empty-refresh ActiveJobs() failed: %v", err)
	}
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	waitForLogMetadataIdle(t, slurmCluster)
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/parent.out"})
	if got := executor.callCount(logSacctKey("12345")); got != 4 {
		t.Fatalf("sacct calls after backoff = %d, want 4", got)
	}
}

func TestFinalizedLogMetadataRefreshOverridesBackoffOnStateChange(t *testing.T) {
	currentTime := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"squeue", "--json"}): {{
			output: []byte(`{"jobs":[{
					"job_id":12345,
					"job_state":"RUNNING",
					"standard_output":"/logs/parent-%j.out",
					"stdout_expanded":"/logs/parent.out"
				}]}`),
		}},
		commandKey([]string{"sacct", "-Xj", "12345", "--duplicates", "--json"}): {{
			output: []byte(`{"jobs":[{
				"job_id":12345,
				"state":{"current":"COMPLETED"},
				"time":{"submission":10,"start":20,"end":30}
			}]}`),
		}},
		logSacctKey("12345"): {
			{err: errors.New("temporary accounting failure")},
			{err: errors.New("temporary accounting failure")},
			{output: []byte(`{"jobs":[{
				"job_id":12345,
				"time":{"submission":10,"start":20},
				"stdout_expanded":"/logs/parent.out",
				"steps":[{
					"step":{
						"id":"12345.0",
						"stdout_expanded":"/logs/final-step.out"
					}
				}]
			}]}`)},
		},
	})
	slurmCluster := newTestLogMetadataCluster(t, executor, func() time.Time { return currentTime })

	jobs, err := slurmCluster.ActiveJobs(context.Background())
	if err != nil {
		t.Fatalf("ActiveJobs() failed: %v", err)
	}
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/parent.out"})
	waitForLogMetadataIdle(t, slurmCluster)

	currentTime = currentTime.Add(time.Second)
	jobs, err = slurmCluster.FinalizedJobs(
		context.Background(),
		[]cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 0}},
	)
	if err != nil {
		t.Fatalf("FinalizedJobs() failed: %v", err)
	}
	requireLogPaths(t, jobs, []string{"/logs/final-step.out", "/logs/parent.out"})
	if got := executor.callCount(logSacctKey("12345")); got != 3 {
		t.Fatalf("supplemental sacct calls = %d, want transition-forced third call", got)
	}
}

func TestActiveLogMetadataEnrichmentHasTotalDeadline(t *testing.T) {
	slurmCluster := newTestLogMetadataCluster(t, blockingLogExecutor{}, nil)
	slurmCluster.logMetadata.enrichmentTimeout = 25 * time.Millisecond

	startedAt := time.Now()
	jobs := slurmCluster.enrichActiveJobs(context.Background(), []squeueObservation{{
		Job: cluster.JobExecutionRecord{
			JobID:             "12345",
			RawState:          "RUNNING",
			SchedulerMetadata: map[string]any{},
		},
		Source: SlurmJob{StdoutExpanded: "/logs/parent.out"},
	}})
	elapsed := time.Since(startedAt)

	if elapsed > time.Second {
		t.Fatalf("enrichment elapsed = %s, want bounded fail-open return", elapsed)
	}
	requireLogPaths(t, jobs, []string{"/logs/parent.out"})
}

func TestActiveLogMetadataEnrichmentRunsIndependentJobsConcurrently(t *testing.T) {
	release := make(chan struct{})
	executor := newConcurrentLogExecutor(release)
	slurmCluster := newTestLogMetadataCluster(t, executor, nil)
	slurmCluster.logMetadata.queryBudget = 2
	slurmCluster.logMetadata.parallelism = 2
	observations := []squeueObservation{
		{
			Job: cluster.JobExecutionRecord{
				JobID:             "1",
				RawState:          "RUNNING",
				SchedulerMetadata: map[string]any{},
			},
			Source: SlurmJob{StdoutExpanded: "/logs/1.out"},
		},
		{
			Job: cluster.JobExecutionRecord{
				JobID:             "2",
				RawState:          "RUNNING",
				SchedulerMetadata: map[string]any{},
			},
			Source: SlurmJob{StdoutExpanded: "/logs/2.out"},
		},
	}

	done := make(chan []cluster.JobExecutionRecord, 1)
	go func() {
		done <- slurmCluster.enrichActiveJobs(context.Background(), observations)
	}()
	for range observations {
		select {
		case <-executor.started:
		case <-time.After(time.Second):
			close(release)
			<-done
			t.Fatal("independent enrichment commands did not overlap")
		}
	}
	close(release)
	jobs := <-done

	if got := executor.maxConcurrency(); got != 2 {
		t.Fatalf("maximum concurrent commands = %d, want 2", got)
	}
	for i := range jobs {
		want := []string{observations[i].Source.StdoutExpanded}
		if got := LogFilePaths(jobs[i]); !slices.Equal(got, want) {
			t.Fatalf("job %s log paths = %#v, want %#v", jobs[i].JobID, got, want)
		}
	}
}

func TestFinalizedLogMetadataEnrichmentRunsIndependentJobsConcurrently(t *testing.T) {
	const parallelism = 2

	release := make(chan struct{})
	executor := newConcurrentLogExecutor(release)
	slurmCluster := newTestLogMetadataCluster(t, executor, nil)
	slurmCluster.logMetadata.queryBudget = 3
	slurmCluster.logMetadata.parallelism = parallelism
	jobs := []cluster.JobExecutionRecord{
		{
			JobID:             "1",
			RawState:          "COMPLETED",
			SchedulerMetadata: map[string]any{},
		},
		{
			JobID:             "2",
			RawState:          "COMPLETED",
			SchedulerMetadata: map[string]any{},
		},
		{
			JobID:             "3",
			RawState:          "COMPLETED",
			SchedulerMetadata: map[string]any{},
		},
	}

	done := make(chan []cluster.JobExecutionRecord, 1)
	go func() {
		done <- slurmCluster.enrichFinalizedJobs(context.Background(), jobs, nil)
	}()
	for range parallelism {
		select {
		case <-executor.started:
		case <-time.After(time.Second):
			close(release)
			<-done
			t.Fatal("independent finalized enrichment commands did not overlap")
		}
	}
	close(release)
	jobs = <-done

	if got := executor.maxConcurrency(); got != parallelism {
		t.Fatalf("maximum concurrent commands = %d, want %d", got, parallelism)
	}
	if got := executor.callCount(); got != len(jobs) {
		t.Fatalf("command count = %d, want %d", got, len(jobs))
	}
	for _, job := range jobs {
		if got := LogFilePaths(job); got != nil {
			t.Fatalf("job %s log paths = %#v, want nil", job.JobID, got)
		}
	}
}

func TestFinalizedLogMetadataEnrichmentReservesBudgetByStablePriority(t *testing.T) {
	release := make(chan struct{})
	close(release)
	executor := newConcurrentLogExecutor(release)
	slurmCluster := newTestLogMetadataCluster(t, executor, nil)
	slurmCluster.logMetadata.queryBudget = 2
	slurmCluster.logMetadata.parallelism = 2
	jobs := []cluster.JobExecutionRecord{
		{JobID: "3", RawState: "COMPLETED", SchedulerMetadata: map[string]any{}},
		{JobID: "1", RawState: "COMPLETED", SchedulerMetadata: map[string]any{}},
		{JobID: "2", RawState: "COMPLETED", SchedulerMetadata: map[string]any{}},
	}

	slurmCluster.enrichFinalizedJobs(context.Background(), jobs, nil)

	got := executor.jobIDs()
	slices.Sort(got)
	if want := []string{"1", "2"}; !slices.Equal(got, want) {
		t.Fatalf("queried job IDs = %v, want %v", got, want)
	}
}

func TestLogMetadataCachePruning(t *testing.T) {
	newEnricher := func() *logMetadataEnricher {
		enricher := newLogMetadataEnricher(Options{
			LogMetadataRefreshInterval: time.Minute,
		})
		enricher.cacheTTL = time.Hour
		enricher.cacheMaxPathBytes = 1 << 20
		return enricher
	}
	observeWithPath := func(
		enricher *logMetadataEnricher,
		ref cluster.JobAttemptRef,
		observedAt time.Time,
		path string,
	) {
		entry, _ := enricher.observe(ref, "RUNNING", observedAt)
		entry.parent.succeed([]string{path}, observedAt)
	}

	t.Run("path byte capacity", func(t *testing.T) {
		enricher := newEnricher()
		enricher.cacheMaxPathBytes = 5
		startedAt := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
		observeWithPath(enricher, cluster.JobAttemptRef{JobID: "1"}, startedAt, "aaaa")
		observeWithPath(
			enricher,
			cluster.JobAttemptRef{JobID: "2"},
			startedAt.Add(time.Second),
			"bbbb",
		)

		enricher.prune(startedAt.Add(time.Second))
		oldest := enricher.attempts[cluster.JobAttemptRef{JobID: "1"}]
		if len(oldest.parent.paths) != 0 {
			t.Fatalf("oldest path payload = %#v, want evicted", oldest.parent.paths)
		}
		if !oldest.parent.initialized {
			t.Fatal("oldest source scheduling state was unexpectedly evicted")
		}
		newest := enricher.attempts[cluster.JobAttemptRef{JobID: "2"}]
		if !slices.Equal(newest.parent.paths, []string{"bbbb"}) {
			t.Fatalf("newest path payload = %#v, want retained", newest.parent.paths)
		}
	})

	t.Run("inactivity TTL", func(t *testing.T) {
		enricher := newEnricher()
		enricher.cacheTTL = time.Minute
		startedAt := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
		observeWithPath(enricher, cluster.JobAttemptRef{JobID: "1"}, startedAt, "/logs/1.out")
		observeWithPath(
			enricher,
			cluster.JobAttemptRef{JobID: "2"},
			startedAt.Add(2*time.Minute),
			"/logs/2.out",
		)

		enricher.prune(startedAt.Add(2 * time.Minute))
		if _, ok := enricher.attempts[cluster.JobAttemptRef{JobID: "1"}]; ok {
			t.Fatal("stale cache entry was not evicted")
		}
		if _, ok := enricher.attempts[cluster.JobAttemptRef{JobID: "2"}]; !ok {
			t.Fatal("current cache entry was unexpectedly evicted")
		}
	})

	t.Run("attempt entry capacity includes pathless attempts", func(t *testing.T) {
		enricher := newEnricher()
		enricher.cacheMaxEntries = 2
		startedAt := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
		for index := range 3 {
			enricher.observe(
				cluster.JobAttemptRef{JobID: strconv.Itoa(index + 1)},
				"RUNNING",
				startedAt.Add(time.Duration(index)*time.Second),
			)
		}

		enricher.prune(startedAt.Add(2 * time.Second))
		if len(enricher.attempts) != 2 {
			t.Fatalf("cache entries = %d, want bounded at 2", len(enricher.attempts))
		}
		if _, ok := enricher.attempts[cluster.JobAttemptRef{JobID: "1"}]; ok {
			t.Fatal("least-recently-observed pathless entry was not evicted")
		}
	})
}

func TestLogMetadataPayloadPruningPreservesBackoff(t *testing.T) {
	currentTime := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	executor := newScriptedLogExecutor(map[string][]commandResult{
		logSacctKey("1"): {{err: errors.New("persistent accounting failure")}},
		logSacctKey("2"): {{output: []byte(
			`{"jobs":[{"job_id":2,"time":{"submission":10,"start":20}}]}`,
		)}},
		logSacctKey("3"): {{output: []byte(
			`{"jobs":[{"job_id":3,"time":{"submission":10,"start":20}}]}`,
		)}},
	})
	slurmCluster := newTestLogMetadataCluster(t, executor, func() time.Time { return currentTime })
	slurmCluster.logMetadata.cacheMaxPathBytes = 1
	observations := make([]squeueObservation, 0, 3)
	for _, jobID := range []string{"1", "2", "3"} {
		observations = append(observations, squeueObservation{
			Job: cluster.JobExecutionRecord{
				JobID:             jobID,
				RawState:          "RUNNING",
				SchedulerMetadata: map[string]any{},
			},
			Source: SlurmJob{StdoutExpanded: "/logs/parent-" + jobID + ".out"},
		})
	}

	slurmCluster.enrichActiveJobs(context.Background(), observations)
	waitForLogMetadataIdle(t, slurmCluster)
	for _, jobID := range []string{"1", "2", "3"} {
		if got := executor.callCount(logSacctKey(jobID)); got != 1 {
			t.Fatalf("sacct calls for job %s = %d, want 1", jobID, got)
		}
	}

	slurmCluster.enrichActiveJobs(context.Background(), observations)
	waitForLogMetadataIdle(t, slurmCluster)
	failed := slurmCluster.logMetadata.attempts[cluster.JobAttemptRef{JobID: "1"}]
	if failed.steps.failures != 1 {
		t.Fatalf("failed source failures = %d, want preserved backoff state", failed.steps.failures)
	}
}

func TestActiveLogMetadataDoesNotCrossAttemptBoundary(t *testing.T) {
	currentTime := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"squeue", "--json"}): {
			{output: []byte(`{"jobs":[{
					"job_id":12345,
					"restart_cnt":0,
					"job_state":"RUNNING",
					"standard_output":"/logs/attempt-%j.out",
					"stdout_expanded":"/logs/attempt-0.out"
				}]}`)},
			{output: []byte(`{"jobs":[{
					"job_id":12345,
					"restart_cnt":1,
					"job_state":"RUNNING",
					"standard_output":"/logs/attempt-%j.out",
					"stdout_expanded":"/logs/attempt-1.out"
				}]}`)},
		},
		logSacctKey("12345"): {
			{output: sacctLogPayload("/logs/attempt-0-step.out")},
			{output: sacctLogPayload("/logs/attempt-0-step.out")},
		},
	})
	slurmCluster := newTestLogMetadataCluster(t, executor, func() time.Time { return currentTime })

	jobs, err := slurmCluster.ActiveJobs(context.Background())
	if err != nil {
		t.Fatalf("attempt 0 ActiveJobs() failed: %v", err)
	}
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	waitForLogMetadataIdle(t, slurmCluster)
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/attempt-0-step.out", "/logs/attempt-0.out"})

	currentTime = currentTime.Add(time.Second)
	jobs, err = slurmCluster.ActiveJobs(context.Background())
	if err != nil {
		t.Fatalf("attempt 1 ActiveJobs() failed: %v", err)
	}
	jobs = slurmCluster.EnrichActiveJobs(context.Background(), jobs)
	requireLogPaths(t, jobs, []string{"/logs/attempt-1.out"})
}

func TestActiveScontrolResultRequiresExactAttemptIdentity(t *testing.T) {
	tests := []struct {
		name   string
		output string
	}{
		{
			name:   "missing restart counter",
			output: "JobId=12345 StdOut=/logs/wrong-attempt.out",
		},
		{
			name:   "different restart counter",
			output: "JobId=12345 Restarts=1 StdOut=/logs/wrong-attempt.out",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := newScriptedLogExecutor(map[string][]commandResult{
				commandKey([]string{"scontrol", "show", "job", "-o", "12345"}): {{
					output: []byte(test.output),
				}},
			})
			slurmCluster := newTestLogMetadataCluster(t, executor, nil)
			ref := cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 0}

			slurmCluster.logMetadata.mu.Lock()
			entry, _ := slurmCluster.logMetadata.observeLocked(ref, "RUNNING", time.Now())
			entry.inFlight = true
			work := logMetadataWork{
				ref:        ref,
				generation: entry.generation,
				parentDue:  true,
			}
			slurmCluster.logMetadata.mu.Unlock()

			failed, stale := slurmCluster.runActiveLogMetadataWork(context.Background(), work)
			if failed || !stale {
				t.Fatalf("runActiveLogMetadataWork() = failed %t, stale %t; want false, true", failed, stale)
			}
			slurmCluster.logMetadata.mu.Lock()
			got := slices.Clone(slurmCluster.logMetadata.attempts[ref].parent.paths)
			initialized := slurmCluster.logMetadata.attempts[ref].parent.initialized
			slurmCluster.logMetadata.mu.Unlock()
			if len(got) != 0 || initialized {
				t.Fatalf("parent cache = paths %#v, initialized %t; want unresolved", got, initialized)
			}
		})
	}
}

func TestActiveLogMetadataCoalescesRepeatedTicks(t *testing.T) {
	release := make(chan struct{})
	executor := newConcurrentLogExecutor(release)
	slurmCluster := newTestLogMetadataCluster(t, executor, nil)
	observation := []squeueObservation{{
		Job: cluster.JobExecutionRecord{
			JobID:             "12345",
			RawState:          "RUNNING",
			SchedulerMetadata: map[string]any{},
		},
		Source: SlurmJob{StdoutExpanded: "/logs/parent.out"},
	}}

	slurmCluster.enrichActiveJobs(context.Background(), observation)
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("background enrichment did not start")
	}
	for range 20 {
		slurmCluster.enrichActiveJobs(context.Background(), observation)
	}
	if got := executor.callCount(); got != 1 {
		close(release)
		t.Fatalf("in-flight command count = %d, want one coalesced item", got)
	}
	close(release)
	waitForLogMetadataIdle(t, slurmCluster)
}

func TestActiveLogMetadataScaleDoesNotDelayTick(t *testing.T) {
	slurmCluster := newTestLogMetadataCluster(t, blockingLogExecutor{}, nil)
	slurmCluster.logMetadata.enrichmentTimeout = 25 * time.Millisecond
	observations := make([]squeueObservation, 300)
	for i := range observations {
		observations[i] = squeueObservation{
			Job: cluster.JobExecutionRecord{
				JobID:             strconv.Itoa(i + 1),
				RawState:          "RUNNING",
				SchedulerMetadata: map[string]any{},
			},
			Source: SlurmJob{StdoutExpanded: "/logs/parent.out"},
		}
	}

	startedAt := time.Now()
	jobs := slurmCluster.enrichActiveJobs(context.Background(), observations)
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("300-job admission took %s, want non-blocking tick work", elapsed)
	}
	if len(jobs) != len(observations) {
		t.Fatalf("published jobs = %d, want %d", len(jobs), len(observations))
	}
}

func TestActiveLogMetadataQueueSaturationIsNonBlockingAndRetryable(t *testing.T) {
	observer := newRecordingLogMetadataObserver()
	enricher := newLogMetadataEnricher(Options{
		LogMetadataObserver: observer,
	})
	enricher.workQueue = make(chan logMetadataWork, 1)
	slurmCluster := &slurmCluster{logMetadata: enricher}
	t.Cleanup(enricher.close)
	observations := []squeueObservation{
		{
			Job:    cluster.JobExecutionRecord{JobID: "1", RawState: "RUNNING", SchedulerMetadata: map[string]any{}},
			Source: SlurmJob{StdoutExpanded: "/logs/1.out"},
		},
		{
			Job:    cluster.JobExecutionRecord{JobID: "2", RawState: "RUNNING", SchedulerMetadata: map[string]any{}},
			Source: SlurmJob{StdoutExpanded: "/logs/2.out"},
		},
	}

	startedAt := time.Now()
	slurmCluster.enrichActiveJobs(context.Background(), observations)
	if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
		t.Fatalf("saturated admission took %s, want non-blocking return", elapsed)
	}
	if got := observer.workCount(LogMetadataWorkAdmitted); got != 1 {
		t.Fatalf("admitted work = %d, want 1", got)
	}
	if got := observer.workCount(LogMetadataWorkQueueFull); got != 1 {
		t.Fatalf("queue-full work = %d, want 1", got)
	}
	enricher.mu.Lock()
	dueAndUnqueued := 0
	for _, entry := range enricher.attempts {
		if !entry.queued && !entry.inFlight && entry.steps.due(time.Now(), false, enricher.refreshInterval) {
			dueAndUnqueued++
		}
	}
	enricher.mu.Unlock()
	if dueAndUnqueued != 1 {
		t.Fatalf("retryable due entries = %d, want 1 rejected item", dueAndUnqueued)
	}
	enricher.close()
	if got := len(enricher.workQueue); got != 0 {
		t.Fatalf("queue depth after shutdown = %d, want 0", got)
	}
}

func TestActiveLogMetadataRecordsCanceledAdmission(t *testing.T) {
	tests := []struct {
		name         string
		cancelCall   bool
		closeService bool
	}{
		{name: "canceled call", cancelCall: true},
		{name: "closed service", closeService: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observer := newRecordingLogMetadataObserver()
			enricher := newLogMetadataEnricher(Options{LogMetadataObserver: observer})
			slurmCluster := &slurmCluster{logMetadata: enricher}
			t.Cleanup(enricher.close)
			if test.closeService {
				enricher.close()
			}
			ctx := context.Background()
			if test.cancelCall {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			slurmCluster.enrichActiveJobs(ctx, []squeueObservation{
				{
					Job:    cluster.JobExecutionRecord{JobID: "1", RawState: "RUNNING", SchedulerMetadata: map[string]any{}},
					Source: SlurmJob{StdoutExpanded: "/logs/1.out"},
				},
				{
					Job:    cluster.JobExecutionRecord{JobID: "2", RawState: "RUNNING", SchedulerMetadata: map[string]any{}},
					Source: SlurmJob{StdoutExpanded: "/logs/2.out"},
				},
			})

			if got := observer.workCount(LogMetadataWorkOffered); got != 2 {
				t.Fatalf("offered work = %d, want 2", got)
			}
			if got := observer.workCount(LogMetadataWorkCanceled); got != 2 {
				t.Fatalf("canceled work = %d, want 2", got)
			}
			if got := observer.workCount(LogMetadataWorkAdmitted); got != 0 {
				t.Fatalf("admitted work = %d, want 0", got)
			}
		})
	}
}

func TestFinalizedJobsKeepsStatusWhenSupplementalDiscoveryFails(t *testing.T) {
	executor := newScriptedLogExecutor(map[string][]commandResult{
		commandKey([]string{"sacct", "-Xj", "12345", "--duplicates", "--json"}): {{
			output: []byte(`{"jobs":[{
				"job_id":12345,
				"state":{"current":"COMPLETED"},
				"time":{"submission":10,"start":20,"end":30}
			}]}`),
		}},
		logSacctKey("12345"): {
			{err: errors.New("supplemental accounting unavailable")},
			{err: errors.New("supplemental accounting unavailable")},
		},
	})
	slurmCluster := newTestLogMetadataCluster(t, executor, nil)

	jobs, err := slurmCluster.FinalizedJobs(
		context.Background(),
		[]cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 0}},
	)
	if err != nil {
		t.Fatalf("FinalizedJobs() failed: %v", err)
	}
	if len(jobs) != 1 || jobs[0].State != cluster.JobStateCompleted {
		t.Fatalf("jobs = %#v, want one completed job", jobs)
	}
	if _, ok := jobs[0].SchedulerMetadata[slurmLogFilePathsMetadataKey]; ok {
		t.Fatalf("scheduler metadata = %#v, want no unknown log paths", jobs[0].SchedulerMetadata)
	}
	assertCommandArgs(t, executor.calls, [][]string{
		{"sacct", "-Xj", "12345", "--duplicates", "--json"},
		{"sacct", "-j", "12345", "--duplicates", "--json=v0.0.43"},
		{"sacct", "-j", "12345", "--duplicates", "--json=v0.0.43"},
	})
}

func newTestLogMetadataCluster(
	t *testing.T,
	executor commandExecutor,
	clock func() time.Time,
) *slurmCluster {
	opts := Options{
		AccountingTimeout:          time.Minute,
		EnableLogMetadata:          true,
		LogMetadataRefreshInterval: time.Minute,
		LogMetadataRetryBase:       10 * time.Second,
		LogMetadataQueryBudget:     16,
		Clock:                      clock,
	}
	slurmCluster := newSlurmCluster(opts)
	slurmCluster.roCommands = executor
	slurmCluster.rwCommands = executor
	t.Cleanup(slurmCluster.Close)
	return slurmCluster
}

func waitForLogMetadataIdle(t *testing.T, slurmCluster *slurmCluster) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		slurmCluster.logMetadata.mu.Lock()
		idle := len(slurmCluster.logMetadata.workQueue) == 0
		for _, entry := range slurmCluster.logMetadata.attempts {
			idle = idle && !entry.queued && !entry.inFlight
		}
		slurmCluster.logMetadata.mu.Unlock()
		if idle {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for log-metadata workers")
		}
		time.Sleep(time.Millisecond)
	}
}

func requireLogPaths(
	t *testing.T,
	jobs []cluster.JobExecutionRecord,
	want []string,
) {
	t.Helper()
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1", len(jobs))
	}
	got, ok := jobs[0].SchedulerMetadata[slurmLogFilePathsMetadataKey].([]string)
	if !ok {
		t.Fatalf(
			"scheduler metadata %q = %#v, want []string",
			slurmLogFilePathsMetadataKey,
			jobs[0].SchedulerMetadata[slurmLogFilePathsMetadataKey],
		)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("log paths = %#v, want %#v", got, want)
	}
}

func logSacctKey(jobID string) string {
	return commandKey([]string{
		"sacct",
		"-j",
		jobID,
		"--duplicates",
		"--json=v0.0.43",
	})
}

func sacctLogPayload(stepPath string) []byte {
	step := ""
	if stepPath != "" {
		step = `,"steps":[{
			"step":{
				"id":{"job_id":12345,"step_id":"0"},
				"stdout_expanded":"` + stepPath + `"
			}
		}]`
	}
	return []byte(`{"jobs":[{
		"job_id":12345,
		"time":{"submission":10,"start":20}` + step + `
	}]}`)
}

type commandResult struct {
	output []byte
	stderr []byte
	err    error
}

type scriptedLogExecutor struct {
	mu      sync.Mutex
	results map[string][]commandResult
	offsets map[string]int
	calls   [][]string
}

func newScriptedLogExecutor(results map[string][]commandResult) *scriptedLogExecutor {
	return &scriptedLogExecutor{
		results: results,
		offsets: make(map[string]int),
	}
}

func (e *scriptedLogExecutor) Execute(cmd *exec.Cmd) (commandOutput, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, append([]string(nil), cmd.Args...))
	key := commandKey(cmd.Args)
	results := e.results[key]
	offset := e.offsets[key]
	if offset >= len(results) {
		return commandOutput{}, errors.New("unexpected command call: " + strings.Join(cmd.Args, " "))
	}
	e.offsets[key] = offset + 1
	return commandOutput{stdout: results[offset].output, stderr: results[offset].stderr}, results[offset].err
}

func (e *scriptedLogExecutor) callCount(key string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.offsets[key]
}

type blockingLogExecutor struct{}

func (blockingLogExecutor) Execute(cmd *exec.Cmd) (commandOutput, error) {
	cmd.Path = "/bin/sleep"
	cmd.Args = []string{"sleep", "5"}
	cmd.Err = nil
	return executor{}.Execute(cmd)
}

type concurrentLogExecutor struct {
	mu           sync.Mutex
	started      chan struct{}
	release      <-chan struct{}
	active       int
	max          int
	calls        int
	calledJobIDs []string
}

type recordingLogMetadataObserver struct {
	NopLogMetadataObserver
	mu   sync.Mutex
	work map[LogMetadataWorkEvent]int
}

func newRecordingLogMetadataObserver() *recordingLogMetadataObserver {
	return &recordingLogMetadataObserver{work: make(map[LogMetadataWorkEvent]int)}
}

func (observer *recordingLogMetadataObserver) ObserveLogMetadataWork(event LogMetadataWorkEvent) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.work[event]++
}

func (observer *recordingLogMetadataObserver) workCount(event LogMetadataWorkEvent) int {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.work[event]
}

func newConcurrentLogExecutor(release <-chan struct{}) *concurrentLogExecutor {
	return &concurrentLogExecutor{
		started: make(chan struct{}, defaultLogMetadataParallelism),
		release: release,
	}
}

func (executor *concurrentLogExecutor) Execute(cmd *exec.Cmd) (commandOutput, error) {
	executor.mu.Lock()
	executor.active++
	executor.max = max(executor.max, executor.active)
	executor.calls++
	if len(cmd.Args) > 2 {
		executor.calledJobIDs = append(executor.calledJobIDs, cmd.Args[2])
	}
	executor.mu.Unlock()
	executor.started <- struct{}{}

	<-executor.release

	executor.mu.Lock()
	executor.active--
	executor.mu.Unlock()
	return commandOutput{stdout: []byte(`{"jobs":[]}`)}, nil
}

func (executor *concurrentLogExecutor) maxConcurrency() int {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.max
}

func (executor *concurrentLogExecutor) callCount() int {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.calls
}

func (executor *concurrentLogExecutor) jobIDs() []string {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return slices.Clone(executor.calledJobIDs)
}
