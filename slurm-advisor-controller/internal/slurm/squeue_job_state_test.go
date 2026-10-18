// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/google/go-cmp/cmp"
)

func TestObservedJobsFromSqueueSelectsEffectiveJobState(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantState cluster.JobState
		wantRaw   string
	}{
		{name: "legacy scalar", value: `"RUNNING"`, wantState: cluster.JobStateRunning, wantRaw: "RUNNING"},
		{name: "legacy completing scalar", value: `"COMPLETING"`, wantState: cluster.JobStateRunning, wantRaw: "COMPLETING"},
		{name: "singleton list", value: `["RUNNING"]`, wantState: cluster.JobStateRunning, wantRaw: "RUNNING"},
		{name: "base plus flag", value: `["RUNNING","COMPLETING"]`, wantState: cluster.JobStateRunning, wantRaw: "RUNNING"},
		{name: "final base plus completing flag", value: `["TIMEOUT","COMPLETING"]`, wantState: cluster.JobStateFailed, wantRaw: "TIMEOUT"},
		{name: "completing flag plus final base", value: `["COMPLETING","TIMEOUT"]`, wantState: cluster.JobStateFailed, wantRaw: "TIMEOUT"},
		{name: "trailing final overrides nonfinal state", value: `["PENDING","REQUEUED"]`, wantState: cluster.JobStateRequeued, wantRaw: "REQUEUED"},
		{name: "unknown first token remains observable", value: `["FUTURE_STATE","RUNNING"]`, wantRaw: "FUTURE_STATE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := `{"jobs":[{"job_id":12345,"job_state":` + tt.value + `}]}`
			jobs, err := observedJobsFromSqueue([]byte(raw))
			if err != nil {
				t.Fatalf("observedJobsFromSqueue() failed: %v", err)
			}
			if len(jobs) != 1 {
				t.Fatalf("len(jobs) = %d, want 1", len(jobs))
			}

			job := jobs[0]
			if job.State != tt.wantState || job.RawState != tt.wantRaw {
				t.Fatalf("state = %q/%q, want %q/%q", job.State, job.RawState, tt.wantState, tt.wantRaw)
			}
			if got := job.SchedulerMetadata["slurm_state"]; got != tt.wantRaw {
				t.Fatalf("metadata[slurm_state] = %#v, want %q", got, tt.wantRaw)
			}
		})
	}
}

// Do not call t.Parallel here; this test temporarily replaces package-global now.
func TestObservedJobsFromSqueueStateListMatchesSelectedScalarRecord(t *testing.T) {
	fixedNow := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	originalNow := now
	now = func() time.Time { return fixedNow }
	t.Cleanup(func() { now = originalNow })

	tests := []struct {
		name   string
		scalar string
		list   string
		held   bool
	}{
		{name: "managed running", scalar: `"RUNNING"`, list: `["RUNNING","COMPLETING"]`},
		{name: "higher held requeued", scalar: `"REQUEUED"`, list: `["PENDING","REQUEUED"]`, held: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observe := func(state string) cluster.JobExecutionRecord {
				t.Helper()
				raw := fmt.Sprintf(`{
					"jobs":[{
						"job_id":12345,
						"job_state":%s,
						"restart_cnt":2,
						"hold":%t,
						"partition":"normal",
						"nodes":"node[001-002]",
						"required_nodes":"node001",
						"excluded_nodes":"node003",
						"comment":"{\"APS\":{\"workflow_id\":\"workflow-a\"}}"
					}]
				}`, state, tt.held)
				jobs, err := observedJobsFromSqueue([]byte(raw))
				if err != nil {
					t.Fatalf("observedJobsFromSqueue() failed: %v", err)
				}
				if len(jobs) != 1 {
					t.Fatalf("len(jobs) = %d, want 1", len(jobs))
				}
				return jobs[0]
			}

			if diff := cmp.Diff(observe(tt.scalar), observe(tt.list)); diff != "" {
				t.Fatalf("scalar/list records mismatch (-scalar +list):\n%s", diff)
			}
		})
	}
}

func TestObservedJobsFromSqueueFlaggedJobDoesNotRejectOtherJobs(t *testing.T) {
	raw := []byte(`{
		"jobs": [
			{"job_id":12345,"job_state":["RUNNING","COMPLETING"]},
			{"job_id":67890,"job_state":"PENDING"}
		]
	}`)
	jobs, err := observedJobsFromSqueue(raw)
	if err != nil {
		t.Fatalf("observedJobsFromSqueue() failed: %v", err)
	}
	if len(jobs) != 2 {
		t.Fatalf("len(jobs) = %d, want 2", len(jobs))
	}
	if jobs[0].RawState != "RUNNING" || jobs[1].RawState != "PENDING" {
		t.Fatalf("raw states = %q/%q, want RUNNING/PENDING", jobs[0].RawState, jobs[1].RawState)
	}
}

func TestObservedJobsFromSqueuePreservesLegacyJobStateSemantics(t *testing.T) {
	tests := []struct {
		name      string
		field     string
		wantState cluster.JobState
		wantRaw   string
	}{
		{
			name:      "string",
			field:     `,"job_state":"RUNNING"`,
			wantState: cluster.JobStateRunning,
			wantRaw:   "RUNNING",
		},
		{name: "null", field: `,"job_state":null`},
		{name: "missing"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := `{"jobs":[{"job_id":12345` + tt.field + `}]}`
			jobs, err := observedJobsFromSqueue([]byte(raw))
			if err != nil {
				t.Fatalf("observedJobsFromSqueue() failed: %v", err)
			}
			if len(jobs) != 1 {
				t.Fatalf("len(jobs) = %d, want 1", len(jobs))
			}
			if jobs[0].State != tt.wantState || jobs[0].RawState != tt.wantRaw {
				t.Fatalf(
					"state = %q/%q, want %q/%q",
					jobs[0].State,
					jobs[0].RawState,
					tt.wantState,
					tt.wantRaw,
				)
			}
		})
	}
}

func TestObservedJobsFromSqueueRejectsMalformedJobStateArrays(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value string
	}{
		{name: "empty", value: `[]`},
		{name: "empty first state", value: `[""]`},
		{name: "empty trailing state", value: `["RUNNING",""]`},
		{name: "null first state", value: `[null]`},
		{name: "null trailing state", value: `["RUNNING",null]`},
		{name: "numeric state", value: `[1]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := `{"jobs":[{"job_id":12345,"job_state":` + tt.value + `}]}`
			jobs, err := observedJobsFromSqueue([]byte(raw))
			if err == nil {
				t.Fatalf("observedJobsFromSqueue() error = nil, jobs = %#v", jobs)
			}
			if !strings.Contains(err.Error(), "failed to unmarshal squeue output") {
				t.Fatalf("observedJobsFromSqueue() error = %q, want parse context", err)
			}
		})
	}
}
