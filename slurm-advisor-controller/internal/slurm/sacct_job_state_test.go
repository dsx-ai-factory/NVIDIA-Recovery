// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestObservedJobsFromSacctSelectsFirstFinalJobState(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		wantState string
		wantRaw   string
	}{
		{name: "legacy scalar", value: `"COMPLETED"`, wantState: "completed", wantRaw: "COMPLETED"},
		{name: "singleton list", value: `["COMPLETED"]`, wantState: "completed", wantRaw: "COMPLETED"},
		{name: "first Python final", value: `["PENDING","REQUEUED"]`, wantState: "requeued", wantRaw: "REQUEUED"},
		{name: "completed after pending", value: `["PENDING","COMPLETED"]`, wantState: "completed", wantRaw: "COMPLETED"},
		{name: "cancelled", value: `"CANCELLED"`, wantState: "cancelled", wantRaw: "CANCELLED"},
		{name: "timeout", value: `"TIMEOUT"`, wantState: "failed", wantRaw: "TIMEOUT"},
		{name: "preempted", value: `"PREEMPTED"`, wantState: "failed", wantRaw: "PREEMPTED"},
		{name: "node failure", value: `"NODE_FAIL"`, wantState: "failed", wantRaw: "NODE_FAIL"},
		{name: "first of multiple finals", value: `["FAILED","NODE_FAIL"]`, wantState: "failed", wantRaw: "FAILED"},
		{name: "no final falls back to first", value: `["PENDING","RUNNING"]`, wantState: "pending", wantRaw: "PENDING"},
		{name: "non-Python final does not override first", value: `["PENDING","SPECIAL_EXIT"]`, wantState: "pending", wantRaw: "PENDING"},
		{name: "revoked does not override first", value: `["PENDING","REVOKED"]`, wantState: "pending", wantRaw: "PENDING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := `{"jobs":[{"job_id":12345,"state":{"current":` + tt.value + `}}]}`
			jobs, err := observedJobsFromSacct([]byte(raw))
			if err != nil {
				t.Fatalf("observedJobsFromSacct() failed: %v", err)
			}
			if len(jobs) != 1 {
				t.Fatalf("len(jobs) = %d, want 1", len(jobs))
			}
			job := jobs[0]
			if string(job.State) != tt.wantState || job.RawState != tt.wantRaw {
				t.Fatalf("state = %q/%q, want %q/%q", job.State, job.RawState, tt.wantState, tt.wantRaw)
			}
			if got := job.SchedulerMetadata["slurm_state"]; got != tt.wantRaw {
				t.Fatalf("metadata[slurm_state] = %#v, want %q", got, tt.wantRaw)
			}
		})
	}
}

// Do not call t.Parallel here; this test temporarily replaces package-global now.
func TestObservedJobsFromSacctStateListMatchesSelectedScalarRecord(t *testing.T) {
	fixedNow := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	originalNow := now
	now = func() time.Time { return fixedNow }
	t.Cleanup(func() { now = originalNow })

	tests := []struct {
		name   string
		scalar string
		list   string
	}{
		{name: "failed", scalar: `"FAILED"`, list: `["FAILED","NODE_FAIL"]`},
		{name: "requeued", scalar: `"REQUEUED"`, list: `["PENDING","REQUEUED"]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			observe := func(state string) any {
				t.Helper()
				raw := fmt.Sprintf(`{
					"jobs":[{
						"job_id":12345,
						"name":"train",
						"state":{"current":%s},
						"partition":"normal",
						"nodes":"node001",
						"failed_node":"node001",
						"time":{"submission":1722495600,"start":1722495660,"end":1722495700},
						"comment":{"job":"{\"APS\":{\"auto_resume_mode\":\"requeue\",\"workflow_id\":\"workflow-a\"}}"}
					}]
				}`, state)
				jobs, err := observedJobsFromSacct([]byte(raw))
				if err != nil {
					t.Fatalf("observedJobsFromSacct() failed: %v", err)
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

func TestObservedJobsFromSacctRejectsEmptyStateList(t *testing.T) {
	jobs, err := observedJobsFromSacct([]byte(
		`{"jobs":[{"job_id":12345,"state":{"current":[]}}]}`,
	))
	if err == nil {
		t.Fatalf("observedJobsFromSacct() error = nil, jobs = %#v", jobs)
	}
	if !strings.Contains(err.Error(), "failed to unmarshal sacct output") {
		t.Fatalf("observedJobsFromSacct() error = %q, want parse context", err)
	}
}
