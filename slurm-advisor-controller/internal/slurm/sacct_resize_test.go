// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"slices"
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

// A --no-kill job that loses a node is resized by Slurm: sacct --duplicates
// then returns a closed PENDING|RESIZING record for the part before the resize
// and a new record, with reset submission and start times, for the rest of the
// same attempt. The shapes below are those Slurm 25.05 returned for a job that
// lost node-3 and then completed on node-[1-2].
func TestObservedJobsFromSacctTreatsResizeAsOneAttempt(t *testing.T) {
	jobs, err := observedJobsFromSacct([]byte(`{
		"jobs": [
			{
				"job_id": 12,
				"nodes": "node-[1-3]",
				"restart_cnt": 0,
				"state": {"current": ["PENDING", "RESIZING"]},
				"time": {"submission": 1793111245, "start": 1793111245, "end": 1793111264}
			},
			{
				"job_id": 12,
				"nodes": "node-[1-2]",
				"restart_cnt": 0,
				"state": {"current": ["COMPLETED"]},
				"time": {"submission": 1793111264, "start": 1793111264, "end": 1793111398}
			}
		]
	}`))
	if err != nil {
		t.Fatalf("observedJobsFromSacct() failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1: a resize does not start a new attempt", len(jobs))
	}

	job := jobs[0]
	if job.AttemptIndex != 0 {
		t.Errorf("AttemptIndex = %d, want 0", job.AttemptIndex)
	}
	if job.State != cluster.JobStateCompleted {
		t.Errorf("State = %v, want %v", job.State, cluster.JobStateCompleted)
	}
	if !slices.Equal(job.AllocatedNodes, []string{"node-1", "node-2"}) {
		t.Errorf("AllocatedNodes = %v, want the nodes held after the resize", job.AllocatedNodes)
	}
	assertObservedTimestamp(t, job.EndTime, job.SchedulerMetadata, "slurm_end_time", 1793111398, true)
}

// A resize must not shift the index of the attempts that follow it: requeuing
// a resized attempt still produces attempt 1, which is what the advice for
// attempt 0 is matched against.
func TestObservedJobsFromSacctKeepsAttemptIndexesAcrossResize(t *testing.T) {
	jobs, err := observedJobsFromSacct([]byte(`{
		"jobs": [
			{
				"job_id": 20,
				"nodes": "n[1-3]",
				"state": {"current": ["PENDING", "RESIZING"]},
				"time": {"submission": 1000, "start": 1000, "end": 1100}
			},
			{
				"job_id": 20,
				"nodes": "n[1-2]",
				"state": {"current": "REQUEUED"},
				"time": {"submission": 1100, "start": 1100, "end": 1200}
			},
			{
				"job_id": 20,
				"nodes": "n[4-6]",
				"state": {"current": "COMPLETED"},
				"time": {"submission": 1300, "start": 1300, "end": 1400}
			}
		]
	}`))
	if err != nil {
		t.Fatalf("observedJobsFromSacct() failed: %v", err)
	}

	got := make([]cluster.JobState, len(jobs))
	for i, job := range jobs {
		if job.AttemptIndex != int32(i) {
			t.Errorf("jobs[%d].AttemptIndex = %d, want %d", i, job.AttemptIndex, i)
		}
		got[i] = job.State
	}
	want := []cluster.JobState{cluster.JobStateRequeued, cluster.JobStateCompleted}
	if !slices.Equal(got, want) {
		t.Fatalf("attempt states = %v, want %v", got, want)
	}
}
