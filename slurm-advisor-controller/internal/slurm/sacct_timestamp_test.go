// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import "testing"

func TestObservedJobsFromSacctOmitsSlurmSentinelTimestamps(t *testing.T) {
	tests := []struct {
		name      string
		startTime string
	}{
		{name: "no value", startTime: "4294967294"},
		{name: "infinite", startTime: "4294967295"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs, err := observedJobsFromSacct([]byte(`{
				"jobs": [{
					"job_id": 12345,
					"state": {"current": "REQUEUED"},
					"time": {
						"submission": 1788882227,
						"start": ` + tt.startTime + `,
						"end": 1788901698
					}
				}]
			}`))
			if err != nil {
				t.Fatalf("observedJobsFromSacct() failed: %v", err)
			}
			if len(jobs) != 1 {
				t.Fatalf("len(jobs) = %d, want 1", len(jobs))
			}

			job := jobs[0]
			assertObservedTimestamp(t, job.SubmitTime, job.SchedulerMetadata, "slurm_submit_time", 1788882227, true)
			assertObservedTimestamp(t, job.StartTime, job.SchedulerMetadata, "slurm_start_time", 0, false)
			assertObservedTimestamp(t, job.EndTime, job.SchedulerMetadata, "slurm_end_time", 1788901698, true)
		})
	}
}
