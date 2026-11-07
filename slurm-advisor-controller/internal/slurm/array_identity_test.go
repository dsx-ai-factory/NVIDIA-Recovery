// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

// These cases prove that discovery publishes concrete tasks under stable task
// IDs and keeps the raw scheduler ID separate, regardless of JSON encoding.
func TestDiscoveryArrayIdentity(t *testing.T) {
	for _, source := range []string{"squeue", "sacct"} {
		t.Run(source, func(t *testing.T) {
			for _, test := range []struct {
				name, parent, task, wantID, wantTask string
			}{
				{"plain task zero", `4200`, `0`, "4200_0", "0"},
				{"plain sibling", `4200`, `7`, "4200_7", "7"},
				{"typed task zero", `{"set":true,"infinite":false,"number":4200}`, `{"set":true,"infinite":false,"number":0}`, "4200_0", "0"},
				{"typed sibling", `{"set":true,"number":4200}`, `{"set":true,"number":7}`, "4200_7", "7"},
				{"parent without set flag", `{"number":4200}`, `{"set":true,"number":0}`, "4200_0", "0"},
				{"ordinary zero parent", `0`, `4294967294`, "9002", ""},
				{"ordinary unset parent", `{"set":false,"number":0}`, `{"set":false,"number":0}`, "9002", ""},
				{"ordinary absent identity", ``, ``, "9002", ""},
			} {
				t.Run(test.name, func(t *testing.T) {
					jobs, err := discoverArrayFixture(t, source, arrayIdentityRow(source, test.parent, test.task), test.wantID)
					if err != nil || len(jobs) != 1 {
						t.Fatalf("discovery returned %d jobs, error %v", len(jobs), err)
					}
					job := jobs[0]
					if job.JobID != test.wantID || job.RawJobID != "9002" || job.ArrayTaskID != test.wantTask {
						t.Fatalf("identity = %q/%q/%q, want %q/9002/%q", job.JobID, job.RawJobID, job.ArrayTaskID, test.wantID, test.wantTask)
					}
					if job.SchedulerMetadata["slurm_job_id"] != test.wantID || job.SchedulerMetadata["slurm_raw_job_id"] != "9002" {
						t.Fatalf("identity metadata = %#v", job.SchedulerMetadata)
					}
					if test.wantTask == "" {
						if _, exists := job.SchedulerMetadata["slurm_array_job_id"]; exists {
							t.Fatal("ordinary job has array metadata")
						}
						if _, exists := job.SchedulerMetadata["slurm_array_task_id"]; exists {
							t.Fatal("ordinary job has task metadata")
						}
					} else if job.ArrayJobID != "4200" || job.SchedulerMetadata["slurm_array_job_id"] != "4200" || job.SchedulerMetadata["slurm_array_task_id"] != test.wantTask {
						t.Fatalf("array metadata = %#v", job.SchedulerMetadata)
					}
				})
			}
		})
	}
}

func TestDiscoverySkipsSyntheticArrayRowsWithoutLosingTaskZero(t *testing.T) {
	for _, source := range []string{"squeue", "sacct"} {
		t.Run(source, func(t *testing.T) {
			synthetic := arrayIdentityRow(source, `4200`, `{"set":false,"infinite":false,"number":0}`)
			concrete := arrayIdentityRow(source, `4200`, `{"set":true,"number":0}`)
			jobs, err := discoverArrayFixture(t, source, synthetic+","+concrete, "4200_0")
			if err != nil || len(jobs) != 1 || jobs[0].JobID != "4200_0" || jobs[0].AttemptIndex != 0 {
				t.Fatalf("synthetic row changed task zero: jobs=%#v error=%v", jobs, err)
			}
		})
	}
}

// Invalid membership must fail discovery rather than publishing the raw job ID
// or a parent-only selector that could later target every task in an array.
func TestDiscoveryRejectsInvalidArrayIdentity(t *testing.T) {
	for _, source := range []string{"squeue", "sacct"} {
		t.Run(source, func(t *testing.T) {
			for _, test := range []struct{ name, parent, task string }{
				{"negative task", `4200`, `-1`},
				{"no value task", `4200`, `4294967294`},
				{"infinite numeric task", `4200`, `4294967295`},
				{"infinite typed task", `4200`, `{"set":true,"infinite":true,"number":0}`},
				{"unset infinite task", `4200`, `{"set":false,"infinite":true,"number":0}`},
				{"negative parent", `-1`, `0`},
				{"sentinel parent", `4294967294`, `0`},
				{"infinite parent", `{"set":true,"infinite":true,"number":4200}`, `0`},
				{"missing task", `4200`, ``},
				{"null task", `4200`, `null`},
				{"task without set", `4200`, `{"number":0}`},
				{"task without number", `4200`, `{"set":true}`},
				{"string task", `4200`, `"0"`},
				{"boolean task", `4200`, `true`},
				{"fractional task", `4200`, `0.5`},
				{"null number", `4200`, `{"set":true,"number":null}`},
				{"null set", `4200`, `{"set":null,"number":0}`},
				{"malformed parent", `"4200"`, `0`},
			} {
				t.Run(test.name, func(t *testing.T) {
					jobs, err := discoverArrayFixture(t, source, arrayIdentityRow(source, test.parent, test.task), "9002")
					if err == nil || len(jobs) != 0 {
						t.Fatalf("invalid identity produced jobs=%#v error=%v", jobs, err)
					}
					if !strings.Contains(err.Error(), source) || !strings.Contains(err.Error(), "identity") {
						t.Fatalf("missing scheduler/identity diagnostic: %v", err)
					}
				})
			}
		})
	}
}

func arrayIdentityRow(source, parent, task string) string {
	var fields []string
	parentKey, taskKey := "array_job_id", "array_task_id"
	if source == "sacct" {
		parentKey, taskKey = "job_id", "task_id"
	}
	if parent != "" {
		fields = append(fields, fmt.Sprintf("%q:%s", parentKey, parent))
	}
	if task != "" {
		fields = append(fields, fmt.Sprintf("%q:%s", taskKey, task))
	}
	identity := strings.Join(fields, ",")
	if source == "sacct" {
		return `{"job_id":9002,"restart_cnt":0,"state":{"current":"FAILED"},"array":{` + identity + `}}`
	}
	if identity != "" {
		identity = "," + identity
	}
	return `{"job_id":9002,"job_state":"RUNNING"` + identity + `}`
}

func discoverArrayFixture(t *testing.T, source, rows, requestedID string) ([]cluster.JobExecutionRecord, error) {
	t.Helper()
	command := "squeue\x00--json"
	if source == "sacct" {
		command = "sacct\x00-Xj\x00" + requestedID + "\x00--duplicates\x00--json"
	}
	scheduler := newSlurmCluster(Options{})
	scheduler.roCommands = &recordingExecutor{results: map[string][]byte{command: []byte(`{"jobs":[` + rows + `]}`)}}
	if source == "squeue" {
		return scheduler.GetJobs(context.Background())
	}
	return scheduler.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{{JobID: requestedID}})
}
