// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"fmt"
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

func TestArrayIdentityAcrossSchedulerFormats(t *testing.T) {
	cases := []struct {
		name, array, task, want string
		invalid                 bool
	}{
		{name: "plain zero", array: `4200`, task: `0`, want: "4200_0"},
		{name: "wrapped zero", array: `{"set":true,"number":4200}`, task: `{"set":true,"number":0}`, want: "4200_0"},
		{name: "mixed", array: `4200`, task: `{"set":true,"number":7}`, want: "4200_7"},
		{name: "mixed parent", array: `{"set":true,"number":4200}`, task: `7`, want: "4200_7"},
		{name: "standalone", array: `0`, task: `4294967294`, want: "4200"},
		{name: "unset parent", array: `{"set":false,"number":4200}`, task: `0`, want: "4200"},
		{name: "synthetic range", array: `4200`, task: `{"set":false,"number":0}`},
		{name: "missing set", array: `4200`, task: `{"number":0}`, invalid: true},
		{name: "missing parent set", array: `{"number":4200}`, task: `0`, invalid: true},
		{name: "empty parent", array: `{}`, task: `0`, invalid: true},
		{name: "null task", array: `4200`, task: `null`},
		{name: "negative task", array: `4200`, task: `-1`, invalid: true},
		{name: "sentinel task", array: `4200`, task: `4294967294`, invalid: true},
		{name: "infinite task", array: `4200`, task: `{"set":true,"infinite":true,"number":0}`, invalid: true},
		{name: "negative array", array: `-1`, task: `0`, invalid: true},
		{name: "sentinel array", array: `4294967295`, task: `0`, invalid: true},
		{name: "infinite array", array: `{"set":true,"infinite":true,"number":4200}`, task: `0`, invalid: true},
		{name: "string task", array: `4200`, task: `"0"`, invalid: true},
		{name: "boolean array", array: `true`, task: `0`, invalid: true},
		{name: "fractional task", array: `4200`, task: `0.5`, invalid: true},
		{name: "missing number", array: `4200`, task: `{"set":true}`, invalid: true},
		{name: "null number", array: `4200`, task: `{"set":true,"number":null}`, invalid: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			for _, source := range []struct {
				name, format string
				parse        func([]byte) ([]cluster.JobExecutionRecord, error)
			}{
				{"squeue", `{"jobs":[{"job_id":4200,"array_job_id":%s,"array_task_id":%s}]}`, observedJobsFromSqueue},
				{"sacct", `{"jobs":[{"job_id":4200,"array":{"job_id":%s,"task_id":%s}}]}`, observedJobsFromSacct},
			} {
				t.Run(source.name, func(t *testing.T) {
					jobs, err := source.parse([]byte(fmt.Sprintf(source.format, test.array, test.task)))
					if test.invalid {
						if err == nil || len(jobs) != 0 {
							t.Fatalf("invalid identity produced %v, %v", jobs, err)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if test.want == "" {
						if len(jobs) != 0 {
							t.Fatalf("synthetic row produced %v", jobs)
						}
						return
					}
					if len(jobs) != 1 || jobs[0].JobID != test.want {
						t.Fatalf("jobs = %v, want %s", jobs, test.want)
					}
					if jobs[0].SchedulerMetadata["slurm_job_id"] != test.want || jobs[0].RawJobID != "4200" {
						t.Fatalf("lost identity: %v", jobs[0])
					}
				})
			}
		})
	}
}

func TestArraySyntheticObservationDoesNotHideConcreteTask(t *testing.T) {
	jobs, err := observedJobsFromSqueue([]byte(`{"jobs":[
		{"job_id":4200,"array_job_id":4200,"array_task_id":{"set":false,"number":0}},
		{"job_id":4201,"array_job_id":4200,"array_task_id":0},
		{"job_id":4200,"array_job_id":4200,"array_task_id":1}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].JobID != "4200_0" || jobs[1].JobID != "4200_1" {
		t.Fatalf("jobs = %v", jobs)
	}
}
