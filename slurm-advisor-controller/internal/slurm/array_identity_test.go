// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobidentity"
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
				{"sacct", `{"jobs":[{"job_id":4200,"restart_cnt":0,"array":{"job_id":%s,"task_id":%s}}]}`, observedJobsFromSacct},
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

func TestArrayAccountingPreservesRetainedAttemptIdentity(t *testing.T) {
	response := []byte(`{"jobs":[
		{"job_id":4200,"array":{"job_id":4200,"task_id":0},"restart_cnt":2,"stdout_expanded":"/logs/a2","state":{"current":"FAILED"}},
		{"job_id":4201,"array":{"job_id":4200,"task_id":1},"restart_cnt":{"set":true,"number":0},"stdout_expanded":"/logs/b0","state":{"current":"COMPLETED"}},
		{"job_id":4200,"array":{"job_id":4200,"task_id":0},"restart_cnt":1,"stdout_expanded":"/logs/a1","state":{"current":"FAILED"}},
		{"job_id":4200,"array":{"job_id":4200,"task_id":0},"restart_cnt":2,"stdout_expanded":"/logs/a2","state":{"current":"FAILED"}}
	]}`)
	executor := &recordingExecutor{results: map[string][]byte{
		"sacct\x00-Xj\x004200_0\x00--duplicates\x00--json": response,
		"sacct\x00-Xj\x004200_1\x00--duplicates\x00--json": response,
	}}
	adapter := newSlurmCluster(Options{})
	adapter.roCommands = executor
	adapter.rwCommands = executor
	want := []cluster.JobAttemptRef{{JobID: "4200_0", AttemptIndex: 1}, {JobID: "4200_0", AttemptIndex: 2}, {JobID: "4200_1"}}
	jobs, err := adapter.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{want[0], want[2]})
	if err != nil {
		t.Fatal(err)
	}
	var got []cluster.JobAttemptRef
	for _, job := range jobs {
		got = append(got, cluster.JobAttemptRef{JobID: job.JobID, AttemptIndex: job.AttemptIndex})
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("retained accounting = %v, want %v", got, want)
	}
	active, err := observedJobsFromSqueue([]byte(`{"jobs":[{"job_id":4200,"array_job_id":4200,"array_task_id":0,"restart_cnt":2}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if jobidentity.HeimdallJobID(jobs[1].JobID, jobs[1].AttemptIndex) != jobidentity.HeimdallJobID(active[0].JobID, active[0].AttemptIndex) {
		t.Fatal("active and retained accounting disagree on attempt UUID")
	}
	paths, err := logPathsFromSacct(response)
	if err != nil {
		t.Fatal(err)
	}
	for index, ref := range want {
		expected := []string{"/logs/a1", "/logs/a2", "/logs/b0"}[index]
		if !reflect.DeepEqual(paths[ref].parent, []string{expected}) {
			t.Fatalf("paths for %v = %v, want %s", ref, paths[ref].parent, expected)
		}
	}
}

func TestArrayAccountingDefersUnresolvedAttempts(t *testing.T) {
	for _, count := range []string{``, `,"restart_cnt":null`, `,"restart_cnt":-1`, `,"restart_cnt":65534`, `,"restart_cnt":65535`, `,"restart_cnt":2147483648`, `,"restart_cnt":{"set":true,"number":65534}`, `,"restart_cnt":{"set":false,"number":2}`} {
		data := []byte(`{"jobs":[{"job_id":4200,"array":{"job_id":4200,"task_id":0}` + count + `}]}`)
		if jobs, err := observedJobsFromSacct(data); err == nil || len(jobs) != 0 {
			t.Fatalf("unresolved accounting produced jobs: %v, %v", jobs, err)
		}
		if paths, err := logPathsFromSacct(data); err == nil || len(paths) != 0 {
			t.Fatalf("unresolved accounting produced paths: %v, %v", paths, err)
		}
	}
}

func TestArrayAccountingKeepsResolvedSiblingWhenAnotherQueryFails(t *testing.T) {
	response := []byte(`{"jobs":[
		{"job_id":4200,"array":{"job_id":4200,"task_id":0},"restart_cnt":2,"stdout_expanded":"/logs/a2","state":{"current":"FAILED"}},
		{"job_id":4201,"array":{"job_id":4200,"task_id":1},"state":{"current":"FAILED"}}
	]}`)
	executor := &recordingExecutor{results: map[string][]byte{
		"sacct\x00-Xj\x004200_0\x00--duplicates\x00--json": response,
		"sacct\x00-Xj\x004200_1\x00--duplicates\x00--json": response,
	}}
	adapter := newSlurmCluster(Options{})
	adapter.roCommands = executor
	jobs, err := adapter.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{
		{JobID: "4200_0", AttemptIndex: 2}, {JobID: "4200_1"},
	})
	if err == nil || len(jobs) != 1 || jobs[0].JobID != "4200_0" || jobs[0].AttemptIndex != 2 {
		t.Fatalf("partial accounting = %v, %v; want resolved task and error", jobs, err)
	}
	paths, err := adapter.queryJSONLogPaths(context.Background(), "4200_0", "test",
		func(context.Context, string, ...string) (commandOutput, error) {
			return commandOutput{stdout: response}, nil
		})
	ref := cluster.JobAttemptRef{JobID: "4200_0", AttemptIndex: 2}
	if err != nil || len(paths) != 1 || !reflect.DeepEqual(paths[ref].parent, []string{"/logs/a2"}) {
		t.Fatalf("resolved task log paths = %v, %v", paths, err)
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
