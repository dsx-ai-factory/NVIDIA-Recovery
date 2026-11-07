// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobidentity"
)

// Accounting can retain only the latest allocation. That row must retain its
// observed restart count even when siblings and duplicates are interleaved.
func TestArrayAccountingPreservesObservedAttemptsFromPartialHistory(t *testing.T) {
	ctx := context.Background()
	current := `{"jobs":[
 {"job_id":4200,"array_job_id":4200,"array_task_id":0,"restart_cnt":2,"submit_time":30,"start_time":40},
 {"job_id":4201,"array_job_id":4200,"array_task_id":1,"restart_cnt":0,"submit_time":10,"start_time":20}]}`
	history := `{"jobs":[
 {"job_id":4201,"array":{"job_id":4200,"task_id":1},"time":{"submission":10,"start":20},"state":{"current":"RUNNING"}},
 {"job_id":4200,"array":{"job_id":4200,"task_id":0},"time":{"submission":30,"start":40},"state":{"current":"FAILED"},"steps":[{"step":{"id":"4200_0.0"}}]},
 {"job_id":4200,"array":{"job_id":4200,"task_id":0},"time":{"submission":30,"start":40},"state":{"current":"FAILED"}}]}`
	executor := &recordingExecutor{results: map[string][]byte{
		"squeue\x00--json": []byte(current),
		"sacct\x00-Xj\x004200_0\x00--duplicates\x00--json": []byte(history),
	}}
	scheduler := newSlurmCluster(Options{})
	scheduler.roCommands = executor
	active, err := scheduler.GetJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := scheduler.FinalizedJobs(ctx, []cluster.JobAttemptRef{{JobID: "4200_0", AttemptIndex: 2}})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%#v error=%v", jobs, err)
	}
	job := jobs[0]
	if job.JobID != "4200_0" || job.AttemptIndex != 2 || job.RawJobID != "4200" || job.State != cluster.JobStateFailed {
		t.Fatalf("wrong task attempt: %#v", job)
	}
	if jobidentity.HeimdallJobID(job.JobID, job.AttemptIndex) != jobidentity.HeimdallJobID(active[0].JobID, active[0].AttemptIndex) {
		t.Fatal("active/accounting UUID disagree")
	}
	if job.SchedulerMetadata["slurm_attempt_index"] != int32(2) {
		t.Fatalf("metadata=%#v", job.SchedulerMetadata)
	}
}

func TestArrayAccountingDefersUnknownOrConflictingAttempt(t *testing.T) {
	for _, test := range []struct {
		name, rows  string
		wantAttempt int32
		wantError   bool
	}{
		{"explicit counter", `{"job_id":4200,"array":{"job_id":4200,"task_id":0},"restart_cnt":3}`, 3, false},
		{"missing counter and anchor", `{"job_id":4200,"array":{"job_id":4200,"task_id":0},"time":{"submission":30,"start":40}}`, 0, true},
		{"negative count", `{"job_id":4200,"array":{"job_id":4200,"task_id":0},"restart_cnt":-1}`, 0, true},
		{"overflow count", `{"job_id":4200,"array":{"job_id":4200,"task_id":0},"restart_cnt":2147483648}`, 0, true},
		{"historical rows share updated count", `{"job_id":4200,"array":{"job_id":4200,"task_id":0},"restart_cnt":2,"time":{"submission":10,"start":20}}, {"job_id":4200,"array":{"job_id":4200,"task_id":0},"restart_cnt":2,"time":{"submission":30,"start":40}}`, 2, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheduler := newSlurmCluster(Options{})
			scheduler.roCommands = &recordingExecutor{results: map[string][]byte{"sacct\x00-Xj\x004200_0\x00--duplicates\x00--json": []byte(`{"jobs":[` + test.rows + `]}`)}}
			jobs, err := scheduler.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{{JobID: "4200_0", AttemptIndex: test.wantAttempt}})
			if test.wantError {
				if err == nil || len(jobs) != 0 {
					t.Fatalf("jobs=%#v error=%v", jobs, err)
				}
				if !strings.Contains(err.Error(), "attempt") {
					t.Fatalf("missing attempt diagnostic: %v", err)
				}
				return
			}
			if err != nil || len(jobs) != 1 || jobs[0].AttemptIndex != test.wantAttempt {
				t.Fatalf("jobs=%#v error=%v", jobs, err)
			}
		})
	}
}

func TestArrayMutationsAlwaysUseConcreteTaskSelector(t *testing.T) {
	ctx := context.Background()
	executor := &recordingExecutor{results: map[string][]byte{"squeue\x00-j\x004200_0\x00-s\x00-o\x00%i\x00-h": []byte("4200_0.batch\n4200_0.0\n")}}
	scheduler := newSlurmCluster(Options{})
	scheduler.roCommands, scheduler.rwCommands = executor, executor
	for _, action := range []func() error{
		func() error { return scheduler.JobRequeueHold(ctx, "4200_0") },
		func() error { return scheduler.JobNodeUpdate(ctx, "4200_0", []string{"node1"}, nil, 0) },
		func() error { return scheduler.JobRelease(ctx, "4200_0") },
		func() error { return scheduler.JobKill(ctx, "4200_0") },
		func() error { return scheduler.JobSignal(ctx, "4200_0", "SIGTERM") },
	} {
		if err := action(); err != nil {
			t.Fatal(err)
		}
	}
	assertCommandArgs(t, executor.calls, [][]string{
		{"scontrol", "requeuehold", "4200_0"},
		{"scontrol", "update", "JobID=4200_0", "ExcNodeList=node1"},
		{"scontrol", "release", "4200_0"},
		{"scancel", "4200_0"},
		{"squeue", "-j", "4200_0", "-s", "-o", "%i", "-h"},
		{"scancel", "--ctld", "--signal=SIGTERM", "4200_0.0"},
	})
}

// A count must never wrap into an earlier attempt and replenish its allowance.
func TestArrayDiscoveryRejectsUnrepresentableRestartCount(t *testing.T) {
	for _, count := range []string{"-1", "2147483648", "4294967296"} {
		t.Run(count, func(t *testing.T) {
			scheduler := newSlurmCluster(Options{})
			scheduler.roCommands = &recordingExecutor{results: map[string][]byte{"squeue\x00--json": []byte(`{"jobs":[{"job_id":4200,"array_job_id":4200,"array_task_id":0,"restart_cnt":` + count + `}]}`)}}
			jobs, err := scheduler.GetJobs(context.Background())
			if err == nil || len(jobs) != 0 {
				t.Fatalf("jobs=%#v error=%v", jobs, err)
			}
		})
	}
}

// Old accounting remains after its completed attempt has left the local cache.
// Neither live discovery nor bootstrap should need to retain its anchor forever.
func TestArrayAccountingIgnoresUnrequestedOlderHistory(t *testing.T) {
	for _, source := range []string{"active", "bootstrap"} {
		t.Run(source, func(t *testing.T) {
			scheduler := newSlurmCluster(Options{})
			old := `{"job_id":4200,"array_job_id":4200,"array_task_id":0,"restart_cnt":0,"submit_time":10,"start_time":20}`
			current := `{"job_id":4200,"array_job_id":4200,"array_task_id":0,"restart_cnt":1,"submit_time":30,"start_time":40}`
			sibling := `{"job_id":4201,"array_job_id":4200,"array_task_id":1,"restart_cnt":0,"submit_time":10,"start_time":20}`
			history := []byte(`{"jobs":[
    {"job_id":4200,"array":{"job_id":4200,"task_id":0},"time":{"submission":10,"start":20},"state":{"current":"FAILED"}},
    {"job_id":4200,"array":{"job_id":4200,"task_id":0},"time":{"submission":30,"start":40},"state":{"current":"FAILED"}},
    {"job_id":4201,"array":{"job_id":4200,"task_id":1},"time":{"submission":10,"start":20},"state":{"current":"FAILED"}}]}`)
			executor := &recordingExecutor{results: map[string][]byte{
				"squeue\x00--json": []byte(`{"jobs":[` + old + `]}`),
				"sacct\x00-Xj\x004200_0\x00--duplicates\x00--json": history,
				"sacct\x00-Xj\x004200_1\x00--duplicates\x00--json": history,
			}}
			scheduler.roCommands = executor
			if source == "active" {
				if _, err := scheduler.GetJobs(context.Background()); err != nil {
					t.Fatal(err)
				}
				executor.results["squeue\x00--json"] = []byte(`{"jobs":[` + current + `,` + sibling + `]}`)
				if _, err := scheduler.GetJobs(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else {
				submittedA, startedA := time.Unix(30, 0), time.Unix(40, 0)
				submittedB, startedB := time.Unix(10, 0), time.Unix(20, 0)
				scheduler.SetAccountingIdentities([]cluster.JobExecutionRecord{
					{JobID: "4200_0", AttemptIndex: 1, SubmitTime: &submittedA, StartTime: &startedA},
					{JobID: "4200_1", AttemptIndex: 0, SubmitTime: &submittedB, StartTime: &startedB},
				})
			}
			requested := []cluster.JobAttemptRef{{JobID: "4200_0", AttemptIndex: 1}, {JobID: "4200_1", AttemptIndex: 0}}
			jobs, err := scheduler.FinalizedJobs(context.Background(), requested)
			if err != nil || len(jobs) != 2 {
				t.Fatalf("jobs=%#v error=%v", jobs, err)
			}
			for index, job := range jobs {
				if jobAttemptRef(job) != requested[index] {
					t.Fatalf("wrong attempt: %#v", job)
				}
			}
		})
	}
}

// Missing evidence for the requested attempt or a possible newer successor
// remains fatal, even if some historical allocation can be identified.
func TestArrayAccountingRejectsUnresolvedRequestedOrNewerAttempt(t *testing.T) {
	for _, test := range []struct {
		name, extra string
		requested   int32
	}{
		{"requested older attempt missing", ``, 0},
		{"unanchored newer successor", `,{"job_id":4200,"array":{"job_id":4200,"task_id":0},"time":{"submission":50,"start":60}}`, 1},
		{"conflicting old counter", `,{"job_id":4200,"array":{"job_id":4200,"task_id":0},"restart_cnt":-1,"time":{"submission":10,"start":20}}`, 1},
		{"ambiguous old timestamps", `,{"job_id":4200,"array":{"job_id":4200,"task_id":0}}`, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			scheduler := newSlurmCluster(Options{})
			submitted, started := time.Unix(30, 0), time.Unix(40, 0)
			scheduler.SetAccountingIdentities([]cluster.JobExecutionRecord{{JobID: "4200_0", AttemptIndex: 1, SubmitTime: &submitted, StartTime: &started}})
			scheduler.roCommands = &recordingExecutor{results: map[string][]byte{
				"sacct\x00-Xj\x004200_0\x00--duplicates\x00--json": []byte(`{"jobs":[{"job_id":4200,"array":{"job_id":4200,"task_id":0},"time":{"submission":30,"start":40}}` + test.extra + `]}`),
			}}
			if _, err := scheduler.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{{JobID: "4200_0", AttemptIndex: test.requested}}); err == nil {
				t.Fatal("uncertain accounting accepted")
			}
		})
	}
}
