// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"go.uber.org/zap"
	zapobserver "go.uber.org/zap/zaptest/observer"
)

func TestMapJobStatePreservesRequeued(t *testing.T) {
	if got := mapJobState("REQUEUED"); got != cluster.JobStateRequeued {
		t.Fatalf("mapJobState(REQUEUED) = %q, want %q", got, cluster.JobStateRequeued)
	}
}

func TestJobReadsWarnOnceForNonpositiveRecoveryCapWithoutChangingEligibility(t *testing.T) {
	for _, source := range []string{"squeue", "sacct"} {
		t.Run(source, func(t *testing.T) {
			core, logs := zapobserver.New(zap.WarnLevel)
			executor := &recordingExecutor{results: map[string][]byte{}}
			adapter := newSlurmCluster(Options{Logger: zap.New(core)})
			adapter.roCommands = executor
			for _, capField := range []string{"", `,"max_requeue_times":3`, `,"max_requeue_times":0`, `,"max_requeue_times":-1`} {
				comment := `{"APS":{"mode":"act","auto_resume_mode":"requeue","workflow_id":"private-workflow"` + capField + `}}`
				executor.results["squeue\x00--json"] = fmt.Appendf(nil, `{"jobs":[{"job_id":123,"job_state":"RUNNING","comment":%q}]}`, comment)
				executor.results["sacct\x00-Xj\x00123\x00--duplicates\x00--json"] = fmt.Appendf(nil, `{"jobs":[{"job_id":123,"state":{"current":"FAILED"},"comment":{"job":%q}}]}`, comment)
				for range 2 {
					var jobs []cluster.JobExecutionRecord
					var err error
					if source == "squeue" {
						jobs, err = adapter.GetJobs(context.Background())
					} else {
						jobs, err = adapter.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{{JobID: "123"}})
					}
					if err != nil || len(jobs) != 1 {
						t.Fatalf("read jobs = %#v, error = %v", jobs, err)
					}
					job := jobs[0]
					if !job.HeimdallManaged || job.ModeRequest.Mode != cluster.JobModeAct || !job.ModeRequest.Valid ||
						job.RecoveryConfig == nil || job.RecoveryConfig.AutoResumeMode != cluster.AutoResumeModeRequeue {
						t.Fatalf("recovery eligibility changed: %#v", job)
					}
					if capField == `,"max_requeue_times":3` {
						if job.RecoveryConfig.MaxRequeueTimes == nil || *job.RecoveryConfig.MaxRequeueTimes != 3 {
							t.Fatalf("positive cap = %v, want 3", job.RecoveryConfig.MaxRequeueTimes)
						}
					} else if job.RecoveryConfig.MaxRequeueTimes != nil {
						t.Fatalf("cap = %v, want absent cap for system-policy fallback", job.RecoveryConfig.MaxRequeueTimes)
					}
				}
			}
			if logs.Len() != 2 {
				t.Fatalf("warnings = %d, want one for each invalid raw value", logs.Len())
			}
			for i, entry := range logs.All() {
				fields := entry.ContextMap()
				if fields["job_id"] != "123" || fields["attempt_index"] != int32(0) || fields["raw_max_requeue_times"] != float64(-i) {
					t.Fatalf("warning context = %#v, want only invalid cap identity", fields)
				}
				if len(fields) != 3 || strings.Contains(fmt.Sprint(entry), "private-workflow") {
					t.Fatalf("warning contains unrelated comment data: %#v", entry)
				}
			}
		})
	}
}

func TestInvalidRecoveryCapWarningsTrackAttemptAndBoundHistory(t *testing.T) {
	core, logs := zapobserver.New(zap.WarnLevel)
	adapter := newSlurmCluster(Options{Logger: zap.New(core)})
	job := cluster.JobExecutionRecord{JobID: "123", Comment: `{"APS":{"max_requeue_times":0}}`}
	adapter.warnInvalidRecoveryCaps([]cluster.JobExecutionRecord{job, job})
	job.AttemptIndex++
	adapter.warnInvalidRecoveryCaps([]cluster.JobExecutionRecord{job, job})
	if logs.Len() != 2 {
		t.Fatalf("warnings = %d, want one per attempt", logs.Len())
	}
	for i := range invalidRecoveryCapWarningLimit {
		job.JobID = fmt.Sprint(i)
		adapter.warnInvalidRecoveryCaps([]cluster.JobExecutionRecord{job})
	}
	if len(adapter.invalidRecoveryCaps.seen) != invalidRecoveryCapWarningLimit {
		t.Fatalf("warning history = %d, want bounded history", len(adapter.invalidRecoveryCaps.seen))
	}
	count := logs.Len()
	adapter.warnInvalidRecoveryCaps([]cluster.JobExecutionRecord{job})
	if logs.Len() != count {
		t.Fatal("most recent observation was not retained")
	}
	job.JobID, job.AttemptIndex = "123", 0
	adapter.warnInvalidRecoveryCaps([]cluster.JobExecutionRecord{job})
	if logs.Len() != count+1 {
		t.Fatal("oldest evicted observation should be announced again")
	}
}
