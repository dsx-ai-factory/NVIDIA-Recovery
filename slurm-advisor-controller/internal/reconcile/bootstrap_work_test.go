// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
)

// Bootstrap must observe backend-active attempts missing from squeue and old
// unresolved Advice targets even though the job list excludes final history.
func TestControllerBootstrapDiscoversActiveAttemptsAndOldAdvice(t *testing.T) {
	ctx := context.Background()
	active := managedJob(cluster.JobExecutionRecord{JobID: "active", WorkflowID: "workflow", State: cluster.JobStateRunning})
	missing := cluster.JobAttemptRef{JobID: "missing", AttemptIndex: 2}
	finished := managedJob(cluster.JobExecutionRecord{JobID: "old-finished", WorkflowID: "workflow", AttemptIndex: 1, State: cluster.JobStateCompleted})
	old := time.Now().Add(-365 * 24 * time.Hour)
	advice := newAdvice(t, adviceFixture{
		adviceID: "unresolved", jobID: finished.JobID, attemptIndex: finished.AttemptIndex,
		state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED), createdAt: &old,
		metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	client := &fakeControllerClient{
		getJobsResponses: map[string]*pb.GetJobsResponse{"": {Jobs: []*pb.Job{
			{JobId: active.JobID, State: pb.JobStatus_JOB_STATUS_RUNNING},
			{JobId: missing.JobID, AttemptIndex: missing.AttemptIndex, State: pb.JobStatus_JOB_STATUS_RUNNING},
		}}},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{advice}}},
	}
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{active})
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{missing, jobAttemptRef(finished)}, []cluster.JobExecutionRecord{finished})
	seen := state.NewMemorySeenJobStore()
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: seen,
		ActionExecutor: newMockJobMutator(t),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}
	result, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotKeys(t, result.Snapshot, []cluster.JobAttemptRef{jobAttemptRef(active), missing, jobAttemptRef(finished)})
	assertSeenRecords(t, seen, []state.SeenJobRecord{jobAttemptRef(active), missing})
	if len(client.statusUpdates) != 1 || client.statusUpdates[0].Status.GetDisposition() != pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED {
		t.Fatalf("status updates = %v, want old termination Advice converged", client.statusUpdates)
	}
}
