// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"go.uber.org/mock/gomock"
)

// A published failure must wait for policy-bearing Advice even after it leaves
// discovery. Restart Advice rediscovers it; termination leaves the failure final.
func TestControllerWaitsForDelayedRecoveryAdvice(t *testing.T) {
	for _, recommendation := range []pb.AdviceState{
		pb.AdviceState_ADVICE_STATE_RESTARTED,
		pb.AdviceState_ADVICE_STATE_TERMINATED,
	} {
		t.Run(recommendation.String(), func(t *testing.T) {
			ctx := context.Background()
			failed := managedJob(cluster.JobExecutionRecord{JobID: "job", AttemptIndex: 1, WorkflowID: "workflow", State: cluster.JobStateFailed})
			key := jobAttemptRef(failed)
			reader := newMockJobReader(t)
			mutator := newMockJobMutator(t)
			publicationErr := errors.New("publication unavailable")
			client := &fakeControllerClient{
				getJobsResponses: map[string]*pb.GetJobsResponse{"": {Jobs: []*pb.Job{{JobId: key.JobID, AttemptIndex: key.AttemptIndex}}}},
				upsertErrByJobID: map[string]error{key.JobID: publicationErr},
			}
			seen := state.NewMemorySeenJobStore()
			controller := Controller{JobReader: reader, HeimdallClient: client, SeenJobs: seen, ActionExecutor: mutator, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true}}

			// A failed publication retains the attempt, without scheduler mutations.
			expectActiveJobs(reader, nil)
			expectFinalizedJobs(reader, []cluster.JobAttemptRef{key}, []cluster.JobExecutionRecord{failed})
			if _, err := controller.SyncOnce(ctx); !errors.Is(err, publicationErr) {
				t.Fatalf("first SyncOnce() = %v, want publication error", err)
			}
			assertSeenRecords(t, seen, []state.SeenJobRecord{key})
			delete(client.upsertErrByJobID, key.JobID)
			expectActiveJobs(reader, nil)
			expectFinalizedJobs(reader, []cluster.JobAttemptRef{key}, []cluster.JobExecutionRecord{failed})
			if _, err := controller.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			assertSeenRecords(t, seen, nil)
			if len(client.upsertRequests) != 2 || client.upsertRequests[1].GetState() != pb.JobStatus_JOB_STATUS_FAILED {
				t.Fatalf("upserts = %v, want retried final failure publication", client.upsertRequests)
			}

			// No repeated bootstrap or accounting is needed while waiting for Advice.
			expectActiveJobs(reader, nil)
			if _, err := controller.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			advice := newAdvice(t, adviceFixture{adviceID: "decision", jobID: key.JobID, attemptIndex: key.AttemptIndex, state: adviceState(recommendation), metadata: map[string]any{"scheduler": cluster.SchedulerSlurm}})
			client.listAdviceResponses = map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{advice}}}
			expectActiveJobs(reader, nil)
			expectFinalizedJobs(reader, []cluster.JobAttemptRef{key}, []cluster.JobExecutionRecord{failed})
			if recommendation == pb.AdviceState_ADVICE_STATE_RESTARTED {
				gomock.InOrder(
					mutator.EXPECT().JobRequeueHold(gomock.Any(), key.JobID).Return(nil),
					mutator.EXPECT().JobRelease(gomock.Any(), key.JobID).Return(nil),
				)
			}
			if _, err := controller.SyncOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if recommendation == pb.AdviceState_ADVICE_STATE_RESTARTED {
				if len(client.statusUpdates) != 0 {
					t.Fatal("command success was incorrectly reported as convergence")
				}
				successor := failed
				successor.AttemptIndex++
				successor.State = cluster.JobStateRunning
				expectActiveJobs(reader, []cluster.JobExecutionRecord{successor})
				expectFinalizedJobs(reader, []cluster.JobAttemptRef{key}, []cluster.JobExecutionRecord{failed})
				if _, err := controller.SyncOnce(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if len(client.statusUpdates) != 1 || client.statusUpdates[0].Status.GetDisposition() != pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED {
				t.Fatalf("statuses = %v, want observed convergence", client.statusUpdates)
			}
			if len(client.getJobsCalls) != 1 {
				t.Fatalf("bootstrap reads = %d, want 1", len(client.getJobsCalls))
			}
		})
	}
}

func TestControllerPublishesCompletingFailureWithoutEagerRecovery(t *testing.T) {
	ctx := context.Background()
	job := managedJob(cluster.JobExecutionRecord{JobID: "job", WorkflowID: "workflow", State: cluster.JobStateRunning, RawState: "COMPLETING", Reason: "NonZeroExitCode", SchedulerMetadata: map[string]any{"slurm_reason": "NonZeroExitCode"}})
	key := jobAttemptRef(job)
	reader := newMockJobReader(t)
	client := &fakeControllerClient{}
	seen := state.NewMemorySeenJobStore()
	controller := Controller{JobReader: reader, HeimdallClient: client, SeenJobs: seen, ActionExecutor: newMockJobMutator(t), ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true}}
	expectActiveJobs(reader, []cluster.JobExecutionRecord{job})
	if _, err := controller.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertSeenRecords(t, seen, []state.SeenJobRecord{key})
	if len(client.upsertRequests) != 1 || client.upsertRequests[0].GetSchedulerMetadata().AsMap()["slurm_reason"] != "NonZeroExitCode" {
		t.Fatalf("upserts = %v, want early failure evidence", client.upsertRequests)
	}
	ended := time.Now()
	job.State, job.RawState, job.EndTime = cluster.JobStateFailed, "FAILED", &ended
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{key}, []cluster.JobExecutionRecord{job})
	if _, err := controller.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	assertSeenRecords(t, seen, nil)
	if len(client.upsertRequests) != 2 || client.upsertRequests[1].GetState() != pb.JobStatus_JOB_STATUS_FAILED {
		t.Fatalf("upserts = %v, want final accounting publication", client.upsertRequests)
	}
}
