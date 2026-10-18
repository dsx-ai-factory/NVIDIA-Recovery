// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"go.uber.org/mock/gomock"
)

func TestFailedJobBudgetLetsAdviceRunAndBoundsAttemptsAcrossTicks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		failedKey := cluster.JobAttemptRef{JobID: "100"}
		activeKey := cluster.JobAttemptRef{JobID: "900"}
		failed := managedJob(cluster.JobExecutionRecord{JobID: failedKey.JobID, WorkflowID: "workflow-budget", State: cluster.JobStateFailed})
		active := managedJob(cluster.JobExecutionRecord{JobID: activeKey.JobID, WorkflowID: "workflow-budget", State: cluster.JobStateRunning})
		reader := newMockJobReader(t)
		reader.EXPECT().ActiveJobs(gomock.Any()).Return([]cluster.JobExecutionRecord{active}, nil).Times(4)
		reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{failedKey}).Return([]cluster.JobExecutionRecord{failed}, nil).Times(3)
		executor := newMockJobMutator(t)
		for range 3 {
			executor.EXPECT().JobRequeueHold(gomock.Any(), failedKey.JobID).DoAndReturn(func(ctx context.Context, _ string) error {
				<-ctx.Done()
				return ctx.Err()
			})
		}
		executor.EXPECT().JobKill(gomock.Any(), activeKey.JobID).DoAndReturn(func(ctx context.Context, _ string) error {
			if err := ctx.Err(); err != nil {
				t.Fatalf("Advice inherited expired failed-job context: %v", err)
			}
			return nil
		}).Times(4)
		advice := newAdvice(t, adviceFixture{
			adviceID: "terminate-900", jobID: activeKey.JobID,
			state:    adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
			metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
		})
		seen := state.NewMemorySeenJobStore(failedKey)
		audit := &recordingAuditObserver{}
		controller := Controller{
			JobReader: reader, ActionExecutor: executor, SeenJobs: seen, AuditObserver: audit,
			HeimdallClient: &fakeControllerClient{
				getJobsResponses:    map[string]*pb.GetJobsResponse{"": {}},
				listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{advice}}},
			},
			Options:    ControllerOptions{EnableEagerRequeue: true, FailedJobRecoveryTimeout: 30 * time.Second},
			ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		}
		for tick := 1; tick <= 3; tick++ {
			started := time.Now()
			if _, err := controller.SyncOnce(ctx); !errors.Is(err, errFailedJobRecoveryBudget) {
				t.Fatalf("tick %d error = %v, want phase deadline", tick, err)
			}
			if elapsed := time.Since(started); elapsed > 30*time.Second {
				t.Fatalf("tick %d took %s, exceeded phase budget before Advice", tick, elapsed)
			}
			if len(audit.adviceActions) != tick {
				t.Fatalf("tick %d completed %d Advice commands", tick, len(audit.adviceActions))
			}
			wantSeen := []state.SeenJobRecord{activeKey}
			if tick < 3 {
				wantSeen = []state.SeenJobRecord{failedKey, activeKey}
			}
			assertSeenRecords(t, seen, wantSeen)
		}
		// The phase deadline must not reset the three-command allowance. After
		// the third deadline, later ticks neither query nor requeue the failure.
		result, err := controller.SyncOnce(ctx)
		if err != nil || len(result.FailedJobs.Requeues) != 0 {
			t.Fatalf("fourth tick = %#v, %v; want no repeated failed-job work", result.FailedJobs, err)
		}
		if len(audit.failedJobActions) != 3 {
			t.Fatalf("audited %d failed-job commands, want 3", len(audit.failedJobActions))
		}
	})
}

func TestFailedJobBudgetGivesLaterJobsATurn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		firstKey := cluster.JobAttemptRef{JobID: "100"}
		secondKey := cluster.JobAttemptRef{JobID: "200"}
		jobs := []cluster.JobExecutionRecord{
			managedJob(cluster.JobExecutionRecord{JobID: firstKey.JobID, WorkflowID: "workflow-budget", State: cluster.JobStateFailed}),
			managedJob(cluster.JobExecutionRecord{JobID: secondKey.JobID, WorkflowID: "workflow-budget", State: cluster.JobStateFailed}),
		}
		reader := newMockJobReader(t)
		reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
		reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{firstKey, secondKey}).Return(jobs, nil).Times(2)
		executor := newMockJobMutator(t)
		stall := func(ctx context.Context, _ string) error {
			<-ctx.Done()
			return ctx.Err()
		}
		gomock.InOrder(
			executor.EXPECT().JobRequeueHold(gomock.Any(), firstKey.JobID).DoAndReturn(stall),
			executor.EXPECT().JobRequeueHold(gomock.Any(), secondKey.JobID).Return(nil),
			executor.EXPECT().JobRequeueHold(gomock.Any(), firstKey.JobID).DoAndReturn(stall),
		)
		seen := state.NewMemorySeenJobStore(firstKey, secondKey)
		controller := Controller{
			JobReader: reader, ActionExecutor: executor, SeenJobs: seen,
			HeimdallClient: &fakeControllerClient{
				getJobsResponses:    map[string]*pb.GetJobsResponse{"": {}},
				listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
			},
			Options:    ControllerOptions{EnableEagerRequeue: true, FailedJobRecoveryTimeout: 30 * time.Second},
			ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		}
		if _, err := controller.SyncOnce(context.Background()); !errors.Is(err, errFailedJobRecoveryBudget) {
			t.Fatalf("first tick = %v, want phase deadline", err)
		}
		assertSeenRecords(t, seen, []state.SeenJobRecord{firstKey, secondKey})
		second, err := controller.SyncOnce(context.Background())
		if !errors.Is(err, errFailedJobRecoveryBudget) || len(second.FailedJobs.Requeues) != 2 ||
			second.FailedJobs.Requeues[0].JobID != secondKey.JobID || !second.FailedJobs.Requeues[0].Applied {
			t.Fatalf("second tick = %#v, %v; want later job recovered ahead of slow retry", second.FailedJobs, err)
		}
		assertSeenRecords(t, seen, []state.SeenJobRecord{firstKey})
	})
}

func TestFailedJobBudgetDuringBackoffPreservesRemainingAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		key := cluster.JobAttemptRef{JobID: "100"}
		job := managedJob(cluster.JobExecutionRecord{JobID: key.JobID, WorkflowID: "workflow-budget", State: cluster.JobStateFailed})
		transientErr := errors.New("scheduler temporarily unavailable")
		reader := newMockJobReader(t)
		reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(3)
		reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{key}).Return([]cluster.JobExecutionRecord{job}, nil).Times(3)
		executor := newMockJobMutator(t)
		executor.EXPECT().JobRequeueHold(gomock.Any(), key.JobID).Return(transientErr).Times(3)
		seen := state.NewMemorySeenJobStore(key)
		controller := Controller{
			JobReader: reader, ActionExecutor: executor, SeenJobs: seen,
			HeimdallClient: &fakeControllerClient{
				getJobsResponses:    map[string]*pb.GetJobsResponse{"": {}},
				listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
			},
			Options:    ControllerOptions{EnableEagerRequeue: true, FailedJobRecoveryTimeout: 100 * time.Millisecond},
			ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		}
		for tick := 1; tick <= 3; tick++ {
			_, err := controller.SyncOnce(context.Background())
			if !errors.Is(err, transientErr) {
				t.Fatalf("tick %d error = %v, want scheduler error preserved", tick, err)
			}
			if tick < 3 {
				if !errors.Is(err, errFailedJobRecoveryBudget) {
					t.Fatalf("tick %d error = %v, want interrupted backoff", tick, err)
				}
				assertSeenRecords(t, seen, []state.SeenJobRecord{key})
			} else {
				assertSeenRecords(t, seen, nil)
			}
		}
	})
}
