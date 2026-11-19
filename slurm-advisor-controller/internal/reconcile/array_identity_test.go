// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"errors"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"go.uber.org/mock/gomock"
)

func TestControllerArrayCommandRetrySurvivesBootstrap(t *testing.T) {
	ctx := context.Background()
	failed := managedJob(cluster.JobExecutionRecord{
		JobID: "4200_0", AttemptIndex: 1, WorkflowID: "array", State: cluster.JobStateFailed,
	})
	sibling := managedJob(cluster.JobExecutionRecord{
		JobID: "4200_1", WorkflowID: "array", State: cluster.JobStateFailed,
	})
	restart := newAdvice(t, adviceFixture{
		adviceID: "retry", jobID: failed.JobID, attemptIndex: failed.AttemptIndex,
		state:    adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	siblingRestart := newAdvice(t, adviceFixture{
		adviceID: "sibling", jobID: sibling.JobID,
		state:    adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{
		"": {Advice: []*pb.Advice{restart, siblingRestart}},
	}}
	reader := newMockJobReader(t)
	mutator := newMockJobMutator(t)
	commandErr := errors.New("temporary requeue failure")
	gomock.InOrder(
		mutator.EXPECT().JobRequeueHold(gomock.Any(), failed.JobID).Return(commandErr),
		mutator.EXPECT().JobRequeueHold(gomock.Any(), sibling.JobID).Return(nil),
		mutator.EXPECT().JobRelease(gomock.Any(), sibling.JobID).Return(nil),
		mutator.EXPECT().JobRequeueHold(gomock.Any(), failed.JobID).Return(nil),
		mutator.EXPECT().JobRelease(gomock.Any(), failed.JobID).Return(nil),
	)
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(),
		ActionExecutor: mutator, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{jobAttemptRef(failed), jobAttemptRef(sibling)}, []cluster.JobExecutionRecord{failed, sibling})
	if _, err := controller.SyncOnce(ctx); !errors.Is(err, commandErr) {
		t.Fatalf("first tick error = %v, want transient command failure", err)
	}
	if len(client.statusUpdates) != 0 {
		t.Fatalf("command results must not establish convergence: %#v", client.statusUpdates)
	}

	siblingSuccessor := sibling
	siblingSuccessor.AttemptIndex = 1
	siblingSuccessor.State = cluster.JobStateRunning
	client.getJobsResponses = map[string]*pb.GetJobsResponse{"": {Jobs: []*pb.Job{
		{JobId: sibling.JobID, AttemptIndex: 1, State: pb.JobStatus_JOB_STATUS_RUNNING},
	}}}
	controller = Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(),
		ActionExecutor: mutator, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{jobAttemptRef(failed), jobAttemptRef(sibling), jobAttemptRef(siblingSuccessor)},
		[]cluster.JobExecutionRecord{failed, sibling, siblingSuccessor})
	if _, err := controller.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(client.getJobsCalls) != 2 {
		t.Fatalf("bootstrap calls = %d, want one per controller", len(client.getJobsCalls))
	}
	if len(client.statusUpdates) != 1 || client.statusUpdates[0].AdviceID != "sibling" || client.statusUpdates[0].Status.GetDisposition() != pb.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED {
		t.Fatalf("only sibling successor is observed: %#v", client.statusUpdates)
	}
	failedPublications := 0
	for _, request := range client.upsertRequests {
		if request.GetJobId() == failed.JobID {
			failedPublications++
			if request.GetAttemptIndex() != 1 {
				t.Fatalf("command retry advanced attempt: %v", request)
			}
		}
	}
	if failedPublications != 2 {
		t.Fatalf("failed-task publications = %d, want same attempt on both ticks", failedPublications)
	}
}
