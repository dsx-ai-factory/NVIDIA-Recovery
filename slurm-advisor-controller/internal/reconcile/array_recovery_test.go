// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobidentity"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/slurm"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/structpb"
)

// A stopped sibling cannot consume or satisfy another task's restart Advice.
// The task whose raw ID is the parent still uses its composite selector.
func TestControllerArrayAdviceIsTaskScoped(t *testing.T) {
	ctx := context.Background()
	end := time.Now()
	first := managedJob(cluster.JobExecutionRecord{JobID: "4200_0", RawJobID: "4200", ArrayJobID: "4200", ArrayTaskID: "0", AttemptIndex: 2, WorkflowID: "train", State: cluster.JobStateFailed, EndTime: &end})
	second := managedJob(cluster.JobExecutionRecord{JobID: "4200_1", RawJobID: "4201", ArrayJobID: "4200", ArrayTaskID: "1", WorkflowID: "train", State: cluster.JobStateFailed, EndTime: &end})
	stop := newAdvice(t, adviceFixture{adviceID: jobidentity.HeimdallJobID(first.JobID, 2), jobID: first.JobID, attemptIndex: 2, state: adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED), metadata: map[string]any{"scheduler": cluster.SchedulerSlurm}})
	restart := newAdvice(t, adviceFixture{adviceID: jobidentity.HeimdallJobID(second.JobID, 0), jobID: second.JobID, state: adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED), metadata: map[string]any{"scheduler": cluster.SchedulerSlurm}})
	reader := newMockJobReader(t)
	mutator := newMockJobMutator(t)
	client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{stop, restart}}}}
	controller := Controller{JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(), ActionExecutor: mutator, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true}}
	expectActiveJobs(reader, []cluster.JobExecutionRecord{first, second})
	gomock.InOrder(mutator.EXPECT().JobRequeueHold(gomock.Any(), second.JobID).Return(nil), mutator.EXPECT().JobRelease(gomock.Any(), second.JobID).Return(nil))
	if _, err := controller.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(client.statusUpdates) != 1 || client.statusUpdates[0].AdviceID != stop.GetHeimdallJobId() {
		t.Fatalf("statuses=%v", client.statusUpdates)
	}
	// Only the matching successor establishes restart convergence.
	successor := second
	successor.AttemptIndex = 1
	successor.State = cluster.JobStateRunning
	expectActiveJobs(reader, []cluster.JobExecutionRecord{first, successor})
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{jobAttemptRef(second)}, []cluster.JobExecutionRecord{second})
	if _, err := controller.SyncOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var converged bool
	for _, update := range client.statusUpdates {
		if update.AdviceID == restart.GetHeimdallJobId() {
			converged = true
		}
	}
	if !converged {
		t.Fatal("matching successor did not converge task Advice")
	}
}

func TestControllerRejectsConflictingArrayAdviceBeforeMutation(t *testing.T) {
	for _, test := range []struct {
		name      string
		metadata  map[string]any
		wrongUUID bool
	}{
		{"sibling metadata", map[string]any{"scheduler": cluster.SchedulerSlurm, "slurm_array_job_id": "4200", "slurm_array_task_id": "1"}, false},
		{"wrong raw task", map[string]any{"scheduler": cluster.SchedulerSlurm, "slurm_raw_job_id": "4201"}, false},
		{"wrong task attempt UUID", map[string]any{"scheduler": cluster.SchedulerSlurm}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			job := managedJob(cluster.JobExecutionRecord{JobID: "4200_0", RawJobID: "4200", ArrayJobID: "4200", ArrayTaskID: "0", WorkflowID: "train", State: cluster.JobStateRunning})
			advice := newAdvice(t, adviceFixture{adviceID: jobidentity.HeimdallJobID(job.JobID, 0), jobID: job.JobID, state: adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED), metadata: test.metadata})
			if test.wrongUUID {
				advice.HeimdallJobId = jobidentity.HeimdallJobID("4200_1", 0)
			}
			reader := newMockJobReader(t)
			expectActiveJobs(reader, []cluster.JobExecutionRecord{job})
			client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{advice}}}}
			controller := Controller{JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(), ActionExecutor: newMockJobMutator(t), ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true}}
			if _, err := controller.SyncOnce(context.Background()); err == nil {
				t.Fatal("conflicting identity was accepted")
			}
			if len(client.statusUpdates) != 0 {
				t.Fatal("invalid Advice claimed convergence")
			}
		})
	}
}

// The optional collaborator records identity evidence, not scheduler state.
type accountingEvidenceReader struct {
	cluster.JobReader
	identities []cluster.JobExecutionRecord
}

func (r *accountingEvidenceReader) SetAccountingIdentities(jobs []cluster.JobExecutionRecord) {
	r.identities = append([]cluster.JobExecutionRecord(nil), jobs...)
}

func TestControllerRetainsBootstrapArrayAttemptEvidence(t *testing.T) {
	ref := cluster.JobAttemptRef{JobID: "4200_0", AttemptIndex: 2}
	metadata, _ := structpb.NewStruct(map[string]any{"scheduler": cluster.SchedulerSlurm, "slurm_job_id": ref.JobID, "slurm_array_job_id": "4200", "slurm_array_task_id": "0", "slurm_submit_time": "2026-11-07T00:00:30Z", "slurm_start_time": "2026-11-07T00:00:40Z"})
	mock := newMockJobReader(t)
	mock.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
	mock.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{ref}).Return(nil, nil).Times(2)
	reader := &accountingEvidenceReader{JobReader: mock}
	client := &fakeControllerClient{getJobsResponses: map[string]*pb.GetJobsResponse{"": {Jobs: []*pb.Job{{JobId: ref.JobID, AttemptIndex: 2, SchedulerMetadata: metadata}}}}}
	controller := Controller{JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(), ActionExecutor: newMockJobMutator(t), ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve}}
	for range 2 {
		if _, err := controller.SyncOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(reader.identities) != 1 || reader.identities[0].AttemptIndex != 2 || reader.identities[0].StartTime == nil || reader.identities[0].State != "" {
			t.Fatalf("retained evidence=%#v", reader.identities)
		}
	}
	if len(client.getJobsCalls) != 1 {
		t.Fatalf("bootstrap reads=%d", len(client.getJobsCalls))
	}
}

// Production accounting may retain completed predecessors whose bootstrap
// records are no longer eligible. They must not prevent either sibling from
// applying valid Advice after a controller restart.
func TestControllerBootstrapArrayRecoveryIgnoresUnrequestedHistory(t *testing.T) {
	directory := t.TempDir()
	history := `{"jobs":[
  {"job_id":4200,"array":{"job_id":4200,"task_id":0},"name":"train","comment":{"job":"{\"APS\":{}}"},"time":{"submission":10,"start":20,"end":25},"state":{"current":"FAILED"}},
  {"job_id":4200,"array":{"job_id":4200,"task_id":0},"name":"train","comment":{"job":"{\"APS\":{}}"},"time":{"submission":30,"start":40,"end":50},"state":{"current":"FAILED"}},
  {"job_id":4201,"array":{"job_id":4200,"task_id":1},"name":"train","comment":{"job":"{\"APS\":{}}"},"time":{"submission":10,"start":20,"end":50},"state":{"current":"FAILED"}}]}`
	for name, body := range map[string]string{"active.json": `{"jobs":[]}`, "history.json": history} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	for command, fixture := range map[string]string{"squeue": "active.json", "sacct": "history.json"} {
		script := "#!/bin/sh\nexec /bin/cat '" + filepath.Join(directory, fixture) + "'\n"
		if err := os.WriteFile(filepath.Join(directory, command), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, run := range []string{"bootstrap", "restart"} {
		t.Run(run, func(t *testing.T) {
			scheduler := slurm.NewCluster(slurm.Options{})
			t.Cleanup(scheduler.Close)
			var stored []*pb.Job
			var advice []*pb.Advice
			for _, ref := range []cluster.JobAttemptRef{{JobID: "4200_0", AttemptIndex: 1}, {JobID: "4200_1", AttemptIndex: 0}} {
				submitted, started := "1970-01-01T00:00:30Z", "1970-01-01T00:00:40Z"
				if ref.JobID == "4200_1" {
					submitted, started = "1970-01-01T00:00:10Z", "1970-01-01T00:00:20Z"
				}
				metadata := map[string]any{"scheduler": cluster.SchedulerSlurm, "slurm_submit_time": submitted, "slurm_start_time": started}
				wire, err := structpb.NewStruct(metadata)
				if err != nil {
					t.Fatal(err)
				}
				stored = append(stored, &pb.Job{JobId: ref.JobID, AttemptIndex: ref.AttemptIndex, SchedulerMetadata: wire})
				advice = append(advice, newAdvice(t, adviceFixture{adviceID: jobidentity.HeimdallJobID(ref.JobID, ref.AttemptIndex), jobID: ref.JobID, attemptIndex: ref.AttemptIndex, state: adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED), metadata: map[string]any{"scheduler": cluster.SchedulerSlurm}}))
			}
			client := &fakeControllerClient{getJobsResponses: map[string]*pb.GetJobsResponse{"": {Jobs: stored}}, listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: advice}}}
			mutator := newMockJobMutator(t)
			for _, id := range []string{"4200_0", "4200_1"} {
				gomock.InOrder(mutator.EXPECT().JobRequeueHold(gomock.Any(), id).Return(nil), mutator.EXPECT().JobRelease(gomock.Any(), id).Return(nil))
			}
			controller := Controller{JobReader: scheduler, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(), ActionExecutor: mutator, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true}}
			result, err := controller.SyncOnce(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Snapshot.AccountingJobs) != 2 || len(client.upsertRequests) != 2 {
				t.Fatalf("accounting=%#v upserts=%d", result.Snapshot.AccountingJobs, len(client.upsertRequests))
			}
		})
	}
}
