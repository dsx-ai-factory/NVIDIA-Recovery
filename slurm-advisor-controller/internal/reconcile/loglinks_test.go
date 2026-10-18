// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/loglinks"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"github.com/google/go-cmp/cmp"
	"go.uber.org/mock/gomock"
)

func TestControllerDoesNotSyncLogLinksForIncompleteSnapshot(t *testing.T) {
	readErr := errors.New("squeue unavailable")
	reader := newMockJobReader(t)
	reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, readErr)
	materializer := &recordingLogLinkMaterializer{}
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses:    map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
		},
		SeenJobs:            state.NewMemorySeenJobStore(),
		LogLinkMaterializer: materializer,
	}

	_, err := controller.SyncOnce(context.Background())
	if !errors.Is(err, readErr) {
		t.Fatalf("SyncOnce() error = %v, want %v", err, readErr)
	}
	if len(materializer.calls) != 0 {
		t.Fatalf("materializer enqueues = %d, want none for incomplete snapshot", len(materializer.calls))
	}
	if materializer.drains != 0 {
		t.Fatalf("materializer drains = %d, want none for incomplete snapshot", materializer.drains)
	}
}

func TestLogLinkTargetsAreDeterministicAndLimitedToManagedActiveNonIgnoredJobs(t *testing.T) {
	snapshot := newControllerSnapshot(time.Now())
	addActiveJob(&snapshot, logLinkJob("200", 2, "100", true, []string{
		"/work/200/z.log",
		"/work/200/a.log",
		"/work/200/z.log",
	}))
	addActiveJob(&snapshot, logLinkJob("200", 1, "100", true, []string{
		"/work/200/attempt-1.log",
	}))
	addActiveJob(&snapshot, logLinkJob("100", 1, "", true, []string{"/work/100/stdout.log"}))
	addActiveJob(&snapshot, logLinkJob("300", 3, "", false, []string{"/work/300/stdout.log"}))
	addAccountingJob(&snapshot, logLinkJob("050", 4, "", true, []string{"/work/050/stdout.log"}))
	snapshot.EffectiveModes[cluster.JobAttemptRef{JobID: "100", AttemptIndex: 1}] = EffectiveModeDecision{
		Mode: cluster.JobModeIgnore,
	}
	snapshot.EffectiveModes[cluster.JobAttemptRef{JobID: "200", AttemptIndex: 2}] = EffectiveModeDecision{
		Mode: cluster.JobModeObserve,
	}
	snapshot.EffectiveModes[cluster.JobAttemptRef{JobID: "200", AttemptIndex: 1}] = EffectiveModeDecision{
		Mode: cluster.JobModeAct,
	}
	snapshot.EffectiveModes[cluster.JobAttemptRef{JobID: "300", AttemptIndex: 3}] = EffectiveModeDecision{
		Mode: cluster.JobModeAct,
	}
	snapshot.EffectiveModes[cluster.JobAttemptRef{JobID: "050", AttemptIndex: 4}] = EffectiveModeDecision{
		Mode: cluster.JobModeObserve,
	}

	got := logLinkTargets(snapshot)
	want := []loglinks.Target{
		{
			JobID:        "200",
			AttemptIndex: 1,
			ArrayJobID:   "100",
			Paths:        []string{"/work/200/attempt-1.log"},
		},
		{
			JobID:        "200",
			AttemptIndex: 2,
			ArrayJobID:   "100",
			Paths:        []string{"/work/200/a.log", "/work/200/z.log"},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("logLinkTargets() mismatch (-want +got):\n%s", diff)
	}
}

func TestControllerLogLinkErrorsDoNotBlockUpsertOrSchedulerAction(t *testing.T) {
	observedAt := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	job := logLinkJob("12345", 7, "12000", true, []string{"/work/12345/stdout.log"})
	job.WorkflowID = "workflow-12345"
	job.State = cluster.JobStateRunning
	job.ModeRequest = cluster.JobModeRequest{
		Mode:      cluster.JobModeAct,
		Specified: true,
		Valid:     true,
	}
	advice := newAdvice(t, adviceFixture{
		adviceID:     "terminate-12345",
		jobID:        job.JobID,
		attemptIndex: job.AttemptIndex,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
	})

	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{job})
	client := &fakeControllerClient{
		getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{
			"": {Advice: []*pb.Advice{advice}},
		},
	}
	executor := newMockJobMutator(t)
	executor.EXPECT().JobKill(gomock.Any(), job.JobID).Return(nil)
	processCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	syncer := newControlledLogLinkSyncer()
	materializer := NewAsyncLogLinkMaterializer(processCtx, syncer)
	observer := &recordingObserver{}
	controller := Controller{
		JobReader:           reader,
		HeimdallClient:      client,
		SeenJobs:            state.NewMemorySeenJobStore(),
		ActionExecutor:      executor,
		LogLinkMaterializer: materializer,
		Observer:            observer,
		ModePolicy:          ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Now:                 func() time.Time { return observedAt },
	}

	type syncResult struct {
		result ControllerSyncResult
		err    error
	}
	resultCh := make(chan syncResult, 1)
	go func() {
		result, err := controller.SyncOnce(context.Background())
		resultCh <- syncResult{result: result, err: err}
	}()

	call := receiveLogLinkSyncCall(t, syncer.started)
	var sync syncResult
	select {
	case sync = <-resultCh:
	case <-time.After(time.Second):
		t.Fatal("SyncOnce() blocked while log-link filesystem synchronization was stuck")
	}
	if sync.err != nil {
		t.Fatalf("SyncOnce() failed despite materializer errors: %v", sync.err)
	}
	if len(client.upsertRequests) != 1 {
		t.Fatalf("job upserts = %d, want 1", len(client.upsertRequests))
	}
	if sync.result.LogLinks.Completed {
		t.Fatalf("result log links = %#v, want no prior completed work", sync.result.LogLinks)
	}
	if !call.observedAt.Equal(observedAt) {
		t.Fatalf("materializer observedAt = %s, want %s", call.observedAt, observedAt)
	}
	wantTargets := []loglinks.Target{{
		JobID:        job.JobID,
		AttemptIndex: job.AttemptIndex,
		ArrayJobID:   job.ArrayJobID,
		Paths:        []string{"/work/12345/stdout.log"},
	}}
	if diff := cmp.Diff(wantTargets, call.targets); diff != "" {
		t.Fatalf("materializer targets mismatch (-want +got):\n%s", diff)
	}
	if len(observer.observations) != 1 || observer.observations[0].LogLinks.Completed {
		t.Fatalf("sync observations = %#v, want no prior completed log-link work", observer.observations)
	}
	if observer.observations[0].Outcome != ObservationOutcomeSuccess {
		t.Fatalf(
			"sync outcome = %q, want success despite materializer errors",
			observer.observations[0].Outcome,
		)
	}

	syncer.responses <- loglinks.Observation{Errors: 2}
}

func logLinkJob(
	jobID string,
	attemptIndex int32,
	arrayJobID string,
	managed bool,
	paths []string,
) cluster.JobExecutionRecord {
	return cluster.JobExecutionRecord{
		JobID:           jobID,
		AttemptIndex:    attemptIndex,
		ArrayJobID:      arrayJobID,
		HeimdallManaged: managed,
		SchedulerMetadata: map[string]any{
			"slurm_log_file_paths": slices.Clone(paths),
		},
	}
}

type logLinkMaterializerCall struct {
	observedAt time.Time
	targets    []loglinks.Target
}

type recordingLogLinkMaterializer struct {
	observation LogLinkObservation
	calls       []logLinkMaterializerCall
	drains      int
}

func (m *recordingLogLinkMaterializer) Enqueue(
	observedAt time.Time,
	targets []loglinks.Target,
) {
	copiedTargets := make([]loglinks.Target, len(targets))
	for index := range targets {
		copiedTargets[index] = targets[index]
		copiedTargets[index].Paths = slices.Clone(targets[index].Paths)
	}
	m.calls = append(m.calls, logLinkMaterializerCall{
		observedAt: observedAt,
		targets:    copiedTargets,
	})
}

func (m *recordingLogLinkMaterializer) Drain() LogLinkObservation {
	m.drains++
	return m.observation
}
