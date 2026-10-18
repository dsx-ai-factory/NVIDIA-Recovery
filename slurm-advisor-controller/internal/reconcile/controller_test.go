// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster/clustermock"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/heimdall"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"go.uber.org/mock/gomock"
)

func TestControllerBuildsBaseSnapshotFromActiveHeimdallAdviceAndSeenKeys(t *testing.T) {
	ctx := context.Background()
	nextAdvicePage := "next"
	activeJob := managedJob(cluster.JobExecutionRecord{
		JobID:        "12345",
		AttemptIndex: 1,
		JobName:      "train",
		WorkflowID:   "workflow-a",
		State:        cluster.JobStateRunning,
		Partition:    "normal",
	})
	heimdallJob := &pb.Job{
		JobId:        "67890",
		AttemptIndex: 2,
		State:        pb.JobStatus_JOB_STATUS_RUNNING,
	}
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata: map[string]any{
			"scheduler":   cluster.SchedulerSlurm,
			"managed_by":  cluster.ManagedBySlurmAdvisor,
			"environment": "prod",
		},
	})
	terminateAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-terminate",
		jobID:        "24680",
		attemptIndex: 0,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{activeJob})
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{
		{JobID: "24680", AttemptIndex: 0},
		{JobID: "67890", AttemptIndex: 2},
		{JobID: "99999", AttemptIndex: 3},
	}, nil)
	client := &fakeControllerClient{
		getJobsResponses: map[string]*pb.GetJobsResponse{
			"": {Jobs: []*pb.Job{heimdallJob}},
		},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{
			"":     {Advice: []*pb.Advice{restartAdvice}, NextPageToken: &nextAdvicePage},
			"next": {Advice: []*pb.Advice{terminateAdvice}},
		},
	}
	seen := state.NewMemorySeenJobStore(state.SeenJobRecord{JobID: "99999", AttemptIndex: 3})
	controller := Controller{
		JobReader:      reader,
		HeimdallClient: client,
		SeenJobs:       seen,
		Options: ControllerOptions{
			JobPageLimit:       25,
			AdvicePageLimit:    2,
			Environment:        "prod",
			PartitionAllowlist: []string{"normal"},
		},
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve},
	}

	result, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}

	if diff := cmp.Diff([]fakeGetJobsCall{{Limit: 25}}, client.getJobsCalls); diff != "" {
		t.Fatalf("GetJobs calls mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]fakeListAdviceCall{{Limit: 2, OnlyActive: true}}, client.listAdviceCalls); diff != "" {
		t.Fatalf("ListAdvice calls mismatch (-want +got):\n%s", diff)
	}
	assertSnapshotKeys(t, result.Snapshot, []cluster.JobAttemptRef{
		{JobID: "12345", AttemptIndex: 1},
		{JobID: "24680", AttemptIndex: 0},
		{JobID: "67890", AttemptIndex: 2},
		{JobID: "99999", AttemptIndex: 3},
	})
	if _, ok := result.Snapshot.ActiveJobs[cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 1}]; !ok {
		t.Fatal("active Slurm job missing from snapshot")
	}
	if _, ok := result.Snapshot.HeimdallJobs[cluster.JobAttemptRef{JobID: "67890", AttemptIndex: 2}]; !ok {
		t.Fatal("Heimdall job missing from snapshot")
	}
	if len(result.Snapshot.Advice[cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 1}]) != 1 {
		t.Fatal("restart advice missing from snapshot")
	}
	wantReq, err := heimdall.BuildCreateOrUpdateJobRequest(activeJob, heimdall.UpsertRequestOptions{})
	if err != nil {
		t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
	}
	assertRequestsEqual(t, client.upsertRequests, []*pb.CreateOrUpdateJobRequest{wantReq})
}

func TestControllerGatesEnvironmentBeforePublicationAndForgetsRejectedSeenJob(t *testing.T) {
	matching := managedJob(cluster.JobExecutionRecord{
		JobID: "blue-job", WorkflowID: "blue-workflow", State: cluster.JobStateRunning,
		EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "blue", Specified: true, Valid: true},
		SchedulerMetadata:  map[string]any{"slurm_log_file_paths": []string{"/data/blue.log"}},
	})
	rejected := managedJob(cluster.JobExecutionRecord{
		JobID: "green-job", WorkflowID: "green-workflow", State: cluster.JobStateFailed,
		ModeRequest:        cluster.JobModeRequest{Mode: cluster.JobModeAct, Specified: true, Valid: true},
		EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "green", Specified: true, Valid: true},
		RecoveryConfig:     &cluster.RecoveryConfig{AutoResumeMode: cluster.AutoResumeModeRequeue},
		SchedulerMetadata:  map[string]any{"slurm_log_file_paths": []string{"/data/green.log"}},
	})
	rejected.ModeRequest = cluster.JobModeRequest{Mode: cluster.JobModeAct, Specified: true, Valid: true}
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{matching, rejected})
	rejectedAdvice := newAdvice(t, adviceFixture{
		adviceID: "terminate-green", jobID: rejected.JobID,
		state:    adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
		metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{rejectedAdvice}}}}
	seen := state.NewMemorySeenJobStore(cluster.JobAttemptRef{JobID: rejected.JobID})
	observer := &recordingObserver{}
	materializer := &recordingLogLinkMaterializer{}
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: seen, Environment: "blue",
		Options: ControllerOptions{Environment: "blue"}, Observer: observer, LogLinkMaterializer: materializer,
		ActionExecutor: newMockJobMutator(t), ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.SyncOnce(context.Background())
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	if len(client.upsertRequests) != 1 || client.upsertRequests[0].GetJobId() != matching.JobID {
		t.Fatalf("upserted jobs = %#v, want only %q", client.upsertRequests, matching.JobID)
	}
	if _, ok := result.Snapshot.ActiveJobs[jobAttemptRef(rejected)]; ok {
		t.Fatal("environment-rejected job reached the admitted snapshot")
	}
	if got := result.Snapshot.EnvironmentAdmissions[jobAttemptRef(rejected)]; got.Admitted || got.Reason != EnvironmentAdmissionMismatch {
		t.Fatalf("rejected decision = %#v, want environment mismatch", got)
	}
	assertSeenRecords(t, seen, []state.SeenJobRecord{{JobID: matching.JobID}})
	if len(observer.observations) != 1 || len(observer.observations[0].ModeDecisions) != 1 {
		t.Fatalf("mode decisions = %#v, want exactly the admitted job", observer.observations)
	}
	if diff := cmp.Diff([]OperationMode{ActMode}, observer.startedModes); diff != "" {
		t.Fatalf("started modes mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]ReconcileStage{
		StageJobStatusPolling,
		StageJobReconciliation,
		StageAdviceReconciliation,
	}, observer.startedStages); diff != "" {
		t.Fatalf("started stages mismatch (-want +got):\n%s", diff)
	}
	if observer.completed != 1 {
		t.Fatalf("completed progress callbacks = %d, want 1", observer.completed)
	}
	if got := result.Snapshot.EnvironmentAdmissionCount(EnvironmentAdmissionMismatch); got != 1 {
		t.Fatalf("environment mismatch count = %d, want 1", got)
	}
	if len(materializer.calls) != 1 || len(materializer.calls[0].targets) != 1 || materializer.calls[0].targets[0].JobID != matching.JobID {
		t.Fatalf("log-link targets = %#v, want only %q", materializer.calls, matching.JobID)
	}
	if len(result.FailedJobs.Requeues) != 0 || len(result.Advice.Skips) != 0 {
		t.Fatalf("rejected job reached recovery/advice reconciliation: failed=%#v advice=%#v", result.FailedJobs, result.Advice)
	}
}

func TestControllerEnrichesOnlyEnvironmentAdmittedActiveJobs(t *testing.T) {
	admitted := managedJob(cluster.JobExecutionRecord{
		JobID: "admitted", State: cluster.JobStateRunning,
		EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "blue", Specified: true, Valid: true},
	})
	rejected := managedJob(cluster.JobExecutionRecord{
		JobID: "rejected", State: cluster.JobStateRunning,
		EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "green", Specified: true, Valid: true},
	})
	blockedOlderAttempt := managedJob(cluster.JobExecutionRecord{
		JobID: "changed-route", AttemptIndex: 0, State: cluster.JobStateRunning,
		EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "blue", Specified: true, Valid: true},
	})
	rejectedCurrentAttempt := managedJob(cluster.JobExecutionRecord{
		JobID: "changed-route", AttemptIndex: 1, State: cluster.JobStateRunning,
		EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "green", Specified: true, Valid: true},
	})

	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{
		admitted,
		rejected,
		blockedOlderAttempt,
		rejectedCurrentAttempt,
	})
	enricher := clustermock.NewMockActiveJobEnricher(gomock.NewController(t))
	enriched := admitted
	enriched.SchedulerMetadata = make(map[string]any, len(admitted.SchedulerMetadata)+1)
	for key, value := range admitted.SchedulerMetadata {
		enriched.SchedulerMetadata[key] = value
	}
	enriched.SchedulerMetadata["enriched"] = true
	enricher.EXPECT().
		EnrichActiveJobs(gomock.Any(), []cluster.JobExecutionRecord{admitted}).
		Return([]cluster.JobExecutionRecord{enriched})

	controller := Controller{
		JobReader:         reader,
		ActiveJobEnricher: enricher,
		HeimdallClient:    &fakeControllerClient{},
		SeenJobs:          state.NewMemorySeenJobStore(),
		Environment:       "blue",
	}

	observation := SyncObservation{}
	snapshot, err := controller.buildBaseSnapshot(context.Background(), &observation)
	if err != nil {
		t.Fatalf("buildBaseSnapshot() failed: %v", err)
	}
	assertSourceObservation(t, observation.Sources, SourceSlurmLogMetadata, ObservationOutcomeSuccess, 1)
	if len(snapshot.ActiveJobs) != 1 {
		t.Fatalf("active jobs = %#v, want only admitted job", snapshot.ActiveJobs)
	}
	got := snapshot.ActiveJobs[jobAttemptRef(admitted)]
	if enriched, _ := got.SchedulerMetadata["enriched"].(bool); !enriched {
		t.Fatalf("admitted job metadata = %#v, want enrichment", got.SchedulerMetadata)
	}
}

func TestControllerReevaluatesEnvironmentFromEachFreshSlurmPoll(t *testing.T) {
	job := managedJob(cluster.JobExecutionRecord{
		JobID: "12345", WorkflowID: "workflow-12345", State: cluster.JobStateRunning,
		EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "blue", Specified: true, Valid: true},
	})
	routedAway := job
	routedAway.EnvironmentRequest.Environment = "green"

	reader := newMockJobReader(t)
	gomock.InOrder(
		reader.EXPECT().ActiveJobs(gomock.Any()).Return([]cluster.JobExecutionRecord{job}, nil),
		reader.EXPECT().ActiveJobs(gomock.Any()).Return([]cluster.JobExecutionRecord{routedAway}, nil),
		reader.EXPECT().ActiveJobs(gomock.Any()).Return([]cluster.JobExecutionRecord{job}, nil),
	)
	client := &fakeControllerClient{}
	seen := state.NewMemorySeenJobStore()
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: seen, Environment: "blue",
		Options: ControllerOptions{Environment: "blue"}, ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve},
	}

	if _, err := controller.SyncOnce(context.Background()); err != nil {
		t.Fatalf("matching SyncOnce() failed: %v", err)
	}
	if len(client.upsertRequests) != 1 {
		t.Fatalf("matching upserts = %d, want 1", len(client.upsertRequests))
	}
	assertSeenRecords(t, seen, []state.SeenJobRecord{{JobID: job.JobID}})

	if _, err := controller.SyncOnce(context.Background()); err != nil {
		t.Fatalf("routed-away SyncOnce() failed: %v", err)
	}
	if len(client.upsertRequests) != 1 {
		t.Fatalf("routed-away upserts = %d, want no new publication", len(client.upsertRequests))
	}
	assertSeenRecords(t, seen, nil)

	if _, err := controller.SyncOnce(context.Background()); err != nil {
		t.Fatalf("routed-back SyncOnce() failed: %v", err)
	}
	if len(client.upsertRequests) != 2 {
		t.Fatalf("routed-back upserts = %d, want publication to resume", len(client.upsertRequests))
	}
	assertSeenRecords(t, seen, []state.SeenJobRecord{{JobID: job.JobID}})
}

func TestControllerGatesAccountingEnvironmentForAdmittedActiveAttempt(t *testing.T) {
	active := managedJob(cluster.JobExecutionRecord{
		JobID: "12345", WorkflowID: "workflow-12345", State: cluster.JobStateRunning,
		RawState:           "NODE_FAIL",
		EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "blue", Specified: true, Valid: true},
	})
	accounting := managedJob(cluster.JobExecutionRecord{
		JobID: "12345", WorkflowID: "workflow-12345", State: cluster.JobStateFailed,
		RawState: "NODE_FAIL", RecoveryConfig: &cluster.RecoveryConfig{AutoResumeMode: cluster.AutoResumeModeRequeue},
		EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "green", Specified: true, Valid: true},
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{active})
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{{JobID: active.JobID}}, []cluster.JobExecutionRecord{accounting})
	client := &fakeControllerClient{}
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: state.NewMemorySeenJobStore(),
		Environment: "blue", Options: ControllerOptions{Environment: "blue"},
		ActionExecutor: newMockJobMutator(t), ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.SyncOnce(context.Background())
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	if _, ok := result.Snapshot.AccountingJobs[jobAttemptRef(accounting)]; ok {
		t.Fatal("environment-rejected accounting record reached the snapshot")
	}
	if len(result.FailedJobs.Requeues) != 0 {
		t.Fatalf("rejected accounting record produced failed recovery: %#v", result.FailedJobs.Requeues)
	}
	if len(client.upsertRequests) != 1 || client.upsertRequests[0].GetState() != pb.JobStatus_JOB_STATUS_RUNNING {
		t.Fatalf("upserts = %#v, want only the admitted active record", client.upsertRequests)
	}
}

func TestControllerRejectedCurrentAttemptBlocksOlderAdviceAndAccounting(t *testing.T) {
	current := managedJob(cluster.JobExecutionRecord{
		JobID: "12345", AttemptIndex: 1, WorkflowID: "workflow-12345", State: cluster.JobStateRunning,
		EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "green", Specified: true, Valid: true},
	})
	oldAdvice := newAdvice(t, adviceFixture{
		adviceID: "restart-old-attempt", jobID: current.JobID, attemptIndex: 0,
		state:    adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{current})
	client := &fakeControllerClient{listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {Advice: []*pb.Advice{oldAdvice}}}}
	seen := state.NewMemorySeenJobStore(
		cluster.JobAttemptRef{JobID: current.JobID, AttemptIndex: 0},
		cluster.JobAttemptRef{JobID: current.JobID, AttemptIndex: 1},
	)
	observer := &recordingObserver{}
	controller := Controller{
		JobReader: reader, HeimdallClient: client, SeenJobs: seen,
		Environment: "blue", Options: ControllerOptions{Environment: "blue"}, Observer: observer,
		ActionExecutor: newMockJobMutator(t), ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.SyncOnce(context.Background())
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	if len(result.Snapshot.ActiveJobs) != 0 || len(result.Snapshot.AccountingJobs) != 0 {
		t.Fatalf("rejected job reached Slurm snapshot maps: active=%#v accounting=%#v", result.Snapshot.ActiveJobs, result.Snapshot.AccountingJobs)
	}
	if len(client.upsertRequests) != 0 || len(result.FailedJobs.Requeues) != 0 || len(result.Advice.Skips) != 0 {
		t.Fatalf("older attempt participated after current rejection: upserts=%#v failed=%#v advice=%#v", client.upsertRequests, result.FailedJobs, result.Advice)
	}
	assertSeenRecords(t, seen, nil)
	if len(observer.observations) != 1 || len(observer.observations[0].ModeDecisions) != 0 {
		t.Fatalf("mode decisions = %#v, want none", observer.observations)
	}
}

func TestControllerActModeSkipsAdviceWithoutCurrentSlurmRecord(t *testing.T) {
	advice := newAdvice(t, adviceFixture{
		adviceID: "terminate-missing",
		jobID:    "24680",
		state:    adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
		metadata: map[string]any{
			"scheduler":  cluster.SchedulerSlurm,
			"managed_by": cluster.ManagedBySlurmAdvisor,
		},
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{{JobID: "24680"}}, nil)
	client := &fakeControllerClient{
		getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{
			"": {Advice: []*pb.Advice{advice}},
		},
	}
	controller := Controller{
		JobReader:      reader,
		HeimdallClient: client,
		SeenJobs:       state.NewMemorySeenJobStore(),
		ActionExecutor: newMockJobMutator(t),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.SyncOnce(context.Background())
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	if len(result.Advice.Skips) != 1 || result.Advice.Skips[0].Reason != AdviceSkipMissingSnapshotJob {
		t.Fatalf("advice skips = %#v, want one %q skip", result.Advice.Skips, AdviceSkipMissingSnapshotJob)
	}
	assertRequestsEqual(t, client.upsertRequests, nil)
}

func TestControllerBuildBaseSnapshotJoinsSourceErrors(t *testing.T) {
	ctx := context.Background()
	activeErr := errors.New("squeue unavailable")
	heimdallErr := errors.New("Heimdall unavailable")
	reader := newMockJobReader(t)
	reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, activeErr)
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsErr:          heimdallErr,
			listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
		},
		SeenJobs: state.NewMemorySeenJobStore(),
	}

	_, err := controller.buildBaseSnapshot(ctx, nil)
	if !errors.Is(err, activeErr) {
		t.Fatalf("buildBaseSnapshot() error = %v, want active jobs error", err)
	}
	if !errors.Is(err, heimdallErr) {
		t.Fatalf("buildBaseSnapshot() error = %v, want Heimdall jobs error", err)
	}
}

func TestDesiredNodeConstraintsPreservesOverlapSentinel(t *testing.T) {
	job := cluster.JobExecutionRecord{
		NodeConstraints: cluster.NodeConstraints{Inclusions: []string{"node1"}},
	}

	_, err := desiredNodeConstraints(job, nil, []string{"node1"})
	if !errors.Is(err, cluster.ErrNodeUpdateOverlap) {
		t.Fatalf("desiredNodeConstraints() error = %v, want %v", err, cluster.ErrNodeUpdateOverlap)
	}
}

func TestControllerRecordsSyncObservation(t *testing.T) {
	ctx := context.Background()
	activeJob := managedJob(cluster.JobExecutionRecord{
		JobID:        "12345",
		AttemptIndex: 1,
		WorkflowID:   "workflow-12345",
		State:        cluster.JobStateRunning,
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{activeJob})
	client := &fakeControllerClient{
		getJobsResponses:    map[string]*pb.GetJobsResponse{"": {}},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
	}
	observer := &recordingObserver{}
	controller := Controller{
		JobReader:      reader,
		HeimdallClient: client,
		SeenJobs:       state.NewMemorySeenJobStore(),
		Observer:       observer,
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeObserve},
	}

	_, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}

	if len(observer.observations) != 1 {
		t.Fatalf("len(observations) = %d, want 1", len(observer.observations))
	}
	observation := observer.observations[0]
	if observation.Mode != ObserveMode || observation.Outcome != ObservationOutcomeSuccess {
		t.Fatalf("observation mode/outcome = %q/%q, want observe/success", observation.Mode, observation.Outcome)
	}
	if observation.Duration <= 0 {
		t.Fatalf("observation duration = %s, want positive", observation.Duration)
	}
	assertSourceObservation(t, observation.Sources, SourceSlurmActive, ObservationOutcomeSuccess, 1)
	assertSourceObservation(t, observation.Sources, SourceHeimdallJobs, ObservationOutcomeSuccess, 0)
	assertSourceObservation(t, observation.Sources, SourceHeimdallAdvice, ObservationOutcomeSuccess, 0)
	assertSourceObservation(t, observation.Sources, SourceSeenStore, ObservationOutcomeSuccess, 0)
	if len(observation.Stages) != 3 {
		t.Fatalf("len(stage observations) = %d, want 3", len(observation.Stages))
	}
	assertStageObservation(t, observation.Stages, StageJobStatusPolling, ObservationOutcomeSuccess)
	assertStageObservation(t, observation.Stages, StageJobReconciliation, ObservationOutcomeSuccess)
	assertStageObservation(t, observation.Stages, StageAdviceReconciliation, ObservationOutcomeSuccess)
	if observation.Snapshot.ActiveJobs != 1 {
		t.Fatalf("Snapshot.ActiveJobs = %d, want 1", observation.Snapshot.ActiveJobs)
	}
	if observation.AttemptedAdviceReconciliations != 0 {
		t.Fatalf("AttemptedAdviceReconciliations = %d, want 0", observation.AttemptedAdviceReconciliations)
	}
	if !observation.JobAttemptCache.Observed || observation.JobAttemptCache.Entries != 1 {
		t.Fatalf("JobAttemptCache = %#v, want one observed entry", observation.JobAttemptCache)
	}
	assertJobUpsertObservation(t, observation.Upserts, JobUpsertSourceSlurmActive, ObservationOutcomeSuccess, 1)
}

func TestControllerReturnsAdviceActionFailure(t *testing.T) {
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	adviceErr := errors.New("kill failed")
	executor := newMockJobMutator(t)
	executor.EXPECT().JobKill(gomock.Any(), "300").Return(adviceErr)
	observer := &recordingObserver{}
	controller := newActionPhaseTestController(t, executor, observer, now)

	_, err := controller.SyncOnce(context.Background())
	if !errors.Is(err, adviceErr) {
		t.Fatalf("SyncOnce() error = %v, want advice action failure", err)
	}
	if len(observer.observations) != 1 {
		t.Fatalf("len(observations) = %d, want 1", len(observer.observations))
	}
	observation := observer.observations[0]
	if observation.Outcome != ObservationOutcomeError {
		t.Fatalf("observation outcome = %q, want error", observation.Outcome)
	}
	if observation.AttemptedAdviceReconciliations != 1 {
		t.Fatalf("AttemptedAdviceReconciliations = %d, want 1", observation.AttemptedAdviceReconciliations)
	}
	if !observation.JobAttemptCache.Observed || observation.JobAttemptCache.Entries != 1 {
		t.Fatalf("JobAttemptCache = %#v, want one observed entry", observation.JobAttemptCache)
	}
	assertStageObservation(t, observation.Stages, StageAdviceReconciliation, ObservationOutcomeError)
	assertActionObservation(t, observation.AdviceActions, ActionAdviceKill, cluster.JobModeAct, ObservationOutcomeError, 1)
}

func TestControllerCancellationStopsAdviceReconciliation(t *testing.T) {
	now := time.Date(2026, 6, 30, 12, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	executor := newMockJobMutator(t)
	executor.EXPECT().JobKill(gomock.Any(), "300").DoAndReturn(func(context.Context, string) error {
		cancel()
		return context.Canceled
	})
	controller := newActionPhaseTestController(t, executor, nil, now)

	_, err := controller.SyncOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SyncOnce() error = %v, want context canceled", err)
	}
}

func TestControllerEffectiveModeGatesTrackingAndAdvice(t *testing.T) {
	tests := []struct {
		name           string
		policy         ModePolicy
		job            cluster.JobExecutionRecord
		wantMode       cluster.JobMode
		wantReason     ModeDecisionReason
		wantUpserts    int
		wantSeen       bool
		wantAdviceSkip AdviceSkipReason
		wantAction     bool
	}{
		{
			name:           "ignore override is not tracked or planned",
			policy:         ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			job:            jobWithMode(cluster.JobModeIgnore),
			wantMode:       cluster.JobModeIgnore,
			wantReason:     ModeReasonJobOverride,
			wantAdviceSkip: AdviceSkipIgnoredJob,
		},
		{
			name:        "observe override plans without mutation",
			policy:      ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			job:         jobWithMode(cluster.JobModeObserve),
			wantMode:    cluster.JobModeObserve,
			wantReason:  ModeReasonJobOverride,
			wantUpserts: 1,
			wantSeen:    true,
		},
		{
			name:        "valid act override mutates when acknowledged",
			policy:      ModePolicy{DefaultMode: cluster.JobModeObserve, AllowAct: true},
			job:         jobWithMode(cluster.JobModeAct),
			wantMode:    cluster.JobModeAct,
			wantReason:  ModeReasonJobOverride,
			wantUpserts: 1,
			wantSeen:    true,
			wantAction:  true,
		},
		{
			name:   "invalid mode is observe",
			policy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			job: cluster.JobExecutionRecord{
				HeimdallManaged: true,
				ModeRequest: cluster.JobModeRequest{
					Mode:      cluster.JobMode("act "),
					Specified: true,
				},
			},
			wantMode:    cluster.JobModeObserve,
			wantReason:  ModeReasonInvalidRequest,
			wantUpserts: 1,
			wantSeen:    true,
		},
		{
			name:        "job act is observe when acknowledgement is false",
			policy:      ModePolicy{DefaultMode: cluster.JobModeObserve},
			job:         jobWithMode(cluster.JobModeAct),
			wantMode:    cluster.JobModeObserve,
			wantReason:  ModeReasonActionDenied,
			wantUpserts: 1,
			wantSeen:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := tt.job
			job.JobID = "12345"
			job.WorkflowID = "workflow-12345"
			job.State = cluster.JobStateRunning

			executor := newMockJobMutator(t)
			if tt.wantAction {
				executor.EXPECT().JobKill(gomock.Any(), job.JobID).Return(nil)
			}
			reader := newMockJobReader(t)
			expectActiveJobs(reader, []cluster.JobExecutionRecord{job})
			advice := newAdvice(t, adviceFixture{
				adviceID: "terminate-12345",
				jobID:    job.JobID,
				state:    adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
				metadata: map[string]any{"scheduler": cluster.SchedulerSlurm},
			})
			client := &fakeControllerClient{
				getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
				listAdviceResponses: map[string]*pb.ListAdviceResponse{
					"": {Advice: []*pb.Advice{advice}},
				},
			}
			seen := state.NewMemorySeenJobStore(state.SeenJobRecord{JobID: job.JobID})
			observer := &recordingObserver{}
			controller := Controller{
				JobReader:      reader,
				HeimdallClient: client,
				SeenJobs:       seen,
				ActionExecutor: executor,
				ModePolicy:     tt.policy,
				Observer:       observer,
			}

			result, err := controller.SyncOnce(context.Background())
			if err != nil {
				t.Fatalf("SyncOnce() failed: %v", err)
			}
			if len(client.upsertRequests) != tt.wantUpserts {
				t.Fatalf("upserts = %d, want %d", len(client.upsertRequests), tt.wantUpserts)
			}
			wantSeen := []state.SeenJobRecord(nil)
			if tt.wantSeen {
				wantSeen = []state.SeenJobRecord{{JobID: job.JobID}}
			}
			assertSeenRecords(t, seen, wantSeen)
			if tt.wantAdviceSkip != "" {
				if len(result.Advice.Skips) != 1 || result.Advice.Skips[0].Reason != tt.wantAdviceSkip {
					t.Fatalf("advice skips = %#v, want %q", result.Advice.Skips, tt.wantAdviceSkip)
				}
			}
			if len(observer.observations) != 1 {
				t.Fatalf("observations = %d, want 1", len(observer.observations))
			}
			observation := observer.observations[0]
			assertModeDecisionObservation(t, observation.ModeDecisions, tt.policy.DefaultMode, tt.wantMode, tt.wantReason, 1)
			if tt.wantAdviceSkip == "" {
				outcome := ObservationOutcomePlanned
				if tt.wantAction {
					outcome = ObservationOutcomeSuccess
				}
				assertActionObservation(t, observation.AdviceActions, ActionAdviceKill, tt.wantMode, outcome, 1)
			}
		})
	}
}

func newActionPhaseTestController(
	t *testing.T,
	executor cluster.JobMutator,
	observer Observer,
	now time.Time,
) Controller {
	t.Helper()

	adviceJob := managedJob(cluster.JobExecutionRecord{JobID: "300", WorkflowID: "workflow-300", State: cluster.JobStateRunning})
	termination := newAdvice(t, adviceFixture{
		adviceID: "terminate-300",
		jobID:    "300",
		state:    adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
		metadata: map[string]any{
			"scheduler":  cluster.SchedulerSlurm,
			"managed_by": cluster.ManagedBySlurmAdvisor,
		},
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{adviceJob})
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{
				"": {Advice: []*pb.Advice{termination}},
			},
		},
		ActionExecutor: executor,
		SeenJobs:       state.NewMemorySeenJobStore(),
		Observer:       observer,
		Options: ControllerOptions{
			AdviceLoopTimeout: time.Hour,
		},
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Now:        func() time.Time { return now },
	}

	return controller
}

func TestControllerBatchesAccountingRequestsAndUpsertsFinalizedJobs(t *testing.T) {
	ctx := context.Background()
	finalJob := managedJob(cluster.JobExecutionRecord{
		JobID:        "12345",
		AttemptIndex: 1,
		WorkflowID:   "workflow-a",
		State:        cluster.JobStateFailed,
		RawState:     "NODE_FAIL",
		FailedNodes:  []string{"node2"},
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 1}}, []cluster.JobExecutionRecord{finalJob})
	client := &fakeControllerClient{
		getJobsResponses:    map[string]*pb.GetJobsResponse{"": {}},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
	}
	seen := state.NewMemorySeenJobStore(state.SeenJobRecord{JobID: "12345", AttemptIndex: 1})
	observer := &recordingObserver{}
	controller := Controller{
		JobReader:      reader,
		HeimdallClient: client,
		SeenJobs:       seen,
		Environment:    "prod",
		Observer:       observer,
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeObserve},
	}

	result, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}

	if _, ok := result.Snapshot.AccountingJobs[cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 1}]; !ok {
		t.Fatal("finalized job missing from accounting snapshot")
	}
	wantReq, err := heimdall.BuildCreateOrUpdateJobRequest(finalJob, heimdall.UpsertRequestOptions{Environment: "prod"})
	if err != nil {
		t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
	}
	assertRequestsEqual(t, client.upsertRequests, []*pb.CreateOrUpdateJobRequest{wantReq})
	assertSeenRecords(t, seen, nil)
	if len(observer.observations) != 1 {
		t.Fatalf("observations = %d, want 1", len(observer.observations))
	}
	cache := observer.observations[0].JobAttemptCache
	if !cache.Observed || cache.Entries != 0 {
		t.Fatalf("JobAttemptCache = %#v, want observed empty cache", cache)
	}
}

func TestControllerIgnoreModeDoesNotTrackFailedJob(t *testing.T) {
	failedJob := managedJob(cluster.JobExecutionRecord{
		JobID:        "12345",
		AttemptIndex: 1,
		WorkflowID:   "workflow-a",
		State:        cluster.JobStateFailed,
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 1}}, []cluster.JobExecutionRecord{failedJob})
	client := &fakeControllerClient{
		getJobsResponses:    map[string]*pb.GetJobsResponse{"": {}},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
	}
	seen := state.NewMemorySeenJobStore(state.SeenJobRecord{JobID: "12345", AttemptIndex: 1})
	controller := Controller{
		JobReader:      reader,
		HeimdallClient: client,
		SeenJobs:       seen,
		ActionExecutor: newMockJobMutator(t),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeIgnore},
	}

	_, err := controller.SyncOnce(context.Background())
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	assertRequestsEqual(t, client.upsertRequests, nil)
	assertSeenRecords(t, seen, nil)
}

func TestControllerReadsAccountingRecordForAdviceWhenJobMissingFromSqueue(t *testing.T) {
	ctx := context.Background()
	accountingJob := managedJob(cluster.JobExecutionRecord{JobID: "12345", AttemptIndex: 1, WorkflowID: "workflow-12345", State: cluster.JobStateRunning})
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(reader,
		[]cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 1}},
		[]cluster.JobExecutionRecord{accountingJob},
	)
	client := &fakeControllerClient{
		getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{
			"": {Advice: []*pb.Advice{restartAdvice}},
		},
	}
	controller := Controller{
		JobReader:      reader,
		HeimdallClient: client,
		SeenJobs:       state.NewMemorySeenJobStore(),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeObserve},
	}

	result, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	if _, ok := result.Snapshot.AccountingJobs[cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 1}]; !ok {
		t.Fatal("advice target missing from accounting snapshot")
	}
	wantReq, err := heimdall.BuildCreateOrUpdateJobRequest(accountingJob, heimdall.UpsertRequestOptions{})
	if err != nil {
		t.Fatalf("BuildCreateOrUpdateJobRequest() failed: %v", err)
	}
	assertRequestsEqual(t, client.upsertRequests, []*pb.CreateOrUpdateJobRequest{wantReq})
}

func TestControllerAppliesRecentAdviceToAccountingJobMissingFromSqueue(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	createdAt := now.Add(-4 * time.Minute)
	accountingJob := managedJob(cluster.JobExecutionRecord{
		JobID:        "12345",
		AttemptIndex: 1,
		WorkflowID:   "workflow-12345",
		State:        cluster.JobStateCompleted,
	})
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
		createdAt:    &createdAt,
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(reader,
		[]cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 1}},
		[]cluster.JobExecutionRecord{accountingJob},
	)
	executor := newMockJobMutator(t)
	gomock.InOrder(
		executor.EXPECT().JobRequeueHold(gomock.Any(), "12345").Return(nil),
		executor.EXPECT().JobRelease(gomock.Any(), "12345").Return(nil),
	)
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{
				"": {Advice: []*pb.Advice{restartAdvice}},
			},
		},
		ActionExecutor: executor,
		SeenJobs:       state.NewMemorySeenJobStore(),
		Options: ControllerOptions{
			AdviceMaxAge:           5 * time.Minute,
			AdviceRequeueHoldDelay: 0,
		},
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Now:        func() time.Time { return now },
	}

	result, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	if len(result.Advice.Skips) != 0 {
		t.Fatalf("advice skips = %#v, want none", result.Advice.Skips)
	}
}

func TestControllerReleasesRequeuedAttemptDiscoveredOnlyThroughAdvice(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	createdAt := now.Add(-time.Minute)
	accountingJob := managedJob(cluster.JobExecutionRecord{
		JobID:        "12345",
		AttemptIndex: 1,
		WorkflowID:   "workflow-12345",
		State:        cluster.JobStateRequeued,
	})
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
		createdAt:    &createdAt,
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(
		reader,
		[]cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 1}},
		[]cluster.JobExecutionRecord{accountingJob},
	)
	executor := newMockJobMutator(t)
	executor.EXPECT().JobRelease(gomock.Any(), "12345").Return(nil)
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			// REQUEUED is intentionally absent from broad job discovery. The
			// exact Advice key must still seed the targeted accounting read.
			getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{
				"": {Advice: []*pb.Advice{restartAdvice}},
			},
		},
		ActionExecutor: executor,
		SeenJobs:       state.NewMemorySeenJobStore(),
		Options:        ControllerOptions{AdviceMaxAge: 5 * time.Minute},
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Now:            func() time.Time { return now },
	}

	result, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	if len(result.Advice.Skips) != 0 {
		t.Fatalf("advice skips = %#v, want none", result.Advice.Skips)
	}
}

func TestControllerDoesNotQueryAccountingForCompletedJobWithoutAdvice(t *testing.T) {
	ctx := context.Background()
	reader := newMockJobReader(t)
	expectActiveJobs(reader, nil)
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			// The reconciliation filter intentionally omits COMPLETED. Without
			// exact-attempt Advice, the historical job must not become a targeted
			// accounting request on every tick.
			getJobsResponses:    map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
		},
		SeenJobs:   state.NewMemorySeenJobStore(),
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve},
	}

	result, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	if len(result.Snapshot.AccountingJobs) != 0 {
		t.Fatalf("accounting jobs = %#v, want none", result.Snapshot.AccountingJobs)
	}
}

func TestControllerSkipsStaleAdviceOnEveryTick(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	createdAt := now.Add(-5 * time.Minute)
	accountingJob := managedJob(cluster.JobExecutionRecord{
		JobID:        "12345",
		AttemptIndex: 1,
		WorkflowID:   "workflow-12345",
		State:        cluster.JobStateCompleted,
	})
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
		createdAt:    &createdAt,
	})
	reader := newMockJobReader(t)
	reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
	reader.EXPECT().FinalizedJobs(
		gomock.Any(),
		[]cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 1}},
	).Return([]cluster.JobExecutionRecord{accountingJob}, nil).Times(2)
	observer := &recordingObserver{}
	client := &fakeControllerClient{
		getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{
			"": {Advice: []*pb.Advice{restartAdvice}},
		},
	}
	controller := Controller{
		JobReader:      reader,
		HeimdallClient: client,
		ActionExecutor: newMockJobMutator(t),
		SeenJobs:       state.NewMemorySeenJobStore(),
		Observer:       observer,
		Options:        ControllerOptions{AdviceMaxAge: 5 * time.Minute},
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
		Now:            func() time.Time { return now },
	}

	for tick := 1; tick <= 2; tick++ {
		result, err := controller.SyncOnce(ctx)
		if err != nil {
			t.Fatalf("SyncOnce() tick %d failed: %v", tick, err)
		}
		if diff := cmp.Diff([]AdviceSkip{{
			AdviceID: "advice-restart",
			JobID:    "12345",
			Reason:   AdviceSkipStaleAdvice,
		}}, result.Advice.Skips); diff != "" {
			t.Fatalf("SyncOnce() tick %d advice skips mismatch (-want +got):\n%s", tick, diff)
		}
	}

	if len(observer.observations) != 2 {
		t.Fatalf("len(observations) = %d, want 2", len(observer.observations))
	}
	if diff := cmp.Diff([]fakeGetJobsCall{{Limit: 100}}, client.getJobsCalls); diff != "" {
		t.Fatalf("GetJobs calls mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]fakeListAdviceCall{
		{Limit: 100, OnlyActive: true},
		{Limit: 100, OnlyActive: true},
	}, client.listAdviceCalls); diff != "" {
		t.Fatalf("ListAdvice calls mismatch (-want +got):\n%s", diff)
	}
	for tick, observation := range observer.observations {
		if len(observation.AdviceActions) != 0 {
			t.Fatalf("tick %d advice actions = %#v, want none", tick+1, observation.AdviceActions)
		}
	}
}

func TestControllerRetriesHeimdallJobBootstrapAfterSourceFailure(t *testing.T) {
	ctx := context.Background()
	getJobsErr := errors.New("Job Manager unavailable")
	reader := newMockJobReader(t)
	reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
	client := &fakeControllerClient{
		getJobsResponses:    map[string]*pb.GetJobsResponse{"": {}},
		getJobsErr:          getJobsErr,
		listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
	}
	controller := Controller{
		JobReader:      reader,
		HeimdallClient: client,
		SeenJobs:       state.NewMemorySeenJobStore(),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeObserve},
	}

	if _, err := controller.SyncOnce(ctx); !errors.Is(err, getJobsErr) {
		t.Fatalf("SyncOnce() bootstrap error = %v, want %v", err, getJobsErr)
	}
	client.getJobsErr = nil
	if _, err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce() retry failed: %v", err)
	}
	if diff := cmp.Diff([]fakeGetJobsCall{{Limit: 100}, {Limit: 100}}, client.getJobsCalls); diff != "" {
		t.Fatalf("GetJobs calls mismatch (-want +got):\n%s", diff)
	}
}

func TestControllerRetriesHeimdallJobBootstrapAfterSeenStoreFailure(t *testing.T) {
	ctx := context.Background()
	key := cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 1}
	reader := newMockJobReader(t)
	reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{key}, nil)
	client := &fakeControllerClient{
		getJobsResponses: map[string]*pb.GetJobsResponse{
			"": {Jobs: []*pb.Job{{JobId: key.JobID, AttemptIndex: key.AttemptIndex}}},
		},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
	}
	markErr := errors.New("seen store unavailable")
	seen := &failOnceMarkSeenStore{
		SeenJobStore: state.NewMemorySeenJobStore(),
		err:          markErr,
	}
	controller := Controller{
		JobReader:      reader,
		HeimdallClient: client,
		SeenJobs:       seen,
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeObserve},
	}

	if _, err := controller.SyncOnce(ctx); !errors.Is(err, markErr) {
		t.Fatalf("SyncOnce() bootstrap error = %v, want %v", err, markErr)
	}
	if _, err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce() retry failed: %v", err)
	}
	if diff := cmp.Diff([]fakeGetJobsCall{{Limit: 100}, {Limit: 100}}, client.getJobsCalls); diff != "" {
		t.Fatalf("GetJobs calls mismatch (-want +got):\n%s", diff)
	}
	assertSeenRecords(t, seen, []state.SeenJobRecord{key})
}

func TestControllerRetainsUnresolvedBootstrappedJobAcrossTicks(t *testing.T) {
	ctx := context.Background()
	key := cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 1}
	completedJob := managedJob(cluster.JobExecutionRecord{
		JobID:        key.JobID,
		AttemptIndex: key.AttemptIndex,
		WorkflowID:   "workflow-12345",
		State:        cluster.JobStateCompleted,
	})
	reader := newMockJobReader(t)
	reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
	firstAccounting := reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{key}).Return(nil, nil)
	reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{key}).Return([]cluster.JobExecutionRecord{completedJob}, nil).After(firstAccounting)
	client := &fakeControllerClient{
		getJobsResponses: map[string]*pb.GetJobsResponse{
			"": {Jobs: []*pb.Job{{JobId: key.JobID, AttemptIndex: key.AttemptIndex}}},
		},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
	}
	seen := state.NewMemorySeenJobStore()
	controller := Controller{
		JobReader:      reader,
		HeimdallClient: client,
		SeenJobs:       seen,
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeObserve},
	}

	if _, err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce() first tick failed: %v", err)
	}
	assertSeenRecords(t, seen, []state.SeenJobRecord{key})
	if _, err := controller.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce() second tick failed: %v", err)
	}
	if diff := cmp.Diff([]fakeGetJobsCall{{Limit: 100}}, client.getJobsCalls); diff != "" {
		t.Fatalf("GetJobs calls mismatch (-want +got):\n%s", diff)
	}
	assertSeenRecords(t, seen, nil)
}

func TestControllerBoundsBootstrappedFailedJobRetries(t *testing.T) {
	transientErr := errors.New("temporary Slurm failure")
	for _, tt := range []struct {
		name       string
		actionErrs []error
		wantErr    error
	}{
		{name: "transient failure recovers within tick", actionErrs: []error{transientErr, nil}},
		{name: "command timeout with live caller retries", actionErrs: []error{context.DeadlineExceeded, nil}},
		{name: "permanent failure stops immediately", actionErrs: []error{cluster.ErrJobRequeuePermanent}, wantErr: cluster.ErrJobRequeuePermanent},
		{name: "unknown failure exhausts local budget", actionErrs: []error{transientErr, transientErr, transientErr}, wantErr: transientErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			key := cluster.JobAttemptRef{JobID: "12345", AttemptIndex: 1}
			failedJob := managedJob(cluster.JobExecutionRecord{
				JobID: key.JobID, AttemptIndex: key.AttemptIndex,
				WorkflowID: "workflow-12345", State: cluster.JobStateFailed, Partition: "normal",
			})
			failedJob.ModeRequest = cluster.JobModeRequest{Mode: cluster.JobModeAct, Specified: true, Valid: true}
			reader := newMockJobReader(t)
			reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, nil).Times(2)
			reader.EXPECT().FinalizedJobs(gomock.Any(), []cluster.JobAttemptRef{key}).Return([]cluster.JobExecutionRecord{failedJob}, nil)
			executor := newMockJobMutator(t)
			var previous *gomock.Call
			for _, actionErr := range tt.actionErrs {
				call := executor.EXPECT().JobRequeueHold(gomock.Any(), key.JobID).Return(actionErr)
				if previous != nil {
					call.After(previous)
				}
				previous = call
			}
			client := &fakeControllerClient{
				getJobsResponses: map[string]*pb.GetJobsResponse{
					"": {Jobs: []*pb.Job{{JobId: key.JobID, AttemptIndex: key.AttemptIndex}}},
				},
				listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
			}
			seen := state.NewMemorySeenJobStore()
			audit := &recordingAuditObserver{}
			controller := Controller{
				Options:   ControllerOptions{EnableEagerRequeue: true},
				JobReader: reader, HeimdallClient: client, SeenJobs: seen,
				ActionExecutor: executor, AuditObserver: audit,
				ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
			}

			result, err := controller.SyncOnce(ctx)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("first SyncOnce() error = %v, want %v", err, tt.wantErr)
			}
			if len(result.FailedJobs.Requeues) != 1 || result.FailedJobs.Requeues[0].Applied != (tt.wantErr == nil) {
				t.Fatalf("first failed-job result = %#v, want one decision with applied=%v", result.FailedJobs, tt.wantErr == nil)
			}
			assertSeenRecords(t, seen, nil)
			if len(audit.failedJobActions) != len(tt.actionErrs) {
				t.Fatalf("audit commands = %d, want %d", len(audit.failedJobActions), len(tt.actionErrs))
			}
			for i, actionErr := range tt.actionErrs {
				wantOutcome := SchedulerActionOutcomeSuccess
				if actionErr != nil {
					wantOutcome = SchedulerActionOutcomeError
				}
				if got := audit.failedJobActions[i]; got.Outcome != wantOutcome || !errors.Is(got.Err, actionErr) {
					t.Fatalf("audit command %d = %#v, want outcome %q and error %v", i, got, wantOutcome, actionErr)
				}
			}

			// A completed local decision must remove terminal accounting work and
			// prevent both another scheduler action and another tick-level error.
			result, err = controller.SyncOnce(ctx)
			if err != nil {
				t.Fatalf("second SyncOnce() failed: %v", err)
			}
			if len(result.FailedJobs.Requeues) != 0 {
				t.Fatalf("second failed-job result = %#v, want no repeated decision", result.FailedJobs)
			}
			if diff := cmp.Diff([]fakeGetJobsCall{{Limit: 100}}, client.getJobsCalls); diff != "" {
				t.Fatalf("GetJobs calls mismatch (-want +got):\n%s", diff)
			}
			assertSeenRecords(t, seen, nil)
		})
	}
}

func TestControllerSkipsAdviceForNonActionableCurrentSqueueState(t *testing.T) {
	ctx := context.Background()
	endedAt := time.Now()
	stoppedJob := managedJob(cluster.JobExecutionRecord{
		JobID:        "12345",
		AttemptIndex: 1,
		WorkflowID:   "workflow-12345",
		State:        cluster.JobStateStopped,
		EndTime:      &endedAt,
	})
	terminateAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-terminate",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_TERMINATED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{stoppedJob})
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{
				"": {Advice: []*pb.Advice{terminateAdvice}},
			},
		},
		ActionExecutor: newMockJobMutator(t),
		SeenJobs:       state.NewMemorySeenJobStore(),
	}

	result, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	if diff := cmp.Diff([]AdviceSkip{{
		AdviceID: "advice-terminate",
		JobID:    "12345",
		Reason:   AdviceSkipAlreadySatisfied,
	}}, result.Advice.Skips); diff != "" {
		t.Fatalf("advice skips mismatch (-want +got):\n%s", diff)
	}
}

func TestControllerActModeExecutesAdviceDirectlyWithoutFingerprintStore(t *testing.T) {
	ctx := context.Background()
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	executor := newMockJobMutator(t)
	gomock.InOrder(
		executor.EXPECT().JobRequeueHold(gomock.Any(), "12345").Return(nil),
		executor.EXPECT().JobRelease(gomock.Any(), "12345").Return(nil),
	)
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{
		managedJob(cluster.JobExecutionRecord{JobID: "12345", AttemptIndex: 1, WorkflowID: "workflow-12345", State: cluster.JobStateRunning}),
	})
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{
				"": {Advice: []*pb.Advice{restartAdvice}},
			},
		},
		ActionExecutor: executor,
		SeenJobs:       state.NewMemorySeenJobStore(),
		Options:        ControllerOptions{UpdateEligibleTimeAfterRequeue: true},
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	_, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
}

func TestControllerRestartWithConstraintsAppliesRequeueUpdateAndRelease(t *testing.T) {
	ctx := context.Background()
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart-with-exclusion",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
		constraints: &pb.NodeConstraints{
			Exclusions: []string{"c2", "c1"},
		},
	})
	executor := newMockJobMutator(t)
	gomock.InOrder(
		executor.EXPECT().JobRequeueHold(gomock.Any(), "12345").Return(nil),
		executor.EXPECT().JobNodeUpdate(gomock.Any(), "12345", []string{"c1", "c2"}, []string(nil), 0).Return(nil),
		executor.EXPECT().JobRelease(gomock.Any(), "12345").Return(nil),
	)
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{
		managedJob(cluster.JobExecutionRecord{JobID: "12345", AttemptIndex: 1, WorkflowID: "workflow-12345", State: cluster.JobStateRunning}),
	})
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{
				"": {Advice: []*pb.Advice{restartAdvice}},
			},
		},
		ActionExecutor: executor,
		SeenJobs:       state.NewMemorySeenJobStore(),
		Options:        ControllerOptions{AdviceRequeueHoldDelay: 0},
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	_, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
}

func TestControllerRestartWithConstraintsUpdatesAndReleasesNewerAttemptOnNextTick(t *testing.T) {
	ctx := context.Background()
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart-with-exclusion",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
		constraints: &pb.NodeConstraints{
			Exclusions: []string{"c2", "c1"},
		},
	})
	executor := newMockJobMutator(t)
	gomock.InOrder(
		executor.EXPECT().JobNodeUpdate(gomock.Any(), "12345", []string{"c1", "c2"}, []string(nil), 0).Return(nil),
		executor.EXPECT().JobRelease(gomock.Any(), "12345").Return(nil),
	)
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{
		managedJob(cluster.JobExecutionRecord{
			JobID:        "12345",
			AttemptIndex: 2,
			WorkflowID:   "workflow-12345",
			State:        cluster.JobStatePending,
			Reason:       "Dependency",
			Held:         true,
		}),
	})
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 1}}, nil)
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{
				"": {Advice: []*pb.Advice{restartAdvice}},
			},
		},
		ActionExecutor: executor,
		SeenJobs:       state.NewMemorySeenJobStore(),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	_, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
}

func TestControllerAdviceItemTimeoutCancelsAction(t *testing.T) {
	ctx := context.Background()
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	executor := newMockJobMutator(t)
	executor.EXPECT().JobRequeueHold(gomock.Any(), "12345").DoAndReturn(func(ctx context.Context, _ string) error {
		<-ctx.Done()
		return ctx.Err()
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{
		managedJob(cluster.JobExecutionRecord{JobID: "12345", AttemptIndex: 1, WorkflowID: "workflow-12345", State: cluster.JobStateRunning}),
	})
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{
				"": {Advice: []*pb.Advice{restartAdvice}},
			},
		},
		ActionExecutor: executor,
		SeenJobs:       state.NewMemorySeenJobStore(),
		Options: ControllerOptions{
			AdviceItemTimeout:      10 * time.Millisecond,
			AdviceLoopTimeout:      time.Hour,
			AdviceRequeueHoldDelay: 0,
		},
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	_, err := controller.SyncOnce(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SyncOnce() error = %v, want context deadline exceeded", err)
	}
}

func TestControllerAdviceLoopTimeoutCancelsAction(t *testing.T) {
	ctx := context.Background()
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
	})
	executor := newMockJobMutator(t)
	executor.EXPECT().JobRequeueHold(gomock.Any(), "12345").DoAndReturn(func(ctx context.Context, _ string) error {
		<-ctx.Done()
		return ctx.Err()
	})
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{
		managedJob(cluster.JobExecutionRecord{JobID: "12345", AttemptIndex: 1, WorkflowID: "workflow-12345", State: cluster.JobStateRunning}),
	})
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{
				"": {Advice: []*pb.Advice{restartAdvice}},
			},
		},
		ActionExecutor: executor,
		SeenJobs:       state.NewMemorySeenJobStore(),
		Options: ControllerOptions{
			AdviceItemTimeout:      time.Hour,
			AdviceLoopTimeout:      10 * time.Millisecond,
			AdviceRequeueHoldDelay: 0,
		},
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	_, err := controller.SyncOnce(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SyncOnce() error = %v, want context deadline exceeded", err)
	}
}

func TestControllerRestartWithSatisfiedConstraintsSkipsAction(t *testing.T) {
	ctx := context.Background()
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart-with-exclusion",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
		constraints: &pb.NodeConstraints{
			Exclusions: []string{"c2", "c1"},
		},
	})
	executor := newMockJobMutator(t)
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{
		managedJob(cluster.JobExecutionRecord{
			JobID:        "12345",
			AttemptIndex: 2,
			WorkflowID:   "workflow-12345",
			State:        cluster.JobStatePending,
			NodeConstraints: cluster.NodeConstraints{
				Exclusions: []string{"c1", "c2"},
			},
		}),
	})
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 1}}, nil)
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{
				"": {Advice: []*pb.Advice{restartAdvice}},
			},
		},
		ActionExecutor: executor,
		SeenJobs:       state.NewMemorySeenJobStore(),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	_, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
}

func TestControllerRestartWithSatisfiedConstraintsDoesNotUpdateEligibleTime(t *testing.T) {
	ctx := context.Background()
	restartAdvice := newAdvice(t, adviceFixture{
		adviceID:     "advice-restart-with-exclusion",
		jobID:        "12345",
		attemptIndex: 1,
		state:        adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED),
		metadata:     map[string]any{"scheduler": cluster.SchedulerSlurm},
		constraints: &pb.NodeConstraints{
			Exclusions: []string{"c2", "c1"},
		},
	})
	executor := newMockJobMutator(t)
	reader := newMockJobReader(t)
	expectActiveJobs(reader, []cluster.JobExecutionRecord{
		managedJob(cluster.JobExecutionRecord{
			JobID:        "12345",
			AttemptIndex: 2,
			WorkflowID:   "workflow-12345",
			State:        cluster.JobStatePending,
			NodeConstraints: cluster.NodeConstraints{
				Exclusions: []string{"c1", "c2"},
			},
		}),
	})
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 1}}, nil)
	controller := Controller{
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses: map[string]*pb.GetJobsResponse{"": {}},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{
				"": {Advice: []*pb.Advice{restartAdvice}},
			},
		},
		ActionExecutor: executor,
		SeenJobs:       state.NewMemorySeenJobStore(),
		Options:        ControllerOptions{UpdateEligibleTimeAfterRequeue: true},
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	_, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}

}

func TestControllerRestartRequeuesFailedJobDiscoveredFromHeimdallWithoutAdvice(t *testing.T) {
	ctx := context.Background()
	failedJob := managedJob(cluster.JobExecutionRecord{
		JobID:        "12345",
		AttemptIndex: 1,
		WorkflowID:   "workflow-12345",
		State:        cluster.JobStateFailed,
		Partition:    "normal",
	})
	failedJob.ModeRequest = cluster.JobModeRequest{
		Mode:      cluster.JobModeAct,
		Specified: true,
		Valid:     true,
	}
	executor := newMockJobMutator(t)
	executor.EXPECT().JobRequeueHold(gomock.Any(), "12345").Return(nil)
	reader := newMockJobReader(t)
	expectActiveJobs(reader, nil)
	expectFinalizedJobs(reader, []cluster.JobAttemptRef{{JobID: "12345", AttemptIndex: 1}}, []cluster.JobExecutionRecord{failedJob})
	controller := Controller{
		Options:   ControllerOptions{EnableEagerRequeue: true},
		JobReader: reader,
		HeimdallClient: &fakeControllerClient{
			getJobsResponses: map[string]*pb.GetJobsResponse{
				"": {Jobs: []*pb.Job{{JobId: "12345", AttemptIndex: 1}}},
			},
			listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
		},
		ActionExecutor: executor,
		SeenJobs:       state.NewMemorySeenJobStore(),
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeAct, AllowAct: true},
	}

	result, err := controller.SyncOnce(ctx)
	if err != nil {
		t.Fatalf("SyncOnce() failed: %v", err)
	}
	if len(result.FailedJobs.Requeues) != 1 || !result.FailedJobs.Requeues[0].Applied {
		t.Fatalf("failed-job result = %#v, want one applied requeue", result.FailedJobs.Requeues)
	}
}

func TestControllerKeepsSeenStateOnSourceFailure(t *testing.T) {
	ctx := context.Background()
	readErr := errors.New("squeue unavailable")
	reader := newMockJobReader(t)
	reader.EXPECT().ActiveJobs(gomock.Any()).Return(nil, readErr)
	client := &fakeControllerClient{
		getJobsResponses:    map[string]*pb.GetJobsResponse{"": {}},
		listAdviceResponses: map[string]*pb.ListAdviceResponse{"": {}},
	}
	seen := state.NewMemorySeenJobStore(state.SeenJobRecord{JobID: "12345", AttemptIndex: 1})
	observer := &recordingObserver{}
	controller := Controller{
		JobReader:      reader,
		HeimdallClient: client,
		SeenJobs:       seen,
		Observer:       observer,
		ModePolicy:     ModePolicy{DefaultMode: cluster.JobModeObserve},
	}

	_, err := controller.SyncOnce(ctx)
	if !errors.Is(err, readErr) {
		t.Fatalf("SyncOnce() error = %v, want %v", err, readErr)
	}

	if len(observer.observations) != 1 {
		t.Fatalf("len(observations) = %d, want 1", len(observer.observations))
	}
	if len(observer.observations[0].Stages) != 1 {
		t.Fatalf("len(stage observations) = %d, want 1", len(observer.observations[0].Stages))
	}
	assertSourceObservation(t, observer.observations[0].Sources, SourceSlurmActive, ObservationOutcomeError, 0)
	assertSourceObservation(t, observer.observations[0].Sources, SourceHeimdallJobs, ObservationOutcomeSuccess, 0)
	assertSourceObservation(t, observer.observations[0].Sources, SourceHeimdallAdvice, ObservationOutcomeSuccess, 0)
	assertSourceObservation(t, observer.observations[0].Sources, SourceSeenStore, ObservationOutcomeSuccess, 1)
	assertStageObservation(t, observer.observations[0].Stages, StageJobStatusPolling, ObservationOutcomeError)
	assertRequestsEqual(t, client.upsertRequests, nil)
	assertSeenRecords(t, seen, []state.SeenJobRecord{{JobID: "12345", AttemptIndex: 1}})
}

func TestControllerOptionsDoNotIncludeClusterScope(t *testing.T) {
	optionType := reflect.TypeOf(ControllerOptions{})
	if _, ok := optionType.FieldByName("ClusterID"); ok {
		t.Fatal("ControllerOptions exposes ClusterID, but Heimdall instances are single-cluster scoped")
	}
}

func TestControllerInlinesUpsertEnvironment(t *testing.T) {
	controllerType := reflect.TypeOf(Controller{})
	if _, ok := controllerType.FieldByName("UpsertOptions"); ok {
		t.Fatal("Controller exposes UpsertOptions, want inlined Environment")
	}
	if _, ok := controllerType.FieldByName("Environment"); !ok {
		t.Fatal("Controller does not expose Environment")
	}
}

type fakeControllerClient struct {
	upsertRequests      []*pb.CreateOrUpdateJobRequest
	upsertErrByJobID    map[string]error
	getJobsResponses    map[string]*pb.GetJobsResponse
	getJobsErr          error
	getJobsCalls        []fakeGetJobsCall
	getJobsOptions      []heimdall.PaginationOptions
	listAdviceResponses map[string]*pb.ListAdviceResponse
	listAdviceErr       error
	listAdviceCalls     []fakeListAdviceCall
	statusUpdates       []fakeStatusUpdate
	statusUpdateErr     error
}

type failOnceMarkSeenStore struct {
	state.SeenJobStore
	err error
}

func (s *failOnceMarkSeenStore) MarkSeen(ctx context.Context, record state.SeenJobRecord) error {
	if s.err != nil {
		err := s.err
		s.err = nil
		return err
	}
	return s.SeenJobStore.MarkSeen(ctx, record)
}

type fakeGetJobsCall struct {
	Limit int32
}

type fakeListAdviceCall struct {
	Limit      int32
	OnlyActive bool
}

type fakeStatusUpdate struct {
	AdviceID string
	Status   *pb.AdviceReconciliationStatus
}

func (c *fakeControllerClient) CreateOrUpdateJob(_ context.Context, req *pb.CreateOrUpdateJobRequest) (*pb.CreateOrUpdateJobResponse, error) {
	c.upsertRequests = append(c.upsertRequests, req)
	if err := c.upsertErrByJobID[req.GetJobId()]; err != nil {
		return nil, err
	}
	return &pb.CreateOrUpdateJobResponse{}, nil
}

func (c *fakeControllerClient) GetJobs(_ context.Context, opts ...heimdall.PaginationOption) ([]*pb.Job, error) {
	pagination := heimdall.ResolvePaginationOptions(opts...)
	c.getJobsCalls = append(c.getJobsCalls, fakeGetJobsCall{Limit: pagination.PageLimit})
	c.getJobsOptions = append(c.getJobsOptions, pagination)
	if c.getJobsErr != nil {
		return nil, c.getJobsErr
	}
	var out []*pb.Job
	pageToken := ""
	for {
		resp := c.getJobsResponses[pageToken]
		if resp == nil {
			return out, nil
		}
		out = append(out, resp.GetJobs()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return out, nil
		}
	}
}

func (c *fakeControllerClient) ListAdvice(_ context.Context, opts ...heimdall.PaginationOption) ([]*pb.Advice, error) {
	pagination := heimdall.ResolvePaginationOptions(opts...)
	c.listAdviceCalls = append(c.listAdviceCalls, fakeListAdviceCall{
		Limit:      pagination.PageLimit,
		OnlyActive: pagination.OnlyActive,
	})
	if c.listAdviceErr != nil {
		return nil, c.listAdviceErr
	}
	var out []*pb.Advice
	pageToken := ""
	for {
		resp := c.listAdviceResponses[pageToken]
		if resp == nil {
			return out, nil
		}
		out = append(out, resp.GetAdvice()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return out, nil
		}
	}
}

func (c *fakeControllerClient) UpdateAdviceReconciliationStatus(
	_ context.Context,
	adviceID string,
	status *pb.AdviceReconciliationStatus,
) (*pb.AdviceReconciliationStatus, error) {
	c.statusUpdates = append(c.statusUpdates, fakeStatusUpdate{AdviceID: adviceID, Status: status})
	if c.statusUpdateErr != nil {
		return nil, c.statusUpdateErr
	}

	return status, nil
}

func newMockJobReader(t *testing.T) *clustermock.MockJobReader {
	t.Helper()
	return clustermock.NewMockJobReader(gomock.NewController(t))
}

func newMockJobMutator(t *testing.T) *clustermock.MockJobMutator {
	t.Helper()
	return clustermock.NewMockJobMutator(gomock.NewController(t))
}

func expectActiveJobs(reader *clustermock.MockJobReader, jobs []cluster.JobExecutionRecord) {
	reader.EXPECT().ActiveJobs(gomock.Any()).Return(jobs, nil)
}

func expectFinalizedJobs(reader *clustermock.MockJobReader, records []cluster.JobAttemptRef, jobs []cluster.JobExecutionRecord) {
	reader.EXPECT().FinalizedJobs(gomock.Any(), records).Return(jobs, nil)
}

type adviceFixture struct {
	adviceID           string
	jobID              string
	attemptIndex       int32
	recoverySequenceID string
	state              *pb.AdviceState
	metadata           map[string]any
	constraints        *pb.NodeConstraints
	createdAt          *time.Time
}

func newAdvice(t *testing.T, fixture adviceFixture) *pb.Advice {
	t.Helper()

	metadata, err := structpb.NewStruct(fixture.metadata)
	if err != nil {
		t.Fatalf("NewStruct() failed: %v", err)
	}

	advice := &pb.Advice{
		HeimdallJobId:     fixture.adviceID,
		JobId:             fixture.jobID,
		AttemptIndex:      fixture.attemptIndex,
		SchedulerMetadata: metadata,
		Advice: &pb.AdviceRecommendation{
			State:           fixture.state,
			NodeConstraints: fixture.constraints,
		},
	}
	if fixture.createdAt != nil {
		advice.CreatedAt = timestamppb.New(*fixture.createdAt)
	}
	if fixture.recoverySequenceID != "" {
		advice.RecoveryContext = &pb.RecoveryContext{SequenceId: fixture.recoverySequenceID}
	}

	return advice
}

func adviceState(value pb.AdviceState) *pb.AdviceState {
	return &value
}

func assertSnapshotKeys(t *testing.T, snapshot ControllerSnapshot, want []cluster.JobAttemptRef) {
	t.Helper()

	got := make([]cluster.JobAttemptRef, 0, len(snapshot.JobKeys))
	for key := range snapshot.JobKeys {
		got = append(got, key)
	}
	sortJobKeys(got)
	sortJobKeys(want)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("snapshot keys mismatch (-want +got):\n%s", diff)
	}
}

func assertRequestsEqual(t *testing.T, got, want []*pb.CreateOrUpdateJobRequest) {
	t.Helper()

	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Fatalf("requests mismatch (-want +got):\n%s", diff)
	}
}

func assertSeenRecords(t *testing.T, store state.SeenJobStore, want []state.SeenJobRecord) {
	t.Helper()

	got, err := store.ListSeenJobs(context.Background())
	if err != nil {
		t.Fatalf("ListSeenJobs() failed: %v", err)
	}
	if len(want) == 0 && len(got) == 0 {
		return
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("seen records mismatch (-want +got):\n%s", diff)
	}
}

func managedJob(job cluster.JobExecutionRecord) cluster.JobExecutionRecord {
	job.HeimdallManaged = true
	job.ModeRequest = cluster.JobModeRequest{Valid: true}
	return job
}

type recordingObserver struct {
	observations  []SyncObservation
	startedModes  []OperationMode
	startedStages []ReconcileStage
	completed     int
}

func (r *recordingObserver) ObserveSync(observation SyncObservation) {
	r.observations = append(r.observations, observation)
}

func (r *recordingObserver) ObserveSyncStarted(mode OperationMode) {
	r.startedModes = append(r.startedModes, mode)
}

func (r *recordingObserver) ObserveStageStarted(stage ReconcileStage) {
	r.startedStages = append(r.startedStages, stage)
}

func (r *recordingObserver) ObserveSyncCompleted() {
	r.completed++
}

func assertSourceObservation(t *testing.T, observations []SourceObservation, source ObservationSource, outcome ObservationOutcome, items int) {
	t.Helper()

	for _, observation := range observations {
		if observation.Source == source && observation.Outcome == outcome && observation.Items == items {
			if observation.Duration <= 0 {
				t.Fatalf("source %q duration = %s, want positive", source, observation.Duration)
			}
			return
		}
	}
	t.Fatalf("source observation %s/%s/%d not found in %#v", source, outcome, items, observations)
}

func assertStageObservation(t *testing.T, observations []StageObservation, stage ReconcileStage, outcome ObservationOutcome) {
	t.Helper()

	for _, observation := range observations {
		if observation.Stage == stage && observation.Outcome == outcome {
			if observation.Duration <= 0 {
				t.Fatalf("stage %q duration = %s, want positive", stage, observation.Duration)
			}
			return
		}
	}
	t.Fatalf("stage observation %q/%q not found in %#v", stage, outcome, observations)
}

func assertJobUpsertObservation(t *testing.T, observations []JobUpsertObservation, source JobUpsertSource, outcome ObservationOutcome, count int) {
	t.Helper()

	for _, observation := range observations {
		if observation.Source == source && observation.Outcome == outcome && observation.Count == count {
			return
		}
	}
	t.Fatalf("job upsert observation %s/%s/%d not found in %#v", source, outcome, count, observations)
}

func assertActionObservation(
	t *testing.T,
	observations []ActionObservation,
	action SchedulerAction,
	mode OperationMode,
	outcome ObservationOutcome,
	count int,
) {
	t.Helper()

	for _, observation := range observations {
		if observation.Action == action && observation.Mode == mode && observation.Outcome == outcome && observation.Count == count {
			return
		}
	}
	t.Fatalf("action observation %s/%s/%s/%d not found in %#v", action, mode, outcome, count, observations)
}

func assertModeDecisionObservation(
	t *testing.T,
	observations []ModeDecisionObservation,
	defaultMode cluster.JobMode,
	effectiveMode cluster.JobMode,
	reason ModeDecisionReason,
	count int,
) {
	t.Helper()

	for _, observation := range observations {
		if observation.DefaultMode == defaultMode &&
			observation.EffectiveMode == effectiveMode &&
			observation.Reason == reason &&
			observation.Count == count {
			return
		}
	}
	t.Fatalf("mode decision %s/%s/%s/%d not found in %#v", defaultMode, effectiveMode, reason, count, observations)
}
