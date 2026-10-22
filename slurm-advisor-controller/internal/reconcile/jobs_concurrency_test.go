// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/heimdall"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
)

func TestReconcileJobsBoundsConcurrentHeimdallUpserts(t *testing.T) {
	release := make(chan struct{})
	client := newConcurrentUpsertClient(func(string) <-chan struct{} { return release })
	controller, snapshot := concurrentUpsertTestController(client, 2, "100", "200", "300", "400")

	errCh := make(chan error, 1)
	go func() {
		errCh <- controller.reconcileJobs(context.Background(), snapshot, nil)
	}()

	for range 2 {
		waitForUpsertStart(t, client.started)
	}
	select {
	case jobID := <-client.started:
		t.Fatalf("upsert %s started above configured worker pool size", jobID)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)

	if err := <-errCh; err != nil {
		t.Fatalf("reconcileJobs() failed: %v", err)
	}
	if got := client.maxConcurrency(); got != 2 {
		t.Fatalf("maximum concurrent upserts = %d, want 2", got)
	}
	if got := client.callCount(); got != 4 {
		t.Fatalf("upsert calls = %d, want 4", got)
	}
}

func TestReconcileJobsCancellationDoesNotStartQueuedUpserts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := newConcurrentUpsertClient(func(string) <-chan struct{} { return make(chan struct{}) })
	controller, snapshot := concurrentUpsertTestController(
		client,
		2,
		"100",
		"200",
		"300",
		"400",
		"500",
		"600",
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- controller.reconcileJobs(ctx, snapshot, nil)
	}()

	for range 2 {
		waitForUpsertStart(t, client.started)
	}
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("reconcileJobs() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reconcileJobs() did not join workers after cancellation")
	}
	if got := client.callCount(); got != 2 {
		t.Fatalf("upserts started after cancellation = %d total calls, want 2", got)
	}
}

func TestReconcileJobsJoinsErrorsInStableJobOrder(t *testing.T) {
	gates := map[string]chan struct{}{
		"100": make(chan struct{}),
		"200": make(chan struct{}),
		"300": make(chan struct{}),
	}
	err100 := errors.New("failure-100")
	err200 := errors.New("failure-200")
	err300 := errors.New("failure-300")
	client := newConcurrentUpsertClient(func(jobID string) <-chan struct{} { return gates[jobID] })
	client.errByJobID = map[string]error{
		"100": err100,
		"200": err200,
		"300": err300,
	}
	controller, snapshot := concurrentUpsertTestController(client, 3, "300", "100", "200")
	observation := SyncObservation{}

	errCh := make(chan error, 1)
	go func() {
		errCh <- controller.reconcileJobs(context.Background(), snapshot, &observation)
	}()

	for range 3 {
		waitForUpsertStart(t, client.started)
	}
	close(gates["300"])
	waitForUpsertCompletion(t, client.completed, "300")
	close(gates["200"])
	waitForUpsertCompletion(t, client.completed, "200")
	close(gates["100"])
	waitForUpsertCompletion(t, client.completed, "100")

	err := <-errCh
	for _, want := range []error{err100, err200, err300} {
		if !errors.Is(err, want) {
			t.Fatalf("reconcileJobs() error = %v, want cause %v", err, want)
		}
	}
	errorText := err.Error()
	index100 := strings.Index(errorText, "job 100")
	index200 := strings.Index(errorText, "job 200")
	index300 := strings.Index(errorText, "job 300")
	if index100 < 0 || index200 <= index100 || index300 <= index200 {
		t.Fatalf("joined error order = %q, want jobs 100, 200, 300", errorText)
	}
	if len(observation.Upserts) != 3 {
		t.Fatalf("upsert observations = %d, want 3", len(observation.Upserts))
	}
	for i, item := range observation.Upserts {
		if item.Source != JobUpsertSourceSlurmActive || item.Outcome != ObservationOutcomeError {
			t.Fatalf("upsert observation %d = %#v, want active/error", i, item)
		}
	}
}

func TestReconcileJobsPreservesSeenStoreOrderingAcrossWorkers(t *testing.T) {
	closed := make(chan struct{})
	close(closed)
	seen := state.NewMemorySeenJobStore(
		state.SeenJobRecord{JobID: "200"},
		state.SeenJobRecord{JobID: "300"},
	)
	err300 := errors.New("failure-300")
	client := newConcurrentUpsertClient(func(string) <-chan struct{} { return closed })
	client.errByJobID = map[string]error{"300": err300}
	client.onStart = func(jobID string) error {
		records, err := seen.ListSeenJobs(context.Background())
		if err != nil {
			return err
		}
		for _, record := range records {
			if record.JobID == jobID {
				return nil
			}
		}
		return errors.New("job was not marked seen before upsert: " + jobID)
	}
	snapshot := newControllerSnapshot(time.Now())
	addActiveJob(&snapshot, managedJob(cluster.JobExecutionRecord{
		JobID:      "100",
		WorkflowID: "workflow-100",
		State:      cluster.JobStateRunning,
	}))
	for _, jobID := range []string{"200", "300"} {
		addAccountingJob(&snapshot, managedJob(cluster.JobExecutionRecord{
			JobID:      jobID,
			WorkflowID: "workflow-" + jobID,
			State:      cluster.JobStateFailed,
		}))
	}
	controller := Controller{
		HeimdallClient: client,
		SeenJobs:       seen,
		Options: ControllerOptions{
			JobUpsertWorkerPoolSize: 3,
		},
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve},
	}

	err := controller.reconcileJobs(context.Background(), snapshot, nil)
	if !errors.Is(err, err300) {
		t.Fatalf("reconcileJobs() error = %v, want failed finalized upsert", err)
	}
	records, listErr := seen.ListSeenJobs(context.Background())
	if listErr != nil {
		t.Fatalf("ListSeenJobs() failed: %v", listErr)
	}
	// Successful final publication removes job 200. The active job and failed
	// publication remain discoverable so a later tick can retry job 300.
	want := []state.SeenJobRecord{{JobID: "100"}, {JobID: "300"}}
	if len(records) != len(want) || records[0] != want[0] || records[1] != want[1] {
		t.Fatalf("seen records = %v, want active and unpublished attempts %v", records, want)
	}
}

func concurrentUpsertTestController(
	client *concurrentUpsertClient,
	workerPoolSize int,
	jobIDs ...string,
) (Controller, ControllerSnapshot) {
	snapshot := newControllerSnapshot(time.Now())
	for _, jobID := range jobIDs {
		addActiveJob(&snapshot, managedJob(cluster.JobExecutionRecord{
			JobID:      jobID,
			WorkflowID: "workflow-" + jobID,
			State:      cluster.JobStateRunning,
		}))
	}
	return Controller{
		HeimdallClient: client,
		SeenJobs:       state.NewMemorySeenJobStore(),
		Options: ControllerOptions{
			JobUpsertWorkerPoolSize: workerPoolSize,
		},
		ModePolicy: ModePolicy{DefaultMode: cluster.JobModeObserve},
	}, snapshot
}

func waitForUpsertStart(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case jobID := <-started:
		return jobID
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for upsert to start")
		return ""
	}
}

func waitForUpsertCompletion(t *testing.T, completed <-chan string, want string) {
	t.Helper()
	select {
	case jobID := <-completed:
		if jobID != want {
			t.Fatalf("upsert completion = %s, want %s", jobID, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for upsert %s to complete", want)
	}
}

type concurrentUpsertClient struct {
	mu          sync.Mutex
	started     chan string
	completed   chan string
	release     func(string) <-chan struct{}
	errByJobID  map[string]error
	onStart     func(string) error
	calls       int
	inFlight    int
	maxInFlight int
}

func newConcurrentUpsertClient(release func(string) <-chan struct{}) *concurrentUpsertClient {
	return &concurrentUpsertClient{
		started:   make(chan string, 32),
		completed: make(chan string, 32),
		release:   release,
	}
}

func (c *concurrentUpsertClient) CreateOrUpdateJob(
	ctx context.Context,
	req *pb.CreateOrUpdateJobRequest,
) (*pb.CreateOrUpdateJobResponse, error) {
	jobID := req.GetJobId()
	c.mu.Lock()
	c.calls++
	c.inFlight++
	c.maxInFlight = max(c.maxInFlight, c.inFlight)
	c.mu.Unlock()
	c.started <- jobID
	var startErr error
	if c.onStart != nil {
		startErr = c.onStart(jobID)
	}

	select {
	case <-c.release(jobID):
	case <-ctx.Done():
	}

	c.mu.Lock()
	c.inFlight--
	err := c.errByJobID[jobID]
	c.mu.Unlock()
	c.completed <- jobID
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}
	if startErr != nil {
		return nil, startErr
	}
	if err != nil {
		return nil, err
	}
	return &pb.CreateOrUpdateJobResponse{}, nil
}

func (*concurrentUpsertClient) GetJobs(context.Context, ...heimdall.PaginationOption) ([]*pb.Job, error) {
	return nil, nil
}

func (*concurrentUpsertClient) ListAdvice(context.Context, ...heimdall.PaginationOption) ([]*pb.Advice, error) {
	return nil, nil
}

func (*concurrentUpsertClient) UpdateAdviceReconciliationStatus(
	context.Context,
	string,
	*pb.AdviceReconciliationStatus,
) (*pb.AdviceReconciliationStatus, error) {
	return nil, nil
}

func (*concurrentUpsertClient) RecordRecoveryMilestone(
	context.Context,
	*pb.RecordRecoveryMilestoneRequest,
) (*pb.RecordRecoveryMilestoneResponse, error) {
	return nil, nil
}

func (c *concurrentUpsertClient) maxConcurrency() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxInFlight
}

func (c *concurrentUpsertClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}
