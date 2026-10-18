// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

func TestNewSlurmClusterWiresAccountingLimits(t *testing.T) {
	slurmCluster := newSlurmCluster(Options{
		AccountingWorkerPoolSize: 7,
		AccountingTimeout:        3 * time.Minute,
	})
	if slurmCluster.accountingWorkerPoolSize != 7 {
		t.Fatalf("accounting worker pool size = %d, want 7", slurmCluster.accountingWorkerPoolSize)
	}
	if slurmCluster.accountingTimeout != 3*time.Minute {
		t.Fatalf("accounting timeout = %s, want 3m", slurmCluster.accountingTimeout)
	}
}

func TestNewSlurmClusterDefaultsAccountingWorkerPoolSize(t *testing.T) {
	slurmCluster := newSlurmCluster(Options{})
	if slurmCluster.accountingWorkerPoolSize != defaultAccountingWorkerPoolSize {
		t.Fatalf(
			"accounting worker pool size = %d, want default %d",
			slurmCluster.accountingWorkerPoolSize,
			defaultAccountingWorkerPoolSize,
		)
	}
}

func TestNewSlurmClusterZeroOptionsRunsFinalizedJobs(t *testing.T) {
	executor := &recordingExecutor{results: map[string][]byte{
		"sacct\x00-Xj\x0012345\x00--duplicates\x00--json": []byte(
			`{"jobs":[{"job_id":12345,"state":{"current":"COMPLETED"}}]}`,
		),
	}}
	slurmCluster := newSlurmCluster(Options{})
	slurmCluster.roCommands = executor

	jobs, err := slurmCluster.FinalizedJobs(
		context.Background(),
		[]cluster.JobAttemptRef{{JobID: "12345"}},
	)
	if err != nil {
		t.Fatalf("FinalizedJobs() failed: %v", err)
	}
	if len(jobs) != 1 || jobs[0].JobID != "12345" {
		t.Fatalf("jobs = %#v, want completed job 12345", jobs)
	}
}

func TestFinalizedJobsBoundsOneQueryPerUniqueJobID(t *testing.T) {
	release := make(chan struct{})
	executor := newConcurrentAccountingExecutor(func(string) <-chan struct{} { return release })
	slurmCluster := &slurmCluster{
		timeout:                  time.Second,
		accountingWorkerPoolSize: 2,
		accountingTimeout:        time.Second,
		roCommands:               executor,
	}
	records := []cluster.JobAttemptRef{
		{JobID: "100", AttemptIndex: 0},
		{JobID: "100", AttemptIndex: 1},
		{JobID: "200", AttemptIndex: 0},
		{JobID: "300", AttemptIndex: 0},
		{JobID: "400", AttemptIndex: 0},
	}

	type finalizedResult struct {
		jobs []cluster.JobExecutionRecord
		err  error
	}
	resultCh := make(chan finalizedResult, 1)
	go func() {
		jobs, err := slurmCluster.FinalizedJobs(context.Background(), records)
		resultCh <- finalizedResult{jobs: jobs, err: err}
	}()

	for range 2 {
		waitForAccountingStart(t, executor.started)
	}
	select {
	case jobID := <-executor.started:
		t.Fatalf("accounting query %s started above configured worker pool size", jobID)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)

	result := <-resultCh
	if result.err != nil {
		t.Fatalf("FinalizedJobs() failed: %v", result.err)
	}
	if got := executor.maxConcurrency(); got != 2 {
		t.Fatalf("maximum concurrent accounting queries = %d, want 2", got)
	}
	calls := executor.callArgs()
	if len(calls) != 4 {
		t.Fatalf("accounting calls = %d, want one per 4 unique job IDs", len(calls))
	}
	wantJobIDs := []string{"100", "200", "300", "400"}
	gotJobIDs := make([]string, 0, len(calls))
	for _, args := range calls {
		if len(args) != 5 || args[0] != "sacct" || args[1] != "-Xj" ||
			args[3] != "--duplicates" || args[4] != "--json" {
			t.Fatalf("accounting command = %v, want one single-job sacct request", args)
		}
		if strings.Contains(args[2], ",") {
			t.Fatalf("accounting command batched job IDs: %v", args)
		}
		gotJobIDs = append(gotJobIDs, args[2])
	}
	slices.Sort(gotJobIDs)
	if !slices.Equal(gotJobIDs, wantJobIDs) {
		t.Fatalf("queried job IDs = %v, want %v", gotJobIDs, wantJobIDs)
	}
}

func TestFinalizedJobsReturnsStableJobAndErrorOrder(t *testing.T) {
	t.Run("jobs", func(t *testing.T) {
		gates := accountingGates("100", "200", "300")
		executor := newConcurrentAccountingExecutor(func(jobID string) <-chan struct{} { return gates[jobID] })
		executor.outputByJobID = map[string][]byte{
			"100": []byte(`{"jobs":[
				{"job_id":100,"state":{"current":"FAILED"},"time":{"submission":10,"start":20,"end":30}},
				{"job_id":100,"state":{"current":"FAILED"},"time":{"submission":40,"start":50,"end":60}},
				{"job_id":100,"state":{"current":"COMPLETED"},"time":{"submission":70,"start":80,"end":90}}
			]}`),
		}
		slurmCluster := &slurmCluster{
			timeout:                  time.Second,
			accountingWorkerPoolSize: 3,
			accountingTimeout:        time.Second,
			roCommands:               executor,
		}

		resultCh := make(chan []cluster.JobExecutionRecord, 1)
		errCh := make(chan error, 1)
		go func() {
			jobs, err := slurmCluster.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{
				{JobID: "300"},
				{JobID: "100", AttemptIndex: 1},
				{JobID: "100", AttemptIndex: 0},
				{JobID: "200"},
			})
			resultCh <- jobs
			errCh <- err
		}()

		for range 3 {
			waitForAccountingStart(t, executor.started)
		}
		releaseAccountingInOrder(t, executor.completed, gates, "300", "200", "100")

		jobs := <-resultCh
		if err := <-errCh; err != nil {
			t.Fatalf("FinalizedJobs() failed: %v", err)
		}
		got := make([]string, 0, len(jobs))
		for _, job := range jobs {
			got = append(got, fmt.Sprintf("%s/%d", job.JobID, job.AttemptIndex))
		}
		if want := []string{"100/0", "100/1", "200/0", "300/0"}; !slices.Equal(got, want) {
			t.Fatalf("job order = %v, want %v", got, want)
		}
	})

	t.Run("errors", func(t *testing.T) {
		gates := accountingGates("100", "200", "300")
		err100 := errors.New("failure-100")
		err200 := errors.New("failure-200")
		err300 := errors.New("failure-300")
		executor := newConcurrentAccountingExecutor(func(jobID string) <-chan struct{} { return gates[jobID] })
		executor.errByJobID = map[string]error{
			"100": err100,
			"200": err200,
			"300": err300,
		}
		slurmCluster := &slurmCluster{
			timeout:                  time.Second,
			accountingWorkerPoolSize: 3,
			accountingTimeout:        time.Second,
			roCommands:               executor,
		}

		errCh := make(chan error, 1)
		go func() {
			_, err := slurmCluster.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{
				{JobID: "300"},
				{JobID: "100"},
				{JobID: "200"},
			})
			errCh <- err
		}()

		for range 3 {
			waitForAccountingStart(t, executor.started)
		}
		releaseAccountingInOrder(t, executor.completed, gates, "300", "200", "100")

		err := <-errCh
		for _, want := range []error{err100, err200, err300} {
			if !errors.Is(err, want) {
				t.Fatalf("FinalizedJobs() error = %v, want cause %v", err, want)
			}
		}
		errorText := err.Error()
		index100 := strings.Index(errorText, "job 100")
		index200 := strings.Index(errorText, "job 200")
		index300 := strings.Index(errorText, "job 300")
		if index100 < 0 || index200 <= index100 || index300 <= index200 {
			t.Fatalf("joined error order = %q, want jobs 100, 200, 300", errorText)
		}
	})
}

func TestFinalizedJobsPhaseTimeoutCancelsWorkersAndQueuedWork(t *testing.T) {
	executor := &sleepAccountingExecutor{started: make(chan string, 8)}
	slurmCluster := &slurmCluster{
		timeout:                  5 * time.Second,
		accountingWorkerPoolSize: 2,
		accountingTimeout:        25 * time.Millisecond,
		roCommands:               executor,
	}

	startedAt := time.Now()
	_, err := slurmCluster.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{
		{JobID: "100"},
		{JobID: "200"},
		{JobID: "300"},
		{JobID: "400"},
		{JobID: "500"},
		{JobID: "600"},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("FinalizedJobs() error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("FinalizedJobs() elapsed = %s, want phase timeout to stop promptly", elapsed)
	}
	if got := executor.callCount(); got != 2 {
		t.Fatalf("accounting calls started across timeout = %d, want 2 in-flight only", got)
	}
}

func TestFinalizedJobsKeepsSuccessfulQueriesAfterFailure(t *testing.T) {
	commandErr := errors.New("accounting unavailable")
	for _, tc := range []struct {
		name   string
		err    error
		output []byte
	}{
		{name: "command", err: commandErr},
		{name: "invalid JSON", output: []byte(`{"jobs":`)},
		{
			name: "invalid record after valid record",
			output: []byte(`{"jobs":[
				{"job_id":200,"state":{"current":"COMPLETED"},"time":{"submission":10,"start":20}},
				{"job_id":200,"nodes":"node[invalid]","time":{"submission":30,"start":40}}
			]}`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			close(release)
			executor := newConcurrentAccountingExecutor(func(string) <-chan struct{} { return release })
			executor.errByJobID = map[string]error{"200": tc.err}
			executor.outputByJobID = map[string][]byte{"200": tc.output}
			slurmCluster := newSlurmCluster(Options{AccountingWorkerPoolSize: 1})
			slurmCluster.roCommands = executor

			jobs, err := slurmCluster.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{
				{JobID: "300"}, {JobID: "200"}, {JobID: "100"},
			})
			if err == nil {
				t.Fatal("FinalizedJobs() succeeded despite failed accounting request")
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("FinalizedJobs() error = %v, want cause %v", err, tc.err)
			}
			var got []cluster.JobAttemptRef
			for _, job := range jobs {
				got = append(got, jobAttemptRef(job))
			}
			want := []cluster.JobAttemptRef{{JobID: "100"}, {JobID: "300"}}
			if !slices.Equal(got, want) {
				t.Fatalf("successful attempts = %v, want %v", got, want)
			}
			if len(executor.callArgs()) != 3 {
				t.Fatalf("queries = %v, want all three attempted despite failure", executor.callArgs())
			}
		})
	}
}

func TestFinalizedJobsKeepsCompletedQueriesOnCancellation(t *testing.T) {
	for _, phaseTimeout := range []bool{false, true} {
		name := "parent cancellation"
		if phaseTimeout {
			name = "accounting deadline"
		}
		t.Run(name, func(t *testing.T) {
			executor := &sleepAccountingExecutor{
				started: make(chan string, 8),
				outputByJobID: map[string][]byte{
					"100": []byte(`{"jobs":[{"job_id":100,"state":{"current":"COMPLETED"}}]}`),
				},
			}
			slurmCluster := newSlurmCluster(Options{
				Timeout:                  5 * time.Second,
				AccountingWorkerPoolSize: 1,
			})
			slurmCluster.roCommands = executor
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wantErr := context.Canceled
			if phaseTimeout {
				slurmCluster.accountingTimeout = 100 * time.Millisecond
				wantErr = context.DeadlineExceeded
			}
			type result struct {
				jobs []cluster.JobExecutionRecord
				err  error
			}
			done := make(chan result, 1)
			go func() {
				jobs, err := slurmCluster.FinalizedJobs(ctx, []cluster.JobAttemptRef{
					{JobID: "100"}, {JobID: "200"}, {JobID: "300"}, {JobID: "400"},
				})
				done <- result{jobs: jobs, err: err}
			}()
			for range 2 {
				waitForAccountingStart(t, executor.started)
			}
			if !phaseTimeout {
				cancel()
			}
			select {
			case got := <-done:
				if !errors.Is(got.err, wantErr) {
					t.Fatalf("FinalizedJobs() error = %v, want %v", got.err, wantErr)
				}
				if len(got.jobs) != 1 || got.jobs[0].JobID != "100" {
					t.Fatalf("jobs = %#v, want completed query for 100", got.jobs)
				}
			case <-time.After(time.Second):
				t.Fatal("FinalizedJobs() did not join canceled workers promptly")
			}
			if got := executor.callCount(); got != 2 {
				t.Fatalf("executed queries = %d, want completed and in-flight queries only", got)
			}
		})
	}
}

func accountingGates(jobIDs ...string) map[string]chan struct{} {
	gates := make(map[string]chan struct{}, len(jobIDs))
	for _, jobID := range jobIDs {
		gates[jobID] = make(chan struct{})
	}
	return gates
}

func releaseAccountingInOrder(
	t *testing.T,
	completed <-chan string,
	gates map[string]chan struct{},
	jobIDs ...string,
) {
	t.Helper()
	for _, jobID := range jobIDs {
		close(gates[jobID])
		select {
		case got := <-completed:
			if got != jobID {
				t.Fatalf("accounting completion = %s, want %s", got, jobID)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for accounting job %s", jobID)
		}
	}
}

func waitForAccountingStart(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case jobID := <-started:
		return jobID
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for accounting query to start")
		return ""
	}
}

type concurrentAccountingExecutor struct {
	mu            sync.Mutex
	started       chan string
	completed     chan string
	release       func(string) <-chan struct{}
	errByJobID    map[string]error
	outputByJobID map[string][]byte
	calls         [][]string
	inFlight      int
	maxInFlight   int
}

func newConcurrentAccountingExecutor(release func(string) <-chan struct{}) *concurrentAccountingExecutor {
	return &concurrentAccountingExecutor{
		started:   make(chan string, 32),
		completed: make(chan string, 32),
		release:   release,
	}
}

func (e *concurrentAccountingExecutor) Execute(cmd *exec.Cmd) (commandOutput, error) {
	args := slices.Clone(cmd.Args)
	jobID := ""
	if len(args) == 5 {
		jobID = args[2]
	}
	e.mu.Lock()
	e.calls = append(e.calls, args)
	e.inFlight++
	e.maxInFlight = max(e.maxInFlight, e.inFlight)
	e.mu.Unlock()
	e.started <- jobID

	<-e.release(jobID)

	e.mu.Lock()
	e.inFlight--
	err := e.errByJobID[jobID]
	e.mu.Unlock()
	e.completed <- jobID
	if err != nil {
		return commandOutput{}, err
	}
	if output := e.outputByJobID[jobID]; output != nil {
		return commandOutput{stdout: slices.Clone(output)}, nil
	}
	return commandOutput{stdout: []byte(fmt.Sprintf(
		`{"jobs":[{"job_id":%s,"state":{"current":"COMPLETED"},"time":{"submission":10,"start":20,"end":30}}]}`,
		jobID,
	))}, nil
}

func (e *concurrentAccountingExecutor) maxConcurrency() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.maxInFlight
}

func (e *concurrentAccountingExecutor) callArgs() [][]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([][]string, len(e.calls))
	for i := range e.calls {
		out[i] = slices.Clone(e.calls[i])
	}
	return out
}

type sleepAccountingExecutor struct {
	mu            sync.Mutex
	started       chan string
	calls         int
	outputByJobID map[string][]byte
}

func (e *sleepAccountingExecutor) Execute(cmd *exec.Cmd) (commandOutput, error) {
	jobID := cmd.Args[2]
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	e.started <- jobID
	if output := e.outputByJobID[jobID]; output != nil {
		return commandOutput{stdout: output}, nil
	}

	cmd.Path = "/bin/sleep"
	cmd.Args = []string{"sleep", "5"}
	cmd.Err = nil
	return executor{}.Execute(cmd)
}

func (e *sleepAccountingExecutor) callCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}
