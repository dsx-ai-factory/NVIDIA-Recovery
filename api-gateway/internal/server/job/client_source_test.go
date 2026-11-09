// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package job

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	v1 "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	jobmanagerBindings "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/jobmanager"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/jobmanager"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestParseJobAndAttemptIndex(t *testing.T) {
	tests := []struct {
		jobRunId     string
		jobId        string
		attemptIndex int32
	}{
		{
			jobRunId:     "job_123-0",
			jobId:        "job_123",
			attemptIndex: 0,
		},
		{
			jobRunId:     "job_123-1",
			jobId:        "job_123",
			attemptIndex: 1,
		},
		{
			jobRunId:     "foo-bar-0",
			jobId:        "foo-bar",
			attemptIndex: 0,
		},
		{
			jobRunId:     "foo-bar-baz-2",
			jobId:        "foo-bar-baz",
			attemptIndex: 2,
		},
	}

	for _, test := range tests {
		parsedRunId, err := parseJobAndAttemptIndex(test.jobRunId)
		if err != nil {
			t.Errorf("parseJobAndAttemptIndex(%s) = %v", test.jobRunId, err)
		}
		if parsedRunId.JobId != test.jobId || parsedRunId.AttemptIndex != test.attemptIndex {
			t.Errorf("parseJobAndAttemptIndex(%s) = %s, %d, want %s, %d", test.jobRunId, parsedRunId.JobId, parsedRunId.AttemptIndex, test.jobId, test.attemptIndex)
		}
	}
}

func TestCreateApplicationIntention(t *testing.T) {
	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/application-intentions" {
			t.Fatalf("request = %s %s, want POST /application-intentions", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if body["job_id"] != "42" || body["desired_state"] != "CANCELLED" {
			t.Fatalf("unexpected request body: %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":7,"job_id":"42","desired_state":"CANCELLED","reason":"training converged","created_at":%q}`, createdAt.Format(time.RFC3339Nano))
	}))
	defer backend.Close()
	reason := "training converged"

	response, err := NewJobClientSource(backend.URL).CreateApplicationIntention(
		context.Background(),
		&v1.CreateApplicationIntentionRequest{
			JobId:        "42",
			DesiredState: v1.ApplicationIntentionDesiredState_APPLICATION_INTENTION_DESIRED_STATE_CANCELLED,
			Reason:       &reason,
		},
	)

	if err != nil {
		t.Fatalf("CreateApplicationIntention() failed: %v", err)
	}
	got := response.GetApplicationIntention()
	if got.GetId() != 7 || got.GetJobId() != "42" || got.GetReason() != reason {
		t.Fatalf("unexpected response: %#v", got)
	}
	if !got.GetCreatedAt().AsTime().Equal(createdAt) {
		t.Fatalf("created_at = %v, want %v", got.GetCreatedAt().AsTime(), createdAt)
	}
}

type testSlurmJobExecutionStatus struct {
	jobId             string
	jobAttemptIndex   int32
	workflowId        string
	jobName           string
	state             string
	submitTime        *time.Time
	startTime         *time.Time
	endTime           *time.Time
	allocatedNodeList []string
	totalGpuCount     int32
	nodeCount         int32
	heimdallJobId     string
}

func (s *testSlurmJobExecutionStatus) ToSlurmJobExecutionStatus() *jobmanager.SlurmJobExecutionStatus {
	job := &jobmanager.SlurmJobExecutionStatus{
		JobId:             s.jobId,
		JobAttemptIndex:   s.jobAttemptIndex,
		WorkflowId:        s.workflowId,
		AllocatedNodeList: s.allocatedNodeList,
	}

	if s.heimdallJobId != "" {
		job.HeimdallJobId.Set(&s.heimdallJobId)
	}

	if s.submitTime != nil {
		job.SubmitTime.Set(s.submitTime)
	}

	if s.startTime != nil {
		job.StartTime.Set(s.startTime)
	}

	if s.endTime != nil {
		job.EndTime.Set(s.endTime)
	}

	if s.jobName != "" {
		job.JobName.Set(&s.jobName)
	}

	if s.state != "" {
		job.State.Set(&s.state)
	}

	if s.totalGpuCount > 0 {
		job.TotalGpuCount.Set(&s.totalGpuCount)
	}

	if s.nodeCount > 0 {
		job.NodeCount.Set(&s.nodeCount)
	}

	return job
}

func WithSubmitTime(submitTime time.Time) func(*testSlurmJobExecutionStatus) {
	return func(s *testSlurmJobExecutionStatus) {
		s.submitTime = &submitTime
	}
}

func WithStartTime(startTime time.Time) func(*testSlurmJobExecutionStatus) {
	return func(s *testSlurmJobExecutionStatus) {
		s.startTime = &startTime
	}
}

func WithEndTime(endTime time.Time) func(*testSlurmJobExecutionStatus) {
	return func(s *testSlurmJobExecutionStatus) {
		s.endTime = &endTime
	}
}

func mustParseTime(timeStr string) time.Time {
	time, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		panic(err)
	}
	return time
}

func testSlurmJobExecutionStatusWithOpts(s *testSlurmJobExecutionStatus, opts ...func(*testSlurmJobExecutionStatus)) *testSlurmJobExecutionStatus {
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func TestSlurmJobExecutionStatusToJobPb(t *testing.T) {
	jobName := "job_123"
	tests := []struct {
		job   *jobmanager.SlurmJobExecutionStatus
		jobPb *v1.Job
	}{
		{
			job: testSlurmJobExecutionStatusWithOpts(&testSlurmJobExecutionStatus{
				jobId:             "job_123",
				jobAttemptIndex:   1,
				workflowId:        "job_123",
				jobName:           "job_123",
				state:             "RUNNING",
				allocatedNodeList: []string{"node1", "node2"},
				totalGpuCount:     16,
				nodeCount:         2,
				heimdallJobId:     "00000000-0000-0000-0000-000000000001",
			},
				WithSubmitTime(mustParseTime("2021-01-01T00:00:00Z")),
				WithStartTime(mustParseTime("2021-01-01T00:00:00Z")),
				WithEndTime(mustParseTime("2021-01-01T00:00:00Z")),
			).ToSlurmJobExecutionStatus(),
			jobPb: &v1.Job{
				HeimdallJobId: "00000000-0000-0000-0000-000000000001",
				JobRunId:      "job_123-1",
				JobId:         "job_123",
				JobName:       &jobName,
				WorkflowId:    "job_123",
				AttemptIndex:  1,
				State:         v1.JobStatus_JOB_STATUS_RUNNING,
				CreatedAt:     timestamppb.New(mustParseTime("2021-01-01T00:00:00Z")),
				Nodes:         []string{"node1", "node2"},
			},
		},
		{
			job: testSlurmJobExecutionStatusWithOpts(&testSlurmJobExecutionStatus{
				jobId:             "job_123",
				jobAttemptIndex:   1,
				workflowId:        "job_123",
				jobName:           "job_123",
				state:             "RUNNING",
				allocatedNodeList: []string{"node1", "node2"},
				totalGpuCount:     16,
				nodeCount:         2,
				heimdallJobId:     "00000000-0000-0000-0000-000000000002",
			},
				WithSubmitTime(mustParseTime("2021-01-01T00:00:00Z")),
				WithStartTime(mustParseTime("2021-01-01T00:00:00Z")),
			).ToSlurmJobExecutionStatus(),
			jobPb: &v1.Job{
				HeimdallJobId: "00000000-0000-0000-0000-000000000002",
				JobRunId:      "job_123-1",
				JobId:         "job_123",
				JobName:       &jobName,
				WorkflowId:    "job_123",
				AttemptIndex:  1,
				State:         v1.JobStatus_JOB_STATUS_RUNNING,
				CreatedAt:     timestamppb.New(mustParseTime("2021-01-01T00:00:00Z")),
				Nodes:         []string{"node1", "node2"},
			},
		},
		{
			job: testSlurmJobExecutionStatusWithOpts(&testSlurmJobExecutionStatus{
				jobId:             "job_123",
				jobAttemptIndex:   1,
				workflowId:        "job_123",
				jobName:           "job_123",
				state:             "RUNNING",
				allocatedNodeList: []string{"node1"},
				totalGpuCount:     16,
				nodeCount:         2,
				heimdallJobId:     "00000000-0000-0000-0000-000000000003",
			},
				WithSubmitTime(mustParseTime("2021-01-01T00:00:00Z")),
				WithStartTime(mustParseTime("2021-01-01T00:00:00Z")),
				WithEndTime(mustParseTime("2021-01-01T00:00:00Z")),
			).ToSlurmJobExecutionStatus(),
			jobPb: &v1.Job{
				HeimdallJobId: "00000000-0000-0000-0000-000000000003",
				JobRunId:      "job_123-1",
				JobId:         "job_123",
				JobName:       &jobName,
				WorkflowId:    "job_123",
				AttemptIndex:  1,
				State:         v1.JobStatus_JOB_STATUS_RUNNING,
				CreatedAt:     timestamppb.New(mustParseTime("2021-01-01T00:00:00Z")),
				Nodes:         []string{"node1"},
			},
		},
		{
			job: testSlurmJobExecutionStatusWithOpts(&testSlurmJobExecutionStatus{
				jobId:             "job_123",
				jobAttemptIndex:   1,
				workflowId:        "job_123",
				jobName:           "job_123",
				state:             "PENDING",
				allocatedNodeList: []string{},
				totalGpuCount:     16,
				nodeCount:         2,
				heimdallJobId:     "00000000-0000-0000-0000-000000000004",
			},
				WithSubmitTime(mustParseTime("2021-01-01T00:00:00Z")),
			).ToSlurmJobExecutionStatus(),
			jobPb: &v1.Job{
				HeimdallJobId: "00000000-0000-0000-0000-000000000004",
				JobRunId:      "job_123-1",
				JobId:         "job_123",
				JobName:       &jobName,
				WorkflowId:    "job_123",
				AttemptIndex:  1,
				State:         v1.JobStatus_JOB_STATUS_PENDING,
				CreatedAt:     timestamppb.New(mustParseTime("2021-01-01T00:00:00Z")),
				Nodes:         []string{},
			},
		},
	}

	for _, test := range tests {
		jobPb, err := slurmJobExecutionStatusToJobPb(context.Background(), test.job)
		if err != nil {
			t.Errorf("slurmJobExecutionStatusToJobPb(%v) = %v", test.job, err)
		}
		if jobPb.HeimdallJobId == "" {
			t.Fatalf("HeimdallJobId is empty")
		}
		if test.jobPb.State != jobPb.State {
			t.Errorf("State mismatch: got %v, want %v (slurmJobExecutionStatusToJobPb(%v))", jobPb.State, test.jobPb.State, test.job)
		}
		if !test.jobPb.CreatedAt.AsTime().Equal(jobPb.CreatedAt.AsTime()) {
			t.Errorf("CreatedAt mismatch: got %v, want %v (slurmJobExecutionStatusToJobPb(%v))", jobPb.CreatedAt.AsTime(), test.jobPb.CreatedAt.AsTime(), test.job)
		}
		if jobPb.LastSeen != nil || jobPb.UpdatedAt != nil {
			t.Errorf("missing source timestamp produced freshness values: last_seen=%v updated_at=%v", jobPb.LastSeen, jobPb.UpdatedAt)
		}
		if !reflect.DeepEqual(test.jobPb.Nodes, jobPb.Nodes) {
			t.Errorf("Nodes mismatch: got %v, want %v (slurmJobExecutionStatusToJobPb(%v))", jobPb.Nodes, test.jobPb.Nodes, test.job)
		}
	}
}

func TestGetJobs(t *testing.T) {
	jobResponse := map[string]interface{}{
		"statuses": []map[string]interface{}{
			{
				"job_id":                     "job_123",
				"job_attempt_index":          1,
				"workflow_id":                "job_123",
				"heimdall_job_id":            "00000000-0000-0000-0000-000000000123",
				"job_name":                   "job_123",
				"state":                      "RUNNING",
				"state_reason":               "NonZeroExitCode",
				"cluster":                    "test-cluster-01",
				"partition":                  "pool0",
				"reservation":                nil,
				"namespace":                  nil,
				"user":                       "test-user",
				"command":                    "/tmp/test-slurm/job12345/slurm_script",
				"dependency":                 nil,
				"allocated_node_list":        []string{"node1", "node2"},
				"inclusion_node_list":        nil,
				"exclusion_node_list":        nil,
				"total_gpu_count":            16,
				"total_cpu_count":            16,
				"total_memory_mb":            3681182,
				"node_count":                 2,
				"submit_time":                "2021-01-01T00:00:00Z",
				"start_time":                 "2021-01-01T00:00:00Z",
				"end_time":                   "2021-01-01T00:00:00Z",
				"sticky_exclusion_node_list": nil,
				"anomalies":                  nil,
				"checkpoints":                nil,
				"aps_spec":                   nil,
				"retry_of":                   nil,
				"last_updated_at":            "2024-09-30T15:13:09Z",
				"log_file_paths":             nil,
				"total_task_count":           nil,
				// Check we can handle new fields without breaking
				"new_field_that_does_not_exist": nil,
				// Include scheduler_metadata to verify they are parsed correctly
				"schedulerMetadata": map[string]interface{}{
					"kind":      "PyTorchJob",
					"namespace": "default",
				},
			},
		},
		"item_count": 1,
	}

	// Standup mock job manager http server
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		response, err := json.Marshal(jobResponse)
		if err != nil {
			t.Fatalf("Failed to marshal job response: %v", err)
		}

		_, err = w.Write(response)
		if err != nil {
			t.Fatalf("Failed to write job response: %v", err)
		}
	}))
	defer jobManagerServer.Close()

	// Create job client source
	jobClientSource := NewJobClientSource(jobManagerServer.URL)

	jobsResponse, err := jobClientSource.GetJobs(context.Background(), &v1.GetJobsRequest{})
	if err != nil {
		t.Fatalf("GetJobs failed: %v", err)
	}

	if jobsResponse.Count != 1 {
		t.Fatalf("TotalCount mismatch: got %d, want %d", jobsResponse.Count, 1)
	}

	if len(jobsResponse.Jobs) != 1 {
		t.Fatalf("Jobs mismatch: got %d, want %d", len(jobsResponse.Jobs), 1)
	}

	job := jobsResponse.Jobs[0]
	if job.HeimdallJobId == "" {
		t.Error("Expected job to have HeimdallJobId")
	}

	// check scheduler metadata
	if job.SchedulerMetadata == nil {
		t.Error("Expected scheduler metadata to be set")
	} else {
		fields := job.SchedulerMetadata.GetFields()
		if len(fields) != 2 {
			t.Errorf("Expected 2 items in scheduler metadata, got %d", len(fields))
		}
		if kindValue, ok := fields["kind"]; !ok {
			t.Error("Expected 'kind' in scheduler metadata")
		} else if kindValue.GetStringValue() != "PyTorchJob" {
			t.Errorf("Expected kind 'PyTorchJob', got '%s'", kindValue.GetStringValue())
		}
	}

	// Additional assertions on the returned job
	jobPb := jobsResponse.Jobs[0]
	if jobPb.HeimdallJobId != "00000000-0000-0000-0000-000000000123" {
		t.Fatalf("HeimdallJobId mismatch: got %s, want %s", jobPb.HeimdallJobId, "00000000-0000-0000-0000-000000000123")
	}
	if jobPb.JobRunId != "job_123-1" {
		t.Fatalf("JobRunId mismatch: got %s, want %s", jobPb.JobRunId, "job_123-1")
	}
	if jobPb.JobId != "job_123" {
		t.Fatalf("JobId mismatch: got %s, want %s", jobPb.JobId, "job_123")
	}
	if jobPb.AttemptIndex != 1 {
		t.Fatalf("AttemptIndex mismatch: got %d, want %d", jobPb.AttemptIndex, 1)
	}
	if jobPb.WorkflowId != "job_123" {
		t.Fatalf("WorkflowId mismatch: got %s, want %s", jobPb.WorkflowId, "job_123")
	}
	if jobPb.State != v1.JobStatus_JOB_STATUS_RUNNING {
		t.Fatalf("State mismatch: got %v, want %v", jobPb.State, v1.JobStatus_JOB_STATUS_RUNNING)
	}
	if jobPb.CreatedAt == nil {
		t.Fatal("CreatedAt should not be nil")
	}
	if jobPb.CreatedAt.AsTime().Format(time.RFC3339) != "2021-01-01T00:00:00Z" {
		t.Fatalf("CreatedAt mismatch: got %s, want %s", jobPb.CreatedAt.AsTime().Format(time.RFC3339), "2021-01-01T00:00:00Z")
	}
	wantSourceTimestamp := mustParseTime("2024-09-30T15:13:09Z")
	if jobPb.LastSeen == nil || !jobPb.LastSeen.AsTime().Equal(wantSourceTimestamp) {
		t.Fatalf("LastSeen = %v, want source timestamp %v", jobPb.LastSeen, wantSourceTimestamp)
	}
	if jobPb.UpdatedAt == nil || !jobPb.UpdatedAt.AsTime().Equal(wantSourceTimestamp) {
		t.Fatalf("UpdatedAt = %v, want source timestamp %v", jobPb.UpdatedAt, wantSourceTimestamp)
	}
	if !reflect.DeepEqual(jobPb.Nodes, []string{"node1", "node2"}) {
		t.Fatalf("Nodes mismatch: got %v, want %v", jobPb.Nodes, []string{"node1", "node2"})
	}

	secondResponse, err := jobClientSource.GetJobs(context.Background(), &v1.GetJobsRequest{})
	if err != nil {
		t.Fatalf("second GetJobs failed: %v", err)
	}
	secondJob := secondResponse.Jobs[0]
	if !secondJob.LastSeen.AsTime().Equal(jobPb.LastSeen.AsTime()) || !secondJob.UpdatedAt.AsTime().Equal(jobPb.UpdatedAt.AsTime()) {
		t.Fatalf("repeated GET changed timestamps: first=(%v, %v) second=(%v, %v)",
			jobPb.LastSeen, jobPb.UpdatedAt, secondJob.LastSeen, secondJob.UpdatedAt)
	}
}

func TestJobClientSource_GetJobsUsesExactWorkflowIDMembershipFilter(t *testing.T) {
	workflowID := "  cafe\u0301/%_\\  "
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/jobs" {
			t.Errorf("path = %q, want /jobs", r.URL.Path)
		}
		if got := r.URL.Query().Get("workflow_id"); got != workflowID {
			t.Errorf("workflow_id = %q, want exact bytes %q", got, workflowID)
		}
		if _, present := r.URL.Query()["job_name"]; present {
			t.Errorf("job_name must be omitted, got %q", r.URL.Query().Get("job_name"))
		}
		if _, present := r.URL.Query()["job_name_match"]; present {
			t.Errorf("job_name_match must be omitted, got %q", r.URL.Query().Get("job_name_match"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"statuses": []map[string]interface{}{{
				"job_id": "job-1", "job_attempt_index": 0, "workflow_id": workflowID,
			}},
			"item_count": 1,
		})
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)
	response, err := jobSource.GetJobs(context.Background(), &v1.GetJobsRequest{WorkflowIds: []string{workflowID}})
	if err != nil {
		t.Fatalf("GetJobs failed: %v", err)
	}
	if len(response.Jobs) != 1 || response.Jobs[0].WorkflowId != workflowID {
		t.Fatalf("jobs = %+v, want one job with workflow ID %q", response.Jobs, workflowID)
	}
}

func TestJobClientSource_GetJobsForwardsReconciliationEligibleJobsFilter(t *testing.T) {
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("reconciliation_eligible_jobs"); got != "true" {
			t.Errorf("reconciliation_eligible_jobs = %q, want true", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"statuses":   []map[string]interface{}{},
			"item_count": 0,
		})
	}))
	defer jobManagerServer.Close()

	reconciliationEligibleJobs := true
	jobSource := NewJobClientSource(jobManagerServer.URL)
	if _, err := jobSource.GetJobs(context.Background(), &v1.GetJobsRequest{ReconciliationEligibleJobs: &reconciliationEligibleJobs}); err != nil {
		t.Fatalf("GetJobs failed: %v", err)
	}
}

// Explicit false must survive the gateway boundary: dropping it silently
// expands SAC bootstrap to the full history. Omission preserves other callers.
func TestJobClientSourceGetJobsCompletionFilter(t *testing.T) {
	for _, tt := range []struct {
		name   string
		filter *bool
		want   string
	}{
		{name: "unfiltered"},
		{name: "non-final", filter: new(false), want: "false"},
		{name: "final", filter: new(true), want: "true"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("completed_jobs"); got != tt.want {
					t.Errorf("completed_jobs = %q, want %q", got, tt.want)
				}
				if r.URL.Query().Has("completed_jobs") != (tt.filter != nil) {
					t.Error("completion filter presence changed at gateway boundary")
				}
				if got := r.URL.Query().Get("offset"); got != "2" {
					t.Errorf("offset = %q, want 2", got)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"statuses":   []map[string]any{{"job_id": "active", "job_attempt_index": 1, "workflow_id": "workflow"}},
					"item_count": 4,
				})
			}))
			defer backend.Close()
			token := "2"
			response, err := NewJobClientSource(backend.URL).GetJobs(context.Background(), &v1.GetJobsRequest{
				CompletedJobs: tt.filter, Limit: 1, PageToken: &token,
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(response.Jobs) != 1 || response.Jobs[0].GetJobId() != "active" || response.GetNextPageToken() != "3" {
				t.Fatalf("response = %v, want filtered backend page and next offset 3", response)
			}
		})
	}
}

func TestJobClientSourceGetJobsFiltersRecentFailuresByEndTime(t *testing.T) {
	cutoff := time.Date(2026, 10, 6, 12, 30, 0, 0, time.UTC)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if query.Get("reconciliation_eligible_jobs") != "true" || query.Has("completed_jobs") {
			t.Errorf("incorrect failure state filters: %v", query)
		}
		if query.Get("time_filter_target") != "END_TIME" || query.Get("time_filter_from") != cutoff.Format(time.RFC3339) {
			t.Errorf("incorrect end-time filter: %v", query)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"statuses": []any{}, "item_count": 0})
	}))
	defer backend.Close()
	_, err := NewJobClientSource(backend.URL).GetJobs(context.Background(), &v1.GetJobsRequest{
		ReconciliationEligibleJobs: new(true), EndTimeFrom: timestamppb.New(cutoff),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSlurmJobExecutionStatusToJobPbUsesPersistedWorkflowID(t *testing.T) {
	jobName := "train"
	workflowID := "experiment-42"
	job := &jobmanager.SlurmJobExecutionStatus{
		JobId:           "job-1",
		JobAttemptIndex: 0,
		WorkflowId:      workflowID,
	}
	job.JobName.Set(&jobName)

	got, err := slurmJobExecutionStatusToJobPb(context.Background(), job)
	if err != nil {
		t.Fatalf("slurmJobExecutionStatusToJobPb failed: %v", err)
	}
	if got.WorkflowId != workflowID {
		t.Fatalf("WorkflowId = %q, want persisted ID %q", got.WorkflowId, workflowID)
	}
	if got.JobName == nil || *got.JobName != jobName {
		t.Fatalf("JobName = %v, want %q", got.JobName, jobName)
	}
}

func TestGetJobsAdvancesPageTokenFromCurrentOffset(t *testing.T) {
	jobResponse := map[string]interface{}{
		"statuses": []map[string]interface{}{
			{
				"job_id":            "job_123",
				"job_attempt_index": 1,
				"workflow_id":       "workflow-page-token",
			},
		},
		"item_count": 3,
	}

	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("offset"); got != "1" {
			t.Errorf("job manager offset = %q, want 1", got)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(jobResponse); err != nil {
			t.Errorf("encode job response: %v", err)
		}
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)
	pageToken := "1"
	response, err := jobSource.GetJobs(context.Background(), &v1.GetJobsRequest{
		Limit:     1,
		PageToken: &pageToken,
	})
	if err != nil {
		t.Fatalf("GetJobs failed: %v", err)
	}
	if got := response.GetNextPageToken(); got != "2" {
		t.Fatalf("next page token = %q, want 2", got)
	}
}

func TestJobClientSource_GetJobsPagination(t *testing.T) {
	pageOne := int32(1)
	pageTwo := int32(2)
	tokenFour := "4"

	tests := []struct {
		name          string
		pageNumber    *int32
		pageToken     *string
		wantOffset    string
		itemCount     int32
		wantNextToken string
	}{
		{name: "page one", pageNumber: &pageOne, wantOffset: "0", itemCount: 6, wantNextToken: "2"},
		{name: "page two", pageNumber: &pageTwo, wantOffset: "2", itemCount: 6, wantNextToken: "4"},
		{name: "exact final page", pageToken: &tokenFour, wantOffset: "4", itemCount: 6},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("limit"); got != "2" {
					t.Errorf("limit = %q, want 2", got)
				}
				if got := r.URL.Query().Get("offset"); got != test.wantOffset {
					t.Errorf("offset = %q, want %s", got, test.wantOffset)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"statuses": []map[string]interface{}{
						{"job_id": "job-1", "job_attempt_index": 0, "workflow_id": "workflow-page"},
						{"job_id": "job-2", "job_attempt_index": 0, "workflow_id": "workflow-page"},
					},
					"item_count": test.itemCount,
				})
			}))
			defer jobManagerServer.Close()

			jobSource := NewJobClientSource(jobManagerServer.URL)
			response, err := jobSource.GetJobs(context.Background(), &v1.GetJobsRequest{
				Limit:      2,
				PageNumber: test.pageNumber,
				PageToken:  test.pageToken,
			})
			if err != nil {
				t.Fatalf("GetJobs failed: %v", err)
			}
			if got := response.GetNextPageToken(); got != test.wantNextToken {
				t.Fatalf("next page token = %q, want %q", got, test.wantNextToken)
			}
		})
	}
}

func TestJobClientSource_GetJobsRejectsInvalidPagination(t *testing.T) {
	pageOne := int32(1)
	pageZero := int32(0)
	negativeToken := "-1"
	overflowToken := "2147483648"
	zeroToken := "0"

	tests := []struct {
		name       string
		pageNumber *int32
		pageToken  *string
	}{
		{name: "page zero", pageNumber: &pageZero},
		{name: "negative token", pageToken: &negativeToken},
		{name: "overflowing token", pageToken: &overflowToken},
		{name: "page and token", pageNumber: &pageOne, pageToken: &zeroToken},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backendCalled := false
			jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				backendCalled = true
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"statuses": []interface{}{}, "item_count": 0})
			}))
			defer jobManagerServer.Close()

			jobSource := NewJobClientSource(jobManagerServer.URL)
			_, err := jobSource.GetJobs(context.Background(), &v1.GetJobsRequest{
				Limit:      2,
				PageNumber: test.pageNumber,
				PageToken:  test.pageToken,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("error code = %s, want %s: %v", status.Code(err), codes.InvalidArgument, err)
			}
			if backendCalled {
				t.Fatal("job manager was called for invalid pagination")
			}
		})
	}
}

func TestJobClientSource_GetWorkflowsPagination(t *testing.T) {
	pageOne := int32(1)
	pageTwo := int32(2)
	tokenFour := "4"

	tests := []struct {
		name          string
		pageNumber    *int32
		pageToken     *string
		wantOffset    string
		wantNextToken string
	}{
		{name: "page one", pageNumber: &pageOne, wantOffset: "0", wantNextToken: "2"},
		{name: "page two", pageNumber: &pageTwo, wantOffset: "2", wantNextToken: "4"},
		{name: "exact final page", pageToken: &tokenFour, wantOffset: "4"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query().Get("limit"); got != "2" {
					t.Errorf("limit = %q, want 2", got)
				}
				if got := r.URL.Query().Get("offset"); got != test.wantOffset {
					t.Errorf("offset = %q, want %s", got, test.wantOffset)
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"workflows":  []map[string]interface{}{{"id": "workflow-1"}, {"id": "workflow-2"}},
					"item_count": 6,
				})
			}))
			defer jobManagerServer.Close()

			jobSource := NewJobClientSource(jobManagerServer.URL)
			response, err := jobSource.GetWorkflows(context.Background(), &v1.GetWorkflowsRequest{
				Limit:      2,
				PageNumber: test.pageNumber,
				PageToken:  test.pageToken,
			})
			if err != nil {
				t.Fatalf("GetWorkflows failed: %v", err)
			}
			if got := response.GetNextPageToken(); got != test.wantNextToken {
				t.Fatalf("next page token = %q, want %q", got, test.wantNextToken)
			}
		})
	}
}

func TestJobClientSource_GetWorkflowUsesExactProjection(t *testing.T) {
	requestedID := "  cafe\u0301/%_\\  "
	returnedID := "canonical-from-storage"
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/workflows" {
			t.Fatalf("path = %q, want /workflows", r.URL.Path)
		}
		if got := r.URL.Query().Get("workflow-id"); got != requestedID {
			t.Fatalf("workflow-id = %q, want exact bytes %q", got, requestedID)
		}
		if got := r.URL.Query().Get("workflow_id_match"); got != "exact" {
			t.Fatalf("workflow_id_match = %q, want exact", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"workflows": []map[string]interface{}{{
				"id": returnedID, "start_time": "2026-07-01T01:02:03Z", "end_time": "2026-07-01T04:05:06Z",
			}},
			"item_count": 1,
		})
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)
	response, err := jobSource.GetWorkflow(context.Background(), &v1.GetWorkflowRequest{WorkflowId: requestedID})
	if err != nil {
		t.Fatalf("GetWorkflow failed: %v", err)
	}
	workflow := response.Workflow
	if workflow == nil {
		t.Fatal("GetWorkflow returned nil workflow")
	}
	if workflow.WorkflowId != returnedID || workflow.Name != returnedID || workflow.SchedulerWorkflowId != returnedID {
		t.Fatalf("workflow identifiers = (%q, %q, %q), want returned projection ID %q", workflow.WorkflowId, workflow.Name, workflow.SchedulerWorkflowId, returnedID)
	}
	if got := workflow.StartTime.AsTime().Format(time.RFC3339); got != "2026-07-01T01:02:03Z" {
		t.Fatalf("start time = %s", got)
	}
	if got := workflow.EndTime.AsTime().Format(time.RFC3339); got != "2026-07-01T04:05:06Z" {
		t.Fatalf("end time = %s", got)
	}
}

func TestJobClientSource_GetWorkflowEnforcesProjectionCardinality(t *testing.T) {
	tests := []struct {
		name      string
		workflows []map[string]interface{}
		wantCode  codes.Code
	}{
		{name: "unknown is not found", wantCode: codes.NotFound},
		{name: "multiple is internal", workflows: []map[string]interface{}{{"id": "workflow-1"}, {"id": "workflow-2"}}, wantCode: codes.Internal},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"workflows": test.workflows, "item_count": len(test.workflows),
				})
			}))
			defer jobManagerServer.Close()

			jobSource := NewJobClientSource(jobManagerServer.URL)
			_, err := jobSource.GetWorkflow(context.Background(), &v1.GetWorkflowRequest{WorkflowId: "workflow-1"})
			if got := status.Code(err); got != test.wantCode {
				t.Fatalf("error code = %s, want %s: %v", got, test.wantCode, err)
			}
		})
	}
}

func TestJobClientSource_GetJobsBySchedulerIDPagination(t *testing.T) {
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("job_id"); got != "12345" {
			t.Errorf("job_id = %q, want 12345", got)
		}
		if _, present := r.URL.Query()["job_attempt_index"]; present {
			t.Errorf("job_attempt_index must be omitted, got %q", r.URL.Query().Get("job_attempt_index"))
		}
		if got := r.URL.Query().Get("limit"); got != "1" {
			t.Errorf("limit = %q, want 1", got)
		}
		if got := r.URL.Query().Get("offset"); got != "0" {
			t.Errorf("offset = %q, want 0", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"statuses": []map[string]interface{}{{
				"job_id": "12345", "job_attempt_index": 12, "workflow_id": "scheduler-workflow",
			}},
			"item_count": 2,
		})
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)
	response, err := jobSource.GetJobsBySchedulerId(context.Background(), &v1.GetJobsBySchedulerIdRequest{
		SchedulerJobId: "12345",
		Limit:          1,
	})
	if err != nil {
		t.Fatalf("GetJobsBySchedulerId failed: %v", err)
	}
	if len(response.Jobs) != 1 || response.Jobs[0].AttemptIndex != 12 {
		t.Fatalf("jobs = %+v, want scheduler attempt 12", response.Jobs)
	}
	if got := response.GetNextPageToken(); got != "1" {
		t.Fatalf("next page token = %q, want 1", got)
	}
}

func TestSlurmJobExecutionStatusToJobPb_BadJobFromJSON(t *testing.T) {
	// Unmarshal the json file to test conversion works correctly and we are not hitting
	// any idiosyncrasies with the openapi generated bindings
	jsonData, err := os.ReadFile("testdata/bad_job.json")
	if err != nil {
		t.Fatalf("Failed to read bad_job.json: %v", err)
	}

	var job jobmanager.SlurmJobExecutionStatus
	err = json.Unmarshal(jsonData, &job)
	if err != nil {
		t.Fatalf("Failed to unmarshal bad_job.json: %v", err)
	}

	jobPb, err := slurmJobExecutionStatusToJobPb(context.Background(), &job)
	if err != nil {
		t.Fatalf("slurmJobExecutionStatusToJobPb failed: %v", err)
	}

	if jobPb.JobRunId != "1438955-5" {
		t.Errorf("JobRunId mismatch: got %s, want %s", jobPb.JobRunId, "1438955-5")
	}
}

// Advice Integration Tests

func createAdviceResponse() map[string]interface{} {
	return map[string]interface{}{
		"advice": []map[string]interface{}{
			{
				"id":                    "advice-001",
				"jobId":                 "job-123",
				"namespace":             "default",
				"restartCount":          2,
				"generation":            3,
				"desiredStateChangedAt": "2025-11-21T15:59:00Z",
				"recoveryContext": map[string]interface{}{
					"sequenceId":  "123e4567-e89b-42d3-a456-426614174000",
					"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
					"tracestate":  "vendor=value",
					"triggeredAt": "2025-11-21T15:58:00Z",
					"operation":   "restart",
				},
				"reconciliationStatus": map[string]interface{}{
					"observedGeneration": 3,
					"disposition":        "CONVERGED",
					"reason":             "DESIRED_STATE_OBSERVED",
					"observedAt":         "2025-11-21T16:10:00Z",
					"transitionedAt":     "2025-11-21T16:10:00Z",
				},
				"advice": map[string]interface{}{
					"state": "Restarted",
					"nodeConstraints": map[string]interface{}{
						"exclusions": []string{"node-bad-1", "node-bad-2"},
						"inclusions": []string{"node-good-1"},
					},
				},
				"issues": []map[string]interface{}{
					{
						"issueType":  "GPU_ERROR",
						"timestamp":  "2025-11-21T10:00:00Z",
						"detectedBy": "watcher",
						"relevantLogLines": []string{
							"CUDA error: out of memory",
							"GPU 0 temperature critical",
						},
						"remediationActions": []map[string]interface{}{
							{
								"actionType": "EXCLUDE_NODE",
								"timestamp":  "2025-11-21T10:01:00Z",
								"reason":     "GPU hardware failure detected",
								"resourceDetails": []map[string]interface{}{
									{
										"resourceType": "node",
										"resourceId":   "node-bad-1",
										"details":      "GPU 0 failed health check",
									},
								},
							},
						},
					},
				},
				// Include scheduler_metadata to verify they are parsed correctly
				"schedulerMetadata": map[string]interface{}{
					"kind":      "PyTorchJob",
					"namespace": "training",
				},
				"createdAt": "2025-11-21T16:00:00Z",
				"updatedAt": "2025-11-21T16:00:00Z",
			},
			{
				"id":           "advice-002",
				"jobId":        "job-456",
				"namespace":    "production",
				"restartCount": 5,
				"advice": map[string]interface{}{
					"state": "Terminated",
					"nodeConstraints": map[string]interface{}{
						"exclusions": []string{"node-critical-1"},
					},
				},
				"issues": []map[string]interface{}{
					{
						"issueType":  "CRITICAL_FAILURE",
						"timestamp":  "2025-11-21T11:00:00Z",
						"detectedBy": "advisor",
						"relevantLogLines": []string{
							"Multiple failures detected",
						},
						"remediationActions": []map[string]interface{}{
							{
								"actionType": "TERMINATE_JOB",
								"timestamp":  "2025-11-21T11:01:00Z",
								"reason":     "Too many restart attempts",
							},
						},
					},
				},
				"createdAt": "2025-11-21T16:30:00Z",
				"updatedAt": "2025-11-21T16:30:00Z",
			},
		},
		"item_count": 2,
	}
}

func TestJobClientSource_ListAdvice_Success(t *testing.T) {
	adviceResponse := createAdviceResponse()

	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the request path
		if r.URL.Path != "/advice" {
			t.Errorf("Expected path /advice, got %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("only_active"); got != "true" {
			t.Errorf("only_active = %q, want true", got)
		}
		if _, present := r.URL.Query()["controller_scope"]; present {
			t.Error("controller_scope query parameter must not be sent")
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		response, _ := json.Marshal(adviceResponse)
		_, _ = w.Write(response)
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	onlyActive := true
	getAdviceResponse, err := jobSource.ListAdvice(context.Background(), &v1.ListAdviceRequest{
		Limit:      10,
		OnlyActive: &onlyActive,
	})
	if err != nil {
		t.Fatalf("ListAdvice failed: %v", err)
	}

	if getAdviceResponse.Count != 2 {
		t.Errorf("Expected count 2, got %d", getAdviceResponse.Count)
	}

	if len(getAdviceResponse.Advice) != 2 {
		t.Fatalf("Expected 2 advice items, got %d", len(getAdviceResponse.Advice))
	}

	// Verify first advice item
	advice1 := getAdviceResponse.Advice[0]
	if advice1.HeimdallJobId != "advice-001" {
		t.Errorf("Expected HeimdallJobId 'advice-001', got '%s'", advice1.HeimdallJobId)
	}
	if advice1.JobId != "job-123" {
		t.Errorf("Expected JobId 'job-123', got '%s'", advice1.JobId)
	}
	if advice1.AttemptIndex != 2 {
		t.Errorf("Expected AttemptIndex 2, got %d", advice1.AttemptIndex)
	}
	if advice1.Generation != 3 {
		t.Errorf("Expected generation 3, got %d", advice1.Generation)
	}
	if advice1.GetRecoveryContext().GetSequenceId() != "123e4567-e89b-42d3-a456-426614174000" {
		t.Errorf("Unexpected recovery sequence ID: %q", advice1.GetRecoveryContext().GetSequenceId())
	}
	if advice1.GetRecoveryContext().GetTraceparent() != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Errorf("Unexpected recovery traceparent: %q", advice1.GetRecoveryContext().GetTraceparent())
	}
	if advice1.GetRecoveryContext().GetTracestate() != "vendor=value" {
		t.Errorf("Unexpected recovery tracestate: %q", advice1.GetRecoveryContext().GetTracestate())
	}
	if advice1.GetRecoveryContext().GetOperation() != v1.RecoveryOperation_RECOVERY_OPERATION_RESTART {
		t.Errorf("Unexpected recovery operation: %s", advice1.GetRecoveryContext().GetOperation())
	}
	if advice1.GetRecoveryContext().GetTriggeredAt().AsTime().Format(time.RFC3339) != "2025-11-21T15:58:00Z" {
		t.Errorf("Unexpected recovery trigger timestamp: %s", advice1.GetRecoveryContext().GetTriggeredAt().AsTime())
	}
	if advice1.DesiredStateChangedAt == nil || advice1.DesiredStateChangedAt.AsTime().Format(time.RFC3339) != "2025-11-21T15:59:00Z" {
		t.Errorf("Unexpected desired-state timestamp: %v", advice1.DesiredStateChangedAt)
	}
	if advice1.ReconciliationStatus == nil {
		t.Fatal("Expected reconciliation status, got nil")
	}
	if advice1.ReconciliationStatus.GetDisposition() != v1.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED {
		t.Fatalf("Unexpected reconciliation disposition: %s", advice1.ReconciliationStatus.GetDisposition())
	}
	if advice1.ReconciliationStatus.GetObservedGeneration() != advice1.GetGeneration() {
		t.Fatalf(
			"Reconciliation generation = %d, want current generation %d",
			advice1.ReconciliationStatus.GetObservedGeneration(),
			advice1.GetGeneration(),
		)
	}

	// Verify advice recommendation
	if advice1.Advice == nil {
		t.Fatal("Expected Advice recommendation, got nil")
	}
	if advice1.Advice.State == nil {
		t.Fatal("Expected State, got nil")
	}
	if *advice1.Advice.State != v1.AdviceState_ADVICE_STATE_RESTARTED {
		t.Errorf("Expected State RESTARTED, got %v", *advice1.Advice.State)
	}

	// Verify node constraints
	if advice1.Advice.NodeConstraints == nil {
		t.Fatal("Expected NodeConstraints, got nil")
	}
	if len(advice1.Advice.NodeConstraints.Exclusions) != 2 {
		t.Errorf("Expected 2 exclusions, got %d", len(advice1.Advice.NodeConstraints.Exclusions))
	}
	if len(advice1.Advice.NodeConstraints.Inclusions) != 1 {
		t.Errorf("Expected 1 inclusion, got %d", len(advice1.Advice.NodeConstraints.Inclusions))
	}

	// Verify issues
	if len(advice1.Issues) != 1 {
		t.Fatalf("Expected 1 issue, got %d", len(advice1.Issues))
	}
	issue := advice1.Issues[0]
	if issue.IssueType != "GPU_ERROR" {
		t.Errorf("Expected IssueType 'GPU_ERROR', got '%s'", issue.IssueType)
	}
	if issue.DetectedBy != "watcher" {
		t.Errorf("Expected DetectedBy 'watcher', got '%s'", issue.DetectedBy)
	}
	if len(issue.RelevantLogLines) != 2 {
		t.Errorf("Expected 2 log lines, got %d", len(issue.RelevantLogLines))
	}

	// Verify remediation actions
	if len(issue.RemediationActions) != 1 {
		t.Fatalf("Expected 1 remediation action, got %d", len(issue.RemediationActions))
	}
	action := issue.RemediationActions[0]
	if action.ActionType != "EXCLUDE_NODE" {
		t.Errorf("Expected ActionType 'EXCLUDE_NODE', got '%s'", action.ActionType)
	}
	if action.Reason != "GPU hardware failure detected" {
		t.Errorf("Expected specific reason, got '%s'", action.Reason)
	}

	// Verify resource details
	if len(action.ResourceDetails) != 1 {
		t.Fatalf("Expected 1 resource detail, got %d", len(action.ResourceDetails))
	}
	detail := action.ResourceDetails[0]
	if detail.ResourceType != "node" {
		t.Errorf("Expected ResourceType 'node', got '%s'", detail.ResourceType)
	}
	if detail.ResourceId != "node-bad-1" {
		t.Errorf("Expected ResourceId 'node-bad-1', got '%s'", detail.ResourceId)
	}

	// Verify timestamps are parsed
	if advice1.CreatedAt == nil {
		t.Error("Expected CreatedAt to be set, got nil")
	}
	if advice1.UpdatedAt == nil {
		t.Error("Expected UpdatedAt to be set, got nil")
	}
	if advice1.RecoveryContext == nil {
		t.Fatal("Expected RecoveryContext to be set, got nil")
	}
	if got := advice1.RecoveryContext.SequenceId; got != "123e4567-e89b-42d3-a456-426614174000" {
		t.Errorf("RecoveryContext.SequenceId = %q", got)
	}
	if got := advice1.RecoveryContext.Operation; got != v1.RecoveryOperation_RECOVERY_OPERATION_RESTART {
		t.Errorf("RecoveryContext.Operation = %v", got)
	}
	if advice1.RecoveryContext.Tracestate == nil || *advice1.RecoveryContext.Tracestate != "vendor=value" {
		t.Errorf("RecoveryContext.Tracestate = %v", advice1.RecoveryContext.Tracestate)
	}

	// Verify scheduler_metadata is transformed correctly
	if advice1.SchedulerMetadata == nil {
		t.Error("Expected SchedulerMetadata to be set, got nil")
	} else {
		fields := advice1.SchedulerMetadata.GetFields()
		if len(fields) != 2 {
			t.Errorf("Expected 2 items in SchedulerMetadata, got %d", len(fields))
		}

		// Verify "kind" field
		if kindValue, ok := fields["kind"]; !ok {
			t.Error("Expected 'kind' in SchedulerMetadata")
		} else if kindValue.GetStringValue() != "PyTorchJob" {
			t.Errorf("Expected kind 'PyTorchJob', got '%s'", kindValue.GetStringValue())
		}

		// Verify "namespace" field
		if nsValue, ok := fields["namespace"]; !ok {
			t.Error("Expected 'namespace' in SchedulerMetadata")
		} else if nsValue.GetStringValue() != "training" {
			t.Errorf("Expected namespace 'training', got '%s'", nsValue.GetStringValue())
		}
	}

	// Verify second advice item
	advice2 := getAdviceResponse.Advice[1]
	if advice2.HeimdallJobId != "advice-002" {
		t.Errorf("Expected HeimdallId 'advice-002', got '%s'", advice2.HeimdallJobId)
	}
	if advice2.Advice.State == nil {
		t.Fatal("Expected State, got nil")
	}
	if *advice2.Advice.State != v1.AdviceState_ADVICE_STATE_TERMINATED {
		t.Errorf("Expected State TERMINATED, got %v", *advice2.Advice.State)
	}
	if advice2.RecoveryContext != nil {
		t.Errorf("Expected absent recovery context, got %v", advice2.RecoveryContext)
	}
}

func TestJobClientSource_UpdateAdviceReconciliationStatus(t *testing.T) {
	observedAt := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/advice/advice-001/reconciliation-status" {
			t.Errorf("path = %s, want Advice status path", r.URL.Path)
		}
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode status body: %v", err)
		}
		if _, present := payload["controllerScope"]; present {
			t.Error("controllerScope must not be sent")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(payload)
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)
	response, err := jobSource.UpdateAdviceReconciliationStatus(
		context.Background(),
		&v1.UpdateAdviceReconciliationStatusRequest{
			HeimdallJobId: "advice-001",
			Status: &v1.AdviceReconciliationStatus{
				ObservedGeneration: 3,
				Disposition:        v1.AdviceReconciliationDisposition_ADVICE_RECONCILIATION_DISPOSITION_CONVERGED,
				Reason:             v1.AdviceReconciliationReason_ADVICE_RECONCILIATION_REASON_DESIRED_STATE_OBSERVED,
				ObservedAt:         timestamppb.New(observedAt),
				TransitionedAt:     timestamppb.New(observedAt),
			},
		},
	)
	if err != nil {
		t.Fatalf("UpdateAdviceReconciliationStatus failed: %v", err)
	}
	if response.GetStatus().GetObservedGeneration() != 3 {
		t.Fatalf("observed generation = %d, want 3", response.GetStatus().GetObservedGeneration())
	}
}

func TestJobClientSource_GetRecoverySequence(t *testing.T) {
	triggeredAt := time.Date(2026, 8, 18, 1, 2, 3, 123456000, time.UTC)
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/recovery/sequences" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Method != http.MethodGet {
			t.Errorf("method = %q", r.Method)
		}
		query := r.URL.Query()
		if query.Get("scheduler_system") != "slurm" || query.Get("scheduler_scope") != "production" || query.Get("job_id") != "1234" || query.Get("source_attempt_index") != "2" || query.Get("operation") != "restart" {
			t.Errorf("unexpected query: %#v", query)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"recoveryContext":{"sequenceId":"0198b9cf-4ce7-72ad-bd18-9ba8938ee2eb","traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","tracestate":"vendor=value","triggeredAt":"2026-08-18T01:02:03.123456Z","operation":"restart"}}`))
	}))
	defer jobManagerServer.Close()

	source := NewJobClientSource(jobManagerServer.URL)
	response, err := source.GetRecoverySequence(context.Background(), &v1.GetRecoverySequenceRequest{
		SchedulerSystem:    "slurm",
		SchedulerScope:     "production",
		JobId:              "1234",
		SourceAttemptIndex: 2,
		Operation:          v1.RecoveryOperation_RECOVERY_OPERATION_RESTART,
	})
	if err != nil {
		t.Fatalf("GetRecoverySequence failed: %v", err)
	}
	if response.RecoveryContext == nil || response.RecoveryContext.SequenceId != "0198b9cf-4ce7-72ad-bd18-9ba8938ee2eb" {
		t.Fatalf("unexpected response: %#v", response)
	}
	if response.RecoveryContext.TriggeredAt.AsTime() != triggeredAt {
		t.Errorf("triggered at = %s, want %s", response.RecoveryContext.TriggeredAt.AsTime(), triggeredAt)
	}
}

func TestJobClientSource_RecoveryErrors(t *testing.T) {
	for _, test := range []struct {
		name       string
		httpStatus int
		wantCode   codes.Code
	}{
		{name: "missing sequence", httpStatus: http.StatusNotFound, wantCode: codes.NotFound},
		{name: "bad request", httpStatus: http.StatusBadRequest, wantCode: codes.InvalidArgument},
		{name: "invalid input", httpStatus: http.StatusUnprocessableEntity, wantCode: codes.InvalidArgument},
		{name: "backend failure", httpStatus: http.StatusInternalServerError, wantCode: codes.Unknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.httpStatus)
				_, _ = w.Write([]byte(`{"detail":"recovery request failed"}`))
			}))
			defer server.Close()
			source := NewJobClientSource(server.URL)
			t.Run("lookup", func(t *testing.T) {
				_, err := source.GetRecoverySequence(context.Background(), &v1.GetRecoverySequenceRequest{
					SchedulerSystem: "slurm", SchedulerScope: "production", JobId: "1234",
					SourceAttemptIndex: 2, Operation: v1.RecoveryOperation_RECOVERY_OPERATION_RESTART,
				})
				if status.Code(err) != test.wantCode {
					t.Fatalf("GetRecoverySequence error = %v, want %s", err, test.wantCode)
				}
			})
			t.Run("milestone", func(t *testing.T) {
				completes := true
				_, err := source.RecordRecoveryMilestone(context.Background(), &v1.RecordRecoveryMilestoneRequest{
					SequenceId: "0198b9cf-4ce7-72ad-bd18-9ba8938ee2eb", IdempotencyKey: "release-1234-3-1",
					Operation:         v1.RecoveryOperation_RECOVERY_OPERATION_RESTART,
					Action:            v1.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_RELEASE,
					Outcome:           v1.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_SUCCESS,
					CompletesSequence: &completes, CompletedAt: timestamppb.Now(),
				})
				if status.Code(err) != test.wantCode {
					t.Fatalf("RecordRecoveryMilestone error = %v, want %s", err, test.wantCode)
				}
			})
		})
	}
}

func TestJobClientSource_RecordRecoveryMilestone(t *testing.T) {
	completedAt := time.Date(2026, 8, 18, 1, 2, 4, 654321000, time.UTC)
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/recovery/milestones" {
			t.Errorf("path = %q", r.URL.Path)
		}
		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if body["action"] != "slurm.release" || body["outcome"] != "success" || body["idempotencyKey"] != "release-1234-3-1" || body["completesSequence"] != true {
			t.Errorf("unexpected request body: %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"duplicate":false,"sequenceCompleted":true,"endToEndDurationSeconds":1.530865}`))
	}))
	defer jobManagerServer.Close()

	source := NewJobClientSource(jobManagerServer.URL)
	completesSequence := true
	response, err := source.RecordRecoveryMilestone(context.Background(), &v1.RecordRecoveryMilestoneRequest{
		SequenceId:        "0198b9cf-4ce7-72ad-bd18-9ba8938ee2eb",
		IdempotencyKey:    "release-1234-3-1",
		Operation:         v1.RecoveryOperation_RECOVERY_OPERATION_RESTART,
		Action:            v1.RecoverySchedulerAction_RECOVERY_SCHEDULER_ACTION_SLURM_RELEASE,
		Outcome:           v1.RecoverySchedulerActionOutcome_RECOVERY_SCHEDULER_ACTION_OUTCOME_SUCCESS,
		CompletesSequence: &completesSequence,
		CompletedAt:       timestamppb.New(completedAt),
	})
	if err != nil {
		t.Fatalf("RecordRecoveryMilestone failed: %v", err)
	}
	if !response.SequenceCompleted || response.Duplicate {
		t.Fatalf("unexpected response: %#v", response)
	}
	if response.EndToEndDurationSeconds == nil || *response.EndToEndDurationSeconds < 1.53 || *response.EndToEndDurationSeconds > 1.54 {
		t.Fatalf("unexpected duration: %v", response.EndToEndDurationSeconds)
	}
}

func TestJobClientSource_ListAdvice_EmptyResponse(t *testing.T) {
	emptyResponse := map[string]interface{}{
		"advice":     []map[string]interface{}{},
		"item_count": 0,
	}

	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		response, _ := json.Marshal(emptyResponse)
		_, _ = w.Write(response)
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	getAdviceResponse, err := jobSource.ListAdvice(context.Background(), &v1.ListAdviceRequest{
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListAdvice failed: %v", err)
	}

	if getAdviceResponse.Count != 0 {
		t.Errorf("Expected count 0, got %d", getAdviceResponse.Count)
	}

	if len(getAdviceResponse.Advice) != 0 {
		t.Errorf("Expected 0 advice items, got %d", len(getAdviceResponse.Advice))
	}
}

func TestJobClientSource_ListAdvice_WithPagination(t *testing.T) {
	adviceResponse := createAdviceResponse()
	adviceResponse["nextPageToken"] = "opaque-next-token"

	var capturedPageSize string
	var capturedPageToken string

	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Capture query parameters
		// The generated client uses page_size instead of limit
		capturedPageSize = r.URL.Query().Get("page_size")
		capturedPageToken = r.URL.Query().Get("page_token")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		response, _ := json.Marshal(adviceResponse)
		_, _ = w.Write(response)
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)
	pageToken := "opaque-current-token"

	response, err := jobSource.ListAdvice(context.Background(), &v1.ListAdviceRequest{
		Limit: 5,
		Page:  &pageToken,
	})
	if err != nil {
		t.Fatalf("ListAdvice failed: %v", err)
	}

	if capturedPageSize != "5" {
		t.Errorf("Expected page_size '5', got '%s'", capturedPageSize)
	}
	if capturedPageToken != pageToken {
		t.Errorf("Expected page_token %q, got %q", pageToken, capturedPageToken)
	}
	if got := response.GetNextPageToken(); got != "opaque-next-token" {
		t.Errorf("Expected next page token %q, got %q", "opaque-next-token", got)
	}
}

func TestJobClientSource_ListAdvice_InvalidState(t *testing.T) {
	adviceResponse := map[string]interface{}{
		"advice": []map[string]interface{}{
			{
				"id":           "advice-003",
				"jobId":        "job-789",
				"namespace":    "test",
				"restartCount": 0,
				"advice":       map[string]interface{}{
					// No state field - testing missing/unset state which should map to UNSPECIFIED
				},
				"issues": []map[string]interface{}{},
			},
		},
		"item_count": 1,
	}

	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		response, _ := json.Marshal(adviceResponse)
		_, _ = w.Write(response)
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	getAdviceResponse, err := jobSource.ListAdvice(context.Background(), &v1.ListAdviceRequest{
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("ListAdvice failed: %v", err)
	}

	if len(getAdviceResponse.Advice) != 1 {
		t.Fatalf("Expected 1 advice item, got %d", len(getAdviceResponse.Advice))
	}

	advice := getAdviceResponse.Advice[0]
	// Missing/unset state field should result in nil state
	if advice.Advice.State != nil {
		t.Errorf("Expected State to be nil for missing state field, got %v", *advice.Advice.State)
	}
}

func TestJobClientSource_ListAdvice_ServerError(t *testing.T) {
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Internal Server Error"))
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	_, err := jobSource.ListAdvice(context.Background(), &v1.ListAdviceRequest{
		Limit: 10,
	})
	if err == nil {
		t.Fatal("Expected error when server returns 500, got nil")
	}
}

func TestJobClientSource_ListAdvice_MalformedJSON(t *testing.T) {
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{ invalid json }"))
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	_, err := jobSource.ListAdvice(context.Background(), &v1.ListAdviceRequest{
		Limit: 10,
	})
	if err == nil {
		t.Fatal("Expected error when JSON is malformed, got nil")
	}
}

// CreateOrUpdateJob Integration Tests

// createMockJobManagerForCreateOrUpdate creates a mock job manager server that handles both POST /jobs and GET /jobs
func createMockJobManagerForCreateOrUpdate(jobId string, attemptIndex int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/jobs" {
			// Decode the request to echo back the input fields
			var input map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				panic(fmt.Sprintf("failed to decode request body: %v", err))
			}

			// Build response echoing back the input fields
			response := map[string]interface{}{
				"job_id":              jobId,
				"job_attempt_index":   attemptIndex,
				"workflow_id":         input["workflow_id"],
				"allocated_node_list": []interface{}{},
				"last_updated_at":     "2025-11-21T10:00:00Z", // Add timestamp for tests
			}

			// Echo back heimdall_job_id if provided
			if heimdallJobId, ok := input["heimdall_job_id"]; ok {
				response["heimdall_job_id"] = heimdallJobId
			}

			// Echo back fields from input
			if state, ok := input["state"]; ok {
				response["state"] = state
			} else {
				response["state"] = "RUNNING"
			}

			if jobName, ok := input["job_name"]; ok {
				response["job_name"] = jobName
			}

			if allocatedNodes, ok := input["allocated_node_list"].([]interface{}); ok && len(allocatedNodes) > 0 {
				response["allocated_node_list"] = allocatedNodes
			}

			if totalGpuCount, ok := input["total_gpu_count"]; ok {
				response["total_gpu_count"] = totalGpuCount
			}

			if inclusionNodeList, ok := input["inclusion_node_list"].([]interface{}); ok && len(inclusionNodeList) > 0 {
				response["inclusion_node_list"] = inclusionNodeList
			}

			if exclusionNodeList, ok := input["exclusion_node_list"].([]interface{}); ok && len(exclusionNodeList) > 0 {
				response["exclusion_node_list"] = exclusionNodeList
			}

			if apsSpec, ok := input["aps_spec"].(map[string]interface{}); ok {
				response["aps_spec"] = apsSpec
			}

			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(response); err != nil {
				panic(fmt.Sprintf("failed to encode response: %v", err))
			}
		} else if r.Method == http.MethodGet && r.URL.Path == "/jobs" {
			// GetJob checks if job exists - return empty list to indicate it doesn't exist (new job)
			response := map[string]interface{}{
				"statuses":   []interface{}{},
				"item_count": 0,
			}
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(response); err != nil {
				panic(fmt.Sprintf("failed to encode response: %v", err))
			}
		} else {
			// For other requests, return 404
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestJobClientSource_CreateOrUpdateJobUsesPersistedWorkflowID(t *testing.T) {
	requestWorkflowID := "request-workflow"
	persistedWorkflowID := "persisted-workflow"
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/jobs" {
			t.Fatalf("request = %s %s, want POST /jobs", r.Method, r.URL.Path)
		}
		var input map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if got := input["workflow_id"]; got != requestWorkflowID {
			t.Fatalf("request workflow_id = %v, want %q", got, requestWorkflowID)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": "job-1", "job_attempt_index": 0, "workflow_id": persistedWorkflowID,
		})
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)
	response, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
		JobId: "job-1", WorkflowId: requestWorkflowID,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateJob failed: %v", err)
	}
	if response.Job == nil || response.Job.WorkflowId != persistedWorkflowID {
		t.Fatalf("response job = %+v, want persisted workflow ID %q", response.Job, persistedWorkflowID)
	}
}

func TestJobClientSourcePreservesDistinctJobNameAndCanonicalWorkflowID(t *testing.T) {
	const workflowID = "experiment-42"
	jobName := "train"

	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/jobs":
			var input map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				t.Fatalf("decode job manager request: %v", err)
			}
			if got := input["job_name"]; got != jobName {
				t.Fatalf("request job_name = %v, want %q", got, jobName)
			}
			if got := input["workflow_id"]; got != workflowID {
				t.Fatalf("request workflow_id = %v, want %q", got, workflowID)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"job_id": "job-1", "job_attempt_index": 0,
				"job_name": jobName, "workflow_id": workflowID,
			})
		case r.Method == http.MethodGet && r.URL.Path == "/workflows":
			if got := r.URL.Query().Get("workflow-id"); got != workflowID {
				t.Fatalf("workflow-id = %q, want %q", got, workflowID)
			}
			if got := r.URL.Query().Get("workflow_id_match"); got != "exact" {
				t.Fatalf("workflow_id_match = %q, want exact", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"workflows":  []map[string]interface{}{{"id": workflowID}},
				"item_count": 1,
			})
		default:
			t.Fatalf("unexpected job manager request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)
	createResponse, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
		JobId: "job-1", JobName: &jobName, WorkflowId: workflowID,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateJob failed: %v", err)
	}
	if createResponse.Job == nil || createResponse.Job.WorkflowId != workflowID ||
		createResponse.Job.JobName == nil || *createResponse.Job.JobName != jobName {
		t.Fatalf("created job = %+v, want job_name %q and workflow_id %q", createResponse.Job, jobName, workflowID)
	}

	workflowResponse, err := jobSource.GetWorkflow(
		context.Background(),
		&v1.GetWorkflowRequest{WorkflowId: workflowID},
	)
	if err != nil {
		t.Fatalf("GetWorkflow failed: %v", err)
	}
	workflow := workflowResponse.Workflow
	if workflow == nil || workflow.WorkflowId != workflowID || workflow.Name != workflowID ||
		workflow.SchedulerWorkflowId != workflowID {
		t.Fatalf("workflow = %+v, want canonical projection %q", workflow, workflowID)
	}

}

func TestJobClientSource_CreateOrUpdateJob_Success(t *testing.T) {
	jobManagerServer := createMockJobManagerForCreateOrUpdate("test-job-1", 0)
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	heimdallId := "test-job-1-0"
	jobName := "training-job"

	maxRequeueTimes := int32(5)
	enableNvrx := true
	createOrUpdateResponse, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
		HeimdallJobId:   &heimdallId,
		JobId:           "test-job-1",
		WorkflowId:      "workflow-123",
		AttemptIndex:    0,
		AllocatedNodes:  []string{"node1", "node2"},
		GpuCountPerNode: 8,
		NodeConstraints: &v1.NodeConstraints{
			Exclusions: []string{"node-bad"},
			Inclusions: []string{"node-good"},
		},
		AdditionalProperties: &v1.JobAdditionalProperties{ //nolint:staticcheck // Verify requests using the deprecated field remain accepted.
			MaxRequeueTimes: &maxRequeueTimes,
			EnableNvrx:      enableNvrx,
		},
		State:   v1.JobStatus_JOB_STATUS_RUNNING,
		JobName: &jobName,
	})

	if err != nil {
		t.Fatalf("CreateOrUpdateJob failed: %v", err)
	}

	if createOrUpdateResponse.Job == nil {
		t.Fatal("Expected job in response, got nil")
	}

	job := createOrUpdateResponse.Job
	if job.HeimdallJobId != heimdallId {
		t.Errorf("Expected HeimdallJobId '%s', got '%s'", heimdallId, job.HeimdallJobId)
	}
	if job.JobId != "test-job-1" {
		t.Errorf("Expected JobId 'test-job-1', got '%s'", job.JobId)
	}
	if job.WorkflowId != "workflow-123" {
		t.Errorf("Expected WorkflowId 'workflow-123', got '%s'", job.WorkflowId)
	}
	if job.AttemptIndex != 0 {
		t.Errorf("Expected AttemptIndex 0, got %d", job.AttemptIndex)
	}
	if len(job.Nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(job.Nodes))
	}
	if job.GpuCountPerNode == nil || *job.GpuCountPerNode != 8 {
		if job.GpuCountPerNode == nil {
			t.Error("Expected GpuCountPerNode 8, got nil")
		} else {
			t.Errorf("Expected GpuCountPerNode 8, got %d", *job.GpuCountPerNode)
		}
	}
	if job.NodeConstraints == nil {
		t.Fatal("Expected NodeConstraints, got nil")
	}
	if len(job.NodeConstraints.Exclusions) != 1 {
		t.Errorf("Expected 1 exclusion, got %d", len(job.NodeConstraints.Exclusions))
	}
	if len(job.NodeConstraints.Inclusions) != 1 {
		t.Errorf("Expected 1 inclusion, got %d", len(job.NodeConstraints.Inclusions))
	}
	if job.State != v1.JobStatus_JOB_STATUS_RUNNING {
		t.Errorf("Expected State RUNNING, got %v", job.State)
	}
	if job.JobName == nil || *job.JobName != "training-job" {
		t.Errorf("Expected JobName 'training-job', got %v", job.JobName)
	}
	if job.CreatedAt == nil {
		t.Error("Expected CreatedAt to be set, got nil")
	}
	if job.LastSeen == nil {
		t.Error("Expected LastSeen to be set, got nil")
	}
	if job.UpdatedAt == nil {
		t.Error("Expected UpdatedAt to be set, got nil")
	}
}

func TestJobClientSource_CreateOrUpdateJob_AutoGenerateHeimdallId(t *testing.T) {
	jobManagerServer := createMockJobManagerForCreateOrUpdate("auto-job-123", 3)
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	// Don't provide HeimdallJobId
	createOrUpdateResponse, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
		JobId:        "auto-job-123",
		WorkflowId:   "workflow-auto",
		AttemptIndex: 3,
		State:        v1.JobStatus_JOB_STATUS_PENDING,
	})

	if err != nil {
		t.Fatalf("CreateOrUpdateJob failed: %v", err)
	}

	if createOrUpdateResponse.Job == nil {
		t.Fatal("Expected job in response, got nil")
	}

	// HeimdallJobId should be empty when not provided (callers can use job_id + attempt_index)
	if createOrUpdateResponse.Job.HeimdallJobId != "" {
		t.Errorf("Expected empty HeimdallId, got '%s'", createOrUpdateResponse.Job.HeimdallJobId)
	}
}

func TestJobClientSource_CreateOrUpdateJob_MinimalFields(t *testing.T) {
	jobManagerServer := createMockJobManagerForCreateOrUpdate("minimal-job", 0)
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	// Provide only required fields
	createOrUpdateResponse, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
		JobId:        "minimal-job",
		WorkflowId:   "minimal-workflow",
		AttemptIndex: 0,
		State:        v1.JobStatus_JOB_STATUS_COMPLETED,
	})

	if err != nil {
		t.Fatalf("CreateOrUpdateJob failed: %v", err)
	}

	if createOrUpdateResponse.Job == nil {
		t.Fatal("Expected job in response, got nil")
	}

	job := createOrUpdateResponse.Job
	if job.JobId != "minimal-job" {
		t.Errorf("Expected JobId 'minimal-job', got '%s'", job.JobId)
	}
	if job.WorkflowId != "minimal-workflow" {
		t.Errorf("Expected WorkflowId 'minimal-workflow', got '%s'", job.WorkflowId)
	}
	if job.HeimdallJobId != "" {
		t.Errorf("Expected empty HeimdallJobId, got '%s'", job.HeimdallJobId)
	}
	if job.State != v1.JobStatus_JOB_STATUS_COMPLETED {
		t.Errorf("Expected State COMPLETED, got %v", job.State)
	}
	// Optional fields should be nil or empty
	if job.NodeConstraints != nil {
		t.Error("Expected NodeConstraints to be nil")
	}
	if job.JobName != nil {
		t.Error("Expected JobName to be nil")
	}
}

func TestJobClientSource_CreateOrUpdateJob_WithEmptyHeimdallId(t *testing.T) {
	jobManagerServer := createMockJobManagerForCreateOrUpdate("job-empty-id", 1)
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	// Provide empty HeimdallJobId (should auto-generate)
	emptyHeimdallId := ""
	createOrUpdateResponse, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
		HeimdallJobId: &emptyHeimdallId,
		JobId:         "job-empty-id",
		WorkflowId:    "workflow-empty",
		AttemptIndex:  1,
		State:         v1.JobStatus_JOB_STATUS_FAILED,
	})

	if err != nil {
		t.Fatalf("CreateOrUpdateJob failed: %v", err)
	}

	// HeimdallJobId should remain empty when provided as empty string
	if createOrUpdateResponse.Job.HeimdallJobId != "" {
		t.Errorf("Expected empty HeimdallJobId, got '%s'", createOrUpdateResponse.Job.HeimdallJobId)
	}
}

func TestJobClientSource_CreateOrUpdateJob_WithAdditionalProperties(t *testing.T) {
	jobManagerServer := createMockJobManagerForCreateOrUpdate("props-job", 0)
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	maxRequeue := int32(10)
	createOrUpdateResponse, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
		JobId:        "props-job",
		WorkflowId:   "props-workflow",
		AttemptIndex: 0,
		State:        v1.JobStatus_JOB_STATUS_RUNNING,
		AdditionalProperties: &v1.JobAdditionalProperties{ //nolint:staticcheck // Verify requests using the deprecated field remain accepted.
			MaxRequeueTimes: &maxRequeue,
			EnableNvrx:      false,
		},
	})

	if err != nil {
		t.Fatalf("CreateOrUpdateJob failed: %v", err)
	}

	job := createOrUpdateResponse.Job
	// Note: AdditionalProperties are not persisted by the job manager
	// They are only part of the API gateway's request schema
	// This test verifies that CreateOrUpdateJob succeeds with AdditionalProperties in the request
	if job.JobId != "props-job" {
		t.Errorf("Expected JobId 'props-job', got '%s'", job.JobId)
	}
	if job.State != v1.JobStatus_JOB_STATUS_RUNNING {
		t.Errorf("Expected State RUNNING, got %v", job.State)
	}
}

func TestJobClientSource_CreateOrUpdateJob_WithRecoveryConfig(t *testing.T) {
	jobManagerServer := createMockJobManagerForCreateOrUpdate("recovery-job", 0)
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	maxRequeueTimes := int32(7)
	requeueOnCompletion := false
	sharedLogPaths := true
	logBasedCrashEnabled := false
	logBasedTimeoutEnabled := true
	timeoutThreshold := int32(30)
	environment := "QA/Canary 01"
	createOrUpdateResponse, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
		JobId:        "recovery-job",
		WorkflowId:   "recovery-workflow",
		AttemptIndex: 0,
		State:        v1.JobStatus_JOB_STATUS_RUNNING,
		RecoveryConfig: &v1.JobRecoveryConfig{
			AutoResumeMode:                   v1.JobRecoveryConfig_AUTO_RESUME_MODE_REQUEUE,
			MaxRequeueTimes:                  &maxRequeueTimes,
			RequeueOnCompletion:              &requeueOnCompletion,
			LogFilePathsSharedAcrossAttempts: &sharedLogPaths,
			Environment:                      &environment,
			AnomalyDetectionConfig: &v1.JobAnomalyDetectionConfig{
				LogBasedCrashEnabled:                &logBasedCrashEnabled,
				LogBasedTimeoutEnabled:              &logBasedTimeoutEnabled,
				LogBasedTimeoutThresholdPretraining: &timeoutThreshold,
			},
			DagDependencies: []*v1.JobDagDependency{
				{
					Target:  &v1.JobDagDependency_JobId{JobId: "123"},
					Trigger: v1.JobDagDependency_TRIGGER_CONDITION_COMPLETED,
				},
				{
					Target:  &v1.JobDagDependency_JobName{JobName: "prepare"},
					Trigger: v1.JobDagDependency_TRIGGER_CONDITION_COMPLETED,
				},
			},
			DagAction: v1.JobRecoveryConfig_DAG_ACTION_RELEASE,
		},
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateJob failed: %v", err)
	}

	if createOrUpdateResponse.Job == nil || createOrUpdateResponse.Job.RecoveryConfig == nil {
		t.Fatalf("Expected recovery config in response, got %#v", createOrUpdateResponse.Job)
	}
	got := createOrUpdateResponse.Job.RecoveryConfig
	if got.AutoResumeMode != v1.JobRecoveryConfig_AUTO_RESUME_MODE_REQUEUE {
		t.Fatalf("AutoResumeMode = %v, want REQUEUE", got.AutoResumeMode)
	}
	if got.MaxRequeueTimes == nil || *got.MaxRequeueTimes != maxRequeueTimes {
		t.Fatalf("MaxRequeueTimes = %v, want %d", got.MaxRequeueTimes, maxRequeueTimes)
	}
	if got.RequeueOnCompletion == nil || *got.RequeueOnCompletion != false {
		t.Fatalf("RequeueOnCompletion = %v, want false with presence", got.RequeueOnCompletion)
	}
	if got.LogFilePathsSharedAcrossAttempts == nil || !*got.LogFilePathsSharedAcrossAttempts {
		t.Fatalf(
			"LogFilePathsSharedAcrossAttempts = %v, want true",
			got.LogFilePathsSharedAcrossAttempts,
		)
	}
	if got.Environment == nil || *got.Environment != environment {
		t.Fatalf("Environment = %v, want exact value %q", got.Environment, environment)
	}
	if got.AnomalyDetectionConfig == nil || got.AnomalyDetectionConfig.LogBasedCrashEnabled == nil || *got.AnomalyDetectionConfig.LogBasedCrashEnabled {
		t.Fatalf("AnomalyDetectionConfig = %#v, want log-based crash false with presence", got.AnomalyDetectionConfig)
	}
	if got.AnomalyDetectionConfig.LogBasedTimeoutEnabled == nil || !*got.AnomalyDetectionConfig.LogBasedTimeoutEnabled {
		t.Fatalf("LogBasedTimeoutEnabled = %v, want true", got.AnomalyDetectionConfig.LogBasedTimeoutEnabled)
	}
	if got.AnomalyDetectionConfig.LogBasedTimeoutThresholdPretraining == nil || *got.AnomalyDetectionConfig.LogBasedTimeoutThresholdPretraining != timeoutThreshold {
		t.Fatalf("LogBasedTimeoutThresholdPretraining = %v, want %d", got.AnomalyDetectionConfig.LogBasedTimeoutThresholdPretraining, timeoutThreshold)
	}
	if got.GetDagAction() != v1.JobRecoveryConfig_DAG_ACTION_RELEASE {
		t.Fatalf("DagAction = %v, want RELEASE", got.GetDagAction())
	}
	if len(got.GetDagDependencies()) != 2 {
		t.Fatalf("len(DagDependencies) = %d, want 2", len(got.GetDagDependencies()))
	}
	if got.GetDagDependencies()[0].GetJobId() != "123" || got.GetDagDependencies()[0].GetTrigger() != v1.JobDagDependency_TRIGGER_CONDITION_COMPLETED {
		t.Fatalf("first dependency = %#v, want completed ID target 123", got.GetDagDependencies()[0])
	}
	if got.GetDagDependencies()[1].GetJobName() != "prepare" || got.GetDagDependencies()[1].GetTrigger() != v1.JobDagDependency_TRIGGER_CONDITION_COMPLETED {
		t.Fatalf("second dependency = %#v, want completed name target prepare", got.GetDagDependencies()[1])
	}
}

func TestRecoveryConfigToApsSpecInputRejectsInvalidDAGPolicy(t *testing.T) {
	tests := map[string]*v1.JobRecoveryConfig{
		"null dependency": {
			DagDependencies: []*v1.JobDagDependency{nil},
		},
		"missing target": {
			DagDependencies: []*v1.JobDagDependency{{Trigger: v1.JobDagDependency_TRIGGER_CONDITION_COMPLETED}},
		},
		"unsupported trigger": {
			DagDependencies: []*v1.JobDagDependency{{
				Target:  &v1.JobDagDependency_JobId{JobId: "123"},
				Trigger: v1.JobDagDependency_TriggerCondition(99),
			}},
		},
		"unsupported action": {
			DagAction: v1.JobRecoveryConfig_DagAction(99),
		},
	}

	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := recoveryConfigToApsSpecInput(config); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("recoveryConfigToApsSpecInput() error = %v, want InvalidArgument", err)
			}
		})
	}
}

func TestRecoveryConfigToApsSpecInputPreservesEnvironmentPresence(t *testing.T) {
	empty := ""
	arbitrary := "QA/Canary 01"
	for _, tt := range []struct {
		name        string
		environment *string
		wantSet     bool
		want        string
	}{
		{name: "absent"},
		{name: "explicit empty", environment: &empty, wantSet: true},
		{name: "arbitrary exact string", environment: &arbitrary, wantSet: true, want: arbitrary},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := recoveryConfigToApsSpecInput(&v1.JobRecoveryConfig{Environment: tt.environment})
			if err != nil {
				t.Fatalf("recoveryConfigToApsSpecInput() failed: %v", err)
			}
			value, set := got.GetEnvironmentOk()
			if set != tt.wantSet {
				t.Fatalf("environment presence = %t, want %t", set, tt.wantSet)
			}
			if tt.wantSet && (value == nil || *value != tt.want) {
				t.Fatalf("environment = %v, want %q", value, tt.want)
			}
		})
	}
}

func TestApsSpecOutputToRecoveryConfigPreservesEnvironmentPresence(t *testing.T) {
	for _, tt := range []struct {
		name    string
		set     func(*jobmanagerBindings.ApsSlurmJobSpecOutput)
		wantSet bool
		want    string
	}{
		{name: "absent"},
		{name: "explicit null", set: func(spec *jobmanagerBindings.ApsSlurmJobSpecOutput) { spec.SetEnvironmentNil() }},
		{name: "explicit empty", set: func(spec *jobmanagerBindings.ApsSlurmJobSpecOutput) { spec.SetEnvironment("") }, wantSet: true},
		{name: "arbitrary exact string", set: func(spec *jobmanagerBindings.ApsSlurmJobSpecOutput) { spec.SetEnvironment("QA/Canary 01") }, wantSet: true, want: "QA/Canary 01"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			spec := jobmanagerBindings.ApsSlurmJobSpecOutput{}
			if tt.set != nil {
				tt.set(&spec)
			}
			got := apsSpecOutputToRecoveryConfig(spec)
			if (got.Environment != nil) != tt.wantSet {
				t.Fatalf("environment presence = %t, want %t", got.Environment != nil, tt.wantSet)
			}
			if tt.wantSet && *got.Environment != tt.want {
				t.Fatalf("environment = %q, want %q", *got.Environment, tt.want)
			}
		})
	}
}

func TestJobClientSource_CreateOrUpdateJob_RejectsInvalidRecoveryConfig(t *testing.T) {
	jobManagerServer := createMockJobManagerForCreateOrUpdate("invalid-recovery-job", 0)
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	maxRequeueTimes := int32(0)
	_, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
		JobId:        "invalid-recovery-job",
		WorkflowId:   "invalid-recovery-workflow",
		AttemptIndex: 0,
		State:        v1.JobStatus_JOB_STATUS_RUNNING,
		RecoveryConfig: &v1.JobRecoveryConfig{
			AutoResumeMode:  v1.JobRecoveryConfig_AUTO_RESUME_MODE_REQUEUE,
			MaxRequeueTimes: &maxRequeueTimes,
		},
	})
	if err == nil {
		t.Fatal("CreateOrUpdateJob succeeded, want invalid recovery config error")
	}
}

func TestJobClientSource_CreateOrUpdateJob_NodeConstraintsOnly(t *testing.T) {
	jobManagerServer := createMockJobManagerForCreateOrUpdate("constraint-job-1", 0)
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	// Test with only exclusions
	createOrUpdateResponse, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
		JobId:        "constraint-job-1",
		WorkflowId:   "constraint-workflow",
		AttemptIndex: 0,
		State:        v1.JobStatus_JOB_STATUS_PENDING,
		NodeConstraints: &v1.NodeConstraints{
			Exclusions: []string{"node-fail-1", "node-fail-2", "node-fail-3"},
		},
	})

	if err != nil {
		t.Fatalf("CreateOrUpdateJob failed: %v", err)
	}

	if createOrUpdateResponse.Job.NodeConstraints == nil {
		t.Fatal("Expected NodeConstraints, got nil")
	}
	if len(createOrUpdateResponse.Job.NodeConstraints.Exclusions) != 3 {
		t.Errorf("Expected 3 exclusions, got %d", len(createOrUpdateResponse.Job.NodeConstraints.Exclusions))
	}
	if len(createOrUpdateResponse.Job.NodeConstraints.Inclusions) != 0 {
		t.Errorf("Expected 0 inclusions, got %d", len(createOrUpdateResponse.Job.NodeConstraints.Inclusions))
	}
}

func TestJobClientSource_CreateOrUpdateJob_MultipleAttempts(t *testing.T) {
	// Create a mock that handles multiple job IDs dynamically
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/jobs" {
			// Decode the request to get the attempt index
			var input map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				panic(fmt.Sprintf("failed to decode request body: %v", err))
			}
			attemptIndex := int32(0)
			if ai, ok := input["job_attempt_index"].(float64); ok {
				attemptIndex = int32(ai)
			}

			response := map[string]interface{}{
				"job_id":              "retry-job",
				"job_attempt_index":   attemptIndex,
				"workflow_id":         input["workflow_id"],
				"state":               "RUNNING",
				"allocated_node_list": []string{},
			}
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(response); err != nil {
				panic(fmt.Sprintf("failed to encode response: %v", err))
			}
		} else if r.Method == http.MethodGet && r.URL.Path == "/jobs" {
			// Return empty list for GetJob check
			response := map[string]interface{}{
				"statuses":   []interface{}{},
				"item_count": 0,
			}
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(response); err != nil {
				panic(fmt.Sprintf("failed to encode response: %v", err))
			}
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	// CreateOrUpdate multiple attempts of the same job
	for attemptIndex := int32(0); attemptIndex < 5; attemptIndex++ {
		createOrUpdateResponse, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
			JobId:        "retry-job",
			WorkflowId:   "retry-workflow",
			AttemptIndex: attemptIndex,
			State:        v1.JobStatus_JOB_STATUS_FAILED,
		})

		if err != nil {
			t.Fatalf("CreateOrUpdateJob failed for attempt %d: %v", attemptIndex, err)
		}

		// HeimdallJobId should be empty when not provided
		if createOrUpdateResponse.Job.HeimdallJobId != "" {
			t.Errorf("Attempt %d: Expected empty HeimdallJobId, got '%s'", attemptIndex, createOrUpdateResponse.Job.HeimdallJobId)
		}
		if createOrUpdateResponse.Job.AttemptIndex != attemptIndex {
			t.Errorf("Attempt %d: Expected AttemptIndex %d, got %d", attemptIndex, attemptIndex, createOrUpdateResponse.Job.AttemptIndex)
		}
	}
}

func TestJobClientSource_CreateOrUpdateJob_AllJobStatuses(t *testing.T) {
	// Create a mock that handles multiple job IDs dynamically
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodPost && r.URL.Path == "/jobs" {
			// Decode the request to get the job ID and state
			var input map[string]interface{}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				panic(fmt.Sprintf("failed to decode request body: %v", err))
			}
			jobId := "status-job-0"
			if jid, ok := input["job_id"].(string); ok {
				jobId = jid
			}

			state := "RUNNING"
			if s, ok := input["state"].(string); ok {
				state = s
			}

			response := map[string]interface{}{
				"job_id":              jobId,
				"job_attempt_index":   0,
				"workflow_id":         input["workflow_id"],
				"state":               state,
				"allocated_node_list": []string{},
			}
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(response); err != nil {
				panic(fmt.Sprintf("failed to encode response: %v", err))
			}
		} else if r.Method == http.MethodGet && r.URL.Path == "/jobs" {
			// Return empty list for GetJob check
			response := map[string]interface{}{
				"statuses":   []interface{}{},
				"item_count": 0,
			}
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(response); err != nil {
				panic(fmt.Sprintf("failed to encode response: %v", err))
			}
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer jobManagerServer.Close()

	jobSource := NewJobClientSource(jobManagerServer.URL)

	statuses := []v1.JobStatus{
		v1.JobStatus_JOB_STATUS_PENDING,
		v1.JobStatus_JOB_STATUS_RUNNING,
		v1.JobStatus_JOB_STATUS_COMPLETED,
		v1.JobStatus_JOB_STATUS_FAILED,
		v1.JobStatus_JOB_STATUS_CANCELLED,
		v1.JobStatus_JOB_STATUS_STOPPED,
	}

	for i, status := range statuses {
		createOrUpdateResponse, err := jobSource.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
			JobId:        fmt.Sprintf("status-job-%d", i),
			WorkflowId:   "status-workflow",
			AttemptIndex: 0,
			State:        status,
		})

		if err != nil {
			t.Fatalf("CreateOrUpdateJob failed for status %v: %v", status, err)
		}

		if createOrUpdateResponse.Job.State != status {
			t.Errorf("Expected State %v, got %v", status, createOrUpdateResponse.Job.State)
		}
	}
}

func TestConvertCreateOrUpdateRequestToJobManagerInput_SetsFilenameWithoutLogFilePaths(t *testing.T) {
	heimdallID := "compat-heimdall-id"
	req := &v1.CreateOrUpdateJobRequest{
		HeimdallJobId: &heimdallID,
		JobId:         "job-compat",
		AttemptIndex:  2,
	}

	input, err := convertCreateOrUpdateRequestToJobManagerInput(req)
	if err != nil {
		t.Fatalf("convertCreateOrUpdateRequestToJobManagerInput() failed: %v", err)
	}

	if !input.HasFilename() {
		t.Fatal("expected filename to be set")
	}

	filename, ok := input.GetFilenameOk()
	if !ok {
		t.Fatal("expected filename to be present")
	}
	if filename == nil || *filename != heimdallID {
		t.Fatalf("expected filename %q, got %v", heimdallID, filename)
	}

	if input.HasLogFilePaths() {
		t.Fatalf("log_file_paths = %#v, want Gateway field omitted", input.LogFilePaths)
	}
}

func TestConvertCreateOrUpdateRequestToJobManagerInput_SetsWorkflowID(t *testing.T) {
	workflowID := "  cafe\u0301/%  "
	req := &v1.CreateOrUpdateJobRequest{
		JobId:        "job-canonical-workflow",
		AttemptIndex: 2,
		WorkflowId:   workflowID,
	}

	input, err := convertCreateOrUpdateRequestToJobManagerInput(req)
	if err != nil {
		t.Fatalf("convertCreateOrUpdateRequestToJobManagerInput() failed: %v", err)
	}

	if input.WorkflowId != workflowID {
		t.Fatalf("WorkflowId = %q, want exact bytes %q", input.WorkflowId, workflowID)
	}
}

func TestConvertCreateOrUpdateRequestToJobManagerInput_DoesNotSetFilenameWithoutHeimdallId(t *testing.T) {
	req := &v1.CreateOrUpdateJobRequest{
		JobId:        "job-no-heimdall",
		AttemptIndex: 0,
	}

	input, err := convertCreateOrUpdateRequestToJobManagerInput(req)
	if err != nil {
		t.Fatalf("convertCreateOrUpdateRequestToJobManagerInput() failed: %v", err)
	}

	if input.HasFilename() {
		t.Fatal("expected filename to be omitted when heimdall_job_id is not provided")
	}
}

func TestMapStateToAdviceStateReleased(t *testing.T) {
	if got := mapStateToAdviceState("Released"); got != v1.AdviceState_ADVICE_STATE_RELEASED {
		t.Fatalf("mapStateToAdviceState(Released) = %v, want RELEASED", got)
	}
}
