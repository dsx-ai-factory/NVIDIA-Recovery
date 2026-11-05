// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package job

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	v1 "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
)

func TestJobClientSourcePreservesArrayIdentityOnPublicationAndDiscovery(t *testing.T) {
	tests := []struct {
		jobID, taskID, rawID, heimdallID string
		attempt                          int32
	}{
		{"4200_0", "0", "4201", "d3032274-eed6-5c01-bfa9-ae8798f5d703", 0},
		{"4200_0", "0", "4201", "e0345de7-1491-548d-b42c-1f3bdab08307", 1},
		{"4200_1", "1", "4200", "87bf7395-c280-5892-a7e2-00620beeaf88", 0},
	}
	for _, tt := range tests {
		t.Run(tt.heimdallID, func(t *testing.T) {
			metadata := map[string]any{
				"scheduler": "slurm", "slurm_job_id": tt.jobID, "slurm_raw_job_id": tt.rawID,
				"slurm_array_job_id": "4200", "slurm_array_task_id": tt.taskID,
			}
			backendJob := map[string]any{
				"job_id": tt.jobID, "job_attempt_index": tt.attempt, "heimdall_job_id": tt.heimdallID,
				"workflow_id": "array-workflow", "schedulerMetadata": metadata,
			}
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				var output any
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/jobs":
					var input map[string]any
					if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					if input["job_id"] != tt.jobID || input["job_attempt_index"] != float64(tt.attempt) || input["heimdall_job_id"] != tt.heimdallID {
						t.Errorf("backend received incorrect task-attempt identity: %#v", input)
					}
					if !reflect.DeepEqual(input["scheduler_metadata"], metadata) {
						t.Errorf("backend metadata = %#v, want %#v", input["scheduler_metadata"], metadata)
					}
					output = backendJob
				case r.Method == http.MethodGet && r.URL.Path == "/jobs":
					output = map[string]any{"statuses": []any{backendJob}, "item_count": 1}
				default:
					t.Errorf("unexpected backend request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if err := json.NewEncoder(w).Encode(output); err != nil {
					t.Error(err)
				}
			}))
			defer backend.Close()

			source := NewJobClientSource(backend.URL)
			published, err := source.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
				JobId: tt.jobID, AttemptIndex: tt.attempt, HeimdallJobId: &tt.heimdallID,
				WorkflowId: "array-workflow", SchedulerMetadata: mustSchedulerMetadata(t, metadata),
			})
			if err != nil {
				t.Fatal(err)
			}
			discovered, err := source.GetJobs(context.Background(), &v1.GetJobsRequest{})
			if err != nil {
				t.Fatal(err)
			}
			if len(discovered.GetJobs()) != 1 {
				t.Fatalf("discovered %d jobs, want one task attempt", len(discovered.GetJobs()))
			}
			for name, got := range map[string]*v1.Job{"publication": published.GetJob(), "discovery": discovered.GetJobs()[0]} {
				if got.GetJobId() != tt.jobID || got.GetAttemptIndex() != tt.attempt || got.GetHeimdallJobId() != tt.heimdallID {
					t.Errorf("%s changed task-attempt identity: %v", name, got)
				}
				if !reflect.DeepEqual(got.GetSchedulerMetadata().AsMap(), metadata) {
					t.Errorf("%s metadata = %#v, want %#v", name, got.GetSchedulerMetadata().AsMap(), metadata)
				}
			}
		})
	}
}
