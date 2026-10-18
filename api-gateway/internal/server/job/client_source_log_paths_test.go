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
	"google.golang.org/protobuf/types/known/structpb"
)

func TestConvertCreateOrUpdateRequestToJobManagerInputPassesSchedulerMetadataOpaquely(t *testing.T) {
	t.Parallel()

	const heimdallID = "opaque-metadata-id"
	metadata := mustSchedulerMetadata(t, map[string]any{
		"scheduler":             "slurm",
		"slurm_log_file_paths":  "scheduler-owned shape",
		"slurm_cluster":         "cluster-a",
		"slurm_partition":       "batch",
		"slurm_user":            "alice",
		"slurm_command":         "/workspace/train.sh",
		"slurm_reason":          "Resources",
		"slurm_node_count":      float64(2),
		"slurm_total_gpu_count": float64(8),
		"slurm_total_cpu_count": float64(16),
		"slurm_total_memory_mb": float64(64000),
		"adapter_extension": map[string]any{
			"nested": []any{"value", float64(7)},
		},
	})

	input, err := convertCreateOrUpdateRequestToJobManagerInput(&v1.CreateOrUpdateJobRequest{
		HeimdallJobId:     stringPointer(heimdallID),
		JobId:             "12345",
		AttemptIndex:      1,
		WorkflowId:        "workflow",
		SchedulerMetadata: metadata,
	})
	if err != nil {
		t.Fatalf("convertCreateOrUpdateRequestToJobManagerInput() error = %v", err)
	}
	if !reflect.DeepEqual(input.SchedulerMetadata, metadata.AsMap()) {
		t.Fatalf("SchedulerMetadata = %#v, want %#v", input.SchedulerMetadata, metadata.AsMap())
	}
	if input.HasLogFilePaths() {
		t.Fatalf("LogFilePaths = %#v, want Gateway field omitted", input.LogFilePaths)
	}
	if !input.HasFilename() || input.GetFilename() != heimdallID {
		t.Fatalf("Filename = %q (set %t), want preserved %q", input.GetFilename(), input.HasFilename(), heimdallID)
	}

	serialized, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal Job Manager input: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(serialized, &payload); err != nil {
		t.Fatalf("unmarshal Job Manager input: %v", err)
	}
	if _, exists := payload["log_file_paths"]; exists {
		t.Fatalf("serialized input contains Gateway-populated log_file_paths: %s", serialized)
	}
}

func TestCreateOrUpdateJobPreservesOpaqueSchedulerMetadataInImmediateResponse(t *testing.T) {
	t.Parallel()

	metadata := mustSchedulerMetadata(t, map[string]any{
		"scheduler":            "slurm",
		"slurm_log_file_paths": map[string]any{"future_shape": true},
		"adapter_extension":    []any{"one", "two"},
	})
	jobManagerServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/jobs" {
			t.Errorf("request = %s %s, want POST /jobs", request.Method, request.URL.Path)
		}

		var input map[string]any
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			t.Errorf("decode request: %v", err)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		if _, exists := input["log_file_paths"]; exists {
			t.Errorf("request contains Gateway-populated log_file_paths: %#v", input["log_file_paths"])
		}
		if !reflect.DeepEqual(input["scheduler_metadata"], metadata.AsMap()) {
			t.Errorf("request scheduler_metadata = %#v, want %#v", input["scheduler_metadata"], metadata.AsMap())
		}

		writer.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(writer).Encode(map[string]any{
			"job_id":            "12345",
			"job_attempt_index": 0,
			"workflow_id":       "workflow",
			"schedulerMetadata": input["scheduler_metadata"],
		}); err != nil {
			t.Errorf("encode response: %v", err)
		}
	}))
	defer jobManagerServer.Close()

	source := NewJobClientSource(jobManagerServer.URL)
	response, err := source.CreateOrUpdateJob(context.Background(), &v1.CreateOrUpdateJobRequest{
		JobId:             "12345",
		WorkflowId:        "workflow",
		SchedulerMetadata: metadata,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateJob() error = %v", err)
	}
	if response.Job.SchedulerMetadata == nil {
		t.Fatal("response SchedulerMetadata = nil")
	}
	if !reflect.DeepEqual(response.Job.SchedulerMetadata.AsMap(), metadata.AsMap()) {
		t.Fatalf("response SchedulerMetadata = %#v, want %#v", response.Job.SchedulerMetadata.AsMap(), metadata.AsMap())
	}
}

func mustSchedulerMetadata(t *testing.T, fields map[string]any) *structpb.Struct {
	t.Helper()

	metadata, err := structpb.NewStruct(fields)
	if err != nil {
		t.Fatalf("structpb.NewStruct() error = %v", err)
	}
	return metadata
}

func stringPointer(value string) *string {
	return &value
}
