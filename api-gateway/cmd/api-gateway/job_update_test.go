// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/job"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
)

// TestJobUpdatePublicTransports proves malformed states cannot reach persistence
// and that the same gateway continues serving valid updates after rejecting them.
// The backend fixture models Job Manager's nullable state for an omitted update.
// gRPC owns its serving goroutine; cleanup stops it before closing the listener.
func TestJobUpdatePublicTransports(t *testing.T) {
	var writes atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/jobs" {
			http.NotFound(w, r)
			return
		}
		var input map[string]any
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		writes.Add(1)
		// Explicit JSON null reproduces the backend response that caused the crash.
		if _, ok := input["state"]; !ok || input["job_id"] == "qa-null-backend-state" {
			input["state"] = nil
		}
		input["last_updated_at"] = nil
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(input); err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(backend.Close)
	listener := bufconn.Listen(1 << 20)
	rpcServer := grpc.NewServer()
	pb.RegisterHeimdallServiceServer(rpcServer, server.NewHeimdallServer(job.NewJobClientSource(backend.URL)))
	go func() { _ = rpcServer.Serve(listener) }()
	t.Cleanup(func() { rpcServer.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///job-update", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	mux := newGatewayMux()
	if err := registerGatewayHandlers(context.Background(), mux, conn); err != nil {
		t.Fatal(err)
	}

	for _, state := range []string{`"NOT_A_JOB_STATUS"`, `999`, `-1`, `"JOB_STATUS_RUNNING","unknownField":true`} {
		t.Run("HTTP/"+state, func(t *testing.T) {
			before := writes.Load()
			body := fmt.Sprintf(`{"jobId":"qa-invalid-state","workflowId":"qa-invalid-state","attemptIndex":0,"state":%s}`, state)
			req := httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			result := httptest.NewRecorder()
			mux.ServeHTTP(result, req)
			if result.Code != http.StatusBadRequest {
				t.Fatalf("HTTP status = %d, want 400: %s", result.Code, result.Body.String())
			}
			if writes.Load() != before {
				t.Fatal("invalid update reached persistence")
			}
		})
	}
	client := pb.NewHeimdallServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, state := range []pb.JobStatus{999, -1} {
		before := writes.Load()
		_, err := client.CreateOrUpdateJob(ctx, &pb.CreateOrUpdateJobRequest{JobId: "qa-invalid-state", WorkflowId: "qa-invalid-state", State: state})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("gRPC status = %v, want InvalidArgument", err)
		}
		if writes.Load() != before {
			t.Fatal("invalid gRPC update reached persistence")
		}
	}

	// Omitted state remains a supported partial update, including nullable output.
	for _, tc := range []struct {
		state string
		want  pb.JobStatus
	}{
		{``, pb.JobStatus_JOB_STATUS_UNSPECIFIED},
		{`,"state":"JOB_STATUS_UNSPECIFIED"`, pb.JobStatus_JOB_STATUS_UNSPECIFIED},
		{`,"state":null`, pb.JobStatus_JOB_STATUS_UNSPECIFIED},
		{`,"state":"JOB_STATUS_PENDING"`, pb.JobStatus_JOB_STATUS_PENDING},
		{`,"state":"JOB_STATUS_RUNNING"`, pb.JobStatus_JOB_STATUS_RUNNING},
		{`,"state":"JOB_STATUS_STOPPED"`, pb.JobStatus_JOB_STATUS_STOPPED},
		{`,"state":4`, pb.JobStatus_JOB_STATUS_COMPLETED},
		{`,"state":"JOB_STATUS_FAILED"`, pb.JobStatus_JOB_STATUS_FAILED},
		{`,"state":"JOB_STATUS_CANCELLED"`, pb.JobStatus_JOB_STATUS_CANCELLED},
		{`,"state":"JOB_STATUS_DELETED"`, pb.JobStatus_JOB_STATUS_DELETED},
	} {
		before := writes.Load()
		body := fmt.Sprintf(`{"jobId":"qa-valid-state","workflowId":"qa-valid-state","attemptIndex":0%s}`, tc.state)
		result := httptest.NewRecorder()
		mux.ServeHTTP(result, httptest.NewRequest(http.MethodPost, "/v1/jobs", strings.NewReader(body)))
		if result.Code != http.StatusOK || writes.Load() != before+1 {
			t.Fatalf("valid HTTP update %s: status %d, writes %d: %s", tc.state, result.Code, writes.Load()-before, result.Body.String())
		}
		var response pb.CreateOrUpdateJobResponse
		if err := protojson.Unmarshal(result.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Job == nil || response.Job.State != tc.want {
			t.Fatalf("returned job = %v, want state %s", response.Job, tc.want)
		}
	}

	response, err := client.CreateOrUpdateJob(ctx, &pb.CreateOrUpdateJobRequest{
		JobId: "qa-null-backend-state", WorkflowId: "qa-null-backend-state", State: pb.JobStatus_JOB_STATUS_RUNNING,
	})
	if err != nil {
		t.Fatalf("nullable backend response: %v", err)
	}
	if response.Job == nil || response.Job.State != pb.JobStatus_JOB_STATUS_UNSPECIFIED || response.Job.UpdatedAt != nil || response.Job.LastSeen != nil {
		t.Fatalf("nullable backend fields should remain unspecified: %v", response.Job)
	}
}
