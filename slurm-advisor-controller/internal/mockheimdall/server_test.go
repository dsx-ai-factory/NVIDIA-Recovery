// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package mockheimdall_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/mockheimdall"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestServerStoresCreateOrUpdateRequests(t *testing.T) {
	ctx := context.Background()
	server := mockheimdall.NewServer(nil)

	_, err := server.CreateOrUpdateJob(ctx, &pb.CreateOrUpdateJobRequest{
		JobId:        "12345",
		AttemptIndex: 2,
		State:        pb.JobStatus_JOB_STATUS_RUNNING,
	})
	if err != nil {
		t.Fatalf("CreateOrUpdateJob() failed: %v", err)
	}

	upserts := server.Upserts()
	if len(upserts) != 1 {
		t.Fatalf("len(upserts) = %d, want 1", len(upserts))
	}
	if upserts[0].GetJobId() != "12345" || upserts[0].GetAttemptIndex() != 2 {
		t.Fatalf("upsert = %#v, want 12345 attempt 2", upserts[0])
	}
}

func TestServerListsAdviceWithPagination(t *testing.T) {
	ctx := context.Background()
	server := mockheimdall.NewServer([]*pb.Advice{
		{HeimdallJobId: "advice-1"},
		{HeimdallJobId: "advice-2"},
		{HeimdallJobId: "advice-3"},
	})

	first, err := server.ListAdvice(ctx, &pb.ListAdviceRequest{Limit: 2})
	if err != nil {
		t.Fatalf("ListAdvice(first) failed: %v", err)
	}
	if len(first.GetAdvice()) != 2 || first.GetAdvice()[0].GetHeimdallJobId() != "advice-1" {
		t.Fatalf("first page = %v, want advice-1/advice-2", first.GetAdvice())
	}
	if first.GetNextPageToken() != "2" {
		t.Fatalf("first next page token = %q, want 2", first.GetNextPageToken())
	}

	second, err := server.ListAdvice(ctx, &pb.ListAdviceRequest{Page: first.NextPageToken, Limit: 2})
	if err != nil {
		t.Fatalf("ListAdvice(second) failed: %v", err)
	}
	if len(second.GetAdvice()) != 1 || second.GetAdvice()[0].GetHeimdallJobId() != "advice-3" {
		t.Fatalf("second page = %v, want advice-3", second.GetAdvice())
	}
	if second.GetNextPageToken() != "" {
		t.Fatalf("second next page token = %q, want empty", second.GetNextPageToken())
	}
}

func TestDebugAdviceEndpointReplacesAdvice(t *testing.T) {
	ctx := context.Background()
	server := mockheimdall.NewServer(nil)
	mux := http.NewServeMux()
	server.RegisterDebugHandlers(mux)

	body := strings.NewReader(`[
		{
			"advice_id": "restart-12345",
			"job_id": "12345",
			"attempt_index": 1,
			"state": "ADVICE_STATE_RESTARTED",
			"scheduler_metadata": {
				"scheduler": "slurm",
				"managed_by": "slurm-advisor-controller"
			}
		}
	]`)
	req := httptest.NewRequest(http.MethodPut, "/advice", body)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("PUT /advice status = %d body = %q, want 204", rec.Code, rec.Body.String())
	}
	resp, err := server.ListAdvice(ctx, &pb.ListAdviceRequest{Limit: 10})
	if err != nil {
		t.Fatalf("ListAdvice() failed: %v", err)
	}
	if len(resp.GetAdvice()) != 1 {
		t.Fatalf("len(advice) = %d, want 1", len(resp.GetAdvice()))
	}
	advice := resp.GetAdvice()[0]
	if advice.GetHeimdallJobId() != "restart-12345" || advice.GetJobId() != "12345" || advice.GetAttemptIndex() != 1 {
		t.Fatalf("advice identity = %#v, want restart-12345 job 12345 attempt 1", advice)
	}
	if advice.GetAdvice().GetState() != pb.AdviceState_ADVICE_STATE_RESTARTED {
		t.Fatalf("advice state = %s, want ADVICE_STATE_RESTARTED", advice.GetAdvice().GetState())
	}
	if advice.GetSchedulerMetadata().GetFields()["scheduler"].GetStringValue() != "slurm" {
		t.Fatalf("scheduler metadata = %v, want slurm", advice.GetSchedulerMetadata())
	}
	if advice.GetCreatedAt() == nil || advice.GetCreatedAt().CheckValid() != nil {
		t.Fatalf("created_at = %v, want valid timestamp", advice.GetCreatedAt())
	}
}

func TestDebugUpsertsEndpointIncludesSchedulerMetadata(t *testing.T) {
	ctx := context.Background()
	server := mockheimdall.NewServer(nil)
	metadata, err := structpb.NewStruct(map[string]any{
		"scheduler":            "slurm",
		"slurm_log_file_paths": []any{"/data/parent-42.out", "/data/step-42-0.out"},
	})
	if err != nil {
		t.Fatalf("build scheduler metadata: %v", err)
	}
	if _, err := server.CreateOrUpdateJob(ctx, &pb.CreateOrUpdateJobRequest{
		JobId:             "42",
		AttemptIndex:      1,
		State:             pb.JobStatus_JOB_STATUS_RUNNING,
		SchedulerMetadata: metadata,
	}); err != nil {
		t.Fatalf("CreateOrUpdateJob() failed: %v", err)
	}

	mux := http.NewServeMux()
	server.RegisterDebugHandlers(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/upserts", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("GET /upserts status = %d body = %q, want 200", rec.Code, rec.Body.String())
	}
	var summaries []mockheimdall.UpsertSummary
	if err := json.Unmarshal(rec.Body.Bytes(), &summaries); err != nil {
		t.Fatalf("decode GET /upserts response: %v", err)
	}
	if len(summaries) != 1 {
		t.Fatalf("len(summaries) = %d, want 1", len(summaries))
	}
	wantPaths := []any{"/data/parent-42.out", "/data/step-42-0.out"}
	if got := summaries[0].SchedulerMetadata["slurm_log_file_paths"]; !reflect.DeepEqual(got, wantPaths) {
		t.Fatalf("scheduler metadata log paths = %#v, want %#v", got, wantPaths)
	}
}

// Independent fixture writers must not erase another scenario's advice. The
// read counter establishes that an operator actually polled newly supplied data.
func TestScopedAdviceIsolationAndObservation(t *testing.T) {
	server := mockheimdall.NewServer([]*pb.Advice{{HeimdallJobId: "legacy"}})
	mux := http.NewServeMux()
	server.RegisterDebugHandlers(mux)
	request := func(method, path, body string, status int) *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(method, path, strings.NewReader(body)))
		if response.Code != status {
			t.Fatalf("%s %s: status %d, body %s", method, path, response.Code, response.Body.String())
		}
		return response
	}
	reads := func(scope string) uint64 {
		t.Helper()
		response := request(http.MethodGet, "/advice/"+scope, "", http.StatusOK)
		var state struct {
			Reads uint64 `json:"reads"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &state); err != nil {
			t.Fatal(err)
		}
		return state.Reads
	}
	list := func() []string {
		t.Helper()
		var ids []string
		page := ""
		for {
			response, err := server.ListAdvice(context.Background(), &pb.ListAdviceRequest{Page: &page, Limit: 1})
			if err != nil {
				t.Fatal(err)
			}
			for _, advice := range response.GetAdvice() {
				ids = append(ids, advice.GetHeimdallJobId())
			}
			page = response.GetNextPageToken()
			if page == "" {
				return ids
			}
		}
	}
	request(http.MethodPut, "/advice/run-b", `[{"advice_id":"b","job_id":"job-b"}]`, http.StatusNoContent)
	request(http.MethodPut, "/advice/run-a", `[{"advice_id":"a","job_id":"job-a"}]`, http.StatusNoContent)
	if got := reads("run-a"); got != 0 {
		t.Fatalf("new scope reads = %d", got)
	}
	if got := list(); !reflect.DeepEqual(got, []string{"legacy", "a", "b"}) {
		t.Fatalf("advice = %v", got)
	}
	if got := reads("run-a"); got != 1 {
		t.Fatalf("paginated poll counted %d times, want 1", got)
	}
	request(http.MethodPut, "/advice/run-a", `[]`, http.StatusNoContent)
	if reads("run-a") != 0 || reads("run-b") != 1 {
		t.Fatal("replacement reset another scope's evidence")
	}
	if got := list(); !reflect.DeepEqual(got, []string{"legacy", "b"}) {
		t.Fatalf("replacement removed other scopes: %v", got)
	}
	request(http.MethodDelete, "/advice/run-a", "", http.StatusNoContent)
	request(http.MethodGet, "/advice/run-a", "", http.StatusNotFound)
	if got := list(); !reflect.DeepEqual(got, []string{"legacy", "b"}) {
		t.Fatalf("cleanup removed other scopes: %v", got)
	}
	// Invalid replacement must leave the previous fixture intact.
	request(http.MethodPut, "/advice/run-b", `[{"job_id":"invalid"}]`, http.StatusBadRequest)
	if got := list(); !reflect.DeepEqual(got, []string{"legacy", "b"}) {
		t.Fatalf("invalid replacement mutated state: %v", got)
	}
}
