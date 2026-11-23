// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/job"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
)

// TestAttributionHTTPCompatibility exercises the public HTTP routes through the
// real Job Manager adapter, protecting evidence fields and indexed query filters.
func TestAttributionHTTPCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, path, backendPath string
		expectedQuery           map[string][]string
	}{
		{"batch", "/v1/error-attributions?jobIds=42&jobIds=43&limit=200&offset=200", "/error_attributions", map[string][]string{"job_ids": {"42", "43"}, "limit": {"200"}, "offset": {"200"}}},
		{"latest", "/v1/job-error-attributions?jobId=42&jobAttemptIndex=-1", "/job_error_attributions", map[string][]string{"job_id": {"42"}, "job_attempt_index": {"-1"}}},
		{"detail", "/v1/job-error-attributions?jobId=42&jobAttemptIndex=0&anomalyUuid=998d15d9-c907-4c53-bc69-b62b42777558", "/job_error_attributions", map[string][]string{"job_id": {"42"}, "job_attempt_index": {"0"}, "anomaly_uuid": {"998d15d9-c907-4c53-bc69-b62b42777558"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.backendPath || !reflect.DeepEqual(map[string][]string(r.URL.Query()), tc.expectedQuery) {
					t.Errorf("backend query = %s %v", r.URL.Path, r.URL.Query())
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"item_count":2,"attribution_summary":"Excluded affected node","attribution_insight":"Inspect the GPU","events":[{"node":"node-a","timestamp":"2026-11-23T00:00:00Z","job_id":"42","job_attempt_index":0,"job_execution_error":{"code":79,"signature":"Xid 79","message":"GPU lost","description":"GPU became unavailable"},"error_attribution":"HW","error_attribution_type":"FAST","error_attribution_origin":"FACT","error_attribution_sources":["SYSLOG"],"exclusion_type":"CONFIRMED","original_exclusion_type":"CONFIRMED","exclusion_applied":"applied","fact_status":"OK"}],"global_events":[{"job_id":"42","job_attempt_index":0,"timestamp":"2026-11-23T00:00:00Z","job_execution_error":{"code":0,"signature":"shutdown","message":"job stopped","description":"job-wide context"},"error_attribution":"UNKNOWN","error_attribution_type":"SLOW","error_attribution_origin":"FACT","error_attribution_sources":[],"fact_status":"OK"}]}`))
			}))
			defer backend.Close()
			mux := runtime.NewServeMux()
			if err := pb.RegisterHeimdallServiceHandlerServer(context.Background(), mux, NewHeimdallServer(job.NewJobClientSource(backend.URL))); err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if response.Code != http.StatusOK {
				t.Fatalf("HTTP %d: %s", response.Code, response.Body.String())
			}
			var body struct {
				Attributions struct {
					Count   int    `json:"count"`
					Summary string `json:"attributionSummary"`
					Insight string `json:"attributionInsight"`
					Events  []struct {
						Node            string                                           `json:"node"`
						JobID           string                                           `json:"jobId"`
						AttributionType string                                           `json:"errorAttributionType"`
						Applied         string                                           `json:"exclusionApplied"`
						ExclusionType   string                                           `json:"exclusionType"`
						OriginalType    string                                           `json:"originalExclusionType"`
						Sources         []string                                         `json:"errorAttributionSources"`
						Error           struct{ Signature, Message, Description string } `json:"jobExecutionError"`
					} `json:"events"`
					Globals []struct {
						JobID string `json:"jobId"`
					} `json:"globalEvents"`
				} `json:"attributions"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			a := body.Attributions
			if a.Count != 2 || a.Summary != "Excluded affected node" || a.Insight != "Inspect the GPU" || len(a.Events) != 1 || len(a.Globals) != 1 || a.Globals[0].JobID != "42" {
				t.Fatalf("unexpected attribution envelope: %s", response.Body.String())
			}
			e := a.Events[0]
			if e.Node != "node-a" || e.JobID != "42" || e.AttributionType != "FAST" || e.Applied != "EXCLUSION_APPLICATION_STATUS_APPLIED" || e.ExclusionType != "CONFIRMED" || e.OriginalType != "CONFIRMED" || !reflect.DeepEqual(e.Sources, []string{"SYSLOG"}) || e.Error.Signature != "Xid 79" || e.Error.Message != "GPU lost" || e.Error.Description != "GPU became unavailable" {
				t.Fatalf("attribution evidence changed: %s", response.Body.String())
			}
		})
	}
}

// TestAttributionHTTPInvalidFilters proves invalid or unbounded scans are
// rejected before an unavailable backend is contacted.
func TestAttributionHTTPInvalidFilters(t *testing.T) {
	mux := runtime.NewServeMux()
	if err := pb.RegisterHeimdallServiceHandlerServer(context.Background(), mux, NewHeimdallServer(job.NewJobClientSource("http://127.0.0.1:1"))); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/v1/error-attributions", "/v1/error-attributions?jobIds=42&offset=-1", "/v1/error-attributions?jobIds=42&limit=251", "/v1/error-attributions?jobIds=42&startTimestamp=invalid", "/v1/job-error-attributions?jobId=42&jobAttemptIndex=-2",
	} {
		t.Run(path, func(t *testing.T) {
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("HTTP %d, want 400: %s", response.Code, response.Body.String())
			}
		})
	}
}

// TestAttributionHTTPBackendFailures distinguishes query failures from an empty
// successful attribution result so callers can retain their retry behavior.
func TestAttributionHTTPBackendFailures(t *testing.T) {
	for _, tc := range []struct{ backendStatus, wantStatus int }{
		{http.StatusUnprocessableEntity, http.StatusBadRequest},
		{http.StatusNotFound, http.StatusNotFound},
		{http.StatusServiceUnavailable, http.StatusInternalServerError},
	} {
		t.Run(http.StatusText(tc.backendStatus), func(t *testing.T) {
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.backendStatus)
				_, _ = w.Write([]byte(`{"detail":"query unavailable"}`))
			}))
			defer backend.Close()
			mux := runtime.NewServeMux()
			if err := pb.RegisterHeimdallServiceHandlerServer(context.Background(), mux, NewHeimdallServer(job.NewJobClientSource(backend.URL))); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"/v1/error-attributions?jobIds=42", "/v1/job-error-attributions?jobId=42"} {
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
				if response.Code != tc.wantStatus {
					t.Errorf("%s: HTTP %d, want %d: %s", path, response.Code, tc.wantStatus, response.Body.String())
				}
			}
		})
	}
}
