// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package audit

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"io"
	"net/http"
	"strings"
	"testing"

	notificationsbindings "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/notifications"
)

func TestCreateAuditEventsSendsDownstreamJSON(t *testing.T) {
	payload := map[string]any{"action": "release", "success": true}

	var received map[string]any
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost {
			t.Fatalf("method = %q, want POST", request.Method)
		}
		if request.URL.String() != "http://audit-service:8080/base/v1/audit/events" {
			t.Fatalf("URL = %q, want audit endpoint", request.URL.String())
		}
		if request.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", request.Header.Get("Content-Type"))
		}
		if request.Header.Get("Authorization") != "" {
			t.Fatalf("Authorization = %q, want absent", request.Header.Get("Authorization"))
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		return jsonResponse(http.StatusAccepted, `{}`), nil
	})}
	client, err := NewClient("http://audit-service:8080/base", httpClient)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}

	response, err := client.CreateAuditEvents(context.Background(), notificationsbindings.ProducerAuditEventBatch{
		Items: []notificationsbindings.ProducerAuditEvent{
			{
				HeimdallJobId:   "job-1",
				EventProducer:   "slurm-advisor-controller",
				EventType:       "scheduler_action_completed",
				EventLevel:      "recovery",
				Payload:         payload,
				EventProducedAt: 1_700_000_000_123_456,
				IdempotencyKey:  "event-1",
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateAuditEvents() failed: %v", err)
	}
	if response.PartialSuccess.Get() != nil {
		t.Fatalf("partial success = %#v, want nil", response.PartialSuccess.Get())
	}

	items, ok := received["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("items = %#v, want one event", received["items"])
	}
	event, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("event = %#v, want object", items[0])
	}
	if event["heimdall_job_id"] != "job-1" || event["event_produced_at"] != float64(1_700_000_000_123_456) {
		t.Fatalf("translated event = %#v", event)
	}
	if _, ok := event["eventProducedAt"]; ok {
		t.Fatalf("translated event contains protobuf JSON field: %#v", event)
	}
	if got := event["payload"].(map[string]any)["action"]; got != "release" {
		t.Fatalf("payload action = %#v, want release", got)
	}
}

func TestCreateAuditEventsReturnsPartialAcceptance(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(
			http.StatusAccepted,
			`{"partial_success":{"rejected_events":2,"error_message":"invalid events"}}`,
		), nil
	})}
	client, err := NewClient("http://audit-service:8080", httpClient)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}

	response, err := client.CreateAuditEvents(context.Background(), notificationsbindings.ProducerAuditEventBatch{})
	if err != nil {
		t.Fatalf("CreateAuditEvents() failed: %v", err)
	}
	if response.PartialSuccess.Get().GetRejectedEvents() != 2 {
		t.Fatalf("rejected events = %d, want 2", response.PartialSuccess.Get().GetRejectedEvents())
	}
	if response.PartialSuccess.Get().GetErrorMessage() != "invalid events" {
		t.Fatalf("error message = %q, want invalid events", response.PartialSuccess.Get().GetErrorMessage())
	}
}

func TestCreateAuditEventsReturnsTypedHTTPErrorWithoutResponseBody(t *testing.T) {
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusTooManyRequests, `{"detail":"private downstream detail"}`), nil
	})}
	client, err := NewClient("http://audit-service:8080", httpClient)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}

	_, err = client.CreateAuditEvents(context.Background(), notificationsbindings.ProducerAuditEventBatch{})
	var httpError *HTTPError
	if !stderrors.As(err, &httpError) {
		t.Fatalf("error = %T %v, want *HTTPError", err, err)
	}
	if httpError.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status code = %d, want %d", httpError.StatusCode, http.StatusTooManyRequests)
	}
	if strings.Contains(err.Error(), "private downstream detail") {
		t.Fatalf("error leaks downstream response body: %v", err)
	}
}

func TestCreateAuditEventsPreservesTransportErrorIdentity(t *testing.T) {
	sentinel := stderrors.New("transport unavailable")
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, sentinel
	})}
	client, err := NewClient("http://audit-service:8080", httpClient)
	if err != nil {
		t.Fatalf("NewClient() failed: %v", err)
	}

	_, err = client.CreateAuditEvents(context.Background(), notificationsbindings.ProducerAuditEventBatch{})
	if !stderrors.Is(err, sentinel) {
		t.Fatalf("error = %v, want transport sentinel in chain", err)
	}
}

func TestCreateAuditEventsRejectsOversizedResponse(t *testing.T) {
	for _, redirect := range []bool{false, true} {
		httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if redirect && request.Method == http.MethodPost {
				response := jsonResponse(http.StatusSeeOther, "")
				response.Header.Set("Location", "/accepted")
				return response, nil
			}
			return jsonResponse(http.StatusAccepted, strings.Repeat("x", int(maxResponseBodySize)+1)), nil
		})}
		client, err := NewClient("http://audit-service:8080", httpClient)
		if err != nil {
			t.Fatalf("NewClient() failed: %v", err)
		}
		_, err = client.CreateAuditEvents(context.Background(), notificationsbindings.ProducerAuditEventBatch{})
		if !stderrors.Is(err, ErrResponseTooLarge) {
			t.Fatalf("redirect=%v: error = %v, want response size error", redirect, err)
		}
	}
}

func TestNewClientRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name       string
		endpoint   string
		httpClient *http.Client
	}{
		{name: "missing scheme", endpoint: "audit-service:8080", httpClient: http.DefaultClient},
		{name: "unsupported scheme", endpoint: "ftp://audit-service:8080", httpClient: http.DefaultClient},
		{name: "missing host", endpoint: "http:///audit", httpClient: http.DefaultClient},
		{name: "credentials", endpoint: "http://user:password@audit-service:8080", httpClient: http.DefaultClient},
		{name: "query", endpoint: "http://audit-service:8080?token=secret", httpClient: http.DefaultClient},
		{name: "fragment", endpoint: "http://audit-service:8080#fragment", httpClient: http.DefaultClient},
		{name: "missing client", endpoint: "http://audit-service:8080"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewClient(test.endpoint, test.httpClient); err == nil {
				t.Fatalf("NewClient(%q) error = nil, want validation error", test.endpoint)
			}
		})
	}
}

func jsonResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Status:     http.StatusText(statusCode),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}
