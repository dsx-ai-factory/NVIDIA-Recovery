// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package server_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/audit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAuditServiceTranslatesGRPCRequestToDownstreamHTTP(t *testing.T) {
	var received map[string]any
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/audit/events" {
			t.Fatalf("downstream request = %s %s, want POST /v1/audit/events", request.Method, request.URL.Path)
		}
		if err := json.NewDecoder(request.Body).Decode(&received); err != nil {
			t.Fatalf("decode downstream request: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusAccepted,
			Status:     "202 Accepted",
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(
				`{"partial_success":{"rejected_events":1,"error_message":"one rejected"}}`,
			)),
		}, nil
	})}

	auditSource, err := audit.NewAuditClientSource("http://audit-service:8080", httpClient)
	if err != nil {
		t.Fatalf("NewAuditClientSource() failed: %v", err)
	}

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterAuditServiceServer(grpcServer, server.NewAuditServer(auditSource))
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	response, err := pb.NewAuditServiceClient(conn).CreateAuditEvents(
		context.Background(),
		&pb.CreateAuditEventsRequest{Items: []*pb.ProducerAuditEvent{{
			HeimdallJobId: "job-1",
			EventProducer: "slurm-advisor-controller",
			EventType:     "scheduler_action_completed",
			EventLevel:    "recovery",
			Payload: &structpb.Struct{Fields: map[string]*structpb.Value{
				"action":  structpb.NewStringValue("release"),
				"success": structpb.NewBoolValue(true),
			}},
			EventProducedAt: 1_700_000_000_123_456,
			IdempotencyKey:  "event-1",
		}}},
	)
	if err != nil {
		t.Fatalf("CreateAuditEvents() failed: %v", err)
	}
	if response.GetPartialSuccess().GetRejectedEvents() != 1 {
		t.Fatalf("partial success = %#v, want one rejected event", response.GetPartialSuccess())
	}

	items, ok := received["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("downstream items = %#v, want one event", received["items"])
	}
	event, ok := items[0].(map[string]any)
	if !ok || event["heimdall_job_id"] != "job-1" || event["event_produced_at"] != float64(1_700_000_000_123_456) {
		t.Fatalf("downstream event = %#v, want translated audit event", items[0])
	}
	payload, ok := event["payload"].(map[string]any)
	if !ok || payload["action"] != "release" || payload["success"] != true {
		t.Fatalf("downstream payload = %#v, want translated protobuf payload", event["payload"])
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}
