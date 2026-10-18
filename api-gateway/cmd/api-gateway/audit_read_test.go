// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// Exercise the same registrations and authentication boundaries as the public
// listeners, including generated HTTP query parsing and protobuf JSON encoding.
func TestAuditReadPublicTransports(t *testing.T) {
	queries := make(chan url.Values, 8)
	largeDescription := strings.Repeat("event detail ", 500)
	payload, err := json.Marshal(map[string]any{
		"cursor": map[string]string{"prev_cursor": "previous", "next_cursor": "next"},
		"items": []any{map[string]any{
			"heimdall_job_id": "job-uuid", "event_type": "recovery_decided",
			"event_level": "recovery", "event_producer": "job-manager",
			"event_produced_at": int64(1700000000123456), "payload": map[string]any{"action": "release"},
			"summary": map[string]any{"title": "Recovery", "description": largeDescription, "granularity": 1},
			"id":      "private-row-id", "idempotency_key": "private-dedup-key",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	downstream := &http.Client{Transport: auditReadTransport(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/v1/audit/events" {
			t.Errorf("unexpected downstream request: %s %s", request.Method, request.URL.Path)
		}
		queries <- request.URL.Query()
		return &http.Response{StatusCode: 200, Status: "200 OK", Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(string(payload)))}, nil
	})}
	source, err := audit.NewAuditClientSource("http://audit.test", downstream)
	if err != nil {
		t.Fatal(err)
	}
	authentication := auth.NewAuth()
	authentication.AddAuthenticator(auditReadAuthentication{})
	listener := bufconn.Listen(1 << 20)
	rpcServer := grpc.NewServer(grpc.UnaryInterceptor(authentication.GrpcAuthInterceptorfunc))
	registerGrpcServices(rpcServer, &pb.UnimplementedHeimdallServiceServer{}, server.NewDisabledApiKeysServer(), server.NewAuditServer(source))
	go func() { _ = rpcServer.Serve(listener) }()
	t.Cleanup(func() { rpcServer.Stop(); _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///audit", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewAuditServiceClient(conn)
	request := &pb.GetAuditEventsRequest{}
	if _, err := client.GetAuditEvents(context.Background(), request); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated read: %v", err)
	}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer allowed"))
	reply, err := client.GetAuditEvents(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Items) != 1 || reply.Items[0].Summary.Description != largeDescription {
		t.Fatalf("truncated gRPC page: %#v", reply)
	}
	<-queries
	mux := newGatewayMux()
	if err := registerGatewayHandlers(context.Background(), mux, conn); err != nil {
		t.Fatal(err)
	}
	handler := authentication.HttpAuthMiddleware(mux)
	for _, token := range []string{"", "Bearer denied", "Bearer allowed"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/audit/events?job_id=123&attempt_index=0&event_type=recovery_decided&event_level=recovery&event_producer=job-manager&min_event_produced_at=0&max_event_produced_at=1700000000123456&sort_by=event_produced_at:asc&cursor=next&limit=100", nil)
		req.Header.Set("Authorization", token)
		result := httptest.NewRecorder()
		handler.ServeHTTP(result, req)
		if token != "Bearer allowed" {
			if result.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated HTTP status: %d", result.Code)
			}
			continue
		}
		if result.Code != http.StatusOK {
			t.Fatalf("HTTP read: %d %s", result.Code, result.Body.String())
		}
		query := <-queries
		for key, want := range map[string]string{"job_id": "123", "attempt_index": "0", "event_type": "recovery_decided", "event_level": "recovery", "event_producer": "job-manager", "min_event_produced_at": "0", "max_event_produced_at": "1700000000123456", "sort_by": "event_produced_at:asc", "cursor": "next", "limit": "100"} {
			if query.Get(key) != want {
				t.Errorf("query %s = %q, want %q", key, query.Get(key), want)
			}
		}
		var body map[string]any
		if err := json.Unmarshal(result.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		event := body["items"].([]any)[0].(map[string]any)
		if event["eventProducedAt"] != "1700000000123456" || event["heimdallJobId"] != "job-uuid" {
			t.Fatalf("unexpected protobuf JSON contract: %#v", event)
		}
		if strings.Contains(result.Body.String(), "private-row-id") || strings.Contains(result.Body.String(), "private-dedup-key") {
			t.Fatal("storage fields leaked")
		}
		if len(result.Body.Bytes()) <= 4096 {
			t.Fatal("fixture must prove a page over ingestion's bound")
		}
		cursor := body["cursor"].(map[string]any)
		if cursor["prevCursor"] != "previous" || cursor["nextCursor"] != "next" {
			t.Fatalf("cursors: %#v", cursor)
		}
	}

	camelRequest := httptest.NewRequest(http.MethodGet, "/v1/audit/events?jobId=123&attemptIndex=0&eventType=recovery_decided&minEventProducedAt=0", nil)
	camelRequest.Header.Set("Authorization", "Bearer allowed")
	camelResponse := httptest.NewRecorder()
	handler.ServeHTTP(camelResponse, camelRequest)
	if camelResponse.Code != http.StatusOK {
		t.Fatalf("camelCase query: %d %s", camelResponse.Code, camelResponse.Body.String())
	}
	camelQuery := <-queries
	for key, want := range map[string]string{"job_id": "123", "attempt_index": "0", "event_type": "recovery_decided", "min_event_produced_at": "0"} {
		if camelQuery.Get(key) != want {
			t.Errorf("camelCase query %s = %q, want %q", key, camelQuery.Get(key), want)
		}
	}
	if len(queries) != 0 {
		t.Fatal("unauthenticated requests reached downstream")
	}
}

type auditReadTransport func(*http.Request) (*http.Response, error)

func (transport auditReadTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

// This deterministic identity provider exercises the production authentication
// middleware independently of the separately tested JWT/OIDC verification.
type auditReadAuthentication struct{}

func (auditReadAuthentication) CheckGrpc(ctx context.Context, _ interface{}, _ *grpc.UnaryServerInfo) (bool, context.Context, error) {
	values, _ := metadata.FromIncomingContext(ctx)
	return len(values.Get("authorization")) == 1 && values.Get("authorization")[0] == "Bearer allowed", ctx, nil
}
func (auditReadAuthentication) CheckHttp(ctx context.Context, request *http.Request) (bool, context.Context, error) {
	return request.Header.Get("Authorization") == "Bearer allowed", ctx, nil
}
