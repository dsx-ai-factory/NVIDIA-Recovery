// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package destinations

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGatewayDeliversCompleteEvents(t *testing.T) {
	payload := map[string]any{
		"reason":  "recovery",
		"attempt": float64(2),
		"details": struct {
			Nodes []string `json:"nodes"`
			Retry bool     `json:"retry"`
		}{Nodes: []string{"node-1", "node-2"}, Retry: true},
		"actions": []string{"slurm.requeue_hold", "slurm.release"},
	}
	events := []audit.ProducerEvent{{
		HeimdallJobID: "job-1", EventProducer: "slurm-advisor-controller",
		EventType: "job-requeued", EventLevel: audit.LevelRecovery,
		Payload: payload, EventProducedAt: 1788888888123456, IdempotencyKey: "event-1",
	}}
	fixture := &auditGatewayFixture{}
	core, observed := observer.New(zap.InfoLevel)
	destination := NewGateway(startAuditGateway(t, fixture), zap.New(core))
	delivered, err := destination.Deliver(context.Background(), events)
	if err != nil || delivered != 1 {
		t.Fatalf("Deliver() = (%d, %v), want (1, nil)", delivered, err)
	}
	wirePayload, err := structpb.NewStruct(map[string]any{
		"reason":  "recovery",
		"attempt": float64(2),
		"details": map[string]any{"nodes": []any{"node-1", "node-2"}, "retry": true},
		"actions": []any{"slurm.requeue_hold", "slurm.release"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := &pb.CreateAuditEventsRequest{Items: []*pb.ProducerAuditEvent{{
		HeimdallJobId: "job-1", EventProducer: "slurm-advisor-controller",
		EventType: "job-requeued", EventLevel: string(audit.LevelRecovery),
		Payload: wirePayload, EventProducedAt: 1788888888123456, IdempotencyKey: "event-1",
	}}}
	requests := fixture.received()
	if len(requests) != 1 || !proto.Equal(requests[0], want) {
		t.Fatalf("received requests = %v, want [%v]", requests, want)
	}
	entries := observed.All()
	if len(entries) != 1 || entries[0].Message != "sent audit event batch" || entries[0].ContextMap()["event_count"] != int64(1) {
		t.Fatalf("success logs = %v, want one sent audit event batch with event_count 1", entries)
	}
}

func TestGatewayDeliversTypedWatchdogEvent(t *testing.T) {
	event := audit.WatchdogReleaseCompletedEvent{
		HeimdallJobID: "held-attempt", JobID: "123", AttemptIndex: 2, Success: true,
		ProducedAt: time.UnixMicro(1788888888123456), IdempotencyKey: "release-1",
	}.ToProducerEvent()
	fixture := &auditGatewayFixture{}
	destination := NewGateway(startAuditGateway(t, fixture), zap.NewNop())
	if delivered, err := destination.Deliver(context.Background(), []audit.ProducerEvent{event}); err != nil || delivered != 1 {
		t.Fatalf("Deliver() = (%d, %v), want (1, nil)", delivered, err)
	}
	request := fixture.received()[0].Items[0]
	if request.EventProducer != string(audit.ProducerSlurmWatchdog) || request.EventType != string(audit.WatchdogReleaseCompleted) ||
		request.EventProducedAt != 1788888888123456 || request.IdempotencyKey != "release-1" {
		t.Fatalf("watchdog envelope changed: %v", request)
	}
	payload := request.Payload.AsMap()
	if payload["job_id"] != "123" || payload["attempt_index"] != float64(2) || payload["success"] != true {
		t.Fatalf("typed watchdog payload changed: %v", payload)
	}
}

func TestGatewayRetriesTransientFailuresWithoutChangingBatch(t *testing.T) {
	fixture := &auditGatewayFixture{failures: []codes.Code{codes.ResourceExhausted, codes.Unavailable, codes.DeadlineExceeded}}
	destination := NewGateway(startAuditGateway(t, fixture), zap.NewNop())
	var jitterInputs, sleeps []time.Duration
	destination.retryPolicy.randomizeDelay = func(delay time.Duration) time.Duration {
		jitterInputs = append(jitterInputs, delay)
		return delay / 2
	}
	destination.retryPolicy.sleep = func(_ context.Context, delay time.Duration) error {
		sleeps = append(sleeps, delay)
		return nil
	}
	events := []audit.ProducerEvent{{IdempotencyKey: "stable-event", EventProducedAt: 1788888888123456, Payload: map[string]any{"reason": "recovery"}}}
	delivered, err := destination.Deliver(context.Background(), events)
	if err != nil || delivered != 1 {
		t.Fatalf("Deliver() = (%d, %v), want (1, nil)", delivered, err)
	}
	requests := fixture.received()
	if len(requests) != 4 {
		t.Fatalf("attempts = %d, want 4", len(requests))
	}
	for _, request := range requests[1:] {
		if !proto.Equal(request, requests[0]) {
			t.Fatalf("retry changed batch: %v, original %v", request, requests[0])
		}
	}
	if want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}; !reflect.DeepEqual(jitterInputs, want) {
		t.Fatalf("jitter inputs = %v, want %v", jitterInputs, want)
	}
	if want := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}; !reflect.DeepEqual(sleeps, want) {
		t.Fatalf("sleeps = %v, want %v", sleeps, want)
	}
}

func TestGatewayInterruptsRetryBackoffOnContextCancellation(t *testing.T) {
	fixture := &auditGatewayFixture{failures: []codes.Code{codes.Unavailable}}
	destination := NewGateway(startAuditGateway(t, fixture), zap.NewNop())
	// A long, deterministic backoff makes the test fail slowly (rather than
	// pass by accident) if ctx cancellation stops being observed.
	destination.retryPolicy.initialDelay = 3 * time.Second
	destination.retryPolicy.maxDelay = 3 * time.Second
	destination.retryPolicy.randomizeDelay = func(delay time.Duration) time.Duration { return delay }

	// Cancel only once backoff has actually started, so a slow runner can't
	// fire cancellation during the first RPC attempt instead of during the
	// sleepUnlessCanceled call this test means to exercise.
	backoffStarted := make(chan struct{})
	realSleep := destination.retryPolicy.sleep
	destination.retryPolicy.sleep = func(ctx context.Context, delay time.Duration) error {
		close(backoffStarted)
		return realSleep(ctx, delay)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-backoffStarted
		cancel()
	}()

	start := time.Now()
	delivered, err := destination.Deliver(ctx, []audit.ProducerEvent{{IdempotencyKey: "event-1"}})
	elapsed := time.Since(start)

	if delivered != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("Deliver() = (%d, %v), want (0, context.Canceled)", delivered, err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Deliver() took %v to return after context cancellation, want well under the 3s backoff", elapsed)
	}
	if requests := fixture.received(); len(requests) != 1 {
		t.Fatalf("attempts = %d, want 1 (canceled during backoff before a second attempt)", len(requests))
	}
}

func TestGatewayDoesNotRetryPartiallyAcceptedBatch(t *testing.T) {
	fixture := &auditGatewayFixture{response: &pb.CreateAuditEventsResponse{PartialSuccess: &pb.AuditEventPartialSuccess{RejectedEvents: 1, ErrorMessage: "invalid event"}}}
	destination := NewGateway(startAuditGateway(t, fixture), zap.NewNop())
	delivered, err := destination.Deliver(context.Background(), []audit.ProducerEvent{{IdempotencyKey: "event-1"}, {IdempotencyKey: "event-2"}, {IdempotencyKey: "event-3"}})
	if err == nil || delivered != 2 {
		t.Fatalf("Deliver() = (%d, %v), want (2, error)", delivered, err)
	}
	if requests := fixture.received(); len(requests) != 1 {
		t.Fatalf("attempts = %d, want 1 to avoid replaying accepted events", len(requests))
	}
}

func TestGatewayRejectsInvalidAcceptanceResponse(t *testing.T) {
	for _, rejected := range []int32{-1, 0, 2} {
		t.Run(strconv.Itoa(int(rejected)), func(t *testing.T) {
			fixture := &auditGatewayFixture{response: &pb.CreateAuditEventsResponse{PartialSuccess: &pb.AuditEventPartialSuccess{RejectedEvents: rejected}}}
			destination := NewGateway(startAuditGateway(t, fixture), zap.NewNop())
			delivered, err := destination.Deliver(context.Background(), []audit.ProducerEvent{{IdempotencyKey: "event-1"}})
			if err == nil || delivered != 0 || !strings.Contains(err.Error(), "invalid rejected event count") {
				t.Fatalf("Deliver() = (%d, %v), want (0, invalid rejected event count)", delivered, err)
			}
			if requests := fixture.received(); len(requests) != 1 {
				t.Fatalf("attempts = %d, want 1", len(requests))
			}
		})
	}
}

func TestGatewayDoesNotRetryPermanentFailure(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied, codes.NotFound, codes.FailedPrecondition, codes.Canceled} {
		t.Run(code.String(), func(t *testing.T) {
			fixture := &auditGatewayFixture{failures: []codes.Code{code}}
			destination := NewGateway(startAuditGateway(t, fixture), zap.NewNop())
			delivered, err := destination.Deliver(context.Background(), []audit.ProducerEvent{{IdempotencyKey: "event-1"}})
			if delivered != 0 || status.Code(err) != code {
				t.Fatalf("Deliver() = (%d, %v), want (0, %s)", delivered, err, code)
			}
			if requests := fixture.received(); len(requests) != 1 {
				t.Fatalf("attempts = %d, want 1", len(requests))
			}
		})
	}
}

func TestGatewayRetryBudgetResetsForEachDelivery(t *testing.T) {
	fixture := &auditGatewayFixture{failures: []codes.Code{
		codes.Unavailable, codes.Unavailable, codes.Unavailable, codes.Unavailable, codes.ResourceExhausted,
		codes.Unavailable,
	}}
	destination := NewGateway(startAuditGateway(t, fixture), zap.NewNop())
	var delays []time.Duration
	destination.retryPolicy.randomizeDelay = func(delay time.Duration) time.Duration {
		delays = append(delays, delay)
		return 0
	}
	destination.retryPolicy.sleep = func(context.Context, time.Duration) error { return nil }
	events := []audit.ProducerEvent{{IdempotencyKey: "event-1"}}
	delivered, err := destination.Deliver(context.Background(), events)
	if delivered != 0 || status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("first Deliver() = (%d, %v), want (0, last ResourceExhausted error)", delivered, err)
	}
	if requests := fixture.received(); len(requests) != 5 {
		t.Fatalf("exhausted batch attempts = %d, want 5", len(requests))
	}
	delivered, err = destination.Deliver(context.Background(), events)
	if delivered != 1 || err != nil {
		t.Fatalf("second Deliver() = (%d, %v), want (1, nil)", delivered, err)
	}
	if requests := fixture.received(); len(requests) != 7 {
		t.Fatalf("total attempts = %d, want 7", len(requests))
	}
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 100 * time.Millisecond}
	if !reflect.DeepEqual(delays, want) {
		t.Fatalf("backoff caps = %v, want %v", delays, want)
	}
}

func TestGatewayBoundsEveryAttemptWithDeadline(t *testing.T) {
	fixture := &auditGatewayFixture{waitForDeadline: true, canceled: make(chan struct{}, 5)}
	destination := NewGateway(startAuditGateway(t, fixture), zap.NewNop())
	destination.requestTimeout = 250 * time.Millisecond
	destination.retryPolicy.sleep = func(context.Context, time.Duration) error { return nil }
	delivered, err := destination.Deliver(context.Background(), []audit.ProducerEvent{{IdempotencyKey: "event-1"}})
	if delivered != 0 || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("Deliver() = (%d, %v), want (0, DeadlineExceeded)", delivered, err)
	}
	for range 5 {
		select {
		case <-fixture.canceled:
		case <-time.After(5 * time.Second):
			t.Fatal("attempt did not reach server or its context was not canceled")
		}
	}
	if requests := fixture.received(); len(requests) != 5 {
		t.Fatalf("attempts = %d, want 5", len(requests))
	}
}

func TestGatewayRejectsUnencodablePayloadBeforeSendingBatch(t *testing.T) {
	fixture := &auditGatewayFixture{}
	destination := NewGateway(startAuditGateway(t, fixture), zap.NewNop())
	delivered, err := destination.Deliver(context.Background(), []audit.ProducerEvent{
		{IdempotencyKey: "valid-event"},
		{Payload: map[string]any{"invalid": make(chan struct{})}},
	})
	if err == nil || delivered != 0 {
		t.Fatalf("Deliver() = (%d, %v), want (0, encoding error)", delivered, err)
	}
	if requests := fixture.received(); len(requests) != 0 {
		t.Fatalf("attempts = %d, want 0 for unencodable batch", len(requests))
	}
}

// auditGatewayFixture serves the public audit protocol and models transient
// outages, partial acceptance, and a stalled backing service. The mutex protects
// the request history from concurrently canceled server handlers.
type auditGatewayFixture struct {
	pb.UnimplementedAuditServiceServer
	mu              sync.Mutex
	requests        []*pb.CreateAuditEventsRequest
	failures        []codes.Code
	response        *pb.CreateAuditEventsResponse
	waitForDeadline bool
	canceled        chan struct{}
}

func (f *auditGatewayFixture) CreateAuditEvents(ctx context.Context, request *pb.CreateAuditEventsRequest) (*pb.CreateAuditEventsResponse, error) {
	f.mu.Lock()
	f.requests = append(f.requests, request)
	code := codes.OK
	if len(f.failures) > 0 {
		code, f.failures = f.failures[0], f.failures[1:]
	}
	f.mu.Unlock()
	if f.waitForDeadline {
		<-ctx.Done()
		f.canceled <- struct{}{}
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if code != codes.OK {
		return nil, status.Error(code, "audit ingestion unavailable")
	}
	if f.response != nil {
		return f.response, nil
	}
	return &pb.CreateAuditEventsResponse{}, nil
}

func (f *auditGatewayFixture) received() []*pb.CreateAuditEventsRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pb.CreateAuditEventsRequest(nil), f.requests...)
}

func startAuditGateway(t *testing.T, fixture *auditGatewayFixture) pb.AuditServiceClient {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterAuditServiceServer(server, fixture)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	conn, err := grpc.NewClient("passthrough:///audit-gateway",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("create gateway connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewAuditServiceClient(conn)
}
