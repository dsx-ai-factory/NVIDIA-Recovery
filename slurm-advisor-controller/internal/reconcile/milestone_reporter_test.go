// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"google.golang.org/protobuf/proto"
)

// A stateful transport simulates response loss after server acceptance: replaying
// an identical occurrence is essential to durable server deduplication.
type milestoneTransport struct {
	mu        sync.Mutex
	requests  []*pb.RecordRecoveryMilestoneRequest
	failFirst bool
	block     bool
	entered   chan struct{}
}

func (c *milestoneTransport) RecordRecoveryMilestone(ctx context.Context, request *pb.RecordRecoveryMilestoneRequest) (*pb.RecordRecoveryMilestoneResponse, error) {
	c.mu.Lock()
	c.requests = append(c.requests, proto.Clone(request).(*pb.RecordRecoveryMilestoneRequest))
	n := len(c.requests)
	c.mu.Unlock()
	if c.entered != nil {
		select {
		case c.entered <- struct{}{}:
		default:
		}
	}
	if c.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if c.failFirst && n == 1 {
		return nil, errors.New("response lost after commit")
	}
	return &pb.RecordRecoveryMilestoneResponse{}, nil
}
func TestMilestoneReporterRetriesImmutableOccurrenceAndPreservesOrder(t *testing.T) {
	client := &milestoneTransport{failFirst: true}
	reporter, stop := StartMilestoneReporter(client, MilestoneReporterOptions{RetryDelay: time.Millisecond})
	completed := time.Date(2026, 10, 8, 12, 0, 0, 123456000, time.UTC)
	first := RecoveryMilestone{Recovery: RecoveryContext{SequenceID: "sequence", Operation: RecoveryOperationRestart}, Action: SchedulerActionObservation{
		Action: SchedulerActionRequeueHold, Outcome: SchedulerActionOutcomeSuccess, CompletedAt: completed, OccurrenceID: "occurrence-one"}}
	reporter.Enqueue(first)
	second := first
	second.Action.Action = SchedulerActionRelease
	second.Action.OccurrenceID = "occurrence-two"
	second.Action.CompletesSequence = true
	reporter.Enqueue(second)
	// Changing the producer's local value cannot change queued command facts.
	first.Action.CompletedAt = completed.Add(time.Hour)
	stop()
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.requests) != 3 {
		t.Fatalf("requests=%d, want retry then release", len(client.requests))
	}
	if !proto.Equal(client.requests[0], client.requests[1]) {
		t.Fatal("retry changed occurrence identity or completion time")
	}
	if !client.requests[0].GetCompletedAt().AsTime().Equal(completed) {
		t.Fatal("delivery time replaced command time")
	}
	if client.requests[2].GetIdempotencyKey() != "occurrence-two" || !client.requests[2].GetCompletesSequence() {
		t.Fatal("release order or explicit completion intent lost")
	}
}
func TestMilestoneReporterBoundsQueueAndShutdown(t *testing.T) {
	client := &milestoneTransport{block: true, entered: make(chan struct{}, 1)}
	telemetry := &milestoneCounts{counts: map[string]int{}}
	reporter, stop := StartMilestoneReporter(client, MilestoneReporterOptions{Capacity: 1, RPCTimeout: time.Minute, ShutdownTimeout: 20 * time.Millisecond, Telemetry: telemetry})
	m := RecoveryMilestone{Recovery: RecoveryContext{Operation: RecoveryOperationRestart}, Action: SchedulerActionObservation{Action: SchedulerActionRelease, CompletedAt: time.Now()}}
	reporter.Enqueue(m)
	select {
	case <-client.entered:
	case <-time.After(time.Second):
		t.Fatal("worker never entered RPC")
	}
	// The in-flight RPC is blocked, but admission stays nonblocking and bounded.
	done := make(chan struct{})
	go func() { reporter.Enqueue(m); reporter.Enqueue(m); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reporting blocked scheduler progress")
	}
	started := time.Now()
	stop()
	if time.Since(started) > time.Second {
		t.Fatal("shutdown did not respect its bound")
	}
	reporter.Enqueue(m)
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	if telemetry.counts["queue_full"] != 1 || telemetry.counts["stopped"] != 1 {
		t.Fatalf("missing independent loss accounting: %v", telemetry.counts)
	}
}

type milestoneCounts struct {
	NopRecoveryTelemetry
	mu     sync.Mutex
	counts map[string]int
}

func (m *milestoneCounts) ObserveMilestoneReport(_ RecoveryOperation, _ SchedulerActionName, result string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[result]++
}
