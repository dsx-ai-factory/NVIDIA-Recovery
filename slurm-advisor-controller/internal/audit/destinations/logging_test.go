// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package destinations

import (
	"context"
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestLogging(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	destination := NewLogging(zap.New(core))
	events := []audit.ProducerEvent{
		{
			HeimdallJobID:   "job-1",
			EventProducer:   "slurm-advisor-controller",
			EventType:       "job-requeued",
			EventLevel:      audit.LevelOperational,
			Payload:         map[string]any{"private": "omitted"},
			EventProducedAt: 1234,
			IdempotencyKey:  "event-1",
		},
		{IdempotencyKey: "event-2"},
	}

	delivered, err := destination.Deliver(context.Background(), events)
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if delivered != len(events) {
		t.Fatalf("Deliver() delivered = %d, want %d", delivered, len(events))
	}

	entries := observed.All()
	if len(entries) != len(events) {
		t.Fatalf("log entry count = %d, want %d", len(entries), len(events))
	}
	if entries[0].Message != "sent audit event" {
		t.Fatalf("log message = %q, want %q", entries[0].Message, "sent audit event")
	}
	fields := entries[0].ContextMap()
	for name, want := range map[string]any{
		"heimdall_job_id":   "job-1",
		"event_producer":    "slurm-advisor-controller",
		"event_type":        "job-requeued",
		"event_level":       "operational",
		"event_produced_at": int64(1234),
		"idempotency_key":   "event-1",
	} {
		if got := fields[name]; got != want {
			t.Errorf("log field %s = %#v, want %#v", name, got, want)
		}
	}
	if _, ok := fields["payload"]; ok {
		t.Error("log entry unexpectedly contains payload")
	}
}

func TestLoggingStopsOnCanceledContext(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	destination := NewLogging(zap.New(core))
	events := []audit.ProducerEvent{
		{IdempotencyKey: "event-1"},
		{IdempotencyKey: "event-2"},
		{IdempotencyKey: "event-3"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	delivered, err := destination.Deliver(ctx, events)
	if delivered != 0 || err != context.Canceled {
		t.Fatalf("Deliver() = (%d, %v), want (0, context.Canceled)", delivered, err)
	}
	if got := observed.Len(); got != 0 {
		t.Fatalf("logged entries = %d, want 0 for an already-canceled context", got)
	}
}
