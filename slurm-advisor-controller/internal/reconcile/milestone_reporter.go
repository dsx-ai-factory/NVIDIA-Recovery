// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"sync"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RecoveryMilestone is a value snapshot of one returned scheduler command.
// Its occurrence ID and command-return timestamp survive all delivery retries.
type RecoveryMilestone struct {
	// Recovery identifies the backend-owned sequence.
	Recovery RecoveryContext
	// Action contains the captured command result and explicit completion intent.
	Action SchedulerActionObservation
}

// MilestoneReporter accepts immutable command results without blocking execution.
type MilestoneReporter interface {
	// Enqueue attempts local admission; a full or stopped queue drops the report.
	Enqueue(RecoveryMilestone)
}

// MilestoneClient is the command reporting boundary, independent of audit delivery.
type MilestoneClient interface {
	// RecordRecoveryMilestone reports one occurrence without executing a command.
	RecordRecoveryMilestone(context.Context, *pb.RecordRecoveryMilestoneRequest) (*pb.RecordRecoveryMilestoneResponse, error)
}

// MilestoneReporterOptions bounds memory, remote calls, retry, and shutdown work.
type MilestoneReporterOptions struct {
	// Capacity limits queued command occurrences; zero selects 256.
	Capacity int
	// RPCTimeout bounds each remote attempt; zero selects two seconds.
	RPCTimeout time.Duration
	// MaxAttempts includes the first delivery and bounds retries of temporary
	// failures only; zero selects three.
	MaxAttempts int
	// RetryDelay separates attempts; zero selects 100 milliseconds.
	RetryDelay time.Duration
	// ShutdownTimeout bounds draining at process shutdown; zero selects two seconds.
	ShutdownTimeout time.Duration
	// Telemetry receives bounded delivery outcomes independently from audit metrics.
	Telemetry RecoveryTelemetry
	// Logger records final delivery failures at this owning boundary.
	Logger *zap.Logger
}

type asyncMilestoneReporter struct {
	mu      sync.Mutex
	stopped bool
	queue   chan RecoveryMilestone
	done    chan struct{}
	cancel  context.CancelFunc
	client  MilestoneClient
	options MilestoneReporterOptions
}

// StartMilestoneReporter starts one FIFO worker and returns its bounded stop
// callback. The controller owns command execution; this worker owns only remote
// reporting. A single worker preserves admitted order within each sequence.
// Stop closes admission under the same lock as Enqueue, drains accepted work,
// then cancels outstanding RPCs when the shutdown budget expires.
//
// TODO: Accepted milestones live only in memory, so a controller crash or a
// report dropped after its last attempt leaves the backend sequence without a
// completion and its restart latency unmeasured. If that gap appears in
// practice, close it with a durable outbox here or by having Job Manager
// reconstruct completion from the matching advice_scheduler_action_completed
// audit event, which carries the same occurrence ID and command-return
// timestamp. ADR 0048 currently excludes both, so either needs a superseding
// decision; see its 2026-10-21 implementation note.
func StartMilestoneReporter(client MilestoneClient, options MilestoneReporterOptions) (MilestoneReporter, func()) {
	if options.Capacity <= 0 {
		options.Capacity = 256
	}
	if options.RPCTimeout <= 0 {
		options.RPCTimeout = 2 * time.Second
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = 3
	}
	if options.RetryDelay <= 0 {
		options.RetryDelay = 100 * time.Millisecond
	}
	if options.ShutdownTimeout <= 0 {
		options.ShutdownTimeout = 2 * time.Second
	}
	if options.Telemetry == nil {
		options.Telemetry = NopRecoveryTelemetry{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	reporter := &asyncMilestoneReporter{
		queue: make(chan RecoveryMilestone, options.Capacity), done: make(chan struct{}),
		cancel: cancel, client: client, options: options,
	}
	go reporter.run(ctx)
	return reporter, reporter.stop
}

func (r *asyncMilestoneReporter) Enqueue(milestone RecoveryMilestone) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		r.observe(milestone, "stopped")
		return
	}
	select {
	case r.queue <- milestone:
	default:
		r.observe(milestone, "queue_full")
	}
}

func (r *asyncMilestoneReporter) stop() {
	r.mu.Lock()
	if !r.stopped {
		r.stopped = true
		close(r.queue)
	}
	r.mu.Unlock()
	timer := time.NewTimer(r.options.ShutdownTimeout)
	defer timer.Stop()
	defer r.cancel()
	select {
	case <-r.done:
	case <-timer.C:
		r.cancel()
		// RPC implementations must honor context cancellation. Shutdown itself must
		// remain bounded even when an external client violates that contract.
	}
}

func (r *asyncMilestoneReporter) run(ctx context.Context) {
	defer close(r.done)
	for milestone := range r.queue {
		if ctx.Err() != nil {
			r.observe(milestone, "shutdown_drop")
			continue
		}
		r.deliver(ctx, milestone)
	}
}

func (r *asyncMilestoneReporter) observe(milestone RecoveryMilestone, outcome string) {
	r.options.Telemetry.ObserveMilestoneReport(milestone.Recovery.Operation, milestone.Action.Action, outcome)
}

func (r *asyncMilestoneReporter) deliver(ctx context.Context, milestone RecoveryMilestone) {
	if r.client == nil {
		r.observe(milestone, "unavailable")
		return
	}
	completes := milestone.Action.CompletesSequence
	request := &pb.RecordRecoveryMilestoneRequest{
		SequenceId: milestone.Recovery.SequenceID, IdempotencyKey: milestone.Action.OccurrenceID,
		Operation:         recoveryOperationToProto(milestone.Recovery.Operation),
		Action:            schedulerActionToProto(milestone.Action.Action),
		Outcome:           schedulerActionOutcomeToProto(milestone.Action.Outcome),
		CompletesSequence: &completes, CompletedAt: timestamppb.New(milestone.Action.CompletedAt),
	}
	var lastErr error
	for attempt := 0; attempt < r.options.MaxAttempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(r.options.RetryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				r.observe(milestone, "shutdown_drop")
				return
			case <-timer.C:
			}
			r.observe(milestone, "retry")
		}
		rpcCtx, cancel := context.WithTimeout(ctx, r.options.RPCTimeout)
		_, lastErr = r.client.RecordRecoveryMilestone(rpcCtx, request)
		cancel()
		if lastErr == nil {
			r.observe(milestone, "success")
			return
		}
		if ctx.Err() != nil {
			r.observe(milestone, "shutdown_drop")
			return
		}
		if !temporaryMilestoneError(lastErr) {
			// The same request would be rejected again, and retrying it would
			// delay every report queued behind it on this single worker.
			r.observe(milestone, "rejected")
			r.logDeliveryFailure(milestone, lastErr)
			return
		}
	}
	r.observe(milestone, "error")
	r.logDeliveryFailure(milestone, lastErr)
}

func (r *asyncMilestoneReporter) logDeliveryFailure(milestone RecoveryMilestone, err error) {
	if r.options.Logger != nil {
		r.options.Logger.Warn("report recovery scheduler action milestone", zap.String("recovery_sequence_id", milestone.Recovery.SequenceID), zap.String("scheduler_action", string(milestone.Action.Action)), zap.Error(err))
	}
}

// temporaryMilestoneError reports whether a later identical request may
// succeed. Reports are idempotent by occurrence ID, so retrying a request that
// the backend actually recorded is safe. Unknown is temporary because the
// gateway forwards unexpected Job Manager failures, such as a database error,
// without a mapped status. InvalidArgument, NotFound, Internal, and
// authentication failures describe the request or deployment and do not
// change between attempts.
func temporaryMilestoneError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Unknown:
		return true
	default:
		return false
	}
}
