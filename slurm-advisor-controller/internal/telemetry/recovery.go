// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const recoveryInstrumentationName = "nvidia-recovery/slurm-advisor-controller"

// WithoutParent removes unrelated polling trace ancestry.
func (r *Recorder) WithoutParent(ctx context.Context) context.Context {
	return trace.ContextWithSpanContext(ctx, trace.SpanContext{})
}

// RestoreParent validates and restores backend-owned W3C ancestry.
func (r *Recorder) RestoreParent(ctx context.Context, recovery reconcile.RecoveryContext) (context.Context, error) {
	if _, err := uuid.Parse(recovery.SequenceID); err != nil {
		return ctx, errors.Wrap(err, "validate recovery sequence ID")
	}
	if recovery.Operation == "" {
		return ctx, errors.New("recovery operation is empty")
	}
	if recovery.Tracestate != "" {
		if _, err := trace.ParseTraceState(recovery.Tracestate); err != nil {
			return ctx, errors.Wrap(err, "validate recovery W3C tracestate")
		}
	}

	carrier := propagation.MapCarrier{"traceparent": recovery.Traceparent}
	if recovery.Tracestate != "" {
		carrier["tracestate"] = recovery.Tracestate
	}
	extracted := propagation.TraceContext{}.Extract(context.Background(), carrier)
	spanContext := trace.SpanContextFromContext(extracted)
	if !spanContext.IsValid() || !spanContext.IsRemote() {
		return ctx, errors.New("recovery W3C trace context is invalid")
	}

	return trace.ContextWithRemoteSpanContext(ctx, spanContext), nil
}

// StartReconcile starts an Advice operation before application work.
func (r *Recorder) StartReconcile(ctx context.Context, attrs reconcile.ReconcileSpanAttributes) (context.Context, reconcile.RecoverySpan) {
	ctx, span := otel.Tracer(recoveryInstrumentationName).Start(
		ctx,
		"job.recovery.reconcile",
		trace.WithAttributes(recoveryAttributes(attrs.Recovery, attrs.JobID, attrs.TargetAttempt, attrs.AdviceID)...),
	)
	span.SetAttributes(attribute.String("advice.state", attrs.AdviceState))
	span.SetAttributes(attribute.Int64("advice.generation", attrs.Generation), attribute.String("operation.occurrence.id", attrs.OccurrenceID))
	span.SetAttributes(attribute.Bool("recovery.slo.eligible", attrs.SLOEligible))
	return ctx, &recoverySpan{
		span:      span,
		ctx:       ctx,
		recorder:  r,
		operation: attrs.Recovery.Operation,
	}
}

// StartSchedulerAction starts a scheduler operation before its command.
func (r *Recorder) StartSchedulerAction(ctx context.Context, attrs reconcile.SchedulerActionSpanAttributes) (context.Context, reconcile.RecoverySpan) {
	ctx, span := otel.Tracer(recoveryInstrumentationName).Start(
		ctx,
		"job.recovery.scheduler_action",
		trace.WithTimestamp(attrs.StartedAt),
		trace.WithAttributes(recoveryAttributes(attrs.Recovery, attrs.JobID, attrs.TargetAttempt, attrs.AdviceID)...),
	)
	span.SetAttributes(
		attribute.String("scheduler.action", string(attrs.Action)),
		attribute.String("scheduler.mode", string(attrs.Mode)),
		attribute.Int64("advice.generation", attrs.Generation), attribute.String("operation.occurrence.id", attrs.OccurrenceID),
		attribute.Bool("recovery.slo.eligible", attrs.SLOEligible),
	)
	return ctx, &recoverySpan{
		span:      span,
		ctx:       ctx,
		recorder:  r,
		operation: attrs.Recovery.Operation,
		action:    attrs.Action,
	}
}

func recoveryAttributes(recovery reconcile.RecoveryContext, jobID string, targetAttempt int32, adviceID string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("recovery.operation", string(recovery.Operation)),
		attribute.String("scheduler.system", "slurm"),
		attribute.String("job.id", jobID),
		attribute.Int64("job.attempt.source", int64(recovery.SourceAttempt)),
		attribute.Int64("job.attempt.target", int64(targetAttempt)),
	}
	if recovery.SequenceID != "" {
		attrs = append(attrs, attribute.String("recovery.sequence.id", recovery.SequenceID))
	}
	if adviceID != "" {
		attrs = append(attrs, attribute.String("advice.id", adviceID))
	}
	return attrs
}

// ObserveRecoveryContext counts bounded context coverage outcomes.
func (r *Recorder) ObserveRecoveryContext(source, outcome string) {
	if r == nil {
		return
	}
	r.recoveryContexts.WithLabelValues(source, outcome).Inc()
}

// ObserveMilestoneReport counts independent report delivery outcomes.
func (r *Recorder) ObserveMilestoneReport(operation reconcile.RecoveryOperation, action reconcile.SchedulerActionName, result string) {
	if r == nil {
		return
	}
	r.recoveryMilestones.WithLabelValues(string(operation), string(action), result).Inc()
}

type recoverySpan struct {
	ctx       context.Context
	span      trace.Span
	recorder  *Recorder
	operation reconcile.RecoveryOperation
	action    reconcile.SchedulerActionName
}

func (s *recoverySpan) EndReconcile(result reconcile.ReconcileSpanResult) {
	s.span.SetAttributes(
		attribute.String("recovery.outcome", string(result.Outcome)),
		attribute.Bool("recovery.terminal", result.Terminal),
	)
	if result.SkipReason != "" {
		s.span.SetAttributes(attribute.String("advice.skip.reason", string(result.SkipReason)))
	}
	if result.Err != nil {
		s.span.RecordError(result.Err)
		s.span.SetStatus(codes.Error, "recovery reconcile failed")
	}
	if result.CompletedAt.IsZero() {
		s.span.End()
	} else {
		s.span.End(trace.WithTimestamp(result.CompletedAt))
	}
	if s.recorder != nil {
		s.recorder.recoveryReconciles.WithLabelValues(
			string(s.operation),
			string(result.Outcome),
			strconv.FormatBool(result.Terminal),
		).Inc()
	}
}

func (s *recoverySpan) EndSchedulerAction(result reconcile.SchedulerActionSpanResult) {
	s.span.SetAttributes(attribute.String("scheduler.action.outcome", string(result.Outcome)), attribute.Bool("recovery.completes_sequence", result.CompletesSequence))
	if result.Err != nil {
		s.span.RecordError(result.Err)
		s.span.SetStatus(codes.Error, "scheduler action failed")
	}
	if result.CompletedAt.IsZero() {
		s.span.End()
	} else {
		s.span.End(trace.WithTimestamp(result.CompletedAt))
	}
	if s.recorder != nil {
		labels := []string{string(s.operation), string(s.action), string(result.Outcome)}
		s.recorder.recoveryActions.WithLabelValues(labels...).Inc()
		s.recorder.recoveryActionDuration.WithLabelValues(labels...).Observe(result.Duration.Seconds())
	}
}

// Correlation returns IDs only for a sampled span. An unsampled span still has
// valid IDs, but nothing is exported under them, so writing them into audit
// events would link operators to a trace that never exists.
func (s *recoverySpan) Correlation() (string, string) {
	context := s.span.SpanContext()
	if !context.IsValid() || !context.IsSampled() {
		return "", ""
	}
	return context.TraceID().String(), context.SpanID().String()
}

func (s *recoverySpan) StartSchedulerAction(attrs reconcile.SchedulerActionSpanAttributes) reconcile.RecoverySpan {
	_, span := s.recorder.StartSchedulerAction(s.ctx, attrs)
	return span
}

func (s *recoverySpan) SetMode(mode cluster.JobMode) {
	s.span.SetAttributes(attribute.String("scheduler.mode", string(mode)), attribute.Bool("recovery.slo.eligible", s.operation == reconcile.RecoveryOperationRestart && mode == cluster.JobModeAct))
}
