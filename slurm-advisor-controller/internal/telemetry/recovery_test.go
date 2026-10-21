// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRecoveryRecorderContinuesStoredRemoteParent(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
	})

	recorder := NewRecorder(prometheus.NewRegistry())
	recovery := reconcile.RecoveryContext{
		SequenceID:    "f47ac10b-58cc-4372-a567-0e02b2c3d479",
		Traceparent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		Operation:     reconcile.RecoveryOperationRestart,
		SourceAttempt: 3,
	}
	parent, err := recorder.RestoreParent(context.Background(), recovery)
	if err != nil {
		t.Fatalf("RestoreParent() failed: %v", err)
	}
	spanCtx, span := recorder.StartReconcile(parent, reconcile.ReconcileSpanAttributes{
		Recovery:      recovery,
		JobID:         "42",
		TargetAttempt: 4,
		AdviceID:      "advice-42",
		AdviceState:   "ADVICE_STATE_RESTARTED",
	})
	commandStarted := time.Now().UTC().Truncate(time.Microsecond)
	_, actionSpan := recorder.StartSchedulerAction(spanCtx, reconcile.SchedulerActionSpanAttributes{
		Recovery:      recovery,
		JobID:         "42",
		TargetAttempt: 4,
		AdviceID:      "advice-42",
		Action:        reconcile.SchedulerActionRelease,
		StartedAt:     commandStarted, Generation: 7, OccurrenceID: "command-occurrence",
		SLOEligible: true,
	})
	commandReturned := time.Now().UTC().Truncate(time.Microsecond)
	traceID, spanID := actionSpan.Correlation()
	actionSpan.EndSchedulerAction(reconcile.SchedulerActionSpanResult{
		Outcome:     reconcile.SchedulerActionOutcomeSuccess,
		CompletedAt: commandReturned,
	})
	span.EndReconcile(reconcile.ReconcileSpanResult{
		Outcome:  reconcile.RecoveryOutcomeApplied,
		Terminal: true,
	})

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("exported spans = %d, want reconcile and scheduler action", len(spans))
	}
	reconcileSpan := spans[1]
	action := spans[0]
	if !action.StartTime.Equal(commandStarted) || !action.EndTime.Equal(commandReturned) {
		t.Fatal("export changed captured scheduler timestamps")
	}
	if traceID != action.SpanContext.TraceID().String() || spanID != action.SpanContext.SpanID().String() {
		t.Fatal("audit correlation does not identify actual action span")
	}
	if reconcileSpan.Name != "job.recovery.reconcile" || action.Name != "job.recovery.scheduler_action" {
		t.Fatalf("span names = (%q, %q), want stable recovery names", reconcileSpan.Name, action.Name)
	}
	if got := reconcileSpan.SpanContext.TraceID().String(); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("reconcile trace ID = %q, want stored trace ID", got)
	}
	if got := reconcileSpan.Parent.SpanID().String(); got != "00f067aa0ba902b7" {
		t.Fatalf("reconcile parent span ID = %q, want stored remote parent", got)
	}
	if action.Parent.SpanID() != reconcileSpan.SpanContext.SpanID() {
		t.Fatalf("scheduler action parent = %s, want reconcile span %s", action.Parent.SpanID(), reconcileSpan.SpanContext.SpanID())
	}
	assertSpanAttribute(t, reconcileSpan.Attributes, "recovery.sequence.id", recovery.SequenceID)
	assertSpanAttribute(t, reconcileSpan.Attributes, "recovery.outcome", string(reconcile.RecoveryOutcomeApplied))
	assertSpanAttribute(t, action.Attributes, "scheduler.action", string(reconcile.SchedulerActionRelease))
	assertSpanAttribute(t, action.Attributes, "scheduler.action.outcome", string(reconcile.SchedulerActionOutcomeSuccess))
}

func TestRecoveryRecorderRejectsInvalidStoredContext(t *testing.T) {
	recorder := NewRecorder(prometheus.NewRegistry())
	for name, recovery := range map[string]reconcile.RecoveryContext{
		"traceparent": {
			SequenceID:  "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			Traceparent: "not-a-traceparent",
			Operation:   reconcile.RecoveryOperationRestart,
		},
		"tracestate": {
			SequenceID:  "f47ac10b-58cc-4372-a567-0e02b2c3d479",
			Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			Tracestate:  "invalid tracestate",
			Operation:   reconcile.RecoveryOperationRestart,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := recorder.RestoreParent(context.Background(), recovery); err == nil {
				t.Fatal("RestoreParent() accepted invalid W3C context")
			}
		})
	}
}

func TestRecoveryRecorderStartsCoverageGapSpanWithoutPollingParent(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
	})

	recorder := NewRecorder(prometheus.NewRegistry())
	pollCtx, poll := otel.Tracer("test").Start(context.Background(), "poll")
	_, recoverySpan := recorder.StartReconcile(
		recorder.WithoutParent(pollCtx),
		reconcile.ReconcileSpanAttributes{
			Recovery: reconcile.RecoveryContext{
				Operation:     reconcile.RecoveryOperationRestart,
				SourceAttempt: 2,
			},
			JobID:         "42",
			TargetAttempt: 3,
		},
	)
	recoverySpan.EndReconcile(reconcile.ReconcileSpanResult{
		Outcome:  reconcile.RecoveryOutcomeApplied,
		Terminal: true,
	})
	poll.End()

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("exported spans = %d, want poll and uncorrelated recovery", len(spans))
	}
	recovery := spans[0]
	if recovery.Parent.IsValid() {
		t.Fatalf("coverage-gap recovery parent = %v, want an uncorrelated root", recovery.Parent)
	}
	for _, attr := range recovery.Attributes {
		if string(attr.Key) == "recovery.sequence.id" {
			t.Fatalf("uncorrelated span contains an invented recovery sequence ID: %s", attr.Value.AsString())
		}
	}
}

func TestRecoveryMetricsUseOnlyBoundedLabels(t *testing.T) {
	registry := prometheus.NewRegistry()
	recorder := NewRecorder(registry)
	recorder.ObserveRecoveryContext("advice", "valid")
	recorder.ObserveMilestoneReport(reconcile.RecoveryOperationRestart, reconcile.SchedulerActionRelease, "success")

	_, reconcileSpan := recorder.StartReconcile(context.Background(), reconcile.ReconcileSpanAttributes{
		Recovery: reconcile.RecoveryContext{Operation: reconcile.RecoveryOperationDAGRelease},
	})
	reconcileSpan.EndReconcile(reconcile.ReconcileSpanResult{
		Outcome:    reconcile.RecoveryOutcomeSkipped,
		Terminal:   true,
		SkipReason: reconcile.AdviceSkipAlreadySatisfied,
	})
	_, actionSpan := recorder.StartSchedulerAction(context.Background(), reconcile.SchedulerActionSpanAttributes{
		Recovery: reconcile.RecoveryContext{Operation: reconcile.RecoveryOperationRestart},
		Action:   reconcile.SchedulerActionRelease,
	})
	actionSpan.EndSchedulerAction(reconcile.SchedulerActionSpanResult{Outcome: reconcile.SchedulerActionOutcomePlanned})

	families := gatherFamilies(t, registry)
	assertCounterValue(t, families, "nvidia_recovery_trace_context_total", 1)
	assertCounterValue(t, families, "nvidia_recovery_reconcile_outcomes_total", 1)
	assertCounterValue(t, families, "nvidia_recovery_scheduler_actions_total", 1)
	assertHistogramCount(t, families, "nvidia_recovery_scheduler_action_duration_seconds", 1)
	assertCounterValue(t, families, "nvidia_recovery_milestone_reports_total", 1)
	assertNoHighCardinalityLabels(t, families)
}

func assertSpanAttribute(t *testing.T, attrs []attribute.KeyValue, key, want string) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) == key {
			if got := attr.Value.AsString(); got != want {
				t.Fatalf("span attribute %q = %q, want %q", key, got, want)
			}
			return
		}
	}
	t.Fatalf("span attribute %q not found", key)
}

// Audit events carry the span IDs they return. A recovery whose backend trace
// was sampled out exports nothing, so its audit events must not point at it.
func TestRecoveryCorrelationOmitsSampledOutSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
	})

	recorder := NewRecorder(prometheus.NewRegistry())
	for _, tt := range []struct {
		name        string
		flags       string
		wantTraceID string
	}{
		{"sampled", "01", "4bf92f3577b34da6a3ce929d0e0e4736"},
		{"sampled out", "00", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			exporter.Reset()
			recovery := reconcile.RecoveryContext{
				SequenceID:  "f47ac10b-58cc-4372-a567-0e02b2c3d479",
				Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-" + tt.flags,
				Operation:   reconcile.RecoveryOperationRestart,
			}
			parent, err := recorder.RestoreParent(context.Background(), recovery)
			if err != nil {
				t.Fatalf("RestoreParent() failed: %v", err)
			}
			_, span := recorder.StartReconcile(parent, reconcile.ReconcileSpanAttributes{Recovery: recovery, JobID: "42"})
			action := span.StartSchedulerAction(reconcile.SchedulerActionSpanAttributes{
				Recovery: recovery, JobID: "42", Action: reconcile.SchedulerActionRelease, StartedAt: time.Now(),
			})
			for name, s := range map[string]reconcile.RecoverySpan{"reconcile": span, "scheduler action": action} {
				traceID, spanID := s.Correlation()
				if traceID != tt.wantTraceID || (spanID == "") != (tt.wantTraceID == "") {
					t.Fatalf("%s Correlation() = (%q, %q), want trace ID %q", name, traceID, spanID, tt.wantTraceID)
				}
			}
			action.EndSchedulerAction(reconcile.SchedulerActionSpanResult{Outcome: reconcile.SchedulerActionOutcomeSuccess})
			span.EndReconcile(reconcile.ReconcileSpanResult{Outcome: reconcile.RecoveryOutcomeApplied})
			if exported := len(exporter.GetSpans()) > 0; exported != (tt.wantTraceID != "") {
				t.Fatalf("exported spans = %d, want IDs only when spans are exported", len(exporter.GetSpans()))
			}
		})
	}
}
