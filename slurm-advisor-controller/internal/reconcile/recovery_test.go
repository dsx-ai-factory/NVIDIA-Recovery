// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/google/uuid"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

// These cases prove that scheduler policy, audit coverage, and command completion
// stay independent from availability and validity of recovery context.
func TestAdviceCommandSharesCapturedResultAcrossProjections(t *testing.T) {
	for _, test := range []struct {
		name           string
		mode           cluster.JobMode
		held           bool
		commandErr     error
		contextMissing bool
		contextInvalid bool
	}{
		{name: "successful release", mode: cluster.JobModeAct, held: true},
		{name: "failed release", mode: cluster.JobModeAct, held: true, commandErr: errors.New("release failed")},
		{name: "missing context", mode: cluster.JobModeAct, held: true, contextMissing: true},
		{name: "invalid context", mode: cluster.JobModeAct, held: true, contextInvalid: true},
		{name: "planned", mode: cluster.JobModeObserve, held: true},
		{name: "already satisfied", mode: cluster.JobModeAct, held: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			capturedAt := time.Date(2026, 10, 8, 12, 0, 0, 123456789, time.UTC)
			telemetry := &capturingRecoveryTelemetry{}
			audit := &recordingAuditObserver{}
			observedNow := capturedAt
			// Simulate time spent after command return: later projection work must
			// never rewrite the shared command completion timestamp.
			reports := &capturingMilestoneReporter{onEnqueue: func() { observedNow = observedNow.Add(2 * time.Second) }}
			executor := newMockJobMutator(t)
			executionCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if test.mode == cluster.JobModeAct && test.held {
				executor.EXPECT().JobRelease(gomock.Any(), "200").DoAndReturn(func(ctx context.Context, _ string) error {
					if ctx != executionCtx {
						t.Error("instrumentation replaced scheduler execution context")
					}
					if telemetry.startedActions != 1 {
						t.Error("action span did not start before command")
					}
					return test.commandErr
				})
			}
			controller := Controller{ActionExecutor: executor, RecoveryTelemetry: telemetry, AuditObserver: audit,
				MilestoneReporter: reports, Now: func() time.Time { return observedNow }}
			advice := newAdvice(t, adviceFixture{adviceID: "release-200", jobID: "200", state: adviceState(pb.AdviceState_ADVICE_STATE_RELEASED)})
			advice.Generation = 7
			if !test.contextMissing {
				advice.RecoveryContext = &pb.RecoveryContext{SequenceId: "123e4567-e89b-12d3-a456-426614174000", Traceparent: testTraceparent,
					TriggeredAt: timestamppb.New(capturedAt.Add(-time.Second)), Operation: pb.RecoveryOperation_RECOVERY_OPERATION_DAG_RELEASE}
			}
			telemetry.rejectParent = test.contextInvalid
			result, err := reconcileAdviceFixture(&controller, executionCtx, advice, cluster.JobExecutionRecord{JobID: "200", State: cluster.JobStatePending, Held: test.held}, test.mode, nil)
			if !errors.Is(err, test.commandErr) {
				t.Fatalf("reconcile error=%v, want %v", err, test.commandErr)
			}
			if test.mode != cluster.JobModeAct || !test.held {
				if len(audit.adviceActions) != 0 || len(reports.items) != 0 {
					t.Fatal("no command must mean no completion reports")
				}
				if !test.held && len(result.TerminalOutcomes) != 1 {
					t.Fatal("fresh observed convergence must retain generation status")
				}
				return
			}
			if len(audit.adviceActions) != 1 {
				t.Fatalf("command audit count=%d", len(audit.adviceActions))
			}
			action := audit.adviceActions[0]
			if id, err := uuid.Parse(action.OccurrenceID); err != nil || id.Version() != 4 {
				t.Fatalf("occurrence is not UUIDv4: %s", action.OccurrenceID)
			}
			if action.CompletedAt.Nanosecond()%1000 != 0 || !action.CompletedAt.Equal(capturedAt.Truncate(time.Microsecond)) {
				t.Fatalf("command completion=%v", action.CompletedAt)
			}
			if action.CompletesSequence != (test.commandErr == nil) {
				t.Fatal("completion intent must follow command success")
			}
			if telemetry.actionResult.CompletedAt != action.CompletedAt || telemetry.actionResult.Outcome != action.Outcome {
				t.Fatal("trace and audit must receive identical command result")
			}
			application := audit.advice[0]
			if application.Generation != 7 || application.OccurrenceID == action.OccurrenceID {
				t.Fatal("application needs historical generation and independent occurrence")
			}
			if test.contextMissing || test.contextInvalid {
				if len(reports.items) != 0 {
					t.Fatal("uncorrelated Advice must not report a milestone")
				}
			} else {
				if len(reports.items) != 1 {
					t.Fatalf("milestone count=%d", len(reports.items))
				}
				if reports.items[0].Action.OccurrenceID != action.OccurrenceID || reports.items[0].Action.CompletedAt != action.CompletedAt {
					t.Fatal("milestone and audit must identify the same captured occurrence")
				}
			}
		})
	}
}

func TestReconcileClassificationUsesGenerationOutcomes(t *testing.T) {
	// Command success alone does not converge Advice. Only the existing state
	// classifier's generation-scoped result may mark the observation terminal.
	applied := classifyReconcileResult(AdviceSyncResult{}, nil, cluster.JobModeAct)
	if applied.Terminal {
		t.Fatal("command application was mistaken for Advice convergence")
	}
	terminal := AdviceSyncResult{Skips: []AdviceSkip{{Reason: AdviceSkipAlreadySatisfied}}, TerminalOutcomes: []AdviceTerminalOutcome{{Generation: 4}}}
	if !classifyReconcileResult(terminal, nil, cluster.JobModeAct).Terminal {
		t.Fatal("generation convergence was lost")
	}
}

type capturingMilestoneReporter struct {
	items     []RecoveryMilestone
	onEnqueue func()
}

func (r *capturingMilestoneReporter) Enqueue(m RecoveryMilestone) {
	r.items = append(r.items, m)
	if r.onEnqueue != nil {
		r.onEnqueue()
	}
}

type capturingRecoveryTelemetry struct {
	NopRecoveryTelemetry
	rejectParent      bool
	startedReconciles int
	reconcileResults  []ReconcileSpanResult
	startedActions    int
	actionResult      SchedulerActionSpanResult
}

func (r *capturingRecoveryTelemetry) RestoreParent(ctx context.Context, _ RecoveryContext) (context.Context, error) {
	if r.rejectParent {
		return ctx, errors.New("invalid context")
	}
	return ctx, nil
}
func (r *capturingRecoveryTelemetry) StartReconcile(context.Context, ReconcileSpanAttributes) (context.Context, RecoverySpan) {
	r.startedReconciles++
	// A canceled instrumentation context must never cancel scheduler execution.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx, &capturingRecoverySpan{owner: r}
}
func (r *capturingRecoveryTelemetry) StartSchedulerAction(ctx context.Context, _ SchedulerActionSpanAttributes) (context.Context, RecoverySpan) {
	r.startedActions++
	return ctx, &capturingRecoverySpan{owner: r}
}

type capturingRecoverySpan struct {
	nopRecoverySpan
	owner *capturingRecoveryTelemetry
}

func (s *capturingRecoverySpan) EndSchedulerAction(result SchedulerActionSpanResult) {
	s.owner.actionResult = result
}

func (s *capturingRecoverySpan) StartSchedulerAction(attrs SchedulerActionSpanAttributes) RecoverySpan {
	_, span := s.owner.StartSchedulerAction(context.Background(), attrs)
	return span
}

// Policy skips are real per-item work: observing them must not create command
// completion, and expiration uses the same durable generation classification.
func TestAdviceAdmissionLifecyclePreservesTerminalClassification(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name     string
		reason   AdviceSkipReason
		terminal bool
	}{
		{"ownership", AdviceSkipNotOwned, false},
		{"expiration", AdviceSkipStaleAdvice, true},
		{"missing job", AdviceSkipMissingSnapshotJob, false},
		{"partition", AdviceSkipPartitionFiltered, false},
		{"ignored", AdviceSkipIgnoredJob, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			telemetry := &capturingRecoveryTelemetry{}
			controller := Controller{HeimdallClient: &fakeControllerClient{}, RecoveryTelemetry: telemetry, Now: func() time.Time { return now }}
			advice := newAdvice(t, adviceFixture{adviceID: "restart-42", jobID: "42", state: adviceState(pb.AdviceState_ADVICE_STATE_RESTARTED), metadata: map[string]any{"scheduler": cluster.SchedulerSlurm}})
			advice.Generation = 9
			snapshot := newControllerSnapshot(now)
			job := cluster.JobExecutionRecord{JobID: "42", State: cluster.JobStateFailed, Partition: "compute"}
			jobs := map[string]cluster.JobExecutionRecord{"42": job}
			snapshot.EffectiveModes[jobAttemptRef(job)] = EffectiveModeDecision{Mode: cluster.JobModeAct}
			switch test.name {
			case "ownership":
				advice.SchedulerMetadata = nil
			case "expiration":
				controller.Options.AdviceMaxAge = time.Minute
				advice.CreatedAt = timestamppb.New(now.Add(-time.Hour))
			case "missing job":
				jobs = nil
			case "partition":
				controller.Options.PartitionAllowlist = []string{"other"}
			case "ignored":
				snapshot.EffectiveModes[jobAttemptRef(job)] = EffectiveModeDecision{Mode: cluster.JobModeIgnore}
			}
			result, err := controller.reconcileAdviceItem(context.Background(), snapshot, jobs, advice, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Skips) != 1 || result.Skips[0].Reason != test.reason {
				t.Fatalf("wrong policy result: %#v", result)
			}
			if telemetry.startedReconciles != 1 || len(telemetry.reconcileResults) != 1 || telemetry.startedActions != 0 {
				t.Fatal("policy work must produce one real application lifecycle and no commands")
			}
			observed := telemetry.reconcileResults[0]
			if observed.Terminal != test.terminal || observed.SkipReason != test.reason || observed.Outcome != RecoveryOutcomeSkipped {
				t.Fatalf("span changed existing policy classification: %#v", observed)
			}
		})
	}
}

func (s *capturingRecoverySpan) EndReconcile(result ReconcileSpanResult) {
	s.owner.reconcileResults = append(s.owner.reconcileResults, result)
}
