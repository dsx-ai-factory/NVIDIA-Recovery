// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"strings"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

type OperationMode = cluster.JobMode

const (
	IgnoreMode  OperationMode = cluster.JobModeIgnore
	ObserveMode OperationMode = cluster.JobModeObserve
	ActMode     OperationMode = cluster.JobModeAct
)

type ObservationOutcome string

const (
	ObservationOutcomeSuccess ObservationOutcome = "success"
	ObservationOutcomeError   ObservationOutcome = "error"
	ObservationOutcomePlanned ObservationOutcome = "planned"
	ObservationOutcomeApplied ObservationOutcome = "applied"
	// ObservationOutcomeSkippedRecoveryMode records a failed-job recovery that
	// APS policy makes ineligible because auto-resume is not set to requeue.
	ObservationOutcomeSkippedRecoveryMode ObservationOutcome = "skipped_recovery_mode"
	// ObservationOutcomeSkippedRequeueLimit records a failed-job recovery that
	// APS policy makes ineligible because the job has exhausted its requeues.
	ObservationOutcomeSkippedRequeueLimit ObservationOutcome = "skipped_requeue_limit"
	// ObservationOutcomeSkippedDisabled records a deliberate skip when action
	// mode is active but the specific scheduler action is not enabled.
	ObservationOutcomeSkippedDisabled ObservationOutcome = "skipped_disabled"
)

type ObservationSource string

const (
	SourceSlurmActive      ObservationSource = "slurm_active"
	SourceSlurmAccounting  ObservationSource = "slurm_accounting"
	SourceSlurmLogMetadata ObservationSource = "slurm_log_metadata"
	SourceHeimdallJobs     ObservationSource = "heimdall_jobs"
	SourceHeimdallAdvice   ObservationSource = "heimdall_advice"
	SourceSeenStore        ObservationSource = "seen_store"
)

// ReconcileStage identifies one bounded phase within a controller tick.
type ReconcileStage string

const (
	StageJobStatusPolling        ReconcileStage = "job_status_polling"
	StageJobReconciliation       ReconcileStage = "job_reconciliation"
	StageFailedJobReconciliation ReconcileStage = "failed_job_reconciliation"
	StageAdviceReconciliation    ReconcileStage = "advice_reconciliation"
)

// JobUpsertSource records which snapshot source produced a Heimdall job upsert.
// Values intentionally match ObservationSource labels used by other metrics.
type JobUpsertSource string

const (
	JobUpsertSourceSlurmActive     JobUpsertSource = "slurm_active"
	JobUpsertSourceSlurmAccounting JobUpsertSource = "slurm_accounting"
)

type SchedulerAction string

const (
	ActionAdviceRequeueHold SchedulerAction = "advice_requeuehold"
	ActionAdviceRelease     SchedulerAction = "advice_release"
	ActionAdviceKill        SchedulerAction = "advice_kill"
	ActionAdviceNodeUpdate  SchedulerAction = "advice_node_update"
)

// Observer receives one SyncObservation after each reconciliation loop.
type Observer interface {
	ObserveSync(SyncObservation)
}

// ProgressObserver optionally exposes a tick before its completion summary is
// available. Implementations use this to publish bounded live-stage metrics;
// controller behavior does not depend on these callbacks.
type ProgressObserver interface {
	ObserveSyncStarted(OperationMode)
	ObserveStageStarted(ReconcileStage)
	ObserveSyncCompleted()
}

// NopObserver is used when reconciliation observations are optional, such as
// when Prometheus metrics are disabled. It avoids nil checks at emit sites.
type NopObserver struct{}

func (NopObserver) ObserveSync(SyncObservation) {}

// SyncObservation is the telemetry summary emitted once per SyncOnce call.
type SyncObservation struct {
	Mode                           OperationMode
	Outcome                        ObservationOutcome
	Duration                       time.Duration
	Stages                         []StageObservation
	Sources                        []SourceObservation
	Snapshot                       SnapshotObservation
	Upserts                        []JobUpsertObservation
	AttemptedAdviceReconciliations int
	AdviceSkips                    []AdviceSkipObservation
	AdviceActions                  []ActionObservation
	AdviceStatusPublications       []AdviceStatusPublicationObservation
	FailedRequeues                 []OutcomeCountObservation
	ModeDecisions                  []ModeDecisionObservation
	EnvironmentAdmissions          []EnvironmentAdmissionObservation
	JobAttemptCache                JobAttemptCacheObservation
	LogLinks                       LogLinkObservation
}

type StageObservation struct {
	Stage    ReconcileStage
	Outcome  ObservationOutcome
	Duration time.Duration
}

type SourceObservation struct {
	Source   ObservationSource
	Outcome  ObservationOutcome
	Items    int
	Duration time.Duration
}

type SnapshotObservation struct {
	ActiveJobs     int
	HeimdallJobs   int
	AccountingJobs int
	SeenJobs       int
	Advice         int
}

type JobUpsertObservation struct {
	Source  JobUpsertSource
	Outcome ObservationOutcome
	Count   int
}

type AdviceSkipObservation struct {
	Reason AdviceSkipReason
	Count  int
}

type ActionObservation struct {
	Action  SchedulerAction
	Mode    OperationMode
	Outcome ObservationOutcome
	Count   int
}

// AdviceStatusPublicationObservation records bounded lifecycle labels and the
// result of publishing one terminal Advice observation.
type AdviceStatusPublicationObservation struct {
	Disposition string
	Reason      string
	Outcome     ObservationOutcome
	Count       int
}

// OutcomeCountObservation records an aggregate count for decisions that only
// need an outcome label.
type OutcomeCountObservation struct {
	Mode    OperationMode
	Outcome ObservationOutcome
	Count   int
}

type ModeDecisionObservation struct {
	DefaultMode   OperationMode
	RequestedMode OperationMode
	EffectiveMode OperationMode
	Reason        ModeDecisionReason
	Count         int
}

type EnvironmentAdmissionObservation struct {
	Reason EnvironmentAdmissionReason
	Count  int
}

type JobAttemptCacheObservation struct {
	Observed bool
	Entries  int
}

func observeSource(
	obs *SyncObservation,
	source ObservationSource,
	err error,
	items int,
	duration time.Duration,
) {
	if obs == nil {
		return
	}
	outcome := ObservationOutcomeSuccess
	if err != nil {
		outcome = ObservationOutcomeError
		items = 0
	}
	obs.Sources = append(obs.Sources, SourceObservation{
		Source:   source,
		Outcome:  outcome,
		Items:    items,
		Duration: duration,
	})
}

func observeStage(obs *SyncObservation, stage ReconcileStage, err error, duration time.Duration) {
	if obs == nil {
		return
	}
	outcome := ObservationOutcomeSuccess
	if err != nil {
		outcome = ObservationOutcomeError
	}
	obs.Stages = append(obs.Stages, StageObservation{
		Stage:    stage,
		Outcome:  outcome,
		Duration: duration,
	})
}

func observeSnapshot(obs *SyncObservation, snapshot ControllerSnapshot) {
	if obs == nil {
		return
	}
	obs.Snapshot = SnapshotObservation{
		ActiveJobs:     len(snapshot.ActiveJobs),
		HeimdallJobs:   len(snapshot.HeimdallJobs),
		AccountingJobs: len(snapshot.AccountingJobs),
		SeenJobs:       snapshot.SeenJobCount,
		Advice:         snapshotAdviceCount(snapshot),
	}
}

func observeJobUpsert(
	obs *SyncObservation,
	source JobUpsertSource,
	outcome ObservationOutcome,
) {
	if obs == nil {
		return
	}
	obs.Upserts = append(obs.Upserts, JobUpsertObservation{
		Source:  source,
		Outcome: outcome,
		Count:   1,
	})
}

func observeAdviceSkip(obs *SyncObservation, reason AdviceSkipReason) {
	if obs == nil {
		return
	}
	obs.AdviceSkips = append(obs.AdviceSkips, AdviceSkipObservation{
		Reason: reason,
		Count:  1,
	})
}

func observeAdviceReconciliationAttempt(obs *SyncObservation) {
	if obs == nil {
		return
	}
	obs.AttemptedAdviceReconciliations++
}

func observeJobAttemptCache(obs *SyncObservation, entries int) {
	if obs == nil {
		return
	}
	obs.JobAttemptCache = JobAttemptCacheObservation{
		Observed: true,
		Entries:  entries,
	}
}

func observeAdviceAction(obs *SyncObservation, action SchedulerAction, mode OperationMode, outcome ObservationOutcome) {
	if obs == nil {
		return
	}
	obs.AdviceActions = append(obs.AdviceActions, ActionObservation{
		Action:  action,
		Mode:    mode,
		Outcome: outcome,
		Count:   1,
	})
}

func observeAdviceStatusPublication(
	obs *SyncObservation,
	disposition pb.AdviceReconciliationDisposition,
	reason pb.AdviceReconciliationReason,
	outcome ObservationOutcome,
) {
	if obs == nil {
		return
	}
	obs.AdviceStatusPublications = append(obs.AdviceStatusPublications, AdviceStatusPublicationObservation{
		Disposition: strings.TrimPrefix(disposition.String(), "ADVICE_RECONCILIATION_DISPOSITION_"),
		Reason:      strings.TrimPrefix(reason.String(), "ADVICE_RECONCILIATION_REASON_"),
		Outcome:     outcome,
		Count:       1,
	})
}

func observeFailedRequeue(obs *SyncObservation, mode OperationMode, outcome ObservationOutcome) {
	if obs == nil {
		return
	}
	obs.FailedRequeues = append(obs.FailedRequeues, OutcomeCountObservation{
		Mode:    mode,
		Outcome: outcome,
		Count:   1,
	})
}

func observeModeDecision(obs *SyncObservation, defaultMode OperationMode, decision EffectiveModeDecision) {
	if obs == nil {
		return
	}
	obs.ModeDecisions = append(obs.ModeDecisions, ModeDecisionObservation{
		DefaultMode:   defaultMode,
		RequestedMode: decision.RequestedMode,
		EffectiveMode: decision.Mode,
		Reason:        decision.Reason,
		Count:         1,
	})
}

func observeEnvironmentAdmissions(obs *SyncObservation, snapshot ControllerSnapshot) {
	if obs == nil {
		return
	}
	counts := map[EnvironmentAdmissionReason]int{}
	for _, decision := range snapshot.EnvironmentAdmissions {
		counts[decision.Reason]++
	}
	for _, reason := range []EnvironmentAdmissionReason{
		EnvironmentAdmissionMatched,
		EnvironmentAdmissionControllerUnspecified,
		EnvironmentAdmissionProductionDefault,
		EnvironmentAdmissionMismatch,
		EnvironmentAdmissionNotOnboarded,
		EnvironmentAdmissionInvalid,
	} {
		if count := counts[reason]; count > 0 {
			obs.EnvironmentAdmissions = append(obs.EnvironmentAdmissions, EnvironmentAdmissionObservation{Reason: reason, Count: count})
		}
	}
}
