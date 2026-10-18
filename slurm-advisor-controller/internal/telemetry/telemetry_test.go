// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/loglinks"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/slurm"
)

func TestRecorderPublishesSyncObservation(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewRecorder(reg)

	recorder.ObserveSync(reconcile.SyncObservation{
		Mode:     reconcile.ObserveMode,
		Outcome:  reconcile.ObservationOutcomeSuccess,
		Duration: 250 * time.Millisecond,
		Stages: []reconcile.StageObservation{
			{Stage: reconcile.StageJobStatusPolling, Outcome: reconcile.ObservationOutcomeSuccess, Duration: 50 * time.Millisecond},
			{Stage: reconcile.StageJobReconciliation, Outcome: reconcile.ObservationOutcomeSuccess, Duration: 75 * time.Millisecond},
			{Stage: reconcile.StageFailedJobReconciliation, Outcome: reconcile.ObservationOutcomeSuccess, Duration: 25 * time.Millisecond},
			{Stage: reconcile.StageAdviceReconciliation, Outcome: reconcile.ObservationOutcomeError, Duration: 100 * time.Millisecond},
		},
		Sources: []reconcile.SourceObservation{
			{Source: reconcile.SourceSlurmActive, Outcome: reconcile.ObservationOutcomeSuccess, Items: 2, Duration: 10 * time.Millisecond},
			{Source: reconcile.SourceSlurmLogMetadata, Outcome: reconcile.ObservationOutcomeSuccess, Items: 1, Duration: 15 * time.Millisecond},
			{Source: reconcile.SourceHeimdallJobs, Outcome: reconcile.ObservationOutcomeSuccess, Items: 1, Duration: 20 * time.Millisecond},
			{Source: reconcile.SourceHeimdallAdvice, Outcome: reconcile.ObservationOutcomeSuccess, Items: 3, Duration: 30 * time.Millisecond},
			{Source: reconcile.SourceSeenStore, Outcome: reconcile.ObservationOutcomeSuccess, Items: 1, Duration: 40 * time.Millisecond},
			{Source: reconcile.SourceSlurmAccounting, Outcome: reconcile.ObservationOutcomeSuccess, Items: 1, Duration: 50 * time.Millisecond},
		},
		Snapshot: reconcile.SnapshotObservation{
			ActiveJobs:     2,
			HeimdallJobs:   1,
			AccountingJobs: 1,
			SeenJobs:       1,
			Advice:         3,
		},
		AttemptedAdviceReconciliations: 2,
		JobAttemptCache: reconcile.JobAttemptCacheObservation{
			Observed: true,
			Entries:  4,
		},
		Upserts: []reconcile.JobUpsertObservation{
			{Source: reconcile.JobUpsertSourceSlurmActive, Outcome: reconcile.ObservationOutcomeSuccess, Count: 2},
			{Source: reconcile.JobUpsertSourceSlurmAccounting, Outcome: reconcile.ObservationOutcomeError, Count: 1},
		},
		AdviceSkips: []reconcile.AdviceSkipObservation{
			{Reason: reconcile.AdviceSkipPartitionFiltered, Count: 2},
		},
		AdviceActions: []reconcile.ActionObservation{
			{Action: reconcile.ActionAdviceRelease, Mode: reconcile.ActMode, Outcome: reconcile.ObservationOutcomeSuccess, Count: 1},
		},
		FailedRequeues: []reconcile.OutcomeCountObservation{
			{Mode: reconcile.ObserveMode, Outcome: reconcile.ObservationOutcomePlanned, Count: 1},
			{Mode: reconcile.ActMode, Outcome: reconcile.ObservationOutcomeApplied, Count: 1},
			{Mode: reconcile.ActMode, Outcome: reconcile.ObservationOutcomeSkippedRequeueLimit, Count: 1},
		},
		ModeDecisions: []reconcile.ModeDecisionObservation{
			{
				DefaultMode:   reconcile.ObserveMode,
				RequestedMode: reconcile.ActMode,
				EffectiveMode: reconcile.ObserveMode,
				Reason:        reconcile.ModeReasonActionDenied,
				Count:         1,
			},
		},
		EnvironmentAdmissions: []reconcile.EnvironmentAdmissionObservation{
			{Reason: reconcile.EnvironmentAdmissionMatched, Count: 2},
			{Reason: reconcile.EnvironmentAdmissionMismatch, Count: 1},
		},
		LogLinks: reconcile.LogLinkObservation{
			Completed: true,
			Observation: loglinks.Observation{
				Created:         1,
				Existing:        2,
				Collisions:      3,
				Rejected:        4,
				Errors:          5,
				Removed:         6,
				Owned:           7,
				Scans:           1,
				ScanDirectories: 8,
				ScanEntries:     9,
				ScanMatches:     10,
				ScanDuration:    125 * time.Millisecond,
				CleanupFailures: 11,
			},
		},
	})

	families := gatherFamilies(t, reg)

	assertCounterValue(t, families, "slurm_advisor_controller_reconcile_loops_total", 1)
	assertHistogramCount(t, families, "slurm_advisor_controller_reconcile_duration_seconds", 1)
	assertHistogramCount(t, families, "slurm_advisor_controller_reconcile_stage_duration_seconds", 4)
	assertHistogramBuckets(t, families, "slurm_advisor_controller_reconcile_duration_seconds", reconcileDurationBuckets)
	assertHistogramBuckets(t, families, "slurm_advisor_controller_reconcile_stage_duration_seconds", reconcileDurationBuckets)
	assertMetricFamilyHasNoLabel(t, families, "slurm_advisor_controller_reconcile_stage_duration_seconds", "mode")
	assertCounterValue(t, families, "slurm_advisor_controller_source_reads_total", 6)
	assertHistogramCount(t, families, "slurm_advisor_controller_source_read_duration_seconds", 6)
	assertHistogramBuckets(t, families, "slurm_advisor_controller_source_read_duration_seconds", sourceReadDurationBuckets)
	assertMetricFamilyHasNoLabel(t, families, "slurm_advisor_controller_source_read_duration_seconds", "mode")
	assertCounterValue(t, families, "slurm_advisor_controller_source_items_total", 9)
	assertGaugeValue(t, families, "slurm_advisor_controller_snapshot_jobs", 5)
	assertGaugeValue(t, families, "slurm_advisor_controller_snapshot_advice", 3)
	assertGaugeValue(t, families, "slurm_advisor_controller_advice_reconciliation_attempts", 2)
	assertGaugeValue(t, families, "slurm_advisor_controller_local_job_attempt_cache_entries", 4)
	assertCounterValue(t, families, "slurm_advisor_controller_job_upserts_total", 3)
	assertCounterValue(t, families, "slurm_advisor_controller_advice_skips_total", 2)
	assertCounterValue(t, families, "slurm_advisor_controller_advice_actions_total", 1)
	assertCounterValue(t, families, "slurm_advisor_controller_failed_requeues_total", 3)
	assertCounterValue(t, families, "slurm_advisor_controller_job_mode_decisions_total", 1)
	assertCounterValue(t, families, "slurm_advisor_controller_job_environment_admissions_total", 3)
	assertCounterValue(t, families, "slurm_advisor_controller_log_links_created_total", 1)
	assertCounterValue(t, families, "slurm_advisor_controller_log_links_existing_total", 2)
	assertCounterValue(t, families, "slurm_advisor_controller_log_link_collisions_total", 3)
	assertCounterValue(t, families, "slurm_advisor_controller_log_links_rejected_total", 4)
	assertCounterValue(t, families, "slurm_advisor_controller_log_link_errors_total", 5)
	assertCounterValue(t, families, "slurm_advisor_controller_log_links_removed_total", 6)
	assertGaugeValue(t, families, "slurm_advisor_controller_log_links_owned", 7)
	assertCounterValue(t, families, "slurm_advisor_controller_log_link_scans_total", 1)
	assertCounterValue(t, families, "slurm_advisor_controller_log_link_scan_directories_total", 8)
	assertCounterValue(t, families, "slurm_advisor_controller_log_link_scan_entries_total", 9)
	assertCounterValue(t, families, "slurm_advisor_controller_log_link_scan_matches_total", 10)
	assertHistogramCount(t, families, "slurm_advisor_controller_log_link_scan_duration_seconds", 1)
	assertCounterValue(t, families, "slurm_advisor_controller_log_link_cleanup_failures_total", 11)
	assertMetricFamiliesHaveNoLabels(t, families, []string{
		"slurm_advisor_controller_reconcile_in_progress",
		"slurm_advisor_controller_reconcile_started_timestamp_seconds",
		"slurm_advisor_controller_advice_reconciliation_attempts",
		"slurm_advisor_controller_local_job_attempt_cache_entries",
		"slurm_advisor_controller_log_links_created_total",
		"slurm_advisor_controller_log_links_existing_total",
		"slurm_advisor_controller_log_link_collisions_total",
		"slurm_advisor_controller_log_links_rejected_total",
		"slurm_advisor_controller_log_link_errors_total",
		"slurm_advisor_controller_log_links_removed_total",
		"slurm_advisor_controller_log_links_owned",
		"slurm_advisor_controller_log_link_scans_total",
		"slurm_advisor_controller_log_link_scan_directories_total",
		"slurm_advisor_controller_log_link_scan_entries_total",
		"slurm_advisor_controller_log_link_scan_matches_total",
		"slurm_advisor_controller_log_link_scan_duration_seconds",
		"slurm_advisor_controller_log_link_cleanup_failures_total",
	})
	assertNoHighCardinalityLabels(t, families)
}

func TestRecorderPublishesAndClearsLiveTickProgress(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewRecorder(reg)

	recorder.ObserveSyncStarted(reconcile.ActMode)
	recorder.ObserveStageStarted(reconcile.StageJobReconciliation)

	families := gatherFamilies(t, reg)
	assertGaugeValue(t, families, "slurm_advisor_controller_reconcile_in_progress", 1)
	assertPositiveGaugeValue(t, families, "slurm_advisor_controller_reconcile_started_timestamp_seconds")
	assertGaugeValue(t, families, "slurm_advisor_controller_reconcile_stage_in_progress", 1)
	assertPositiveGaugeValue(t, families, "slurm_advisor_controller_reconcile_stage_started_timestamp_seconds")
	assertNoHighCardinalityLabels(t, families)

	recorder.ObserveSyncCompleted()
	families = gatherFamilies(t, reg)
	assertGaugeValue(t, families, "slurm_advisor_controller_reconcile_in_progress", 0)
	assertGaugeValue(t, families, "slurm_advisor_controller_reconcile_started_timestamp_seconds", 0)
	assertGaugeValue(t, families, "slurm_advisor_controller_reconcile_stage_in_progress", 0)
	assertGaugeValue(t, families, "slurm_advisor_controller_reconcile_stage_started_timestamp_seconds", 0)
}

func TestRecorderPublishesBoundedLogMetadataMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewRecorder(reg)

	recorder.ObserveLogMetadataCacheAccess(slurm.LogMetadataCacheSourceParent, slurm.LogMetadataCacheMiss)
	recorder.ObserveLogMetadataWork(slurm.LogMetadataWorkAdmitted)
	recorder.ObserveLogMetadataCommand(
		slurm.LogMetadataCommandSacct,
		slurm.LogMetadataCommandSuccess,
		25*time.Millisecond,
	)
	recorder.ObserveLogMetadataEviction(slurm.LogMetadataEvictionTTL, 2)
	recorder.ObserveLogMetadataPathsDiscovered(slurm.LogMetadataCacheSourceSteps, 3)
	recorder.ObserveLogMetadataPublished(1, 4)
	recorder.ObserveLogMetadataCache(slurm.LogMetadataCacheObservation{
		PathBytes:       100,
		Attempts:        5,
		PathByteLimit:   1_000,
		AttemptLimit:    50,
		QueueDepth:      6,
		QueueCapacity:   60,
		WorkersInFlight: 7,
		WorkerLimit:     8,
		OldestQueuedAge: 9 * time.Second,
	})

	families := gatherFamilies(t, reg)
	assertCounterValue(t, families, "slurm_advisor_controller_log_metadata_cache_accesses_total", 1)
	assertCounterValue(t, families, "slurm_advisor_controller_log_metadata_work_total", 1)
	assertHistogramCount(t, families, "slurm_advisor_controller_log_metadata_command_duration_seconds", 1)
	assertCounterValue(t, families, "slurm_advisor_controller_log_metadata_evictions_total", 2)
	assertCounterValue(t, families, "slurm_advisor_controller_log_metadata_paths_discovered_total", 3)
	assertCounterValue(t, families, "slurm_advisor_controller_log_metadata_attempts_published_total", 1)
	assertCounterValue(t, families, "slurm_advisor_controller_log_metadata_paths_published_total", 4)
	assertGaugeValue(t, families, "slurm_advisor_controller_log_metadata_cache_path_bytes", 100)
	assertGaugeValue(t, families, "slurm_advisor_controller_log_metadata_cache_attempts", 5)
	assertGaugeValue(t, families, "slurm_advisor_controller_log_metadata_cache_limit", 1_050)
	assertGaugeValue(t, families, "slurm_advisor_controller_log_metadata_queue", 66)
	assertGaugeValue(t, families, "slurm_advisor_controller_log_metadata_workers", 15)
	assertGaugeValue(t, families, "slurm_advisor_controller_log_metadata_oldest_queued_age_seconds", 9)
	assertNoHighCardinalityLabels(t, families)
}

func TestReconcileDurationBucketsCoverProductionTickRange(t *testing.T) {
	want := []float64{
		1.0,
		2.5,
		5.0,
		7.5,
		10.0,
		15.0,
		20.0,
		25.0,
		30.0,
		40.0,
		50.0,
		60.0,
		75.0,
		90.0,
		120.0,
		150.0,
		180.0,
		240.0,
		300.0,
		360.0,
		450.0,
		600.0,
	}
	if !slices.Equal(reconcileDurationBuckets, want) {
		t.Fatalf("reconcile duration buckets = %v, want %v", reconcileDurationBuckets, want)
	}
}

func TestRecorderPreservesLogLinkMetricsWithoutCompletedWork(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewRecorder(reg)

	recorder.ObserveSync(reconcile.SyncObservation{
		Mode:    reconcile.ObserveMode,
		Outcome: reconcile.ObservationOutcomeSuccess,
		LogLinks: reconcile.LogLinkObservation{
			Completed: true,
			Observation: loglinks.Observation{
				Created: 1,
				Owned:   7,
			},
		},
	})
	recorder.ObserveSync(reconcile.SyncObservation{
		Mode:    reconcile.ObserveMode,
		Outcome: reconcile.ObservationOutcomeError,
		Sources: []reconcile.SourceObservation{{
			Source:  reconcile.SourceSlurmActive,
			Outcome: reconcile.ObservationOutcomeError,
		}},
	})

	families := gatherFamilies(t, reg)
	assertCounterValue(t, families, "slurm_advisor_controller_log_links_created_total", 1)
	assertGaugeValue(t, families, "slurm_advisor_controller_log_links_owned", 7)
}

func TestRecorderUpdatesLastTickGaugesToZero(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewRecorder(reg)

	recorder.ObserveSync(reconcile.SyncObservation{
		AttemptedAdviceReconciliations: 3,
		JobAttemptCache: reconcile.JobAttemptCacheObservation{
			Observed: true,
			Entries:  2,
		},
	})
	recorder.ObserveSync(reconcile.SyncObservation{
		JobAttemptCache: reconcile.JobAttemptCacheObservation{Observed: true},
	})

	families := gatherFamilies(t, reg)
	assertGaugeValue(t, families, "slurm_advisor_controller_advice_reconciliation_attempts", 0)
	assertGaugeValue(t, families, "slurm_advisor_controller_local_job_attempt_cache_entries", 0)
}

func TestHandlerServesPrometheusMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewRecorder(reg)
	recorder.ObserveSync(reconcile.SyncObservation{
		Mode:     reconcile.ObserveMode,
		Outcome:  reconcile.ObservationOutcomeSuccess,
		Duration: time.Millisecond,
		Stages: []reconcile.StageObservation{{
			Stage:    reconcile.StageJobStatusPolling,
			Outcome:  reconcile.ObservationOutcomeSuccess,
			Duration: time.Millisecond,
		}},
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	resp := httptest.NewRecorder()

	Handler(reg).ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() failed: %v", err)
	}
	if !contains(string(body), "slurm_advisor_controller_reconcile_loops_total") {
		t.Fatalf("metrics body missing reconcile counter:\n%s", string(body))
	}
	if !contains(string(body), "slurm_advisor_controller_reconcile_stage_duration_seconds") {
		t.Fatalf("metrics body missing reconcile stage duration:\n%s", string(body))
	}
	if !contains(string(body), `stage="job_status_polling"`) {
		t.Fatalf("metrics body missing job-status polling stage label:\n%s", string(body))
	}
	if !contains(string(body), `le="600"`) || !contains(string(body), `le="+Inf"`) {
		t.Fatalf("metrics body missing terminal reconcile duration buckets:\n%s", string(body))
	}
}

func gatherFamilies(t *testing.T, reg *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()

	got, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() failed: %v", err)
	}
	families := make(map[string]*dto.MetricFamily, len(got))
	for _, family := range got {
		families[family.GetName()] = family
	}
	return families
}

func assertCounterValue(t *testing.T, families map[string]*dto.MetricFamily, name string, want float64) {
	t.Helper()

	family := requireFamily(t, families, name)
	var got float64
	for _, metric := range family.GetMetric() {
		got += metric.GetCounter().GetValue()
	}
	if got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func assertGaugeValue(t *testing.T, families map[string]*dto.MetricFamily, name string, want float64) {
	t.Helper()

	family := requireFamily(t, families, name)
	var got float64
	for _, metric := range family.GetMetric() {
		got += metric.GetGauge().GetValue()
	}
	if got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func assertPositiveGaugeValue(t *testing.T, families map[string]*dto.MetricFamily, name string) {
	t.Helper()
	family := families[name]
	if family == nil {
		t.Fatalf("missing gauge metric %q", name)
	}
	for _, metric := range family.GetMetric() {
		if metric.GetGauge().GetValue() > 0 {
			return
		}
	}
	t.Fatalf("gauge %q has no positive value", name)
}

func assertHistogramCount(t *testing.T, families map[string]*dto.MetricFamily, name string, want uint64) {
	t.Helper()

	family := requireFamily(t, families, name)
	var got uint64
	for _, metric := range family.GetMetric() {
		got += metric.GetHistogram().GetSampleCount()
	}
	if got != want {
		t.Fatalf("%s count = %v, want %v", name, got, want)
	}
}

func assertHistogramBuckets(t *testing.T, families map[string]*dto.MetricFamily, name string, want []float64) {
	t.Helper()

	family := requireFamily(t, families, name)
	for _, metric := range family.GetMetric() {
		buckets := metric.GetHistogram().GetBucket()
		got := make([]float64, 0, len(buckets))
		for _, bucket := range buckets {
			got = append(got, bucket.GetUpperBound())
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%s buckets = %v, want %v", name, got, want)
		}
	}
}

func assertMetricFamilyHasNoLabel(t *testing.T, families map[string]*dto.MetricFamily, name, labelName string) {
	t.Helper()

	family := requireFamily(t, families, name)
	for _, metric := range family.GetMetric() {
		for _, label := range metric.GetLabel() {
			if label.GetName() == labelName {
				t.Fatalf("metric %s contains unexpected label %q", name, labelName)
			}
		}
	}
}

func requireFamily(t *testing.T, families map[string]*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()

	family := families[name]
	if family == nil {
		t.Fatalf("metric family %q not found", name)
	}
	return family
}

func assertNoHighCardinalityLabels(t *testing.T, families map[string]*dto.MetricFamily) {
	t.Helper()

	disallowed := map[string]struct{}{
		"job_id":       {},
		"advice_id":    {},
		"workflow_id":  {},
		"user":         {},
		"job_name":     {},
		"node":         {},
		"reason":       {},
		"partition":    {},
		"command_path": {},
		"path":         {},
		"source_path":  {},
		"destination":  {},
		"filename":     {},
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if _, ok := disallowed[label.GetName()]; ok {
					t.Fatalf("metric %s uses disallowed label %q", family.GetName(), label.GetName())
				}
			}
		}
	}
}

func assertMetricFamiliesHaveNoLabels(
	t *testing.T,
	families map[string]*dto.MetricFamily,
	names []string,
) {
	t.Helper()

	for _, name := range names {
		family := requireFamily(t, families, name)
		for _, metric := range family.GetMetric() {
			if labels := metric.GetLabel(); len(labels) != 0 {
				t.Fatalf("metric %s labels = %#v, want none", name, labels)
			}
		}
	}
}

func contains(value, substr string) bool {
	for i := 0; i+len(substr) <= len(value); i++ {
		if value[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestRecorderCommandTimeoutsHaveBoundedLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	recorder := NewRecorder(reg)
	assertCounts := func(want map[string]float64) {
		t.Helper()
		family := requireFamily(t, gatherFamilies(t, reg), "slurm_advisor_controller_slurm_command_timeouts_total")
		if len(family.Metric) != len(want) {
			t.Fatalf("timeout series = %d, want %d", len(family.Metric), len(want))
		}
		for _, metric := range family.Metric {
			if len(metric.Label) != 1 || metric.Label[0].GetName() != "command" {
				t.Fatalf("unexpected timeout labels: %v", metric.Label)
			}
			command := metric.Label[0].GetValue()
			value, known := want[command]
			if !known || metric.GetCounter().GetValue() != value {
				t.Errorf("command %q timeout count = %v, want %v (known=%t)", command, metric.GetCounter().GetValue(), value, known)
			}
		}
	}
	// A zero baseline makes the first observed timeout visible to increase().
	assertCounts(map[string]float64{"squeue": 0, "sacct": 0, "scontrol": 0, "scancel": 0, "other": 0})
	for _, name := range []string{"squeue", "sacct", "scontrol", "scontrol", "scancel", "job-123", "job-456"} {
		recorder.ObserveCommandTimeout(name)
	}
	assertCounts(map[string]float64{"squeue": 1, "sacct": 1, "scontrol": 2, "scancel": 1, "other": 2})
}
