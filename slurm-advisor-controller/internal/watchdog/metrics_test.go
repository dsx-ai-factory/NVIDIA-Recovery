// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package watchdog

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	admission "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/environment"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestWatchdogMetricsRecordsBoundedPollAndReleaseOutcomes(t *testing.T) {
	registry := NewMetricsRegistry()
	metrics := NewMetrics(registry)
	metrics.ObservePoll(ModeAct, SyncResult{
		ObservedJobs: 4,
		ManagedJobs:  3,
		TrackedJobs:  2,
		EnvironmentAdmissions: map[admission.AdmissionReason]int{
			admission.AdmissionMatched:  3,
			admission.AdmissionMismatch: 1,
		},
		Releases: []Release{
			{JobID: "customer-job-100", Outcome: ReleaseApplied},
			{JobID: "customer-job-101", Outcome: ReleaseError},
		},
	}, errors.New("poll failed"), 250*time.Millisecond)

	families := gatherMetricFamilies(t, registry)
	assertMetricCount(t, families, "slurm_watchdog_polls_total", 1)
	assertMetricCount(t, families, "slurm_watchdog_release_candidates_total", 2)
	assertMetricCount(t, families, "slurm_watchdog_environment_admissions_total", 4)
	assertMetricGauge(t, families, "slurm_watchdog_observed_jobs", 4)
	assertMetricGauge(t, families, "slurm_watchdog_managed_jobs", 3)
	assertMetricGauge(t, families, "slurm_watchdog_tracked_jobs", 2)

	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "job_id" || label.GetValue() == "customer-job-100" || label.GetValue() == "customer-job-101" {
					t.Fatalf("metric %s contains high-cardinality job label %s=%s", family.GetName(), label.GetName(), label.GetValue())
				}
			}
		}
	}
}

func TestWatchdogMetricsHandlerServesCustomRegistry(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := NewMetrics(registry)
	metrics.ObservePoll(ModeObserve, SyncResult{}, nil, time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	resp := httptest.NewRecorder()
	MetricsHandler(registry).ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() failed: %v", err)
	}
	if !containsText(string(body), "slurm_watchdog_polls_total") {
		t.Fatalf("metrics body missing watchdog poll counter:\n%s", body)
	}
}

func gatherMetricFamilies(t *testing.T, registry *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather() failed: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		out[family.GetName()] = family
	}
	return out
}

func assertMetricCount(t *testing.T, families map[string]*dto.MetricFamily, name string, want float64) {
	t.Helper()
	family := requireMetricFamily(t, families, name)
	var got float64
	for _, metric := range family.GetMetric() {
		got += metric.GetCounter().GetValue()
	}
	if got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func assertMetricGauge(t *testing.T, families map[string]*dto.MetricFamily, name string, want float64) {
	t.Helper()
	family := requireMetricFamily(t, families, name)
	if len(family.GetMetric()) != 1 {
		t.Fatalf("%s metric count = %d, want 1", name, len(family.GetMetric()))
	}
	if got := family.GetMetric()[0].GetGauge().GetValue(); got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}

func requireMetricFamily(t *testing.T, families map[string]*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()
	family := families[name]
	if family == nil {
		t.Fatalf("missing metric family %s", name)
	}
	return family
}

func containsText(value, fragment string) bool {
	for i := 0; i+len(fragment) <= len(value); i++ {
		if value[i:i+len(fragment)] == fragment {
			return true
		}
	}
	return false
}
