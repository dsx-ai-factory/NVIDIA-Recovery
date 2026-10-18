// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package watchdog

import (
	"net/http"
	"time"

	admission "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/environment"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	polls                 *prometheus.CounterVec
	pollDuration          *prometheus.HistogramVec
	observedJobs          prometheus.Gauge
	managedJobs           prometheus.Gauge
	trackedJobs           prometheus.Gauge
	releaseCandidates     *prometheus.CounterVec
	environmentAdmissions *prometheus.CounterVec
}

func NewMetricsRegistry() *prometheus.Registry {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewBuildInfoCollector(),
	)
	return registry
}

func NewMetrics(registerer prometheus.Registerer) *Metrics {
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	factory := promauto.With(registerer)
	return &Metrics{
		polls: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_watchdog_polls_total",
			Help: "Total number of standalone Slurm watchdog polls.",
		}, []string{"mode", "outcome"}),
		pollDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "slurm_watchdog_poll_duration_seconds",
			Help:    "Duration of standalone Slurm watchdog polls.",
			Buckets: prometheus.DefBuckets,
		}, []string{"mode", "outcome"}),
		observedJobs: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_watchdog_observed_jobs",
			Help: "Number of Slurm jobs returned by the most recent watchdog poll.",
		}),
		managedJobs: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_watchdog_managed_jobs",
			Help: "Number of Heimdall-managed Slurm jobs in the most recent watchdog poll.",
		}),
		trackedJobs: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_watchdog_tracked_jobs",
			Help: "Number of exact Slurm job identities retained in watchdog memory.",
		}),
		releaseCandidates: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_watchdog_release_candidates_total",
			Help: "Total number of watchdog release candidates by bounded outcome.",
		}, []string{"mode", "outcome"}),
		environmentAdmissions: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_watchdog_environment_admissions_total",
			Help: "Total number of Slurm job environment admission decisions by bounded reason.",
		}, []string{"reason"}),
	}
}

func (m *Metrics) ObservePoll(mode Mode, result SyncResult, err error, duration time.Duration) {
	if m == nil {
		return
	}
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	m.polls.WithLabelValues(string(mode), outcome).Inc()
	m.pollDuration.WithLabelValues(string(mode), outcome).Observe(duration.Seconds())
	m.observedJobs.Set(float64(result.ObservedJobs))
	m.managedJobs.Set(float64(result.ManagedJobs))
	m.trackedJobs.Set(float64(result.TrackedJobs))
	for _, release := range result.Releases {
		m.releaseCandidates.WithLabelValues(string(mode), string(release.Outcome)).Inc()
	}
	for _, reason := range admission.AdmissionReasons {
		if count := result.EnvironmentAdmissionCount(reason); count > 0 {
			m.environmentAdmissions.WithLabelValues(string(reason)).Add(float64(count))
		}
	}
}

func MetricsHandler(gatherer prometheus.Gatherer) http.Handler {
	if gatherer == nil {
		gatherer = prometheus.DefaultGatherer
	}
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
}
