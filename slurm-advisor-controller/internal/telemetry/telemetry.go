// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"net/http"
	"slices"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/slurm"
)

type Recorder struct {
	reconcileLoops           *prometheus.CounterVec
	reconcileDuration        *prometheus.HistogramVec
	reconcileStageDuration   *prometheus.HistogramVec
	reconcileInProgress      prometheus.Gauge
	reconcileStartedAt       prometheus.Gauge
	stageInProgress          *prometheus.GaugeVec
	stageStartedAt           *prometheus.GaugeVec
	sourceReads              *prometheus.CounterVec
	sourceReadDuration       *prometheus.HistogramVec
	sourceItems              *prometheus.CounterVec
	snapshotJobs             *prometheus.GaugeVec
	snapshotAdvice           prometheus.Gauge
	adviceReconciliations    prometheus.Gauge
	jobAttemptCacheEntries   prometheus.Gauge
	jobUpserts               *prometheus.CounterVec
	adviceSkips              *prometheus.CounterVec
	adviceActions            *prometheus.CounterVec
	adviceStatusPublications *prometheus.CounterVec
	failedRequeues           *prometheus.CounterVec
	commandTimeouts          *prometheus.CounterVec
	modeDecisions            *prometheus.CounterVec
	environmentAdmissions    *prometheus.CounterVec
	logLinksCreated          prometheus.Counter
	logLinksExisting         prometheus.Counter
	logLinkCollisions        prometheus.Counter
	logLinksRejected         prometheus.Counter
	logLinkErrors            prometheus.Counter
	logLinksRemoved          prometheus.Counter
	logLinksOwned            prometheus.Gauge
	logLinkScans             prometheus.Counter
	logLinkScanDirs          prometheus.Counter
	logLinkScanEntries       prometheus.Counter
	logLinkScanMatches       prometheus.Counter
	logLinkScanDuration      prometheus.Histogram
	logLinkCleanupFailures   prometheus.Counter
	logMetadataCacheAccesses *prometheus.CounterVec
	logMetadataWork          *prometheus.CounterVec
	logMetadataCommands      *prometheus.HistogramVec
	logMetadataEvictions     *prometheus.CounterVec
	logMetadataDiscovered    *prometheus.CounterVec
	logMetadataPublished     prometheus.Counter
	logMetadataPublishedPath prometheus.Counter
	logMetadataCacheBytes    prometheus.Gauge
	logMetadataCacheAttempts prometheus.Gauge
	logMetadataCacheLimits   *prometheus.GaugeVec
	logMetadataQueue         *prometheus.GaugeVec
	logMetadataWorkers       *prometheus.GaugeVec
	logMetadataOldestQueued  prometheus.Gauge
}

// reconcileDurationBuckets covers controller ticks and their stages from one
// second through ten minutes. Prometheus adds the terminal +Inf bucket
// implicitly.
var reconcileDurationBuckets = []float64{
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

// sourceReadDurationBuckets retain the production tick range while adding
// sub-second resolution for fast local and backend reads.
var sourceReadDurationBuckets = []float64{
	0.005,
	0.01,
	0.025,
	0.05,
	0.1,
	0.25,
	0.5,
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

var commandTimeoutLabels = []string{"squeue", "sacct", "scontrol", "scancel", "other"}

var reconcileStages = []reconcile.ReconcileStage{
	reconcile.StageJobStatusPolling,
	reconcile.StageJobReconciliation,
	reconcile.StageFailedJobReconciliation,
	reconcile.StageAdviceReconciliation,
}

// NewRegistry returns an isolated Prometheus registry with standard Go and
// process collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	return reg
}

// NewRecorder registers controller telemetry metrics and records reconciliation
// observations into them.
func NewRecorder(reg prometheus.Registerer) *Recorder {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	factory := promauto.With(reg)

	recorder := &Recorder{
		reconcileLoops: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_reconcile_loops_total",
			Help: "Total number of Slurm advisor controller reconciliation loops.",
		}, []string{"mode", "outcome"}),
		reconcileDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "slurm_advisor_controller_reconcile_duration_seconds",
			Help:    "Duration of Slurm advisor controller reconciliation loops.",
			Buckets: reconcileDurationBuckets,
		}, []string{"mode", "outcome"}),
		reconcileStageDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "slurm_advisor_controller_reconcile_stage_duration_seconds",
			Help:    "Duration of bounded stages within Slurm advisor controller reconciliation loops.",
			Buckets: reconcileDurationBuckets,
		}, []string{"stage", "outcome"}),
		reconcileInProgress: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_reconcile_in_progress",
			Help: "Whether a Slurm advisor controller reconciliation loop is currently in progress.",
		}),
		reconcileStartedAt: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_reconcile_started_timestamp_seconds",
			Help: "Unix timestamp when the current reconciliation loop started, or zero when idle.",
		}),
		stageInProgress: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_reconcile_stage_in_progress",
			Help: "Whether the current reconciliation loop is executing a bounded stage.",
		}, []string{"stage"}),
		stageStartedAt: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_reconcile_stage_started_timestamp_seconds",
			Help: "Unix timestamp when the current reconciliation stage started, or zero when inactive.",
		}, []string{"stage"}),
		sourceReads: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_source_reads_total",
			Help: "Total number of controller source reads by source and outcome.",
		}, []string{"source", "outcome"}),
		sourceReadDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "slurm_advisor_controller_source_read_duration_seconds",
			Help:    "Duration of individual controller source reads by source and outcome.",
			Buckets: sourceReadDurationBuckets,
		}, []string{"source", "outcome"}),
		sourceItems: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_source_items_total",
			Help: "Total number of items returned by controller source reads.",
		}, []string{"source"}),
		snapshotJobs: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_snapshot_jobs",
			Help: "Number of jobs in the most recent controller snapshot by source.",
		}, []string{"source"}),
		snapshotAdvice: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_snapshot_advice",
			Help: "Number of advice records fetched into the most recent controller snapshot.",
		}),
		adviceReconciliations: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_advice_reconciliation_attempts",
			Help: "Number of advice records not blocked by environment routing that entered reconciliation in the most recent controller reconciliation loop, including skips and failed actions.",
		}),
		jobAttemptCacheEntries: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_local_job_attempt_cache_entries",
			Help: "Number of entries in the local job-attempt cache after the most recent controller reconciliation loop.",
		}),
		jobUpserts: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_job_upserts_total",
			Help: "Total number of Heimdall job upsert attempts by source and outcome.",
		}, []string{"source", "outcome"}),
		adviceSkips: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_advice_skips_total",
			Help: "Total number of skipped advice records by reason.",
		}, []string{"skip_reason"}),
		adviceActions: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_advice_actions_total",
			Help: "Total number of scheduler actions attempted for advice by action, outcome, and mode.",
		}, []string{"action", "outcome", "mode"}),
		adviceStatusPublications: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_advice_status_publications_total",
			Help: "Total number of terminal Advice status publications by bounded disposition, reason, and outcome.",
		}, []string{"disposition", "reason", "outcome"}),
		commandTimeouts: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_slurm_command_timeouts_total",
			Help: "Total Slurm commands terminated by command or enclosing operation deadlines.",
		}, []string{"command"}),
		failedRequeues: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_failed_requeues_total",
			Help: "Total number of direct failed-job requeuehold actions by outcome and mode.",
		}, []string{"outcome", "mode"}),
		modeDecisions: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_job_mode_decisions_total",
			Help: "Total number of per-job mode decisions by default, requested, effective mode, and reason.",
		}, []string{"default_mode", "requested_mode", "effective_mode", "decision_reason"}),
		environmentAdmissions: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_job_environment_admissions_total",
			Help: "Total number of Slurm job environment admission decisions by bounded reason.",
		}, []string{"decision_reason"}),
		logLinksCreated: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_links_created_total",
			Help: "Total number of Slurm log links created by the optional materializer.",
		}),
		logLinksExisting: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_links_existing_total",
			Help: "Total number of Slurm log links already present with the expected target.",
		}),
		logLinkCollisions: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_link_collisions_total",
			Help: "Total number of Slurm log-link destination collisions left unchanged.",
		}),
		logLinksRejected: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_links_rejected_total",
			Help: "Total number of Slurm log paths rejected by materializer validation or bounds.",
		}),
		logLinkErrors: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_link_errors_total",
			Help: "Total number of best-effort Slurm log-link operation errors.",
		}),
		logLinksRemoved: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_links_removed_total",
			Help: "Total number of owned Slurm log links removed after retention.",
		}),
		logLinksOwned: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_log_links_owned",
			Help: "Number of manifest-owned Slurm log links after the most recent sync.",
		}),
		logLinkScans: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_link_scans_total",
			Help: "Total number of bounded adjacent application-log scans.",
		}),
		logLinkScanDirs: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_link_scan_directories_total",
			Help: "Total number of directories examined by adjacent application-log scans.",
		}),
		logLinkScanEntries: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_link_scan_entries_total",
			Help: "Total number of directory entries examined by adjacent application-log scans.",
		}),
		logLinkScanMatches: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_link_scan_matches_total",
			Help: "Total number of application-log paths matched by adjacent scans.",
		}),
		logLinkScanDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "slurm_advisor_controller_log_link_scan_duration_seconds",
			Help:    "Duration of bounded adjacent application-log scans.",
			Buckets: prometheus.DefBuckets,
		}),
		logLinkCleanupFailures: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_link_cleanup_failures_total",
			Help: "Total number of manifest-scoped log-link cleanup failures.",
		}),
		logMetadataCacheAccesses: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_metadata_cache_accesses_total",
			Help: "Total log-metadata cache reads by bounded source and result.",
		}, []string{"source", "result"}),
		logMetadataWork: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_metadata_work_total",
			Help: "Total background log-metadata work transitions by bounded event.",
		}, []string{"event"}),
		logMetadataCommands: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "slurm_advisor_controller_log_metadata_command_duration_seconds",
			Help:    "Duration of asynchronous supplemental Slurm commands by command and outcome.",
			Buckets: sourceReadDurationBuckets,
		}, []string{"command", "outcome"}),
		logMetadataEvictions: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_metadata_evictions_total",
			Help: "Total log-metadata cache evictions by bounded reason.",
		}, []string{"eviction_reason"}),
		logMetadataDiscovered: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_metadata_paths_discovered_total",
			Help: "Total validated log paths discovered by bounded source.",
		}, []string{"source"}),
		logMetadataPublished: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_metadata_attempts_published_total",
			Help: "Total job attempts published with cached Slurm log metadata.",
		}),
		logMetadataPublishedPath: factory.NewCounter(prometheus.CounterOpts{
			Name: "slurm_advisor_controller_log_metadata_paths_published_total",
			Help: "Total cached Slurm log paths published on fresh job observations.",
		}),
		logMetadataCacheBytes: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_log_metadata_cache_path_bytes",
			Help: "Bytes currently retained in validated log-metadata paths.",
		}),
		logMetadataCacheAttempts: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_log_metadata_cache_attempts",
			Help: "Exact job attempts currently retained in the log-metadata cache.",
		}),
		logMetadataCacheLimits: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_log_metadata_cache_limit",
			Help: "Configured log-metadata cache limits by bounded resource.",
		}, []string{"resource"}),
		logMetadataQueue: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_log_metadata_queue",
			Help: "Background log-metadata queue depth and capacity.",
		}, []string{"measure"}),
		logMetadataWorkers: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_log_metadata_workers",
			Help: "Background log-metadata workers in flight and configured limit.",
		}, []string{"measure"}),
		logMetadataOldestQueued: factory.NewGauge(prometheus.GaugeOpts{
			Name: "slurm_advisor_controller_log_metadata_oldest_queued_age_seconds",
			Help: "Age in seconds of the oldest queued log-metadata item.",
		}),
	}
	// Publish zero samples before the first timeout so increase() can observe
	// that first event after the initial scrape.
	for _, command := range commandTimeoutLabels {
		recorder.commandTimeouts.WithLabelValues(command)
	}
	return recorder
}

func (r *Recorder) ObserveLogMetadataCacheAccess(source slurm.LogMetadataCacheSource, result slurm.LogMetadataCacheResult) {
	if r != nil {
		r.logMetadataCacheAccesses.WithLabelValues(string(source), string(result)).Inc()
	}
}

func (r *Recorder) ObserveLogMetadataWork(event slurm.LogMetadataWorkEvent) {
	if r != nil {
		r.logMetadataWork.WithLabelValues(string(event)).Inc()
	}
}

func (r *Recorder) ObserveLogMetadataCommand(command slurm.LogMetadataCommand, outcome slurm.LogMetadataCommandOutcome, duration time.Duration) {
	if r != nil {
		r.logMetadataCommands.WithLabelValues(string(command), string(outcome)).Observe(duration.Seconds())
	}
}

func (r *Recorder) ObserveLogMetadataEviction(reason slurm.LogMetadataEvictionReason, count int) {
	if r != nil {
		addPositiveCount(r.logMetadataEvictions.WithLabelValues(string(reason)), count)
	}
}

func (r *Recorder) ObserveLogMetadataPathsDiscovered(source slurm.LogMetadataCacheSource, count int) {
	if r != nil {
		addPositiveCount(r.logMetadataDiscovered.WithLabelValues(string(source)), count)
	}
}

func (r *Recorder) ObserveLogMetadataPublished(attempts, paths int) {
	if r == nil {
		return
	}
	addPositiveCount(r.logMetadataPublished, attempts)
	addPositiveCount(r.logMetadataPublishedPath, paths)
}

func (r *Recorder) ObserveLogMetadataCache(observation slurm.LogMetadataCacheObservation) {
	if r == nil {
		return
	}
	r.logMetadataCacheBytes.Set(float64(observation.PathBytes))
	r.logMetadataCacheAttempts.Set(float64(observation.Attempts))
	r.logMetadataCacheLimits.WithLabelValues("path_bytes").Set(float64(observation.PathByteLimit))
	r.logMetadataCacheLimits.WithLabelValues("attempts").Set(float64(observation.AttemptLimit))
	r.logMetadataQueue.WithLabelValues("depth").Set(float64(observation.QueueDepth))
	r.logMetadataQueue.WithLabelValues("capacity").Set(float64(observation.QueueCapacity))
	r.logMetadataWorkers.WithLabelValues("in_flight").Set(float64(observation.WorkersInFlight))
	r.logMetadataWorkers.WithLabelValues("limit").Set(float64(observation.WorkerLimit))
	r.logMetadataOldestQueued.Set(observation.OldestQueuedAge.Seconds())
}

// ObserveSyncStarted publishes live state for a tick whose completion
// histogram does not exist yet.
func (r *Recorder) ObserveSyncStarted(_ reconcile.OperationMode) {
	if r == nil {
		return
	}
	r.reconcileInProgress.Set(1)
	r.reconcileStartedAt.Set(float64(time.Now().Unix()))
	for _, stage := range reconcileStages {
		r.stageInProgress.WithLabelValues(string(stage)).Set(0)
		r.stageStartedAt.WithLabelValues(string(stage)).Set(0)
	}
}

// ObserveStageStarted identifies the current bounded stage and its age without
// introducing job- or Advice-derived metric labels.
func (r *Recorder) ObserveStageStarted(current reconcile.ReconcileStage) {
	if r == nil {
		return
	}
	for _, stage := range reconcileStages {
		active := 0.0
		startedAt := 0.0
		if stage == current {
			active = 1
			startedAt = float64(time.Now().Unix())
		}
		r.stageInProgress.WithLabelValues(string(stage)).Set(active)
		r.stageStartedAt.WithLabelValues(string(stage)).Set(startedAt)
	}
}

// ObserveSyncCompleted clears live tick state after the completion summary has
// been recorded.
func (r *Recorder) ObserveSyncCompleted() {
	if r == nil {
		return
	}
	r.reconcileInProgress.Set(0)
	r.reconcileStartedAt.Set(0)
	for _, stage := range reconcileStages {
		r.stageInProgress.WithLabelValues(string(stage)).Set(0)
		r.stageStartedAt.WithLabelValues(string(stage)).Set(0)
	}
}

// Handler returns an HTTP handler that serves metrics from reg.
func Handler(reg prometheus.Gatherer) http.Handler {
	if reg == nil {
		reg = prometheus.DefaultGatherer
	}

	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

func (r *Recorder) ObserveSync(obs reconcile.SyncObservation) {
	if r == nil {
		return
	}
	mode := string(obs.Mode)
	outcome := string(obs.Outcome)
	r.reconcileLoops.WithLabelValues(mode, outcome).Inc()
	r.reconcileDuration.WithLabelValues(mode, outcome).Observe(obs.Duration.Seconds())
	for _, stage := range obs.Stages {
		r.reconcileStageDuration.WithLabelValues(string(stage.Stage), string(stage.Outcome)).Observe(stage.Duration.Seconds())
	}

	for _, source := range obs.Sources {
		r.sourceReads.WithLabelValues(string(source.Source), string(source.Outcome)).Inc()
		r.sourceReadDuration.WithLabelValues(string(source.Source), string(source.Outcome)).Observe(source.Duration.Seconds())
		if source.Items > 0 {
			r.sourceItems.WithLabelValues(string(source.Source)).Add(float64(source.Items))
		}
	}

	r.snapshotJobs.WithLabelValues(string(reconcile.SourceSlurmActive)).Set(float64(obs.Snapshot.ActiveJobs))
	r.snapshotJobs.WithLabelValues(string(reconcile.SourceHeimdallJobs)).Set(float64(obs.Snapshot.HeimdallJobs))
	r.snapshotJobs.WithLabelValues(string(reconcile.SourceSlurmAccounting)).Set(float64(obs.Snapshot.AccountingJobs))
	r.snapshotJobs.WithLabelValues(string(reconcile.SourceSeenStore)).Set(float64(obs.Snapshot.SeenJobs))
	r.snapshotAdvice.Set(float64(obs.Snapshot.Advice))
	r.adviceReconciliations.Set(float64(obs.AttemptedAdviceReconciliations))
	if obs.JobAttemptCache.Observed {
		r.jobAttemptCacheEntries.Set(float64(obs.JobAttemptCache.Entries))
	}

	for _, upsert := range obs.Upserts {
		addPositiveCount(r.jobUpserts.WithLabelValues(string(upsert.Source), string(upsert.Outcome)), upsert.Count)
	}
	for _, skip := range obs.AdviceSkips {
		addPositiveCount(r.adviceSkips.WithLabelValues(string(skip.Reason)), skip.Count)
	}
	for _, action := range obs.AdviceActions {
		addPositiveCount(r.adviceActions.WithLabelValues(string(action.Action), string(action.Outcome), string(action.Mode)), action.Count)
	}
	for _, publication := range obs.AdviceStatusPublications {
		addPositiveCount(r.adviceStatusPublications.WithLabelValues(
			publication.Disposition,
			publication.Reason,
			string(publication.Outcome),
		), publication.Count)
	}
	for _, requeue := range obs.FailedRequeues {
		addPositiveCount(r.failedRequeues.WithLabelValues(string(requeue.Outcome), string(requeue.Mode)), requeue.Count)
	}
	for _, decision := range obs.ModeDecisions {
		addPositiveCount(r.modeDecisions.WithLabelValues(
			string(decision.DefaultMode),
			string(decision.RequestedMode),
			string(decision.EffectiveMode),
			string(decision.Reason),
		), decision.Count)
	}
	for _, admission := range obs.EnvironmentAdmissions {
		addPositiveCount(r.environmentAdmissions.WithLabelValues(string(admission.Reason)), admission.Count)
	}

	if obs.LogLinks.Completed {
		logLinks := obs.LogLinks.Observation
		addPositiveCount(r.logLinksCreated, logLinks.Created)
		addPositiveCount(r.logLinksExisting, logLinks.Existing)
		addPositiveCount(r.logLinkCollisions, logLinks.Collisions)
		addPositiveCount(r.logLinksRejected, logLinks.Rejected)
		addPositiveCount(r.logLinkErrors, logLinks.Errors)
		addPositiveCount(r.logLinksRemoved, logLinks.Removed)
		r.logLinksOwned.Set(float64(logLinks.Owned))
		addPositiveCount(r.logLinkScans, logLinks.Scans)
		addPositiveCount(r.logLinkScanDirs, logLinks.ScanDirectories)
		addPositiveCount(r.logLinkScanEntries, logLinks.ScanEntries)
		addPositiveCount(r.logLinkScanMatches, logLinks.ScanMatches)
		if logLinks.Scans > 0 {
			r.logLinkScanDuration.Observe(logLinks.ScanDuration.Seconds())
		}
		addPositiveCount(r.logLinkCleanupFailures, logLinks.CleanupFailures)
	}
}

func addPositiveCount(counter prometheus.Counter, count int) {
	if count <= 0 {
		return
	}
	counter.Add(float64(count))
}

// ObserveCommandTimeout records an execution timeout using only known Slurm
// command names; unexpected names share a bounded fallback label.
func (r *Recorder) ObserveCommandTimeout(command string) {
	if r == nil {
		return
	}
	if !slices.Contains(commandTimeoutLabels, command) {
		command = "other"
	}
	r.commandTimeouts.WithLabelValues(command).Inc()
}

var _ slurm.CommandObserver = (*Recorder)(nil)
