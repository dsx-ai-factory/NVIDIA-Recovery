// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/destinations"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/producer"
	recoveryaudit "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/recovery"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/config"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/heimdall"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/loglinks"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/slurm"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/state"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/telemetry"
	"go.uber.org/zap"
)

func main() {
	os.Exit(run())
}

// run owns process resources so startup errors and signals both flush telemetry
// before main exits. Deferred cleanup runs after the synchronous controller stops.
func run() int {
	bootstrapLogger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer bootstrapLogger.Sync()

	cfg, err := config.FromEnv()
	if err != nil {
		bootstrapLogger.Error("load configuration", zap.Error(err))
		return 1
	}
	if err := cfg.Validate(); err != nil {
		bootstrapLogger.Error("validate configuration", zap.Error(err))
		return 1
	}

	logger, shutdownLogs, err := newLogger(context.Background(), cfg)
	if err != nil {
		bootstrapLogger.Error("create logger", zap.Error(err))
		return 1
	}
	defer logger.Sync()
	defer func() {
		// The signal context is already canceled during normal shutdown.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownLogs(ctx); err != nil {
			bootstrapLogger.Warn("shutdown log exporter", zap.Error(err))
		}
	}()
	warnUserActionSelection(cfg, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logLinkSyncer, recovery, err := newLogLinkMaterializer(cfg, logger)
	if err != nil {
		logger.Error("initialize Slurm log-link materializer", zap.Error(err))
		return 1
	}
	var logLinkMaterializer reconcile.LogLinkMaterializer
	if logLinkSyncer != nil {
		logLogLinkRecovery(logger, recovery)
		logLinkMaterializer = reconcile.NewAsyncLogLinkMaterializer(ctx, logLinkSyncer)
	}

	var observer reconcile.Observer = reconcile.NopObserver{}
	var metricsRecorder *telemetry.Recorder
	var metricsHandler http.Handler
	if cfg.MetricsEnabled {
		registry := telemetry.NewRegistry()
		metricsRecorder = telemetry.NewRecorder(registry)
		observer = metricsRecorder
		metricsHandler = telemetry.Handler(registry)
	}
	if _, _, err := startMetricsServer(ctx, cfg, newMetricsMux(metricsHandler), logger); err != nil {
		logger.Error("start metrics server", zap.String("metrics_addr", cfg.MetricsAddr), zap.Error(err))
		return 1
	}

	apiClient, closeClient, err := heimdall.DialGRPCClient(ctx, cfg.HeimdallEndpoint, cfg.ServiceAccountTokenPath, cfg.GatewayTLS, logger)
	if err != nil {
		logger.Error("connect Heimdall endpoint", zap.String("heimdall_endpoint", cfg.HeimdallEndpoint), zap.Error(err))
		return 1
	}
	defer func() {
		if err := closeClient(); err != nil {
			logger.Warn("close Heimdall client", zap.Error(err))
		}
	}()

	eventDestination := audit.EventDestination[audit.ProducerEvent](destinations.NewLogging(logger))
	if cfg.AuditTrailDestination == "gateway" {
		eventDestination = destinations.NewGateway(apiClient, logger)
	}
	auditSink, stopAudit := producer.Start(producer.Options{
		Destination:   eventDestination,
		MaxBatchSize:  cfg.AuditTrailMaxBatchSize,
		BufferSize:    cfg.AuditTrailBufferSize,
		FlushInterval: cfg.AuditTrailFlushInterval,
		Logger:        logger,
	})
	// The controller loop is synchronous, so shutdown reaches this defer only
	// after all audit producers have stopped.
	defer stopAudit()
	auditObserver := recoveryaudit.NewObserver(auditSink)

	slurmCluster := slurm.NewCluster(slurm.Options{
		Timeout:                      cfg.SlurmExecTimeout,
		AccountingWorkerPoolSize:     cfg.AccountingWorkerPoolSize,
		AccountingTimeout:            cfg.AccountingTimeout,
		VersionedSqueueJSON:          cfg.VersionedSqueueJSON,
		EnableLogMetadata:            true,
		LogMetadataRefreshInterval:   cfg.LogMetadataRefreshInterval,
		LogMetadataEnrichmentTimeout: cfg.LogMetadataEnrichmentTimeout,
		LogMetadataRetryBase:         cfg.PollInterval,
		LogMetadataObserver:          metricsRecorder,
		CommandObserver:              metricsRecorder,
		Logger:                       logger,
	})
	defer slurmCluster.Close()
	if err := slurmCluster.DetectLogMetadataMode(ctx); err != nil {
		logger.Warn("Slurm version detection failed; using JSON log metadata", zap.Error(err))
	}
	seenJobs := state.NewMemorySeenJobStore()

	controller := newController(
		cfg,
		apiClient,
		slurmCluster,
		slurmCluster,
		slurmCluster,
		seenJobs,
		logLinkMaterializer,
		observer,
		auditObserver,
		logger,
	)
	logger.Info("starting Slurm advisor controller")

	runOnce := func() {
		startedAt := time.Now()
		result, err := controller.SyncOnce(ctx)
		duration := time.Since(startedAt)
		if err != nil {
			logger.Error("controller sync failed", zap.Duration("duration", duration), zap.Error(err))
			return
		}
		logger.Debug("controller sync completed",
			zap.Duration("duration", duration),
			zap.Int("active_jobs", len(result.Snapshot.ActiveJobs)),
			zap.Int("accounting_jobs", len(result.Snapshot.AccountingJobs)),
			zap.Int("heimdall_jobs", len(result.Snapshot.HeimdallJobs)),
			zap.Int("advice_items", totalAdviceItems(result.Snapshot.Advice)),
		)
		logEnvironmentAdmissionSummary(logger, cfg.Environment, result.Snapshot)
		for _, requeue := range result.FailedJobs.Requeues {
			if requeue.SkipReason != reconcile.FailedJobRequeueSkipNone {
				fields := []zap.Field{
					zap.String("effective_mode", string(requeue.Mode)),
					zap.String("requeue_id", requeue.RequeueID),
					zap.String("job_id", requeue.JobID),
					zap.Int32("attempt_index", requeue.AttemptIndex),
					zap.String("skip_reason", string(requeue.SkipReason)),
				}
				if requeue.MaxRequeueTimes != nil {
					fields = append(fields, zap.Int32("max_requeue_times", *requeue.MaxRequeueTimes))
				}
				logger.Info("skipped failed-job requeuehold", fields...)
				continue
			}
			if requeue.Applied {
				logger.Info("applied failed-job requeuehold",
					zap.String("effective_mode", string(requeue.Mode)),
					zap.String("requeue_id", requeue.RequeueID),
					zap.String("job_id", requeue.JobID),
					zap.Int32("attempt_index", requeue.AttemptIndex),
				)
				continue
			}
			logger.Info("planned failed-job requeuehold",
				zap.String("effective_mode", string(requeue.Mode)),
				zap.String("requeue_id", requeue.RequeueID),
				zap.String("job_id", requeue.JobID),
				zap.Int32("attempt_index", requeue.AttemptIndex),
			)
		}
	}

	runOnce()
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return 0
		case <-ticker.C:
			runOnce()
		}
	}
}

// warnUserActionSelection reports the surprising default/list combination once
// at startup. Explicit job modes still take precedence over either selection.
func warnUserActionSelection(cfg config.Config, logger *zap.Logger) {
	if cfg.Mode == config.ModeAct && len(cfg.UserAllowlist) > 0 {
		logger.Warn("act default with rollout user selection: jobs without an explicit mode act only for listed users; explicit job modes take precedence",
			zap.Int("selected_user_count", len(cfg.UserAllowlist)),
		)
	}
}

func startMetricsServer(ctx context.Context, cfg config.Config, handler http.Handler, logger *zap.Logger) (*http.Server, string, error) {
	if !cfg.MetricsEnabled {
		return nil, "", nil
	}
	if handler == nil {
		handler = http.NotFoundHandler()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	listener, err := net.Listen("tcp", cfg.MetricsAddr)
	if err != nil {
		return nil, "", err
	}
	addr := listener.Addr().String()
	server := &http.Server{Handler: handler}

	go func() {
		logger.Info("metrics server listening", zap.String("metrics_addr", addr))
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server stopped unexpectedly", zap.String("metrics_addr", addr), zap.Error(err))
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn("shutdown metrics server", zap.String("metrics_addr", addr), zap.Error(err))
		}
	}()

	return server, addr, nil
}

func newMetricsMux(metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	if metricsHandler != nil {
		mux.Handle("/metrics", metricsHandler)
	}

	return mux
}

func newController(
	cfg config.Config,
	client heimdall.HeimdallClient,
	reader cluster.JobReader,
	activeJobEnricher cluster.ActiveJobEnricher,
	actionExecutor cluster.JobMutator,
	seenJobs state.SeenJobStore,
	logLinkMaterializer reconcile.LogLinkMaterializer,
	observer reconcile.Observer,
	auditObserver reconcile.AuditObserver,
	logger *zap.Logger,
) reconcile.Controller {
	if cfg.EnableEagerRequeue && logger != nil {
		logger.Warn("eager requeue compatibility mode can bypass retry-limit and checkpoint-stop Advice",
			zap.Bool("enable_eager_requeue", true))
	}
	return reconcile.Controller{
		HeimdallClient:      client,
		JobReader:           reader,
		ActiveJobEnricher:   activeJobEnricher,
		ActionExecutor:      actionExecutor,
		SeenJobs:            seenJobs,
		LogLinkMaterializer: logLinkMaterializer,
		Environment:         cfg.Environment,
		Options: reconcile.ControllerOptions{
			AdvicePageLimit:                100,
			JobPageLimit:                   100,
			JobUpsertWorkerPoolSize:        cfg.JobUpsertWorkerPoolSize,
			FailedJobRecoveryTimeout:       cfg.FailedJobRecoveryTimeout,
			AdviceLoopTimeout:              cfg.AdviceLoopTimeout,
			AdviceItemTimeout:              cfg.AdviceItemTimeout,
			AdviceMaxAge:                   cfg.AdviceMaxAge,
			AdviceRequeueHoldDelay:         cfg.AdviceRequeueHoldDelay,
			EnableEagerRequeue:             cfg.EnableEagerRequeue,
			BootstrapFailureLookback:       cfg.BootstrapFailureLookback,
			Environment:                    cfg.Environment,
			UserAllowlist:                  cfg.UserAllowlist,
			PartitionAllowlist:             cfg.PartitionAllowlist,
			UpdateEligibleTimeAfterRequeue: cfg.UpdateEligibleTimeAfterRequeue,
		},
		Observer:      observer,
		AuditObserver: auditObserver,
		Logger:        logger,
		ModePolicy: reconcile.ModePolicy{
			DefaultMode: cluster.JobMode(cfg.Mode),
			AllowAct:    cfg.Safety.AcknowledgeSchedulerActions,
		},
	}
}

func newLogLinkMaterializer(
	cfg config.Config,
	logger *zap.Logger,
) (*loglinks.Materializer, loglinks.RecoveryReport, error) {
	if !cfg.LogLinks.Enabled {
		return nil, loglinks.RecoveryReport{}, nil
	}

	materializer, recovery, err := loglinks.New(loglinks.Config{
		Destination:            cfg.LogLinks.Destination,
		SourceRoots:            slices.Clone(cfg.LogLinks.SourceRoots),
		ApplicationPatterns:    slices.Clone(cfg.LogLinks.ApplicationPatterns),
		ScanInterval:           cfg.LogLinks.ScanInterval,
		CleanupInterval:        cfg.LogLinks.CleanupInterval,
		Retention:              cfg.LogLinks.Retention,
		ScanTimeout:            cfg.LogLinks.ScanTimeout,
		MaxPathLength:          cfg.LogLinks.MaxPathLength,
		MaxPathsPerSync:        cfg.LogLinks.MaxPathsPerSync,
		MaxPatterns:            cfg.LogLinks.MaxPatterns,
		MaxScanDirectories:     cfg.LogLinks.MaxScanDirectories,
		MaxEntriesPerDirectory: cfg.LogLinks.MaxEntriesPerDirectory,
		MaxMatchesPerScan:      cfg.LogLinks.MaxMatchesPerScan,
		MaxOwnedLinks:          cfg.LogLinks.MaxOwnedLinks,
	}, logger)
	if err != nil {
		return nil, loglinks.RecoveryReport{}, errors.Wrap(err, "initialize log-link filesystem state")
	}

	return materializer, recovery, nil
}

func logLogLinkRecovery(logger *zap.Logger, report loglinks.RecoveryReport) {
	if logger == nil {
		logger = zap.NewNop()
	}
	fields := []zap.Field{
		zap.Int("owned_links", report.Owned),
		zap.Int("invalid_manifest_entries", report.InvalidEntries),
		zap.Bool("manifest_error", report.ManifestError),
	}
	if report.ManifestError || report.InvalidEntries > 0 {
		logger.Warn("recovered Slurm log-link ownership manifest with issues", fields...)
		return
	}
	logger.Info("recovered Slurm log-link ownership manifest", fields...)
}

func totalAdviceItems(advice map[cluster.JobAttemptRef][]*pb.Advice) int {
	count := 0
	for _, items := range advice {
		count += len(items)
	}

	return count
}
