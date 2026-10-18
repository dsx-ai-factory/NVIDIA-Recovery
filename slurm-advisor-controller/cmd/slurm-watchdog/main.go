// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/destinations"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/producer"
	watchdogaudit "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/watchdog"
	admission "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/environment"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/grpcauth"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobpolicy"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/slurm"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/watchdog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

type syncer interface {
	SyncOnce(context.Context) (watchdog.SyncResult, error)
}

func main() {
	bootstrapLogger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer func() { _ = bootstrapLogger.Sync() }()

	cfg, err := watchdog.FromEnv()
	if err != nil {
		bootstrapLogger.Fatal("load watchdog configuration", zap.Error(err))
	}
	if err := cfg.Validate(); err != nil {
		bootstrapLogger.Fatal("validate watchdog configuration", zap.Error(err))
	}

	logger, err := newLogger(cfg)
	if err != nil {
		bootstrapLogger.Fatal("create watchdog logger", zap.Error(err))
	}
	defer func() { _ = logger.Sync() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	eventDestination := audit.EventDestination[audit.ProducerEvent](destinations.NewLogging(logger))
	if cfg.AuditTrailDestination == "gateway" {
		dialOptions, err := grpcauth.DialOptions(cfg.GatewayTLS, cfg.ServiceAccountTokenPath)
		if err != nil {
			logger.Fatal("configure audit gateway transport", zap.Error(err))
		}
		conn, err := grpc.NewClient(cfg.AuditGatewayEndpoint, dialOptions...)
		if err != nil {
			logger.Fatal("connect audit gateway", zap.Error(err))
		}
		// The later stopAudit defer drains the buffer before this connection closes.
		defer func() {
			if err := conn.Close(); err != nil {
				logger.Warn("close audit gateway", zap.Error(err))
			}
		}()
		eventDestination = destinations.NewGateway(pb.NewAuditServiceClient(conn), logger)
	}
	auditSink, stopAudit := producer.Start(producer.Options{
		Destination:   eventDestination,
		MaxBatchSize:  cfg.AuditTrailMaxBatchSize,
		BufferSize:    cfg.AuditTrailBufferSize,
		FlushInterval: cfg.AuditTrailFlushInterval,
		Logger:        logger,
	})
	defer stopAudit()

	var metrics *watchdog.Metrics
	var metricsHandler http.Handler
	if cfg.MetricsEnabled {
		registry := watchdog.NewMetricsRegistry()
		metrics = watchdog.NewMetrics(registry)
		metricsHandler = watchdog.MetricsHandler(registry)
	}
	if _, _, err := startMetricsServer(ctx, cfg, metricsHandler, logger); err != nil {
		logger.Fatal("start watchdog metrics server", zap.String("metrics_addr", cfg.MetricsAddr), zap.Error(err))
	}

	slurmCluster := slurm.NewCluster(slurm.Options{
		DryRun:              cfg.Mode == watchdog.ModeObserve,
		Timeout:             cfg.SlurmExecTimeout,
		VersionedSqueueJSON: cfg.VersionedSqueueJSON,
	})
	defer slurmCluster.Close()
	engine, err := watchdog.New(slurmCluster, slurmCluster, watchdog.Options{
		Mode:                        cfg.Mode,
		AcknowledgeSchedulerActions: cfg.AcknowledgeSchedulerActions,
		ReleaseTimeout:              cfg.ReleaseTimeout,
		CleanupGracePeriod:          cfg.CleanupGracePeriod,
		EnvironmentPattern:          cfg.EnvironmentPattern,
		PartitionAllowlist:          cfg.PartitionAllowlist,
		JobPolicy: jobpolicy.Policy{
			DefaultMode: cfg.JobDefaultMode, AllowAct: cfg.JobActionsAcknowledged, UserAllowlist: cfg.UserAllowlist,
		},
		ReleaseLifecycleObserver: watchdogaudit.NewObserver(auditSink),
	})
	if err != nil {
		logger.Fatal("create watchdog", zap.Error(err))
	}

	logger.Info("starting standalone Slurm watchdog",
		zap.Duration("poll_interval", cfg.PollInterval),
		zap.Duration("release_timeout", cfg.ReleaseTimeout),
		zap.Duration("cleanup_grace_period", cfg.CleanupGracePeriod),
		zap.String("environment_pattern", cfg.EnvironmentPattern),
	)
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()
	run(ctx, ticker.C, logger, func(pollCtx context.Context) {
		pollOnce(pollCtx, engine, metrics, logger, cfg.Mode)
	})
}

func run(ctx context.Context, ticks <-chan time.Time, logger *zap.Logger, poll func(context.Context)) {
	pollSafely(ctx, logger, poll)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			pollSafely(ctx, logger, poll)
		}
	}
}

func pollSafely(ctx context.Context, logger *zap.Logger, poll func(context.Context)) {
	defer func() {
		if recovered := recover(); recovered != nil {
			logger.Error("watchdog poll panicked",
				zap.Any("panic", recovered),
				zap.Stack("stacktrace"),
			)
		}
	}()
	poll(ctx)
}

func pollOnce(ctx context.Context, engine syncer, metrics *watchdog.Metrics, logger *zap.Logger, mode watchdog.Mode) {
	startedAt := time.Now()
	result, err := engine.SyncOnce(ctx)
	duration := time.Since(startedAt)
	metrics.ObservePoll(mode, result, err, duration)

	for _, release := range result.Releases {
		logger.Info("watchdog release candidate",
			zap.String("outcome", string(release.Outcome)),
			zap.String("job_id", release.JobID),
			zap.Int32("attempt_index", release.AttemptIndex),
			zap.String("partition", release.Partition),
			zap.Time("held_since", release.HeldSince),
		)
	}
	if err != nil {
		logger.Error("watchdog poll failed", zap.Duration("duration", duration), zap.Error(err))
		return
	}
	logger.Info("watchdog APS environment admission summary",
		zap.Int("matched", result.EnvironmentAdmissionCount(admission.AdmissionMatched)),
		zap.Int("controller_unspecified", result.EnvironmentAdmissionCount(admission.AdmissionControllerUnspecified)),
		zap.Int("prod_default", result.EnvironmentAdmissionCount(admission.AdmissionProductionDefault)),
		zap.Int("environment_mismatch", result.EnvironmentAdmissionCount(admission.AdmissionMismatch)),
		zap.Int("not_onboarded", result.EnvironmentAdmissionCount(admission.AdmissionNotOnboarded)),
		zap.Int("invalid_environment", result.EnvironmentAdmissionCount(admission.AdmissionInvalid)),
	)
	logger.Debug("watchdog poll completed",
		zap.Duration("duration", duration),
		zap.Int("observed_jobs", result.ObservedJobs),
		zap.Int("managed_jobs", result.ManagedJobs),
		zap.Int("tracked_jobs", result.TrackedJobs),
		zap.Int("release_candidates", len(result.Releases)),
	)
}

func newLogger(cfg watchdog.Config) (*zap.Logger, error) {
	var level zapcore.Level
	if err := level.Set(cfg.LogLevel); err != nil {
		return nil, errors.Wrapf(err, "parse watchdog log level %q", cfg.LogLevel)
	}
	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(level)
	logger, err := zapConfig.Build()
	if err != nil {
		return nil, err
	}
	return logger.With(
		zap.String("component", "slurm-watchdog"),
		zap.String("mode", string(cfg.Mode)),
	), nil
}

func startMetricsServer(
	ctx context.Context,
	cfg watchdog.Config,
	handler http.Handler,
	logger *zap.Logger,
) (*http.Server, string, error) {
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
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("watchdog metrics server listening", zap.String("metrics_addr", addr))
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("watchdog metrics server stopped unexpectedly", zap.String("metrics_addr", addr), zap.Error(err))
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn("shutdown watchdog metrics server", zap.String("metrics_addr", addr), zap.Error(err))
		}
	}()

	return server, addr, nil
}
