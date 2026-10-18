// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/config"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// logEnvironmentAdmissionSummary includes the values in the message because
// some log consumers display only msg (or the OTLP body), without attributes.
func logEnvironmentAdmissionSummary(logger *zap.Logger, environment string, snapshot reconcile.ControllerSnapshot) {
	matched := snapshot.EnvironmentAdmissionCount(reconcile.EnvironmentAdmissionMatched)
	controllerUnspecified := snapshot.EnvironmentAdmissionCount(reconcile.EnvironmentAdmissionControllerUnspecified)
	prodDefault := snapshot.EnvironmentAdmissionCount(reconcile.EnvironmentAdmissionProductionDefault)
	environmentMismatch := snapshot.EnvironmentAdmissionCount(reconcile.EnvironmentAdmissionMismatch)
	notOnboarded := snapshot.EnvironmentAdmissionCount(reconcile.EnvironmentAdmissionNotOnboarded)
	invalidEnvironment := snapshot.EnvironmentAdmissionCount(reconcile.EnvironmentAdmissionInvalid)
	logger.Info(fmt.Sprintf("Slurm APS environment admission summary: controller_environment=%q matched=%d controller_unspecified=%d prod_default=%d environment_mismatch=%d not_onboarded=%d invalid_environment=%d",
		environment, matched, controllerUnspecified, prodDefault, environmentMismatch, notOnboarded, invalidEnvironment),
		zap.String("controller_environment", environment),
		zap.Int("matched", matched),
		zap.Int("controller_unspecified", controllerUnspecified),
		zap.Int("prod_default", prodDefault),
		zap.Int("environment_mismatch", environmentMismatch),
		zap.Int("not_onboarded", notOnboarded),
		zap.Int("invalid_environment", invalidEnvironment),
	)
}

// newLogger tees controller logs to stderr and, when configured, a bounded
// background OTLP batch processor. The caller owns shutdown and must invoke it
// with a fresh, bounded context after log producers stop; Zap Sync alone does
// not flush the OpenTelemetry provider.
func newLogger(ctx context.Context, cfg config.Config) (*zap.Logger, func(context.Context) error, error) {
	var level zapcore.Level
	if err := level.Set(cfg.LogLevel); err != nil {
		return nil, nil, errors.Wrapf(err, "parse log level %q", cfg.LogLevel)
	}
	if err := cfg.OTLP.Validate(); err != nil {
		return nil, nil, err
	}
	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(level)
	logger, err := zapConfig.Build()
	if err != nil {
		return nil, nil, err
	}
	shutdown := func(context.Context) error { return nil }
	if cfg.OTLP.Enabled() {
		exporter, err := newLogExporter(ctx, cfg.OTLP)
		if err != nil {
			return nil, nil, errors.Wrap(err, "create OTLP log exporter")
		}
		provider := sdklog.NewLoggerProvider(
			sdklog.WithResource(resource.NewSchemaless(semconv.ServiceName("slurm-advisor-controller"))),
			sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
		)
		shutdown = provider.Shutdown
		otelCore := otelzap.NewCore("slurm-advisor-controller", otelzap.WithLoggerProvider(provider))
		// The bridge accepts every severity by default. Apply SAC's level to
		// both outputs so export cannot re-enable filtered debug messages.
		logger = logger.WithOptions(zap.WrapCore(func(console zapcore.Core) zapcore.Core {
			exported := zap.New(otelCore, zap.IncreaseLevel(level)).Core()
			return zapcore.NewTee(console, newErrorMessageCore(exported))
		}))
	}
	return logger.With(
		zap.String("component", "slurm-advisor-controller"),
		zap.String("default_mode", string(cfg.Mode)),
		zap.String("environment", cfg.Environment),
	), shutdown, nil
}

// errorMessageCore appends the concise top-level error to OTLP log messages
// while forwarding all structured fields and entry metadata unchanged.
type errorMessageCore struct {
	zapcore.Core
	contextError    error
	namespaceOpened bool
}

type topLevelErrorResult struct {
	value           error
	found           bool
	namespaceOpened bool
}

// newErrorMessageCore wraps only the OTLP core without changing its filtering.
func newErrorMessageCore(core zapcore.Core) zapcore.Core {
	return &errorMessageCore{Core: core}
}

// With remembers a top-level contextual error and forwards every field intact.
func (c *errorMessageCore) With(fields []zapcore.Field) zapcore.Core {
	result := topLevelError(fields, c.namespaceOpened)
	if !result.found {
		result.value = c.contextError
	}
	return &errorMessageCore{
		Core:            c.Core.With(fields),
		contextError:    result.value,
		namespaceOpened: result.namespaceOpened,
	}
}

// Check registers the wrapper for entries enabled by the underlying core.
func (c *errorMessageCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return checked.AddCore(entry, c)
	}
	return checked
}

// Write gives an entry error precedence over the contextual error, changes only
// the OTLP message, and forwards every field and the stack trace intact.
func (c *errorMessageCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	result := topLevelError(fields, c.namespaceOpened)
	if !result.found {
		result.value = c.contextError
	}
	if result.value != nil {
		if entry.Message != "" {
			entry.Message += " |"
		}
		entry.Message += " error=" + strconv.Quote(result.value.Error())
	}
	return c.Core.Write(entry, fields)
}

// topLevelError returns the last ordinary zap.Error before any namespace. Zap
// namespaces stay open for subsequent fields, including across logger.With.
func topLevelError(fields []zapcore.Field, namespaceOpened bool) topLevelErrorResult {
	result := topLevelErrorResult{namespaceOpened: namespaceOpened}
	for _, field := range fields {
		if field.Type == zapcore.NamespaceType {
			result.namespaceOpened = true
			continue
		}
		if result.namespaceOpened || field.Key != "error" || field.Type != zapcore.ErrorType {
			continue
		}
		if candidate, ok := field.Interface.(error); ok {
			result.value = candidate
			result.found = true
		}
	}
	return result
}

// newLogExporter keeps credential construction identical for both transports.
// Explicit options bind the endpoint and token to the validated configuration.
func newLogExporter(ctx context.Context, cfg config.OTLPConfig) (sdklog.Exporter, error) {
	headers := map[string]string{"Authorization": "Bearer " + cfg.Token}
	if cfg.Protocol == "grpc" {
		return otlploggrpc.New(ctx,
			otlploggrpc.WithEndpointURL(cfg.Endpoint),
			otlploggrpc.WithHeaders(headers),
		)
	}
	// OTEL_EXPORTER_OTLP_ENDPOINT is a base URL. WithEndpointURL instead
	// takes a signal URL, so preserve any base path and append /v1/logs.
	endpoint, err := url.JoinPath(cfg.Endpoint, "v1/logs")
	if err != nil {
		return nil, errors.New("invalid OTLP log endpoint")
	}
	return otlploghttp.New(ctx,
		otlploghttp.WithEndpointURL(endpoint),
		otlploghttp.WithHeaders(headers),
	)
}
