// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestDefaultConfigUsesObserveMode(t *testing.T) {
	cfg := WithDefaults(Config{
		HeimdallEndpoint: "127.0.0.1:8080",
	})

	if cfg.Mode != ModeObserve {
		t.Fatalf("Mode = %q, want %q", cfg.Mode, ModeObserve)
	}
	if cfg.PollInterval != 15*time.Second {
		t.Fatalf("PollInterval = %s, want 15s", cfg.PollInterval)
	}
	if cfg.SlurmExecTimeout != 5*time.Second {
		t.Fatalf("SlurmExecTimeout = %s, want 5s", cfg.SlurmExecTimeout)
	}
	if cfg.LogMetadataRefreshInterval != 5*time.Minute {
		t.Fatalf(
			"LogMetadataRefreshInterval = %s, want 5m",
			cfg.LogMetadataRefreshInterval,
		)
	}
	if cfg.LogMetadataEnrichmentTimeout != 2*time.Second {
		t.Fatalf(
			"LogMetadataEnrichmentTimeout = %s, want 2s",
			cfg.LogMetadataEnrichmentTimeout,
		)
	}
	if cfg.JobUpsertWorkerPoolSize != 8 {
		t.Fatalf("JobUpsertWorkerPoolSize = %d, want 8", cfg.JobUpsertWorkerPoolSize)
	}
	if cfg.AccountingWorkerPoolSize != 8 {
		t.Fatalf("AccountingWorkerPoolSize = %d, want 8", cfg.AccountingWorkerPoolSize)
	}
	if cfg.AccountingTimeout != 5*time.Minute {
		t.Fatalf("AccountingTimeout = %s, want 5m", cfg.AccountingTimeout)
	}
	if cfg.FailedJobRecoveryTimeout != 30*time.Second {
		t.Fatalf("FailedJobRecoveryTimeout = %s, want 30s", cfg.FailedJobRecoveryTimeout)
	}
	if cfg.BootstrapFailureLookback != 30*time.Minute {
		t.Fatalf("BootstrapFailureLookback = %s, want 30m", cfg.BootstrapFailureLookback)
	}
	if cfg.AdviceLoopTimeout <= 0 {
		t.Fatalf("AdviceLoopTimeout = %s, want positive duration", cfg.AdviceLoopTimeout)
	}
	if cfg.AdviceItemTimeout <= 0 {
		t.Fatalf("AdviceItemTimeout = %s, want positive duration", cfg.AdviceItemTimeout)
	}
	if cfg.AdviceMaxAge != 5*time.Minute {
		t.Fatalf("AdviceMaxAge = %s, want 5m", cfg.AdviceMaxAge)
	}
	if cfg.AdviceRequeueHoldDelay != time.Second {
		t.Fatalf("AdviceRequeueHoldDelay = %s, want 1s", cfg.AdviceRequeueHoldDelay)
	}
	if cfg.LogLevel != "info" {
		t.Fatalf("LogLevel = %q, want info", cfg.LogLevel)
	}
	if !cfg.MetricsEnabled {
		t.Fatal("MetricsEnabled = false, want true by default")
	}
	if cfg.MetricsAddr != ":9090" {
		t.Fatalf("MetricsAddr = %q, want :9090", cfg.MetricsAddr)
	}
	if cfg.AuditTrailDestination != "logging" {
		t.Fatalf("AuditTrailDestination = %q, want logging", cfg.AuditTrailDestination)
	}
	if cfg.AuditTrailMaxBatchSize != 100 {
		t.Fatalf("AuditTrailMaxBatchSize = %d, want 100", cfg.AuditTrailMaxBatchSize)
	}
	if cfg.AuditTrailBufferSize != 1000 {
		t.Fatalf("AuditTrailBufferSize = %d, want 1000", cfg.AuditTrailBufferSize)
	}
	if cfg.AuditTrailFlushInterval != 30*time.Second {
		t.Fatalf("AuditTrailFlushInterval = %s, want 30s", cfg.AuditTrailFlushInterval)
	}
	if cfg.ServiceAccountTokenPath != "" {
		t.Fatalf("ServiceAccountTokenPath = %q, want disabled by default", cfg.ServiceAccountTokenPath)
	}
	if cfg.LogLinks.Enabled {
		t.Fatal("LogLinks.Enabled = true, want disabled by default")
	}
	wantLogLinks := LogLinksConfig{
		ScanInterval:           time.Minute,
		CleanupInterval:        time.Hour,
		Retention:              24 * time.Hour,
		ScanTimeout:            5 * time.Second,
		MaxPathLength:          4096,
		MaxPathsPerSync:        4096,
		MaxPatterns:            32,
		MaxScanDirectories:     64,
		MaxEntriesPerDirectory: 4096,
		MaxMatchesPerScan:      512,
		MaxOwnedLinks:          16384,
	}
	if !reflect.DeepEqual(cfg.LogLinks, wantLogLinks) {
		t.Fatalf("LogLinks = %#v, want %#v", cfg.LogLinks, wantLogLinks)
	}
}

func TestFromEnvEagerRequeueRequiresExplicitOptIn(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   string
		set     bool
		want    bool
		wantErr bool
	}{
		{name: "absent"},
		{name: "disabled", value: "false", set: true},
		{name: "enabled", value: "true", set: true, want: true},
		{name: "invalid", value: "sometimes", set: true, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			values := map[string]string{"HEIMDALL_ENDPOINT": "127.0.0.1:8080"}
			if tt.set {
				values["SLURM_ADVISOR_ENABLE_EAGER_REQUEUE"] = tt.value
			}
			setConfigEnv(t, values)
			cfg, err := FromEnv()
			if tt.wantErr {
				if err == nil {
					t.Fatal("FromEnv() succeeded for an invalid recovery flag")
				}
				return
			}
			if err != nil {
				t.Fatalf("FromEnv(): %v", err)
			}
			if cfg.EnableEagerRequeue != tt.want {
				t.Fatalf("EnableEagerRequeue = %t, want %t", cfg.EnableEagerRequeue, tt.want)
			}
		})
	}
}

// Bootstrap must have a bounded recovery window, including after defaults are
// reapplied; explicitly invalid operator settings must fail startup.
func TestFromEnvBootstrapFailureLookback(t *testing.T) {
	for _, tt := range []struct {
		name          string
		value         string
		set           bool
		want          time.Duration
		decodeError   bool
		validateError bool
	}{
		{name: "absent", want: 30 * time.Minute},
		{name: "custom", value: "45m", set: true, want: 45 * time.Minute},
		{name: "zero", value: "0s", set: true, validateError: true},
		{name: "negative", value: "-1m", set: true, want: -time.Minute, validateError: true},
		{name: "empty", set: true, decodeError: true},
		{name: "malformed", value: "recent", set: true, decodeError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			values := map[string]string{"HEIMDALL_ENDPOINT": "127.0.0.1:8080"}
			if tt.set {
				values["SLURM_ADVISOR_BOOTSTRAP_FAILURE_LOOKBACK"] = tt.value
			}
			setConfigEnv(t, values)
			cfg, err := FromEnv()
			if tt.decodeError {
				if err == nil {
					t.Fatal("FromEnv() accepted an invalid bootstrap failure lookback")
				}
				return
			}
			if err != nil {
				t.Fatalf("FromEnv(): %v", err)
			}
			cfg = WithDefaults(cfg)
			if cfg.BootstrapFailureLookback != tt.want {
				t.Fatalf("BootstrapFailureLookback = %s, want %s", cfg.BootstrapFailureLookback, tt.want)
			}
			if err := cfg.Validate(); (err != nil) != tt.validateError {
				t.Fatalf("Validate() = %v, want error: %t", err, tt.validateError)
			}
		})
	}
}

func TestActModeRequiresSchedulerActionAcknowledgement(t *testing.T) {
	cfg := WithDefaults(Config{
		HeimdallEndpoint: "127.0.0.1:8080",
		Mode:             ModeAct,
	})

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() succeeded for unsafe action mode, want error")
	}

	cfg.Safety = SafetyChecks{
		AcknowledgeSchedulerActions: true,
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with action safety inputs failed: %v", err)
	}
}

func TestIgnoreModeDoesNotRequireSchedulerActionAcknowledgement(t *testing.T) {
	cfg := WithDefaults(Config{
		HeimdallEndpoint: "127.0.0.1:8080",
		Mode:             ModeIgnore,
	})

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() failed for ignore mode: %v", err)
	}
}

func TestFromEnvLoadsIgnoreMode(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"SLURM_ADVISOR_MODE": string(ModeIgnore),
	})

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if cfg.Mode != ModeIgnore {
		t.Fatalf("Mode = %q, want %q", cfg.Mode, ModeIgnore)
	}
}

func TestFromEnvReadsProcessEnvironment(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"HEIMDALL_ENDPOINT":           "127.0.0.1:8080",
		"SLURM_ADVISOR_POLL_INTERVAL": "20s",
	})

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if cfg.HeimdallEndpoint != "127.0.0.1:8080" {
		t.Fatalf("HeimdallEndpoint = %q, want 127.0.0.1:8080", cfg.HeimdallEndpoint)
	}
	if cfg.PollInterval != 20*time.Second {
		t.Fatalf("PollInterval = %s, want 20s", cfg.PollInterval)
	}
	if cfg.AdviceLoopTimeout != 20*time.Second {
		t.Fatalf("AdviceLoopTimeout = %s, want 20s", cfg.AdviceLoopTimeout)
	}
	if cfg.AdviceItemTimeout != 5*time.Second {
		t.Fatalf("AdviceItemTimeout = %s, want 5s", cfg.AdviceItemTimeout)
	}
	if !cfg.MetricsEnabled {
		t.Fatal("MetricsEnabled = false, want true when env is unset")
	}
	if cfg.MetricsAddr != ":9090" {
		t.Fatalf("MetricsAddr = %q, want :9090 when env is unset", cfg.MetricsAddr)
	}
}

func TestFromEnvLoadsAuditTrailConfiguration(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"HEIMDALL_ENDPOINT":                        "127.0.0.1:8080",
		"SLURM_ADVISOR_AUDIT_TRAIL_MAX_BATCH_SIZE": "25",
		"SLURM_ADVISOR_AUDIT_TRAIL_BUFFER_SIZE":    "250",
		"SLURM_ADVISOR_AUDIT_TRAIL_FLUSH_INTERVAL": "5s",
	})

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if cfg.AuditTrailMaxBatchSize != 25 {
		t.Fatalf("AuditTrailMaxBatchSize = %d, want 25", cfg.AuditTrailMaxBatchSize)
	}
	if cfg.AuditTrailBufferSize != 250 {
		t.Fatalf("AuditTrailBufferSize = %d, want 250", cfg.AuditTrailBufferSize)
	}
	if cfg.AuditTrailFlushInterval != 5*time.Second {
		t.Fatalf("AuditTrailFlushInterval = %s, want 5s", cfg.AuditTrailFlushInterval)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}
}

func TestFromEnvLoadsMilestoneReportingConfiguration(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"HEIMDALL_ENDPOINT":                        "127.0.0.1:8080",
		"SLURM_ADVISOR_MILESTONE_QUEUE_CAPACITY":   "1024",
		"SLURM_ADVISOR_MILESTONE_RPC_TIMEOUT":      "5s",
		"SLURM_ADVISOR_MILESTONE_MAX_ATTEMPTS":     "5",
		"SLURM_ADVISOR_MILESTONE_RETRY_DELAY":      "500ms",
		"SLURM_ADVISOR_MILESTONE_SHUTDOWN_TIMEOUT": "10s",
	})

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	want := MilestoneReportingConfig{
		QueueCapacity:   1024,
		RPCTimeout:      5 * time.Second,
		MaxAttempts:     5,
		RetryDelay:      500 * time.Millisecond,
		ShutdownTimeout: 10 * time.Second,
	}
	if cfg.MilestoneReporting != want {
		t.Fatalf("MilestoneReporting = %+v, want %+v", cfg.MilestoneReporting, want)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}
}

// An explicit zero would disable reporting or shutdown draining, so it must be
// rejected rather than silently replaced by a default.
func TestFromEnvRejectsExplicitZeroMilestoneReportingSettings(t *testing.T) {
	for _, key := range []string{
		"SLURM_ADVISOR_MILESTONE_QUEUE_CAPACITY",
		"SLURM_ADVISOR_MILESTONE_RPC_TIMEOUT",
		"SLURM_ADVISOR_MILESTONE_MAX_ATTEMPTS",
		"SLURM_ADVISOR_MILESTONE_RETRY_DELAY",
		"SLURM_ADVISOR_MILESTONE_SHUTDOWN_TIMEOUT",
	} {
		t.Run(key, func(t *testing.T) {
			setConfigEnv(t, map[string]string{"HEIMDALL_ENDPOINT": "127.0.0.1:8080", key: "0"})

			cfg, err := FromEnv()
			if err != nil {
				t.Fatalf("FromEnv() failed: %v", err)
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() succeeded for explicit zero, want error")
			}
		})
	}
}

func TestFromEnvPreservesExplicitZeroAuditTrailSettingsForValidation(t *testing.T) {
	settings := map[string]string{
		"SLURM_ADVISOR_AUDIT_TRAIL_MAX_BATCH_SIZE": "0",
		"SLURM_ADVISOR_AUDIT_TRAIL_BUFFER_SIZE":    "0",
		"SLURM_ADVISOR_AUDIT_TRAIL_FLUSH_INTERVAL": "0s",
	}
	for key, value := range settings {
		t.Run(key, func(t *testing.T) {
			setConfigEnv(t, map[string]string{
				"HEIMDALL_ENDPOINT": "127.0.0.1:8080",
				key:                 value,
			})

			cfg, err := FromEnv()
			if err != nil {
				t.Fatalf("FromEnv() failed: %v", err)
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() succeeded for explicit zero, want error")
			}
		})
	}
}

func TestFromEnvReadsServiceAccountTokenPath(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"SLURM_ADVISOR_SERVICE_ACCOUNT_TOKEN_PATH": "/var/run/secrets/tokens/heimdall",
	})

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if cfg.ServiceAccountTokenPath != "/var/run/secrets/tokens/heimdall" {
		t.Fatalf("ServiceAccountTokenPath = %q, want projected token path", cfg.ServiceAccountTokenPath)
	}
}

func TestFromEnvRejectsExplicitEmptyServiceAccountTokenPath(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"SLURM_ADVISOR_SERVICE_ACCOUNT_TOKEN_PATH": "   ",
	})

	_, err := FromEnv()
	if err == nil {
		t.Fatal("FromEnv() succeeded, want error")
	}
	if !strings.Contains(err.Error(), "SLURM_ADVISOR_SERVICE_ACCOUNT_TOKEN_PATH must not be empty") {
		t.Fatalf("FromEnv() error = %q, want empty token-path error", err)
	}
}

func TestFromEnvIgnoresRemovedEmbeddedWatchdogVariables(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"SLURM_ADVISOR_WATCHDOG_FAIL_OPEN_ENABLED":    "not-a-bool",
		"SLURM_ADVISOR_WATCHDOG_RELEASE_TIMEOUT":      "not-a-duration",
		"SLURM_ADVISOR_WATCHDOG_CLEANUP_GRACE_PERIOD": "not-a-duration",
	})

	if _, err := FromEnv(); err != nil {
		t.Fatalf("FromEnv() failed for removed watchdog variables: %v", err)
	}
}

func TestFromEnvPreservesDurationDecodeCause(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"SLURM_ADVISOR_POLL_INTERVAL": "not-a-duration",
	})

	_, err := FromEnv()
	if err == nil {
		t.Fatal("FromEnv() succeeded, want a decoding error")
	}
	for _, want := range []string{"SLURM_ADVISOR_POLL_INTERVAL", "PollInterval", "time.Duration"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("FromEnv() error = %q, want context %q", err, want)
		}
	}
	for _, sensitive := range []string{"not-a-duration", "time: invalid duration"} {
		if strings.Contains(err.Error(), sensitive) {
			t.Fatalf("FromEnv() error exposed decoder detail %q: %q", sensitive, err)
		}
	}
	_, wantCause := time.ParseDuration("not-a-duration")
	if got := deepestErrorCause(err); got.Error() != wantCause.Error() {
		t.Fatalf("FromEnv() root cause = %q, want %q", got, wantCause)
	}
}

func TestWrapEnvconfigErrorPreservesCauses(t *testing.T) {
	t.Run("parse error", func(t *testing.T) {
		cause := errors.New("decoder exposed secret")
		err := wrapEnvconfigError(&envconfig.ParseError{
			KeyName:   "SETTING",
			FieldName: "Setting",
			TypeName:  "string",
			Value:     "do-not-echo",
			Err:       cause,
		})

		if !errors.Is(err, cause) {
			t.Fatalf("wrapEnvconfigError() error = %v, want original cause", err)
		}
		for _, want := range []string{"SETTING", "Setting", "string"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("wrapEnvconfigError() error = %q, want context %q", err, want)
			}
		}
		for _, sensitive := range []string{"do-not-echo", "decoder exposed secret"} {
			if strings.Contains(err.Error(), sensitive) {
				t.Fatalf("wrapEnvconfigError() error exposed %q: %q", sensitive, err)
			}
		}
	})

	t.Run("non-parse error", func(t *testing.T) {
		cause := errors.New("invalid specification")
		err := wrapEnvconfigError(cause)

		if !errors.Is(err, cause) {
			t.Fatalf("wrapEnvconfigError() error = %v, want original cause", err)
		}
		if !strings.Contains(err.Error(), "decode environment configuration") {
			t.Fatalf("wrapEnvconfigError() error = %q, want configuration context", err)
		}
	})
}

func TestFromEnvPreservesBoolDecodeCause(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"SLURM_ADVISOR_UPDATE_ELIGIBLE_TIME_AFTER_REQUEUE": "not-a-bool",
	})

	_, err := FromEnv()
	if err == nil {
		t.Fatal("FromEnv() succeeded, want a decoding error")
	}
	var numErr *strconv.NumError
	if !errors.As(err, &numErr) {
		t.Fatalf("FromEnv() error = %v, want wrapped *strconv.NumError", err)
	}
	for _, want := range []string{
		"SLURM_ADVISOR_UPDATE_ELIGIBLE_TIME_AFTER_REQUEUE",
		"UpdateEligibleTimeAfterRequeue",
		"bool",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("FromEnv() error = %q, want context %q", err, want)
		}
	}
	for _, sensitive := range []string{"not-a-bool", "strconv.ParseBool", "invalid syntax"} {
		if strings.Contains(err.Error(), sensitive) {
			t.Fatalf("FromEnv() error exposed decoder detail %q: %q", sensitive, err)
		}
	}
}

func TestFromEnvErrorLogsWithoutVerboseCause(t *testing.T) {
	const invalidValue = "secret-invalid-bool"
	setConfigEnv(t, map[string]string{
		"SLURM_ADVISOR_UPDATE_ELIGIBLE_TIME_AFTER_REQUEUE": invalidValue,
	})

	_, err := FromEnv()
	if err == nil {
		t.Fatal("FromEnv() succeeded, want a decoding error")
	}

	var output bytes.Buffer
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(&output),
		zapcore.ErrorLevel,
	)
	logger := zap.New(core)
	logger.Error("load configuration", zap.Error(err))

	var entry map[string]any
	if unmarshalErr := json.Unmarshal(output.Bytes(), &entry); unmarshalErr != nil {
		t.Fatalf("decode Zap entry %q: %v", output.String(), unmarshalErr)
	}
	loggedError, ok := entry["error"].(string)
	if !ok {
		t.Fatalf("Zap entry error = %#v, want string", entry["error"])
	}
	for _, want := range []string{
		"SLURM_ADVISOR_UPDATE_ELIGIBLE_TIME_AFTER_REQUEUE",
		"UpdateEligibleTimeAfterRequeue",
		"bool",
	} {
		if !strings.Contains(loggedError, want) {
			t.Fatalf("Zap error = %q, want context %q", loggedError, want)
		}
	}
	for _, sensitive := range []string{invalidValue, "strconv.ParseBool", "invalid syntax"} {
		if strings.Contains(loggedError, sensitive) {
			t.Fatalf("Zap error exposed decoder detail %q: %q", sensitive, loggedError)
		}
	}
	if verbose, ok := entry["errorVerbose"]; ok {
		t.Fatalf("Zap entry contains errorVerbose = %q", verbose)
	}
}

func TestEnvironmentConfigDelegatesDefaultsToWithDefaults(t *testing.T) {
	typeOfConfig := reflect.TypeFor[environmentConfig]()
	for i := range typeOfConfig.NumField() {
		field := typeOfConfig.Field(i)
		if value, ok := field.Tag.Lookup("default"); ok {
			t.Errorf("environmentConfig.%s default tag = %q, want defaults owned by WithDefaults", field.Name, value)
		}
	}
}

func TestFromEnvLoadsEndpointAndDurations(t *testing.T) {
	values := map[string]string{
		"HEIMDALL_ENDPOINT":                                "127.0.0.1:8080",
		"SLURM_ADVISOR_MODE":                               string(ModeObserve),
		"SLURM_ADVISOR_LOG_LEVEL":                          " DEBUG ",
		"SLURM_ADVISOR_POLL_INTERVAL":                      "20s",
		"SLURM_EXEC_TIMEOUT":                               "45s",
		"SLURM_ADVISOR_LOG_METADATA_REFRESH_INTERVAL":      "6m",
		"SLURM_ADVISOR_LOG_METADATA_ENRICHMENT_TIMEOUT":    "8s",
		"SLURM_ADVISOR_JOB_UPSERT_WORKER_POOL_SIZE":        "7",
		"SLURM_ADVISOR_ACCOUNTING_WORKER_POOL_SIZE":        "6",
		"SLURM_ADVISOR_ACCOUNTING_TIMEOUT":                 "4m",
		"SLURM_ADVISOR_FAILED_JOB_RECOVERY_TIMEOUT":        "18s",
		"SLURM_ADVISOR_ADVICE_LOOP_TIMEOUT":                "12s",
		"SLURM_ADVISOR_ADVICE_ITEM_TIMEOUT":                "3s",
		"SLURM_ADVISOR_ADVICE_MAX_AGE":                     "7m",
		"SLURM_ADVISOR_REQUEUE_HOLD_DELAY":                 "250ms",
		"SLURM_ADVISOR_ENVIRONMENT":                        "dev",
		"SLURM_ADVISOR_METRICS_ENABLED":                    "false",
		"SLURM_ADVISOR_METRICS_ADDR":                       "127.0.0.1:19090",
		"SLURM_ADVISOR_VERSIONED_SQUEUE_JSON":              "true",
		"SLURM_ADVISOR_UPDATE_ELIGIBLE_TIME_AFTER_REQUEUE": "true",
	}
	setConfigEnv(t, values)

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}

	if cfg.HeimdallEndpoint != "127.0.0.1:8080" {
		t.Fatalf("HeimdallEndpoint = %q, want 127.0.0.1:8080", cfg.HeimdallEndpoint)
	}
	if cfg.PollInterval != 20*time.Second {
		t.Fatalf("PollInterval = %s, want 20s", cfg.PollInterval)
	}
	if cfg.SlurmExecTimeout != 45*time.Second {
		t.Fatalf("SlurmExecTimeout = %s, want 45s", cfg.SlurmExecTimeout)
	}
	if cfg.LogMetadataRefreshInterval != 6*time.Minute {
		t.Fatalf(
			"LogMetadataRefreshInterval = %s, want 6m",
			cfg.LogMetadataRefreshInterval,
		)
	}
	if cfg.LogMetadataEnrichmentTimeout != 8*time.Second {
		t.Fatalf(
			"LogMetadataEnrichmentTimeout = %s, want 8s",
			cfg.LogMetadataEnrichmentTimeout,
		)
	}
	if cfg.JobUpsertWorkerPoolSize != 7 {
		t.Fatalf("JobUpsertWorkerPoolSize = %d, want 7", cfg.JobUpsertWorkerPoolSize)
	}
	if cfg.AccountingWorkerPoolSize != 6 {
		t.Fatalf("AccountingWorkerPoolSize = %d, want 6", cfg.AccountingWorkerPoolSize)
	}
	if cfg.AccountingTimeout != 4*time.Minute {
		t.Fatalf("AccountingTimeout = %s, want 4m", cfg.AccountingTimeout)
	}
	if cfg.FailedJobRecoveryTimeout != 18*time.Second {
		t.Fatalf("FailedJobRecoveryTimeout = %s, want 18s", cfg.FailedJobRecoveryTimeout)
	}
	if cfg.AdviceLoopTimeout != 12*time.Second {
		t.Fatalf("AdviceLoopTimeout = %s, want 12s", cfg.AdviceLoopTimeout)
	}
	if cfg.AdviceItemTimeout != 3*time.Second {
		t.Fatalf("AdviceItemTimeout = %s, want 3s", cfg.AdviceItemTimeout)
	}
	if cfg.AdviceMaxAge != 7*time.Minute {
		t.Fatalf("AdviceMaxAge = %s, want 7m", cfg.AdviceMaxAge)
	}
	if cfg.AdviceRequeueHoldDelay != 250*time.Millisecond {
		t.Fatalf("AdviceRequeueHoldDelay = %s, want 250ms", cfg.AdviceRequeueHoldDelay)
	}
	if cfg.Environment != "dev" {
		t.Fatalf("Environment = %q, want dev", cfg.Environment)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("LogLevel = %q, want debug", cfg.LogLevel)
	}
	if cfg.MetricsEnabled {
		t.Fatal("MetricsEnabled = true, want false from env")
	}
	if cfg.MetricsAddr != "127.0.0.1:19090" {
		t.Fatalf("MetricsAddr = %q, want 127.0.0.1:19090", cfg.MetricsAddr)
	}
	if !cfg.VersionedSqueueJSON {
		t.Fatal("VersionedSqueueJSON = false, want true")
	}
	if !cfg.UpdateEligibleTimeAfterRequeue {
		t.Fatal("UpdateEligibleTimeAfterRequeue = false, want true")
	}
}

func TestFromEnvLoadsSchedulerActionAcknowledgement(t *testing.T) {
	values := map[string]string{
		"HEIMDALL_ENDPOINT":                           "127.0.0.1:8080",
		"SLURM_ADVISOR_MODE":                          string(ModeAct),
		"SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS": "true",
	}
	setConfigEnv(t, values)

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with act mode safety env failed: %v", err)
	}
	if !cfg.Safety.AcknowledgeSchedulerActions {
		t.Fatal("AcknowledgeSchedulerActions = false, want true")
	}
}

func TestFromEnvLoadsLogLinksConfiguration(t *testing.T) {
	values := map[string]string{
		"HEIMDALL_ENDPOINT":                                 "127.0.0.1:8080",
		"SLURM_ADVISOR_LOG_LINKS_ENABLED":                   "true",
		"SLURM_ADVISOR_LOG_LINKS_DESTINATION":               "/var/log/heimdall/links",
		"SLURM_ADVISOR_LOG_LINKS_SOURCE_ROOTS_JSON":         `["/work/a","/work/b,with-comma"]`,
		"SLURM_ADVISOR_LOG_LINKS_APPLICATION_PATTERNS_JSON": `["^app-{job_id}\\\\.log$","^worker-[0-9]+\\\\.log$"]`,
		"SLURM_ADVISOR_LOG_LINKS_SCAN_INTERVAL":             "2m",
		"SLURM_ADVISOR_LOG_LINKS_CLEANUP_INTERVAL":          "3h",
		"SLURM_ADVISOR_LOG_LINKS_RETENTION":                 "36h",
		"SLURM_ADVISOR_LOG_LINKS_SCAN_TIMEOUT":              "7s",
		"SLURM_ADVISOR_LOG_LINKS_MAX_PATH_LENGTH":           "3000",
		"SLURM_ADVISOR_LOG_LINKS_MAX_PATHS_PER_SYNC":        "3001",
		"SLURM_ADVISOR_LOG_LINKS_MAX_PATTERNS":              "30",
		"SLURM_ADVISOR_LOG_LINKS_MAX_SCAN_DIRECTORIES":      "31",
		"SLURM_ADVISOR_LOG_LINKS_MAX_ENTRIES_PER_DIRECTORY": "3002",
		"SLURM_ADVISOR_LOG_LINKS_MAX_MATCHES_PER_SCAN":      "3003",
		"SLURM_ADVISOR_LOG_LINKS_MAX_OWNED_LINKS":           "3004",
	}
	setConfigEnv(t, values)

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}

	want := LogLinksConfig{
		Enabled:                      true,
		Destination:                  "/var/log/heimdall/links",
		SourceRoots:                  []string{"/work/a", "/work/b,with-comma"},
		ApplicationPatterns:          []string{`^app-{job_id}\\.log$`, `^worker-[0-9]+\\.log$`},
		ScanInterval:                 2 * time.Minute,
		CleanupInterval:              3 * time.Hour,
		Retention:                    36 * time.Hour,
		ScanTimeout:                  7 * time.Second,
		MaxPathLength:                3000,
		MaxPathsPerSync:              3001,
		MaxPatterns:                  30,
		MaxScanDirectories:           31,
		MaxEntriesPerDirectory:       3002,
		MaxMatchesPerScan:            3003,
		MaxOwnedLinks:                3004,
		scanIntervalEnvSet:           true,
		cleanupIntervalEnvSet:        true,
		retentionEnvSet:              true,
		scanTimeoutEnvSet:            true,
		maxPathLengthEnvSet:          true,
		maxPathsPerSyncEnvSet:        true,
		maxPatternsEnvSet:            true,
		maxScanDirectoriesEnvSet:     true,
		maxEntriesPerDirectoryEnvSet: true,
		maxMatchesPerScanEnvSet:      true,
		maxOwnedLinksEnvSet:          true,
	}
	if !reflect.DeepEqual(cfg.LogLinks, want) {
		t.Fatalf("LogLinks = %#v, want %#v", cfg.LogLinks, want)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}
}

func TestFromEnvPreservesExplicitZeroLogLinksSettingsForValidation(t *testing.T) {
	settings := map[string]string{
		"SLURM_ADVISOR_LOG_LINKS_SCAN_INTERVAL":             "0s",
		"SLURM_ADVISOR_LOG_LINKS_CLEANUP_INTERVAL":          "0s",
		"SLURM_ADVISOR_LOG_LINKS_RETENTION":                 "0s",
		"SLURM_ADVISOR_LOG_LINKS_SCAN_TIMEOUT":              "0s",
		"SLURM_ADVISOR_LOG_LINKS_MAX_PATH_LENGTH":           "0",
		"SLURM_ADVISOR_LOG_LINKS_MAX_PATHS_PER_SYNC":        "0",
		"SLURM_ADVISOR_LOG_LINKS_MAX_PATTERNS":              "0",
		"SLURM_ADVISOR_LOG_LINKS_MAX_SCAN_DIRECTORIES":      "0",
		"SLURM_ADVISOR_LOG_LINKS_MAX_ENTRIES_PER_DIRECTORY": "0",
		"SLURM_ADVISOR_LOG_LINKS_MAX_MATCHES_PER_SCAN":      "0",
		"SLURM_ADVISOR_LOG_LINKS_MAX_OWNED_LINKS":           "0",
	}
	for key, value := range settings {
		t.Run(key, func(t *testing.T) {
			setConfigEnv(t, map[string]string{
				"HEIMDALL_ENDPOINT":                         "127.0.0.1:8080",
				"SLURM_ADVISOR_LOG_LINKS_ENABLED":           "true",
				"SLURM_ADVISOR_LOG_LINKS_DESTINATION":       "/var/log/heimdall/links",
				"SLURM_ADVISOR_LOG_LINKS_SOURCE_ROOTS_JSON": `["/work"]`,
				key: value,
			})

			cfg, err := FromEnv()
			if err != nil {
				t.Fatalf("FromEnv() failed: %v", err)
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() succeeded for explicit zero, want error")
			}
		})
	}
}

func TestFromEnvRejectsInvalidLogLinksJSONArrays(t *testing.T) {
	for _, key := range []string{
		"SLURM_ADVISOR_LOG_LINKS_SOURCE_ROOTS_JSON",
		"SLURM_ADVISOR_LOG_LINKS_APPLICATION_PATTERNS_JSON",
	} {
		t.Run(key, func(t *testing.T) {
			setConfigEnv(t, map[string]string{key: `"not-an-array"`})

			_, err := FromEnv()
			if err == nil {
				t.Fatal("FromEnv() succeeded, want error")
			}
			if !strings.Contains(err.Error(), key) {
				t.Fatalf("FromEnv() error = %q, want key %q", err, key)
			}
			if strings.Contains(err.Error(), "not-an-array") {
				t.Fatalf("FromEnv() error exposed configured value: %q", err)
			}
		})
	}
}

func TestFromEnvRejectsInvalidLogLinksTypedSettings(t *testing.T) {
	for key, value := range map[string]string{
		"SLURM_ADVISOR_LOG_LINKS_SCAN_INTERVAL":   "not-a-duration",
		"SLURM_ADVISOR_LOG_LINKS_MAX_PATH_LENGTH": "not-an-integer",
	} {
		t.Run(key, func(t *testing.T) {
			setConfigEnv(t, map[string]string{key: value})

			_, err := FromEnv()
			if err == nil {
				t.Fatal("FromEnv() succeeded, want error")
			}
			if !strings.Contains(err.Error(), key) {
				t.Fatalf("FromEnv() error = %q, want key %q", err, key)
			}
			if strings.Contains(err.Error(), value) {
				t.Fatalf("FromEnv() error exposed configured value: %q", err)
			}
		})
	}
}

func TestValidateLogLinksConfiguration(t *testing.T) {
	valid := WithDefaults(Config{
		HeimdallEndpoint: "127.0.0.1:8080",
		LogLinks: LogLinksConfig{
			Enabled:     true,
			Destination: "/var/log/heimdall/links",
			SourceRoots: []string{"/work"},
		},
	})
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid configuration failed: %v", err)
	}

	tests := map[string]func(*Config){
		"empty destination": func(cfg *Config) { cfg.LogLinks.Destination = " " },
		"missing roots":     func(cfg *Config) { cfg.LogLinks.SourceRoots = nil },
		"empty root":        func(cfg *Config) { cfg.LogLinks.SourceRoots = []string{"/work", " "} },
		"scan interval":     func(cfg *Config) { cfg.LogLinks.ScanInterval = -time.Second },
		"cleanup interval":  func(cfg *Config) { cfg.LogLinks.CleanupInterval = -time.Second },
		"retention":         func(cfg *Config) { cfg.LogLinks.Retention = -time.Second },
		"scan timeout":      func(cfg *Config) { cfg.LogLinks.ScanTimeout = -time.Second },
		"max path length":   func(cfg *Config) { cfg.LogLinks.MaxPathLength = -1 },
		"max paths":         func(cfg *Config) { cfg.LogLinks.MaxPathsPerSync = -1 },
		"max patterns":      func(cfg *Config) { cfg.LogLinks.MaxPatterns = -1 },
		"max directories":   func(cfg *Config) { cfg.LogLinks.MaxScanDirectories = -1 },
		"max entries":       func(cfg *Config) { cfg.LogLinks.MaxEntriesPerDirectory = -1 },
		"max matches":       func(cfg *Config) { cfg.LogLinks.MaxMatchesPerScan = -1 },
		"max owned":         func(cfg *Config) { cfg.LogLinks.MaxOwnedLinks = -1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			cfg.LogLinks.SourceRoots = slices.Clone(valid.LogLinks.SourceRoots)
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() succeeded, want error")
			}
		})
	}
}

func TestValidateDisabledLogLinksDoesNotInspectFilesystemSettings(t *testing.T) {
	cfg := WithDefaults(Config{
		HeimdallEndpoint: "127.0.0.1:8080",
		LogLinks: LogLinksConfig{
			Destination:            " ",
			SourceRoots:            []string{""},
			ScanInterval:           -time.Second,
			CleanupInterval:        -time.Second,
			Retention:              -time.Second,
			ScanTimeout:            -time.Second,
			MaxPathLength:          -1,
			MaxPathsPerSync:        -1,
			MaxPatterns:            -1,
			MaxScanDirectories:     -1,
			MaxEntriesPerDirectory: -1,
			MaxMatchesPerScan:      -1,
			MaxOwnedLinks:          -1,
		},
	})

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() failed for disabled log links: %v", err)
	}
}

func TestEnvironmentConfigLogLinksTags(t *testing.T) {
	want := map[string]string{
		"LogLinksEnabled":                "SLURM_ADVISOR_LOG_LINKS_ENABLED",
		"LogLinksDestination":            "SLURM_ADVISOR_LOG_LINKS_DESTINATION",
		"LogLinksSourceRoots":            "SLURM_ADVISOR_LOG_LINKS_SOURCE_ROOTS_JSON",
		"LogLinksApplicationPatterns":    "SLURM_ADVISOR_LOG_LINKS_APPLICATION_PATTERNS_JSON",
		"LogLinksScanInterval":           "SLURM_ADVISOR_LOG_LINKS_SCAN_INTERVAL",
		"LogLinksCleanupInterval":        "SLURM_ADVISOR_LOG_LINKS_CLEANUP_INTERVAL",
		"LogLinksRetention":              "SLURM_ADVISOR_LOG_LINKS_RETENTION",
		"LogLinksScanTimeout":            "SLURM_ADVISOR_LOG_LINKS_SCAN_TIMEOUT",
		"LogLinksMaxPathLength":          "SLURM_ADVISOR_LOG_LINKS_MAX_PATH_LENGTH",
		"LogLinksMaxPathsPerSync":        "SLURM_ADVISOR_LOG_LINKS_MAX_PATHS_PER_SYNC",
		"LogLinksMaxPatterns":            "SLURM_ADVISOR_LOG_LINKS_MAX_PATTERNS",
		"LogLinksMaxScanDirectories":     "SLURM_ADVISOR_LOG_LINKS_MAX_SCAN_DIRECTORIES",
		"LogLinksMaxEntriesPerDirectory": "SLURM_ADVISOR_LOG_LINKS_MAX_ENTRIES_PER_DIRECTORY",
		"LogLinksMaxMatchesPerScan":      "SLURM_ADVISOR_LOG_LINKS_MAX_MATCHES_PER_SCAN",
		"LogLinksMaxOwnedLinks":          "SLURM_ADVISOR_LOG_LINKS_MAX_OWNED_LINKS",
	}
	typ := reflect.TypeFor[environmentConfig]()
	for fieldName, tag := range want {
		field, ok := typ.FieldByName(fieldName)
		if !ok {
			t.Fatalf("environmentConfig.%s missing", fieldName)
		}
		if got := field.Tag.Get("envconfig"); got != tag {
			t.Fatalf("environmentConfig.%s envconfig tag = %q, want %q", fieldName, got, tag)
		}
	}
}

func TestEnvironmentConfigConcurrencyTags(t *testing.T) {
	want := map[string]string{
		"JobUpsertWorkerPoolSize":  "SLURM_ADVISOR_JOB_UPSERT_WORKER_POOL_SIZE",
		"AccountingWorkerPoolSize": "SLURM_ADVISOR_ACCOUNTING_WORKER_POOL_SIZE",
		"AccountingTimeout":        "SLURM_ADVISOR_ACCOUNTING_TIMEOUT",
	}
	typ := reflect.TypeFor[environmentConfig]()
	for fieldName, tag := range want {
		field, ok := typ.FieldByName(fieldName)
		if !ok {
			t.Fatalf("environmentConfig.%s missing", fieldName)
		}
		if got := field.Tag.Get("envconfig"); got != tag {
			t.Fatalf("environmentConfig.%s envconfig tag = %q, want %q", fieldName, got, tag)
		}
	}
}

func TestFromEnvPreservesExplicitZeroAdviceMaxAgeForValidation(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"HEIMDALL_ENDPOINT":            "127.0.0.1:8080",
		"SLURM_ADVISOR_ADVICE_MAX_AGE": "0s",
	})

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if cfg.AdviceMaxAge != 0 {
		t.Fatalf("AdviceMaxAge = %s, want explicit zero", cfg.AdviceMaxAge)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() succeeded for zero advice max age, want error")
	}
}

func TestFromEnvPreservesExplicitZeroLogMetadataRefreshForValidation(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"HEIMDALL_ENDPOINT":                           "127.0.0.1:8080",
		"SLURM_ADVISOR_LOG_METADATA_REFRESH_INTERVAL": "0s",
	})

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if cfg.LogMetadataRefreshInterval != 0 {
		t.Fatalf(
			"LogMetadataRefreshInterval = %s, want explicit zero",
			cfg.LogMetadataRefreshInterval,
		)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() succeeded for zero log metadata refresh interval, want error")
	}
}

func TestFromEnvPreservesExplicitZeroLogMetadataEnrichmentTimeoutForValidation(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"HEIMDALL_ENDPOINT":                             "127.0.0.1:8080",
		"SLURM_ADVISOR_LOG_METADATA_ENRICHMENT_TIMEOUT": "0s",
	})

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if cfg.LogMetadataEnrichmentTimeout != 0 {
		t.Fatalf(
			"LogMetadataEnrichmentTimeout = %s, want explicit zero",
			cfg.LogMetadataEnrichmentTimeout,
		)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() succeeded for zero log metadata enrichment timeout, want error")
	}
}

func TestFromEnvPreservesExplicitZeroConcurrencySettingsForValidation(t *testing.T) {
	for name, environment := range map[string]string{
		"job upsert worker pool size": "SLURM_ADVISOR_JOB_UPSERT_WORKER_POOL_SIZE",
		"accounting worker pool size": "SLURM_ADVISOR_ACCOUNTING_WORKER_POOL_SIZE",
	} {
		t.Run(name, func(t *testing.T) {
			setConfigEnv(t, map[string]string{
				"HEIMDALL_ENDPOINT": "127.0.0.1:8080",
				environment:         "0",
			})

			cfg, err := FromEnv()
			if err != nil {
				t.Fatalf("FromEnv() failed: %v", err)
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() succeeded for explicit zero, want error")
			}
		})
	}
}

func TestFromEnvPreservesExplicitZeroAccountingTimeoutForValidation(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"HEIMDALL_ENDPOINT":                "127.0.0.1:8080",
		"SLURM_ADVISOR_ACCOUNTING_TIMEOUT": "0s",
	})

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if cfg.AccountingTimeout != 0 {
		t.Fatalf("AccountingTimeout = %s, want explicit zero", cfg.AccountingTimeout)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() succeeded for zero accounting timeout, want error")
	}
}

func TestFromEnvPreservesNegativeAccountingTimeoutForValidation(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"HEIMDALL_ENDPOINT":                "127.0.0.1:8080",
		"SLURM_ADVISOR_ACCOUNTING_TIMEOUT": "-1s",
	})

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if cfg.AccountingTimeout != -time.Second {
		t.Fatalf("AccountingTimeout = %s, want -1s", cfg.AccountingTimeout)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() succeeded for negative accounting timeout, want error")
	}
}

// Invalid explicit budgets must fail startup instead of silently disabling or
// replacing the limit that keeps failed-job recovery from starving Advice.
func TestFromEnvRejectsInvalidFailedJobRecoveryTimeout(t *testing.T) {
	for _, value := range []string{"0s", "-1s", "", "invalid"} {
		t.Run(value, func(t *testing.T) {
			setConfigEnv(t, map[string]string{
				"HEIMDALL_ENDPOINT":                         "127.0.0.1:8080",
				"SLURM_ADVISOR_FAILED_JOB_RECOVERY_TIMEOUT": value,
			})
			cfg, err := FromEnv()
			if err == nil {
				err = cfg.Validate()
			}
			if err == nil {
				t.Fatalf("configuration accepted failed-job recovery timeout %q", value)
			}
		})
	}
}

func TestFromEnvParsesPartitionAllowlist(t *testing.T) {
	values := map[string]string{
		"HEIMDALL_ENDPOINT":                 "127.0.0.1:8080",
		"SLURM_ADVISOR_PARTITION_ALLOWLIST": "normal,debug, normal ,,gpu",
	}
	setConfigEnv(t, values)

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}

	if !slices.Equal(cfg.PartitionAllowlist, []string{"normal", "debug", "gpu"}) {
		t.Fatalf("PartitionAllowlist = %v, want normal,debug,gpu", cfg.PartitionAllowlist)
	}
}

func TestFromEnvUserAllowlist(t *testing.T) {
	stringPointer := func(value string) *string { return &value }
	for _, tt := range []struct {
		name      string
		value     *string
		want      []string
		wantError bool
	}{
		{name: "unset disables filter"},
		{name: "empty disables filter", value: stringPointer("")},
		{name: "whitespace disables filter", value: stringPointer("  ")},
		{name: "normalizes CSV without folding case", value: stringPointer("alice, bob,alice,,Alice"), want: []string{"alice", "bob", "Alice"}},
		{name: "separators cannot accidentally disable filter", value: stringPointer(", ,"), wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			values := map[string]string{"HEIMDALL_ENDPOINT": "127.0.0.1:8080"}
			if tt.value != nil {
				values["SLURM_ADVISOR_USER_ALLOWLIST"] = *tt.value
			}
			setConfigEnv(t, values)
			cfg, err := FromEnv()
			if (err != nil) != tt.wantError {
				t.Fatalf("FromEnv() error = %v, want error %v", err, tt.wantError)
			}
			if !tt.wantError && !slices.Equal(cfg.UserAllowlist, tt.want) {
				t.Fatalf("UserAllowlist = %v, want %v", cfg.UserAllowlist, tt.want)
			}
		})
	}
}

func TestValidateRejectsInvalidAdviceTimeouts(t *testing.T) {
	for name, cfg := range map[string]Config{
		"loop timeout": {
			HeimdallEndpoint:       "127.0.0.1:8080",
			AdviceLoopTimeout:      -time.Second,
			AdviceItemTimeout:      time.Second,
			AdviceRequeueHoldDelay: time.Second,
		},
		"item timeout": {
			HeimdallEndpoint:       "127.0.0.1:8080",
			AdviceLoopTimeout:      time.Second,
			AdviceItemTimeout:      -time.Second,
			AdviceRequeueHoldDelay: time.Second,
		},
		"requeue hold delay": {
			HeimdallEndpoint:       "127.0.0.1:8080",
			AdviceLoopTimeout:      time.Second,
			AdviceItemTimeout:      time.Second,
			AdviceRequeueHoldDelay: -time.Second,
		},
		"max age": {
			HeimdallEndpoint:       "127.0.0.1:8080",
			AdviceLoopTimeout:      time.Second,
			AdviceItemTimeout:      time.Second,
			AdviceMaxAge:           -time.Second,
			AdviceRequeueHoldDelay: time.Second,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg = WithDefaults(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() succeeded, want error")
			}
		})
	}
}

func TestFromEnvRejectsInvalidObservabilityValues(t *testing.T) {
	for name, values := range map[string]map[string]string{
		"invalid log level": {
			"SLURM_ADVISOR_LOG_LEVEL": "verbose",
		},
		"empty log level": {
			"SLURM_ADVISOR_LOG_LEVEL": "",
		},
		"invalid metrics enabled": {
			"SLURM_ADVISOR_METRICS_ENABLED": "sometimes",
		},
		"empty metrics address": {
			"SLURM_ADVISOR_METRICS_ADDR": "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			setConfigEnv(t, values)
			_, err := FromEnv()
			if err == nil {
				t.Fatal("FromEnv() succeeded, want error")
			}
		})
	}
}

func TestFromEnvLoadsOTLPTracingContract(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT":        "http://collector:4317",
		"OTEL_EXPORTER_OTLP_PROTOCOL":        "grpc",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://traces:4318/v1/traces",
	})

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if cfg.Tracing.Endpoint != "http://traces:4318/v1/traces" {
		t.Fatalf("Tracing.Endpoint = %q, want signal-specific endpoint", cfg.Tracing.Endpoint)
	}
	if cfg.Tracing.Protocol != "grpc" {
		t.Fatalf("Tracing.Protocol = %q, want grpc", cfg.Tracing.Protocol)
	}
}

// The shared OTLP endpoint is a base URL (as the log exporter treats it);
// OTLP/HTTP sends traces to /v1/traces under it, gRPC has no signal path, and
// a signal-specific endpoint is already the full trace URL.
func TestFromEnvResolvesOTLPTraceEndpoint(t *testing.T) {
	tests := []struct {
		name         string
		env          map[string]string
		want         string
		wantProtocol string
	}{
		{"shared http base", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318", "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf"}, "http://collector:4318/v1/traces", "http/protobuf"},
		{"shared http with prefix", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://gateway/otlp/", "OTEL_EXPORTER_OTLP_PROTOCOL": "http"}, "https://gateway/otlp/v1/traces", "http"},
		{"shared grpc", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317", "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, "http://collector:4317", "grpc"},
		// Logs default to grpc, so traces to the same collector must too.
		{"shared without protocol", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317"}, "http://collector:4317", "grpc"},
		{"trace protocol overrides shared", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318", "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc", "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "http/protobuf"}, "http://collector:4318/v1/traces", "http/protobuf"},
		{"signal-specific", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://traces:4318/custom"}, "http://traces:4318/custom", "http/protobuf"},
		{"export disabled", map[string]string{}, "", "grpc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.env["HEIMDALL_ENDPOINT"] = "127.0.0.1:8080"
			setConfigEnv(t, tt.env)
			cfg, err := FromEnv()
			if err != nil {
				t.Fatalf("FromEnv() failed: %v", err)
			}
			if cfg.Tracing.Endpoint != tt.want {
				t.Fatalf("Tracing.Endpoint = %q, want %q", cfg.Tracing.Endpoint, tt.want)
			}
			if cfg.Tracing.Protocol != tt.wantProtocol {
				t.Fatalf("Tracing.Protocol = %q, want %q", cfg.Tracing.Protocol, tt.wantProtocol)
			}
			if tt.env["OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"] == "" && tt.env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == "" &&
				cfg.Tracing.Protocol != cfg.OTLP.Protocol {
				t.Fatalf("traces use %q but logs use %q on the shared endpoint", cfg.Tracing.Protocol, cfg.OTLP.Protocol)
			}
		})
	}
}

func TestFromEnvPassesOTLPTokenToTraceExport(t *testing.T) {
	setConfigEnv(t, map[string]string{
		"HEIMDALL_ENDPOINT":           "127.0.0.1:8080",
		"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318",
		"OTEL_EXPORTER_OTLP_TOKEN":    "secret-token",
	})
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if cfg.Tracing.Token != "secret-token" {
		t.Fatal("Tracing.Token was not loaded from OTEL_EXPORTER_OTLP_TOKEN")
	}
}

func TestValidateDoesNotMakeOTLPTracingAControllerPrecondition(t *testing.T) {
	cfg := WithDefaults(Config{
		HeimdallEndpoint: "127.0.0.1:8080",
		Tracing: TracingConfig{
			Endpoint: "http://collector:4318",
			Protocol: "http/json",
		},
	})

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() failed for optional OTLP trace configuration: %v", err)
	}
}

func setConfigEnv(t *testing.T, values map[string]string) {
	t.Helper()

	typeOfConfig := reflect.TypeFor[environmentConfig]()
	for i := range typeOfConfig.NumField() {
		field := typeOfConfig.Field(i)
		key := field.Tag.Get("envconfig")
		if key == "" {
			t.Fatalf("environmentConfig.%s is missing an envconfig tag", field.Name)
		}
		value, wasSet := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
		t.Cleanup(func() {
			var err error
			if wasSet {
				err = os.Setenv(key, value)
			} else {
				err = os.Unsetenv(key)
			}
			if err != nil {
				t.Errorf("restore %s: %v", key, err)
			}
		})
	}

	for key, value := range values {
		t.Setenv(key, value)
	}
}

func deepestErrorCause(err error) error {
	for {
		cause := errors.Unwrap(err)
		if cause == nil {
			return err
		}
		err = cause
	}
}

func TestValidateRejectsEnabledMetricsWithoutAddress(t *testing.T) {
	cfg := Config{
		HeimdallEndpoint:       "127.0.0.1:8080",
		Mode:                   ModeObserve,
		PollInterval:           time.Second,
		SlurmExecTimeout:       time.Second,
		AdviceLoopTimeout:      time.Second,
		AdviceItemTimeout:      time.Second,
		AdviceRequeueHoldDelay: time.Second,
		LogLevel:               "info",
		MetricsEnabled:         true,
		MetricsAddr:            "",
	}

	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() succeeded, want error")
	}
}

func TestFromEnvSelectsAuditDestination(t *testing.T) {
	for _, test := range []struct {
		name    string
		value   string
		want    string
		invalid bool
	}{
		{name: "unset", want: "logging"},
		{name: "empty", value: "", want: "logging"},
		{name: "logging", value: "logging", want: "logging"},
		{name: "gateway", value: "gateway", want: "gateway"},
		{name: "HTTP URL", value: "http://notifications:8080/v1/audit/events", invalid: true},
		{name: "unknown", value: "unknown", invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := map[string]string{
				"HEIMDALL_ENDPOINT": "127.0.0.1:8080",
			}
			if test.name != "unset" {
				values["SLURM_ADVISOR_AUDIT_TRAIL_DESTINATION"] = test.value
			}
			setConfigEnv(t, values)
			cfg, err := FromEnv()
			if err != nil {
				t.Fatal(err)
			}
			err = cfg.Validate()
			if test.invalid {
				if err == nil || !strings.Contains(err.Error(), "audit trail destination must be logging or gateway") {
					t.Fatalf("Validate() = %v, want invalid audit destination", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.AuditTrailDestination != test.want {
				t.Fatalf("AuditTrailDestination = %q, want %q", cfg.AuditTrailDestination, test.want)
			}
		})
	}
}

func TestValidateAuditTrailGatewayBatchLimit(t *testing.T) {
	for _, test := range []struct {
		name         string
		destination  string
		maxBatchSize int
		wantError    bool
	}{
		{name: "gateway limit", destination: "gateway", maxBatchSize: 1000},
		{name: "gateway over limit", destination: "gateway", maxBatchSize: 1001, wantError: true},
		{name: "logging over gateway limit", destination: "logging", maxBatchSize: 1001},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := WithDefaults(Config{
				HeimdallEndpoint:             "127.0.0.1:8080",
				AuditTrailDestination:        test.destination,
				AuditTrailMaxBatchSize:       test.maxBatchSize,
				auditTrailMaxBatchSizeEnvSet: true,
			})

			err := cfg.Validate()
			if test.wantError {
				if err == nil || !strings.Contains(err.Error(), "audit trail max batch size for gateway destination must not exceed 1000") {
					t.Fatalf("Validate() = %v, want gateway batch limit error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate() failed: %v", err)
			}
		})
	}
}

func TestFromEnvGatewayTLS(t *testing.T) {
	setConfigEnv(t, map[string]string{"HEIMDALL_ENDPOINT": "gateway:443"})
	t.Setenv("SLURM_ADVISOR_TLS_ENABLED", "true")
	t.Setenv("SLURM_ADVISOR_TLS_CA_FILE", " /run/ca.pem ")
	t.Setenv("SLURM_ADVISOR_TLS_SERVER_NAME", " gateway.example ")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.GatewayTLS.Enabled || cfg.GatewayTLS.CAFile != "/run/ca.pem" || cfg.GatewayTLS.ServerName != "gateway.example" {
		t.Fatalf("GatewayTLS = %+v", cfg.GatewayTLS)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SLURM_ADVISOR_TLS_ENABLED", "false")
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("accepted TLS trust settings with plaintext transport")
	}
}
