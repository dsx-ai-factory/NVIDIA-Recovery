// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package watchdog

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	admission "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/environment"
)

func TestWatchdogConfigDefaultsDoNotRequireHeimdallEndpoint(t *testing.T) {
	clearWatchdogEnv(t)
	t.Setenv("HEIMDALL_ENDPOINT", "")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() failed without Heimdall endpoint: %v", err)
	}

	if cfg.Mode != ModeObserve {
		t.Fatalf("Mode = %q, want observe", cfg.Mode)
	}
	if cfg.PollInterval != 15*time.Second {
		t.Fatalf("PollInterval = %s, want 15s", cfg.PollInterval)
	}
	if cfg.ReleaseTimeout != 10*time.Minute {
		t.Fatalf("ReleaseTimeout = %s, want 10m", cfg.ReleaseTimeout)
	}
	if cfg.CleanupGracePeriod != 10*time.Minute {
		t.Fatalf("CleanupGracePeriod = %s, want 10m", cfg.CleanupGracePeriod)
	}
	if cfg.SlurmExecTimeout != 5*time.Second {
		t.Fatalf("SlurmExecTimeout = %s, want 5s", cfg.SlurmExecTimeout)
	}
	if cfg.EnvironmentPattern != admission.Unspecified {
		t.Fatalf("EnvironmentPattern = %q, want %q", cfg.EnvironmentPattern, admission.Unspecified)
	}
	if cfg.VersionedSqueueJSON {
		t.Fatal("VersionedSqueueJSON = true, want false")
	}
	if cfg.LogLevel != "info" {
		t.Fatalf("LogLevel = %q, want info", cfg.LogLevel)
	}
	if !cfg.MetricsEnabled {
		t.Fatal("MetricsEnabled = false, want true")
	}
	if cfg.MetricsAddr != ":9091" {
		t.Fatalf("MetricsAddr = %q, want :9091", cfg.MetricsAddr)
	}
	if cfg.AuditTrailDestination != "logging" {
		t.Fatalf("audit trail destination = %q, want logging", cfg.AuditTrailDestination)
	}
	if cfg.AuditTrailMaxBatchSize != 100 || cfg.AuditTrailBufferSize != 1000 || cfg.AuditTrailFlushInterval != 30*time.Second {
		t.Fatalf("audit trail batching = %d/%d/%s, want 100/1000/30s",
			cfg.AuditTrailMaxBatchSize, cfg.AuditTrailBufferSize, cfg.AuditTrailFlushInterval)
	}
}

func TestWatchdogConfigLoadsDedicatedEnvironment(t *testing.T) {
	clearWatchdogEnv(t)
	values := map[string]string{
		"SLURM_WATCHDOG_MODE":                          "act",
		"SLURM_WATCHDOG_ACKNOWLEDGE_SCHEDULER_ACTIONS": "true",
		"SLURM_WATCHDOG_POLL_INTERVAL":                 "2s",
		"SLURM_WATCHDOG_RELEASE_TIMEOUT":               "3m",
		"SLURM_WATCHDOG_CLEANUP_GRACE_PERIOD":          "4m",
		"SLURM_WATCHDOG_SLURM_EXEC_TIMEOUT":            "7s",
		"SLURM_WATCHDOG_ENVIRONMENT_PATTERN":           "customer/*",
		"SLURM_WATCHDOG_PARTITION_ALLOWLIST":           "gpu, debug, gpu",
		"SLURM_WATCHDOG_VERSIONED_SQUEUE_JSON":         "true",
		"SLURM_WATCHDOG_LOG_LEVEL":                     " DEBUG ",
		"SLURM_WATCHDOG_METRICS_ENABLED":               "false",
		"SLURM_WATCHDOG_METRICS_ADDR":                  "127.0.0.1:19091",
		"SLURM_WATCHDOG_AUDIT_TRAIL_DESTINATION":       " gateway ",
		"SLURM_WATCHDOG_AUDIT_GATEWAY_ENDPOINT":        " gateway:9090 ",
		"SLURM_WATCHDOG_SERVICE_ACCOUNT_TOKEN_PATH":    "/run/token",
		"SLURM_WATCHDOG_AUDIT_TRAIL_MAX_BATCH_SIZE":    "25",
		"SLURM_WATCHDOG_AUDIT_TRAIL_BUFFER_SIZE":       "250",
		"SLURM_WATCHDOG_AUDIT_TRAIL_FLUSH_INTERVAL":    "5s",
	}
	for key, value := range values {
		t.Setenv(key, value)
	}

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	if cfg.Mode != ModeAct || !cfg.AcknowledgeSchedulerActions {
		t.Fatalf("action safety = %q/%t, want act/true", cfg.Mode, cfg.AcknowledgeSchedulerActions)
	}
	if cfg.PollInterval != 2*time.Second || cfg.ReleaseTimeout != 3*time.Minute ||
		cfg.CleanupGracePeriod != 4*time.Minute || cfg.SlurmExecTimeout != 7*time.Second {
		t.Fatalf("durations = %s/%s/%s/%s, want 2s/3m/4m/7s",
			cfg.PollInterval, cfg.ReleaseTimeout, cfg.CleanupGracePeriod, cfg.SlurmExecTimeout)
	}
	if !slices.Equal(cfg.PartitionAllowlist, []string{"gpu", "debug"}) {
		t.Fatalf("PartitionAllowlist = %v, want gpu,debug", cfg.PartitionAllowlist)
	}
	if cfg.EnvironmentPattern != "customer/*" {
		t.Fatalf("EnvironmentPattern = %q, want customer/*", cfg.EnvironmentPattern)
	}
	if !cfg.VersionedSqueueJSON || cfg.LogLevel != "debug" || cfg.MetricsEnabled || cfg.MetricsAddr != "127.0.0.1:19091" {
		t.Fatalf("other config = versioned:%t log:%q metrics:%t addr:%q",
			cfg.VersionedSqueueJSON, cfg.LogLevel, cfg.MetricsEnabled, cfg.MetricsAddr)
	}
	if cfg.AuditGatewayEndpoint != "gateway:9090" || cfg.ServiceAccountTokenPath != "/run/token" {
		t.Fatalf("gateway endpoint/token path = %q/%q", cfg.AuditGatewayEndpoint, cfg.ServiceAccountTokenPath)
	}
	if cfg.AuditTrailDestination != "gateway" ||
		cfg.AuditTrailMaxBatchSize != 25 || cfg.AuditTrailBufferSize != 250 || cfg.AuditTrailFlushInterval != 5*time.Second {
		t.Fatalf("audit trail config = destination:%q batch:%d buffer:%d flush:%s",
			cfg.AuditTrailDestination, cfg.AuditTrailMaxBatchSize,
			cfg.AuditTrailBufferSize, cfg.AuditTrailFlushInterval)
	}
}

func TestWatchdogAuditTrailValidation(t *testing.T) {
	for name, cfg := range map[string]Config{
		"HTTP destination":         {AuditTrailDestination: "https://audit.example/events"},
		"missing gateway endpoint": {AuditTrailDestination: "gateway"},
		"relative destination": {
			AuditTrailDestination: "notifications/audit",
		},
		"zero batch size": {
			AuditTrailMaxBatchSize: 0, auditTrailMaxBatchSizeSet: true,
		},
		"zero buffer size": {
			AuditTrailBufferSize: 0, auditTrailBufferSizeSet: true,
		},
		"zero flush interval": {
			AuditTrailFlushInterval: 0, auditTrailFlushIntervalSet: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg = WithDefaults(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want audit trail validation error")
			}
		})
	}
}

func TestWatchdogAuditTrailGatewayBatchLimit(t *testing.T) {
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
				AuditTrailDestination:     test.destination,
				AuditGatewayEndpoint:      "127.0.0.1:8080",
				AuditTrailMaxBatchSize:    test.maxBatchSize,
				auditTrailMaxBatchSizeSet: true,
			})

			err := cfg.Validate()
			if test.wantError {
				if err == nil || !strings.Contains(err.Error(), "watchdog audit trail max batch size for gateway destination must not exceed 1000") {
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

func TestWatchdogAuditTrailExplicitZeroSurvivesEnvironmentDefaults(t *testing.T) {
	for key, value := range map[string]string{
		"SLURM_WATCHDOG_AUDIT_TRAIL_MAX_BATCH_SIZE": "0",
		"SLURM_WATCHDOG_AUDIT_TRAIL_BUFFER_SIZE":    "0",
		"SLURM_WATCHDOG_AUDIT_TRAIL_FLUSH_INTERVAL": "0s",
	} {
		t.Run(key, func(t *testing.T) {
			clearWatchdogEnv(t)
			t.Setenv(key, value)
			cfg, err := FromEnv()
			if err != nil {
				t.Fatalf("FromEnv() failed: %v", err)
			}
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want explicit zero rejection")
			}
		})
	}
}

func TestWatchdogConfigRejectsExplicitEmptyEnvironmentPattern(t *testing.T) {
	clearWatchdogEnv(t)
	t.Setenv("SLURM_WATCHDOG_ENVIRONMENT_PATTERN", "")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv() failed: %v", err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want explicit-empty environment pattern error")
	}
}

func TestWatchdogConfigRejectsInvalidDurations(t *testing.T) {
	for name, values := range map[string]map[string]string{
		"invalid poll interval": {
			"SLURM_WATCHDOG_POLL_INTERVAL": "not-a-duration",
		},
		"zero release timeout": {
			"SLURM_WATCHDOG_RELEASE_TIMEOUT": "0s",
		},
		"negative cleanup grace": {
			"SLURM_WATCHDOG_CLEANUP_GRACE_PERIOD": "-1s",
		},
		"zero Slurm timeout": {
			"SLURM_WATCHDOG_SLURM_EXEC_TIMEOUT": "0s",
		},
	} {
		t.Run(name, func(t *testing.T) {
			clearWatchdogEnv(t)
			for key, value := range values {
				t.Setenv(key, value)
			}
			if _, err := FromEnv(); err == nil {
				t.Fatal("FromEnv() error = nil, want duration error")
			}
		})
	}
}

func TestWatchdogConfigActionModeRequiresAcknowledgement(t *testing.T) {
	cfg := WithDefaults(Config{Mode: ModeAct})
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want action acknowledgement error")
	}

	cfg.AcknowledgeSchedulerActions = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() with acknowledgement failed: %v", err)
	}
}

func TestWatchdogConfigRejectsInvalidModeLogLevelAndMetricsAddress(t *testing.T) {
	for name, cfg := range map[string]Config{
		"mode": {
			Mode: "automatic",
		},
		"log level": {
			LogLevel: "verbose",
		},
		"metrics address": {
			MetricsEnabled:       true,
			MetricsAddr:          " ",
			metricsEnabledEnvSet: true,
			metricsAddrEnvSet:    true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg = WithDefaults(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want validation error")
			}
		})
	}
}

func clearWatchdogEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"SLURM_WATCHDOG_TLS_ENABLED",
		"SLURM_WATCHDOG_TLS_CA_FILE",
		"SLURM_WATCHDOG_TLS_SERVER_NAME",
		"SLURM_WATCHDOG_JOB_DEFAULT_MODE",
		"SLURM_WATCHDOG_JOB_ACTIONS_ACKNOWLEDGED",
		"SLURM_WATCHDOG_MODE",
		"SLURM_WATCHDOG_ACKNOWLEDGE_SCHEDULER_ACTIONS",
		"SLURM_WATCHDOG_POLL_INTERVAL",
		"SLURM_WATCHDOG_RELEASE_TIMEOUT",
		"SLURM_WATCHDOG_CLEANUP_GRACE_PERIOD",
		"SLURM_WATCHDOG_SLURM_EXEC_TIMEOUT",
		"SLURM_WATCHDOG_ENVIRONMENT_PATTERN",
		"SLURM_WATCHDOG_PARTITION_ALLOWLIST",
		"SLURM_WATCHDOG_USER_ALLOWLIST",
		"SLURM_WATCHDOG_VERSIONED_SQUEUE_JSON",
		"SLURM_WATCHDOG_LOG_LEVEL",
		"SLURM_WATCHDOG_METRICS_ENABLED",
		"SLURM_WATCHDOG_METRICS_ADDR",
		"SLURM_WATCHDOG_AUDIT_TRAIL_DESTINATION",
		"SLURM_WATCHDOG_AUDIT_TRAIL_MAX_BATCH_SIZE",
		"SLURM_WATCHDOG_AUDIT_TRAIL_BUFFER_SIZE",
		"SLURM_WATCHDOG_AUDIT_TRAIL_FLUSH_INTERVAL",
	} {
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
}

func TestWatchdogAuditLoggingDoesNotRequireGateway(t *testing.T) {
	for _, selected := range []string{"", "logging"} {
		cfg := WithDefaults(Config{AuditTrailDestination: selected})
		if err := cfg.Validate(); err != nil {
			t.Fatalf("logging selection %q: %v", selected, err)
		}
	}
	cfg := WithDefaults(Config{AuditTrailDestination: "gateway"})
	if err := cfg.Validate(); err == nil {
		t.Fatal("gateway audit without endpoint must fail validation")
	}
}

func TestWatchdogRejectsExplicitEmptyTokenPath(t *testing.T) {
	clearWatchdogEnv(t)
	t.Setenv("SLURM_WATCHDOG_SERVICE_ACCOUNT_TOKEN_PATH", " ")
	if _, err := FromEnv(); err == nil {
		t.Fatal("explicit empty token path silently disabled credentials")
	}
}

func TestWatchdogUserAllowlistConfig(t *testing.T) {
	for _, tt := range []struct {
		name      string
		value     string
		unset     bool
		want      []string
		wantError bool
	}{
		{name: "unset", unset: true},
		{name: "empty"},
		{name: "CSV", value: "alice, bob,alice,,Alice", want: []string{"alice", "bob", "Alice"}},
		{name: "separators cannot disable filtering", value: ", ,", wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clearWatchdogEnv(t)
			if !tt.unset {
				t.Setenv("SLURM_WATCHDOG_USER_ALLOWLIST", tt.value)
			}
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

func TestWatchdogJobPolicyConfiguration(t *testing.T) {
	for _, tt := range []struct {
		name      string
		mode      string
		ack       string
		wantMode  string
		wantAck   bool
		wantError bool
	}{
		{name: "safe defaults", wantMode: "observe"},
		{name: "act and acknowledged", mode: "act", ack: "true", wantMode: "act", wantAck: true},
		{name: "observe and acknowledged", mode: "observe", ack: "true", wantMode: "observe", wantAck: true},
		{name: "ignore", mode: "ignore", wantMode: "ignore"},
		{name: "invalid mode", mode: "automatic", wantError: true},
		{name: "invalid acknowledgement", ack: "yes", wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			clearWatchdogEnv(t)
			if tt.mode != "" {
				t.Setenv("SLURM_WATCHDOG_JOB_DEFAULT_MODE", tt.mode)
			}
			if tt.ack != "" {
				t.Setenv("SLURM_WATCHDOG_JOB_ACTIONS_ACKNOWLEDGED", tt.ack)
			}
			cfg, err := FromEnv()
			if err == nil {
				err = cfg.Validate()
			}
			if (err != nil) != tt.wantError {
				t.Fatalf("configuration error = %v, want error %t", err, tt.wantError)
			}
			if tt.wantError {
				return
			}
			if string(cfg.JobDefaultMode) != tt.wantMode || cfg.JobActionsAcknowledged != tt.wantAck {
				t.Fatalf("job policy config = %s/%t, want %s/%t", cfg.JobDefaultMode, cfg.JobActionsAcknowledged, tt.wantMode, tt.wantAck)
			}
			if cfg.Mode != ModeObserve || cfg.AcknowledgeSchedulerActions {
				t.Fatal("SAC policy configuration changed watchdog action safeguards")
			}
		})
	}
}

func TestFromEnvGatewayTLS(t *testing.T) {
	clearWatchdogEnv(t)
	t.Setenv("SLURM_WATCHDOG_TLS_ENABLED", "true")
	t.Setenv("SLURM_WATCHDOG_TLS_CA_FILE", " /run/ca.pem ")
	t.Setenv("SLURM_WATCHDOG_TLS_SERVER_NAME", " gateway.example ")
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
	t.Setenv("SLURM_WATCHDOG_TLS_ENABLED", "false")
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("accepted TLS trust settings with plaintext transport")
	}
}
