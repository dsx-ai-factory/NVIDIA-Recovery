// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package watchdog

import (
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	admission "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/environment"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/tlsconfig"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/useradmission"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	// GatewayTLS configures verified TLS for API Gateway RPCs.
	GatewayTLS tlsconfig.Config
	// UserAllowlist selects the temporary initial-rollout action cohort, matching SAC.
	// Empty uses JobDefaultMode; this setting may be deprecated without notice.
	UserAllowlist []string
	// JobDefaultMode matches SAC's default when no job mode or user list applies.
	JobDefaultMode cluster.JobMode
	// JobActionsAcknowledged mirrors SAC's action ceiling, separate from watchdog safety.
	JobActionsAcknowledged bool

	Mode                        Mode
	AcknowledgeSchedulerActions bool
	PollInterval                time.Duration
	ReleaseTimeout              time.Duration
	CleanupGracePeriod          time.Duration
	SlurmExecTimeout            time.Duration
	EnvironmentPattern          string
	PartitionAllowlist          []string
	VersionedSqueueJSON         bool
	LogLevel                    string
	MetricsEnabled              bool
	MetricsAddr                 string
	// AuditTrailDestination selects logging (the default) or gateway delivery.
	AuditTrailDestination string
	// AuditGatewayEndpoint is required only for gateway audit delivery.
	AuditGatewayEndpoint string
	// ServiceAccountTokenPath optionally supplies rotating gateway credentials.
	ServiceAccountTokenPath    string
	AuditTrailMaxBatchSize     int
	AuditTrailBufferSize       int
	AuditTrailFlushInterval    time.Duration
	metricsEnabledEnvSet       bool
	metricsAddrEnvSet          bool
	environmentPatternEnvSet   bool
	auditTrailMaxBatchSizeSet  bool
	auditTrailBufferSizeSet    bool
	auditTrailFlushIntervalSet bool
}

type environmentConfig struct {
	TLSEnabled                  bool               `envconfig:"SLURM_WATCHDOG_TLS_ENABLED"`
	TLSCAFile                   string             `envconfig:"SLURM_WATCHDOG_TLS_CA_FILE"`
	TLSServerName               string             `envconfig:"SLURM_WATCHDOG_TLS_SERVER_NAME"`
	JobDefaultMode              cluster.JobMode    `envconfig:"SLURM_WATCHDOG_JOB_DEFAULT_MODE"`
	JobActionsAcknowledged      bool               `envconfig:"SLURM_WATCHDOG_JOB_ACTIONS_ACKNOWLEDGED"`
	Mode                        Mode               `envconfig:"SLURM_WATCHDOG_MODE"`
	AcknowledgeSchedulerActions bool               `envconfig:"SLURM_WATCHDOG_ACKNOWLEDGE_SCHEDULER_ACTIONS"`
	PollInterval                positiveDuration   `envconfig:"SLURM_WATCHDOG_POLL_INTERVAL"`
	ReleaseTimeout              positiveDuration   `envconfig:"SLURM_WATCHDOG_RELEASE_TIMEOUT"`
	CleanupGracePeriod          positiveDuration   `envconfig:"SLURM_WATCHDOG_CLEANUP_GRACE_PERIOD"`
	SlurmExecTimeout            positiveDuration   `envconfig:"SLURM_WATCHDOG_SLURM_EXEC_TIMEOUT"`
	EnvironmentPattern          optionalString     `envconfig:"SLURM_WATCHDOG_ENVIRONMENT_PATTERN"`
	PartitionAllowlist          partitionAllowlist `envconfig:"SLURM_WATCHDOG_PARTITION_ALLOWLIST"`
	VersionedSqueueJSON         bool               `envconfig:"SLURM_WATCHDOG_VERSIONED_SQUEUE_JSON"`
	LogLevel                    logLevel           `envconfig:"SLURM_WATCHDOG_LOG_LEVEL"`
	MetricsEnabled              optionalBool       `envconfig:"SLURM_WATCHDOG_METRICS_ENABLED"`
	MetricsAddr                 optionalString     `envconfig:"SLURM_WATCHDOG_METRICS_ADDR"`
	AuditTrailDestination       string             `envconfig:"SLURM_WATCHDOG_AUDIT_TRAIL_DESTINATION"`
	AuditGatewayEndpoint        string             `envconfig:"SLURM_WATCHDOG_AUDIT_GATEWAY_ENDPOINT"`
	ServiceAccountTokenPath     optionalString     `envconfig:"SLURM_WATCHDOG_SERVICE_ACCOUNT_TOKEN_PATH"`
	AuditTrailMaxBatchSize      optionalInt        `envconfig:"SLURM_WATCHDOG_AUDIT_TRAIL_MAX_BATCH_SIZE"`
	AuditTrailBufferSize        optionalInt        `envconfig:"SLURM_WATCHDOG_AUDIT_TRAIL_BUFFER_SIZE"`
	AuditTrailFlushInterval     optionalDuration   `envconfig:"SLURM_WATCHDOG_AUDIT_TRAIL_FLUSH_INTERVAL"`

	UserAllowlist useradmission.Allowlist `envconfig:"SLURM_WATCHDOG_USER_ALLOWLIST"`
}

func FromEnv() (Config, error) {
	var settings environmentConfig
	if err := envconfig.Process("", &settings); err != nil {
		return Config{}, wrapEnvconfigError(err)
	}
	if settings.MetricsAddr.set && strings.TrimSpace(settings.MetricsAddr.value) == "" {
		return Config{}, errors.New("SLURM_WATCHDOG_METRICS_ADDR must not be empty")
	}

	if settings.ServiceAccountTokenPath.set && strings.TrimSpace(settings.ServiceAccountTokenPath.value) == "" {
		return Config{}, errors.New("SLURM_WATCHDOG_SERVICE_ACCOUNT_TOKEN_PATH must not be empty")
	}

	return WithDefaults(Config{
		GatewayTLS:                  tlsconfig.Config{Enabled: settings.TLSEnabled, CAFile: strings.TrimSpace(settings.TLSCAFile), ServerName: strings.TrimSpace(settings.TLSServerName)},
		JobDefaultMode:              settings.JobDefaultMode,
		JobActionsAcknowledged:      settings.JobActionsAcknowledged,
		UserAllowlist:               []string(settings.UserAllowlist),
		Mode:                        settings.Mode,
		AcknowledgeSchedulerActions: settings.AcknowledgeSchedulerActions,
		PollInterval:                time.Duration(settings.PollInterval),
		ReleaseTimeout:              time.Duration(settings.ReleaseTimeout),
		CleanupGracePeriod:          time.Duration(settings.CleanupGracePeriod),
		SlurmExecTimeout:            time.Duration(settings.SlurmExecTimeout),
		EnvironmentPattern:          settings.EnvironmentPattern.value,
		PartitionAllowlist:          []string(settings.PartitionAllowlist),
		VersionedSqueueJSON:         settings.VersionedSqueueJSON,
		LogLevel:                    string(settings.LogLevel),
		MetricsEnabled:              settings.MetricsEnabled.value,
		MetricsAddr:                 settings.MetricsAddr.value,
		AuditTrailDestination:       strings.TrimSpace(settings.AuditTrailDestination),
		AuditGatewayEndpoint:        strings.TrimSpace(settings.AuditGatewayEndpoint),
		ServiceAccountTokenPath:     strings.TrimSpace(settings.ServiceAccountTokenPath.value),
		AuditTrailMaxBatchSize:      settings.AuditTrailMaxBatchSize.value,
		AuditTrailBufferSize:        settings.AuditTrailBufferSize.value,
		AuditTrailFlushInterval:     settings.AuditTrailFlushInterval.value,
		metricsEnabledEnvSet:        settings.MetricsEnabled.set,
		metricsAddrEnvSet:           settings.MetricsAddr.set,
		environmentPatternEnvSet:    settings.EnvironmentPattern.set,
		auditTrailMaxBatchSizeSet:   settings.AuditTrailMaxBatchSize.set,
		auditTrailBufferSizeSet:     settings.AuditTrailBufferSize.set,
		auditTrailFlushIntervalSet:  settings.AuditTrailFlushInterval.set,
	}), nil
}

func WithDefaults(cfg Config) Config {
	if cfg.JobDefaultMode == "" {
		cfg.JobDefaultMode = cluster.JobModeObserve
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeObserve
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 15 * time.Second
	}
	if cfg.ReleaseTimeout == 0 {
		cfg.ReleaseTimeout = 10 * time.Minute
	}
	if cfg.CleanupGracePeriod == 0 {
		cfg.CleanupGracePeriod = 10 * time.Minute
	}
	if cfg.SlurmExecTimeout == 0 {
		cfg.SlurmExecTimeout = 5 * time.Second
	}
	if !cfg.environmentPatternEnvSet && cfg.EnvironmentPattern == "" {
		cfg.EnvironmentPattern = admission.Unspecified
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	cfg.LogLevel = strings.ToLower(strings.TrimSpace(cfg.LogLevel))
	if !cfg.metricsEnabledEnvSet {
		cfg.MetricsEnabled = true
	}
	if cfg.MetricsAddr == "" && !cfg.metricsAddrEnvSet {
		cfg.MetricsAddr = ":9091"
	}
	if cfg.AuditTrailDestination == "" {
		cfg.AuditTrailDestination = "logging"
	}
	if cfg.AuditTrailMaxBatchSize == 0 && !cfg.auditTrailMaxBatchSizeSet {
		cfg.AuditTrailMaxBatchSize = 100
	}
	if cfg.AuditTrailBufferSize == 0 && !cfg.auditTrailBufferSizeSet {
		cfg.AuditTrailBufferSize = 1000
	}
	if cfg.AuditTrailFlushInterval == 0 && !cfg.auditTrailFlushIntervalSet {
		cfg.AuditTrailFlushInterval = 30 * time.Second
	}

	return cfg
}

func (cfg Config) Validate() error {
	if err := cfg.GatewayTLS.Validate(); err != nil {
		return err
	}
	if err := validateJobDefaultMode(cfg.JobDefaultMode); err != nil {
		return err
	}
	if cfg.PollInterval <= 0 {
		return errors.Newf("watchdog poll interval must be positive: %s", cfg.PollInterval)
	}
	if cfg.ReleaseTimeout <= 0 {
		return errors.Newf("watchdog release timeout must be positive: %s", cfg.ReleaseTimeout)
	}
	if cfg.CleanupGracePeriod <= 0 {
		return errors.Newf("watchdog cleanup grace period must be positive: %s", cfg.CleanupGracePeriod)
	}
	if cfg.SlurmExecTimeout <= 0 {
		return errors.Newf("watchdog Slurm exec timeout must be positive: %s", cfg.SlurmExecTimeout)
	}
	if cfg.EnvironmentPattern == "" {
		return errors.New("SLURM_WATCHDOG_ENVIRONMENT_PATTERN must not be empty")
	}
	if _, err := admission.NewMatcher(cfg.EnvironmentPattern); err != nil {
		return errors.Wrap(err, "validate watchdog environment pattern")
	}
	if !validLogLevel(cfg.LogLevel) {
		return errors.Newf("unsupported watchdog log level %q", cfg.LogLevel)
	}
	if cfg.MetricsEnabled && strings.TrimSpace(cfg.MetricsAddr) == "" {
		return errors.New("watchdog metrics address is required when metrics are enabled")
	}
	if cfg.AuditTrailMaxBatchSize <= 0 {
		return errors.Newf("watchdog audit trail max batch size must be positive: %d", cfg.AuditTrailMaxBatchSize)
	}
	if cfg.AuditTrailDestination == "gateway" &&
		cfg.AuditTrailMaxBatchSize > audit.MaxGatewayBatchSize {
		return errors.Newf(
			"watchdog audit trail max batch size for gateway destination must not exceed %d: %d",
			audit.MaxGatewayBatchSize,
			cfg.AuditTrailMaxBatchSize,
		)
	}
	if cfg.AuditTrailBufferSize <= 0 {
		return errors.Newf("watchdog audit trail buffer size must be positive: %d", cfg.AuditTrailBufferSize)
	}
	if cfg.AuditTrailFlushInterval <= 0 {
		return errors.Newf("watchdog audit trail flush interval must be positive: %s", cfg.AuditTrailFlushInterval)
	}
	if cfg.AuditTrailDestination != "logging" && cfg.AuditTrailDestination != "gateway" {
		return errors.Newf("watchdog audit trail destination must be logging or gateway: %q", cfg.AuditTrailDestination)
	}
	if cfg.AuditTrailDestination == "gateway" && strings.TrimSpace(cfg.AuditGatewayEndpoint) == "" {
		return errors.New("watchdog audit gateway endpoint is required for gateway delivery")
	}

	switch cfg.Mode {
	case ModeObserve:
		return nil
	case ModeAct:
		if !cfg.AcknowledgeSchedulerActions {
			return errors.New("watchdog act mode requires explicit scheduler action acknowledgement")
		}
		return nil
	default:
		return errors.Newf("unsupported watchdog mode %q", cfg.Mode)
	}
}

type positiveDuration time.Duration

func (duration *positiveDuration) Decode(value string) error {
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	if parsed <= 0 {
		return errors.Newf("duration must be positive: %s", parsed)
	}
	*duration = positiveDuration(parsed)
	return nil
}

type partitionAllowlist []string

func (allowlist *partitionAllowlist) Decode(value string) error {
	out := []string{}
	seen := map[string]struct{}{}
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	*allowlist = out
	return nil
}

type logLevel string

func (level *logLevel) Decode(value string) error {
	value = strings.ToLower(strings.TrimSpace(value))
	if !validLogLevel(value) {
		return errors.Newf("unsupported watchdog log level %q", value)
	}
	*level = logLevel(value)
	return nil
}

func validLogLevel(level string) bool {
	switch level {
	case "debug", "info", "warn", "error", "dpanic", "panic", "fatal":
		return true
	default:
		return false
	}
}

type optionalBool struct {
	value bool
	set   bool
}

type optionalInt struct {
	value int
	set   bool
}

func (setting *optionalInt) Decode(value string) error {
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return err
	}
	setting.value = parsed
	setting.set = true
	return nil
}

type optionalDuration struct {
	value time.Duration
	set   bool
}

func (setting *optionalDuration) Decode(value string) error {
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	setting.value = parsed
	setting.set = true
	return nil
}

func (setting *optionalBool) Decode(value string) error {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return err
	}
	setting.value = parsed
	setting.set = true
	return nil
}

type optionalString struct {
	value string
	set   bool
}

func (setting *optionalString) Decode(value string) error {
	setting.value = value
	setting.set = true
	return nil
}

func wrapEnvconfigError(err error) error {
	parseErr, ok := err.(*envconfig.ParseError)
	if !ok || parseErr.Err == nil {
		return errors.Wrap(err, "decode watchdog environment configuration")
	}

	return &configDecodeError{
		keyName:   parseErr.KeyName,
		fieldName: parseErr.FieldName,
		typeName:  parseErr.TypeName,
		cause:     parseErr.Err,
	}
}

type configDecodeError struct {
	keyName   string
	fieldName string
	typeName  string
	cause     error
}

func (e *configDecodeError) Error() string {
	return "decode watchdog environment variable " + e.keyName +
		" into field " + e.fieldName +
		" as " + e.typeName
}

func (e *configDecodeError) Unwrap() error {
	return e.cause
}

// validateJobDefaultMode accepts an absent mode as the safe observe default.
func validateJobDefaultMode(mode cluster.JobMode) error {
	switch mode {
	case "", cluster.JobModeIgnore, cluster.JobModeObserve, cluster.JobModeAct:
		return nil
	default:
		return errors.Newf("unsupported watchdog job default mode %q", mode)
	}
}
