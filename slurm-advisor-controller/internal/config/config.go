// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/tlsconfig"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/useradmission"
	"github.com/kelseyhightower/envconfig"
)

type Mode string

const (
	ModeIgnore  Mode = "ignore"
	ModeObserve Mode = "observe"
	ModeAct     Mode = "act"
)

type SafetyChecks struct {
	AcknowledgeSchedulerActions bool
}

// TracingConfig selects optional OTLP trace export.
type TracingConfig struct {
	// Endpoint is the full trace URL: the signal-specific endpoint as given, or
	// the shared base endpoint with the OTLP/HTTP trace path appended.
	Endpoint string
	// Protocol selects grpc or HTTP/protobuf (http is an alias).
	Protocol string
	// Token is the bearer credential supplied by OTEL_EXPORTER_OTLP_TOKEN.
	Token string
}

// MilestoneReportingConfig bounds the background worker that reports recovery
// milestones to Job Manager. Reporting never delays scheduler commands; these
// settings trade memory, backend load, and shutdown time against report loss.
type MilestoneReportingConfig struct {
	// QueueCapacity limits command results waiting to be reported; a full
	// queue drops new reports.
	QueueCapacity int
	// RPCTimeout bounds each delivery attempt.
	RPCTimeout time.Duration
	// MaxAttempts includes the first delivery and applies only to temporary
	// failures.
	MaxAttempts int
	// RetryDelay separates delivery attempts.
	RetryDelay time.Duration
	// ShutdownTimeout bounds draining queued reports when the process stops.
	ShutdownTimeout time.Duration
}

type LogLinksConfig struct {
	Enabled                bool
	Destination            string
	SourceRoots            []string
	ApplicationPatterns    []string
	ScanInterval           time.Duration
	CleanupInterval        time.Duration
	Retention              time.Duration
	ScanTimeout            time.Duration
	MaxPathLength          int
	MaxPathsPerSync        int
	MaxPatterns            int
	MaxScanDirectories     int
	MaxEntriesPerDirectory int
	MaxMatchesPerScan      int
	MaxOwnedLinks          int

	// These track environment values whose explicit zero values must survive
	// WithDefaults so enabled configurations reject them during validation.
	scanIntervalEnvSet           bool
	cleanupIntervalEnvSet        bool
	retentionEnvSet              bool
	scanTimeoutEnvSet            bool
	maxPathLengthEnvSet          bool
	maxPathsPerSyncEnvSet        bool
	maxPatternsEnvSet            bool
	maxScanDirectoriesEnvSet     bool
	maxEntriesPerDirectoryEnvSet bool
	maxMatchesPerScanEnvSet      bool
	maxOwnedLinksEnvSet          bool
}

type Config struct {
	// GatewayTLS configures verified TLS for API Gateway RPCs.
	GatewayTLS              tlsconfig.Config
	HeimdallEndpoint        string
	ServiceAccountTokenPath string
	Mode                    Mode
	LogLevel                string
	// OTLP configures optional export of controller logs to the shared collector.
	OTLP                         OTLPConfig
	PollInterval                 time.Duration
	SlurmExecTimeout             time.Duration
	LogMetadataRefreshInterval   time.Duration
	LogMetadataEnrichmentTimeout time.Duration
	JobUpsertWorkerPoolSize      int
	AccountingWorkerPoolSize     int
	AccountingTimeout            time.Duration
	// FailedJobRecoveryTimeout bounds each tick's failed-job recovery phase.
	FailedJobRecoveryTimeout time.Duration
	AdviceLoopTimeout        time.Duration
	AdviceItemTimeout        time.Duration
	AdviceMaxAge             time.Duration
	AdviceRequeueHoldDelay   time.Duration
	// EnableEagerRequeue allows failed-job recovery before restart Advice; disabled by default.
	EnableEagerRequeue bool
	// BootstrapFailureLookback bounds rediscovery of finished failures for eager recovery by end time.
	BootstrapFailureLookback time.Duration
	// AuditTrailDestination selects logging (the default) or gateway delivery.
	AuditTrailDestination          string
	AuditTrailMaxBatchSize         int
	AuditTrailBufferSize           int
	AuditTrailFlushInterval        time.Duration
	Environment                    string
	MetricsEnabled                 bool
	MetricsAddr                    string
	PartitionAllowlist             []string
	VersionedSqueueJSON            bool
	UpdateEligibleTimeAfterRequeue bool
	Safety                         SafetyChecks
	Tracing                        TracingConfig
	MilestoneReporting             MilestoneReportingConfig
	LogLinks                       LogLinksConfig

	// UserAllowlist temporarily selects actions for named users without hiding other jobs.
	// Explicit job mode wins. Empty uses Mode; removable without notice.
	UserAllowlist []string
	// These track environment values whose explicit zero values must survive
	// WithDefaults.
	adviceMaxAgeEnvSet                 bool
	bootstrapFailureLookbackEnvSet     bool
	logMetadataRefreshIntervalEnvSet   bool
	logMetadataEnrichmentTimeoutEnvSet bool
	jobUpsertWorkerPoolSizeEnvSet      bool
	accountingWorkerPoolSizeEnvSet     bool
	accountingTimeoutEnvSet            bool
	failedJobRecoveryTimeoutEnvSet     bool
	metricsEnabledEnvSet               bool
	metricsAddrEnvSet                  bool
	auditTrailMaxBatchSizeEnvSet       bool
	auditTrailBufferSizeEnvSet         bool
	auditTrailFlushIntervalEnvSet      bool
	milestoneQueueCapacityEnvSet       bool
	milestoneRPCTimeoutEnvSet          bool
	milestoneMaxAttemptsEnvSet         bool
	milestoneRetryDelayEnvSet          bool
	milestoneShutdownTimeoutEnvSet     bool
}

type environmentConfig struct {
	TLSEnabled                     bool               `envconfig:"SLURM_ADVISOR_TLS_ENABLED"`
	TLSCAFile                      string             `envconfig:"SLURM_ADVISOR_TLS_CA_FILE"`
	TLSServerName                  string             `envconfig:"SLURM_ADVISOR_TLS_SERVER_NAME"`
	HeimdallEndpoint               string             `envconfig:"HEIMDALL_ENDPOINT"`
	ServiceAccountTokenPath        optionalString     `envconfig:"SLURM_ADVISOR_SERVICE_ACCOUNT_TOKEN_PATH"`
	Mode                           Mode               `envconfig:"SLURM_ADVISOR_MODE"`
	LogLevel                       logLevel           `envconfig:"SLURM_ADVISOR_LOG_LEVEL"`
	OTLPEndpoint                   string             `envconfig:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	OTLPProtocol                   string             `envconfig:"OTEL_EXPORTER_OTLP_PROTOCOL"`
	OTLPToken                      string             `envconfig:"OTEL_EXPORTER_OTLP_TOKEN"`
	PollInterval                   time.Duration      `envconfig:"SLURM_ADVISOR_POLL_INTERVAL"`
	SlurmExecTimeout               time.Duration      `envconfig:"SLURM_EXEC_TIMEOUT"`
	LogMetadataRefreshInterval     optionalDuration   `envconfig:"SLURM_ADVISOR_LOG_METADATA_REFRESH_INTERVAL"`
	LogMetadataEnrichmentTimeout   optionalDuration   `envconfig:"SLURM_ADVISOR_LOG_METADATA_ENRICHMENT_TIMEOUT"`
	JobUpsertWorkerPoolSize        optionalInt        `envconfig:"SLURM_ADVISOR_JOB_UPSERT_WORKER_POOL_SIZE"`
	AccountingWorkerPoolSize       optionalInt        `envconfig:"SLURM_ADVISOR_ACCOUNTING_WORKER_POOL_SIZE"`
	AccountingTimeout              optionalDuration   `envconfig:"SLURM_ADVISOR_ACCOUNTING_TIMEOUT"`
	FailedJobRecoveryTimeout       optionalDuration   `envconfig:"SLURM_ADVISOR_FAILED_JOB_RECOVERY_TIMEOUT"`
	AdviceLoopTimeout              time.Duration      `envconfig:"SLURM_ADVISOR_ADVICE_LOOP_TIMEOUT"`
	AdviceItemTimeout              time.Duration      `envconfig:"SLURM_ADVISOR_ADVICE_ITEM_TIMEOUT"`
	AdviceMaxAge                   optionalDuration   `envconfig:"SLURM_ADVISOR_ADVICE_MAX_AGE"`
	AdviceRequeueHoldDelay         time.Duration      `envconfig:"SLURM_ADVISOR_REQUEUE_HOLD_DELAY"`
	EnableEagerRequeue             bool               `envconfig:"SLURM_ADVISOR_ENABLE_EAGER_REQUEUE"`
	BootstrapFailureLookback       optionalDuration   `envconfig:"SLURM_ADVISOR_BOOTSTRAP_FAILURE_LOOKBACK"`
	Environment                    string             `envconfig:"SLURM_ADVISOR_ENVIRONMENT"`
	MetricsEnabled                 optionalBool       `envconfig:"SLURM_ADVISOR_METRICS_ENABLED"`
	MetricsAddr                    optionalString     `envconfig:"SLURM_ADVISOR_METRICS_ADDR"`
	AuditTrailDestination          string             `envconfig:"SLURM_ADVISOR_AUDIT_TRAIL_DESTINATION"`
	AuditTrailMaxBatchSize         optionalInt        `envconfig:"SLURM_ADVISOR_AUDIT_TRAIL_MAX_BATCH_SIZE"`
	AuditTrailBufferSize           optionalInt        `envconfig:"SLURM_ADVISOR_AUDIT_TRAIL_BUFFER_SIZE"`
	AuditTrailFlushInterval        optionalDuration   `envconfig:"SLURM_ADVISOR_AUDIT_TRAIL_FLUSH_INTERVAL"`
	MilestoneQueueCapacity         optionalInt        `envconfig:"SLURM_ADVISOR_MILESTONE_QUEUE_CAPACITY"`
	MilestoneRPCTimeout            optionalDuration   `envconfig:"SLURM_ADVISOR_MILESTONE_RPC_TIMEOUT"`
	MilestoneMaxAttempts           optionalInt        `envconfig:"SLURM_ADVISOR_MILESTONE_MAX_ATTEMPTS"`
	MilestoneRetryDelay            optionalDuration   `envconfig:"SLURM_ADVISOR_MILESTONE_RETRY_DELAY"`
	MilestoneShutdownTimeout       optionalDuration   `envconfig:"SLURM_ADVISOR_MILESTONE_SHUTDOWN_TIMEOUT"`
	PartitionAllowlist             partitionAllowlist `envconfig:"SLURM_ADVISOR_PARTITION_ALLOWLIST"`
	VersionedSqueueJSON            bool               `envconfig:"SLURM_ADVISOR_VERSIONED_SQUEUE_JSON"`
	UpdateEligibleTimeAfterRequeue bool               `envconfig:"SLURM_ADVISOR_UPDATE_ELIGIBLE_TIME_AFTER_REQUEUE"`
	AcknowledgeSchedulerActions    bool               `envconfig:"SLURM_ADVISOR_ACKNOWLEDGE_SCHEDULER_ACTIONS"`
	OTLPTraceEndpoint              string             `envconfig:"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"`
	OTLPTraceProtocol              string             `envconfig:"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"`
	LogLinksEnabled                bool               `envconfig:"SLURM_ADVISOR_LOG_LINKS_ENABLED"`
	LogLinksDestination            string             `envconfig:"SLURM_ADVISOR_LOG_LINKS_DESTINATION"`
	LogLinksSourceRoots            jsonStringArray    `envconfig:"SLURM_ADVISOR_LOG_LINKS_SOURCE_ROOTS_JSON"`
	LogLinksApplicationPatterns    jsonStringArray    `envconfig:"SLURM_ADVISOR_LOG_LINKS_APPLICATION_PATTERNS_JSON"`
	LogLinksScanInterval           optionalDuration   `envconfig:"SLURM_ADVISOR_LOG_LINKS_SCAN_INTERVAL"`
	LogLinksCleanupInterval        optionalDuration   `envconfig:"SLURM_ADVISOR_LOG_LINKS_CLEANUP_INTERVAL"`
	LogLinksRetention              optionalDuration   `envconfig:"SLURM_ADVISOR_LOG_LINKS_RETENTION"`
	LogLinksScanTimeout            optionalDuration   `envconfig:"SLURM_ADVISOR_LOG_LINKS_SCAN_TIMEOUT"`
	LogLinksMaxPathLength          optionalInt        `envconfig:"SLURM_ADVISOR_LOG_LINKS_MAX_PATH_LENGTH"`
	LogLinksMaxPathsPerSync        optionalInt        `envconfig:"SLURM_ADVISOR_LOG_LINKS_MAX_PATHS_PER_SYNC"`
	LogLinksMaxPatterns            optionalInt        `envconfig:"SLURM_ADVISOR_LOG_LINKS_MAX_PATTERNS"`
	LogLinksMaxScanDirectories     optionalInt        `envconfig:"SLURM_ADVISOR_LOG_LINKS_MAX_SCAN_DIRECTORIES"`
	LogLinksMaxEntriesPerDirectory optionalInt        `envconfig:"SLURM_ADVISOR_LOG_LINKS_MAX_ENTRIES_PER_DIRECTORY"`
	LogLinksMaxMatchesPerScan      optionalInt        `envconfig:"SLURM_ADVISOR_LOG_LINKS_MAX_MATCHES_PER_SCAN"`
	LogLinksMaxOwnedLinks          optionalInt        `envconfig:"SLURM_ADVISOR_LOG_LINKS_MAX_OWNED_LINKS"`

	UserAllowlist useradmission.Allowlist `envconfig:"SLURM_ADVISOR_USER_ALLOWLIST"`
}

func FromEnv() (Config, error) {
	var settings environmentConfig
	if err := envconfig.Process("", &settings); err != nil {
		return Config{}, wrapEnvconfigError(err)
	}
	if settings.MetricsAddr.set && strings.TrimSpace(settings.MetricsAddr.value) == "" {
		return Config{}, errors.New("SLURM_ADVISOR_METRICS_ADDR must not be empty")
	}
	if settings.ServiceAccountTokenPath.set && strings.TrimSpace(settings.ServiceAccountTokenPath.value) == "" {
		return Config{}, errors.New("SLURM_ADVISOR_SERVICE_ACCOUNT_TOKEN_PATH must not be empty")
	}

	// Traces sent to the shared endpoint use its protocol, including the
	// controller's grpc default for logs, so one collector serves both signals.
	// Only a trace-specific endpoint keeps the OTLP http/protobuf default.
	traceProtocol := firstNonempty(settings.OTLPTraceProtocol, settings.OTLPProtocol)
	if traceProtocol == "" && strings.TrimSpace(settings.OTLPTraceEndpoint) == "" {
		traceProtocol = "grpc"
	}
	cfg := Config{
		GatewayTLS:              tlsconfig.Config{Enabled: settings.TLSEnabled, CAFile: strings.TrimSpace(settings.TLSCAFile), ServerName: strings.TrimSpace(settings.TLSServerName)},
		HeimdallEndpoint:        settings.HeimdallEndpoint,
		ServiceAccountTokenPath: strings.TrimSpace(settings.ServiceAccountTokenPath.value),
		Mode:                    settings.Mode,
		LogLevel:                string(settings.LogLevel),
		OTLP: OTLPConfig{
			Endpoint: settings.OTLPEndpoint,
			Protocol: settings.OTLPProtocol,
			Token:    settings.OTLPToken,
		},
		PollInterval:                   settings.PollInterval,
		SlurmExecTimeout:               settings.SlurmExecTimeout,
		LogMetadataRefreshInterval:     settings.LogMetadataRefreshInterval.value,
		LogMetadataEnrichmentTimeout:   settings.LogMetadataEnrichmentTimeout.value,
		JobUpsertWorkerPoolSize:        settings.JobUpsertWorkerPoolSize.value,
		AccountingWorkerPoolSize:       settings.AccountingWorkerPoolSize.value,
		AccountingTimeout:              settings.AccountingTimeout.value,
		FailedJobRecoveryTimeout:       settings.FailedJobRecoveryTimeout.value,
		AdviceLoopTimeout:              settings.AdviceLoopTimeout,
		AdviceItemTimeout:              settings.AdviceItemTimeout,
		AdviceMaxAge:                   settings.AdviceMaxAge.value,
		AdviceRequeueHoldDelay:         settings.AdviceRequeueHoldDelay,
		EnableEagerRequeue:             settings.EnableEagerRequeue,
		BootstrapFailureLookback:       settings.BootstrapFailureLookback.value,
		AuditTrailDestination:          settings.AuditTrailDestination,
		AuditTrailMaxBatchSize:         settings.AuditTrailMaxBatchSize.value,
		AuditTrailBufferSize:           settings.AuditTrailBufferSize.value,
		AuditTrailFlushInterval:        settings.AuditTrailFlushInterval.value,
		Environment:                    settings.Environment,
		MetricsEnabled:                 settings.MetricsEnabled.value,
		MetricsAddr:                    settings.MetricsAddr.value,
		UserAllowlist:                  []string(settings.UserAllowlist),
		PartitionAllowlist:             []string(settings.PartitionAllowlist),
		VersionedSqueueJSON:            settings.VersionedSqueueJSON,
		UpdateEligibleTimeAfterRequeue: settings.UpdateEligibleTimeAfterRequeue,
		Safety: SafetyChecks{
			AcknowledgeSchedulerActions: settings.AcknowledgeSchedulerActions,
		},
		Tracing: TracingConfig{
			Endpoint: tracingEndpoint(settings.OTLPTraceEndpoint, settings.OTLPEndpoint, traceProtocol),
			Protocol: traceProtocol,
			Token:    strings.TrimSpace(settings.OTLPToken),
		},
		MilestoneReporting: MilestoneReportingConfig{
			QueueCapacity:   settings.MilestoneQueueCapacity.value,
			RPCTimeout:      settings.MilestoneRPCTimeout.value,
			MaxAttempts:     settings.MilestoneMaxAttempts.value,
			RetryDelay:      settings.MilestoneRetryDelay.value,
			ShutdownTimeout: settings.MilestoneShutdownTimeout.value,
		},
		LogLinks: LogLinksConfig{
			Enabled:                      settings.LogLinksEnabled,
			Destination:                  settings.LogLinksDestination,
			SourceRoots:                  []string(settings.LogLinksSourceRoots),
			ApplicationPatterns:          []string(settings.LogLinksApplicationPatterns),
			ScanInterval:                 settings.LogLinksScanInterval.value,
			CleanupInterval:              settings.LogLinksCleanupInterval.value,
			Retention:                    settings.LogLinksRetention.value,
			ScanTimeout:                  settings.LogLinksScanTimeout.value,
			MaxPathLength:                settings.LogLinksMaxPathLength.value,
			MaxPathsPerSync:              settings.LogLinksMaxPathsPerSync.value,
			MaxPatterns:                  settings.LogLinksMaxPatterns.value,
			MaxScanDirectories:           settings.LogLinksMaxScanDirectories.value,
			MaxEntriesPerDirectory:       settings.LogLinksMaxEntriesPerDirectory.value,
			MaxMatchesPerScan:            settings.LogLinksMaxMatchesPerScan.value,
			MaxOwnedLinks:                settings.LogLinksMaxOwnedLinks.value,
			scanIntervalEnvSet:           settings.LogLinksScanInterval.set,
			cleanupIntervalEnvSet:        settings.LogLinksCleanupInterval.set,
			retentionEnvSet:              settings.LogLinksRetention.set,
			scanTimeoutEnvSet:            settings.LogLinksScanTimeout.set,
			maxPathLengthEnvSet:          settings.LogLinksMaxPathLength.set,
			maxPathsPerSyncEnvSet:        settings.LogLinksMaxPathsPerSync.set,
			maxPatternsEnvSet:            settings.LogLinksMaxPatterns.set,
			maxScanDirectoriesEnvSet:     settings.LogLinksMaxScanDirectories.set,
			maxEntriesPerDirectoryEnvSet: settings.LogLinksMaxEntriesPerDirectory.set,
			maxMatchesPerScanEnvSet:      settings.LogLinksMaxMatchesPerScan.set,
			maxOwnedLinksEnvSet:          settings.LogLinksMaxOwnedLinks.set,
		},
		adviceMaxAgeEnvSet:                 settings.AdviceMaxAge.set,
		bootstrapFailureLookbackEnvSet:     settings.BootstrapFailureLookback.set,
		logMetadataRefreshIntervalEnvSet:   settings.LogMetadataRefreshInterval.set,
		logMetadataEnrichmentTimeoutEnvSet: settings.LogMetadataEnrichmentTimeout.set,
		jobUpsertWorkerPoolSizeEnvSet:      settings.JobUpsertWorkerPoolSize.set,
		accountingWorkerPoolSizeEnvSet:     settings.AccountingWorkerPoolSize.set,
		accountingTimeoutEnvSet:            settings.AccountingTimeout.set,
		failedJobRecoveryTimeoutEnvSet:     settings.FailedJobRecoveryTimeout.set,
		metricsEnabledEnvSet:               settings.MetricsEnabled.set,
		metricsAddrEnvSet:                  settings.MetricsAddr.set,
		auditTrailMaxBatchSizeEnvSet:       settings.AuditTrailMaxBatchSize.set,
		auditTrailBufferSizeEnvSet:         settings.AuditTrailBufferSize.set,
		auditTrailFlushIntervalEnvSet:      settings.AuditTrailFlushInterval.set,
		milestoneQueueCapacityEnvSet:       settings.MilestoneQueueCapacity.set,
		milestoneRPCTimeoutEnvSet:          settings.MilestoneRPCTimeout.set,
		milestoneMaxAttemptsEnvSet:         settings.MilestoneMaxAttempts.set,
		milestoneRetryDelayEnvSet:          settings.MilestoneRetryDelay.set,
		milestoneShutdownTimeoutEnvSet:     settings.MilestoneShutdownTimeout.set,
	}

	return WithDefaults(cfg), nil
}

func wrapEnvconfigError(err error) error {
	parseErr, ok := err.(*envconfig.ParseError)
	if !ok || parseErr.Err == nil {
		return errors.Wrap(err, "decode environment configuration")
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
	return "decode environment variable " + e.keyName +
		" into field " + e.fieldName +
		" as " + e.typeName
}

func (e *configDecodeError) Unwrap() error {
	return e.cause
}

type optionalBool struct {
	value bool
	set   bool
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

func (setting *optionalString) Decode(value string) error {
	setting.value = value
	setting.set = true
	return nil
}

type jsonStringArray []string

func (values *jsonStringArray) Decode(value string) error {
	var decoded []string
	if err := json.Unmarshal([]byte(value), &decoded); err != nil {
		return err
	}
	if decoded == nil && strings.TrimSpace(value) != "[]" {
		return errors.New("value must be a JSON array of strings")
	}

	*values = decoded
	return nil
}

type logLevel string

func (level *logLevel) Decode(value string) error {
	parsed, err := parseLogLevel(value)
	if err != nil {
		return err
	}

	*level = logLevel(parsed)
	return nil
}

func parseLogLevel(value string) (string, error) {
	level := strings.ToLower(strings.TrimSpace(value))
	if !validLogLevel(level) {
		return "", errors.Newf("unsupported SLURM_ADVISOR_LOG_LEVEL %q", value)
	}

	return level, nil
}

func validLogLevel(level string) bool {
	switch level {
	case "debug", "info", "warn", "error", "dpanic", "panic", "fatal":
		return true
	default:
		return false
	}
}

type partitionAllowlist []string

func (allowlist *partitionAllowlist) Decode(value string) error {
	*allowlist = parseCSV(value)
	return nil
}

func parseCSV(value string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, item := range strings.Split(value, ",") {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}

	return out
}

func (cfg MilestoneReportingConfig) validate() error {
	if cfg.QueueCapacity <= 0 {
		return errors.Newf("milestone queue capacity must be positive: %d", cfg.QueueCapacity)
	}
	if cfg.RPCTimeout <= 0 {
		return errors.Newf("milestone RPC timeout must be positive: %s", cfg.RPCTimeout)
	}
	if cfg.MaxAttempts <= 0 {
		return errors.Newf("milestone max attempts must be positive: %d", cfg.MaxAttempts)
	}
	if cfg.RetryDelay <= 0 {
		return errors.Newf("milestone retry delay must be positive: %s", cfg.RetryDelay)
	}
	if cfg.ShutdownTimeout <= 0 {
		return errors.Newf("milestone shutdown timeout must be positive: %s", cfg.ShutdownTimeout)
	}
	return nil
}

// tracingEndpoint follows the OTLP exporter specification: a signal-specific
// endpoint is used as given, while the shared endpoint is a base URL to which
// OTLP/HTTP appends /v1/traces. gRPC endpoints carry no signal path.
func tracingEndpoint(traceEndpoint, sharedEndpoint, protocol string) string {
	if endpoint := strings.TrimSpace(traceEndpoint); endpoint != "" {
		return endpoint
	}
	shared := strings.TrimSpace(sharedEndpoint)
	if shared == "" || strings.EqualFold(strings.TrimSpace(protocol), "grpc") {
		return shared
	}
	endpoint, err := url.JoinPath(shared, "v1/traces")
	if err != nil {
		// Leave the value for the exporter to reject; trace export is optional.
		return shared
	}
	return endpoint
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func WithDefaults(cfg Config) Config {
	if cfg.Mode == "" {
		cfg.Mode = ModeObserve
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	cfg.LogLevel = strings.ToLower(cfg.LogLevel)
	if cfg.OTLP.Protocol == "" {
		cfg.OTLP.Protocol = "grpc"
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 15 * time.Second
	}
	if cfg.SlurmExecTimeout == 0 {
		cfg.SlurmExecTimeout = 5 * time.Second
	}
	if cfg.LogMetadataRefreshInterval == 0 && !cfg.logMetadataRefreshIntervalEnvSet {
		cfg.LogMetadataRefreshInterval = 5 * time.Minute
	}
	if cfg.LogMetadataEnrichmentTimeout == 0 && !cfg.logMetadataEnrichmentTimeoutEnvSet {
		cfg.LogMetadataEnrichmentTimeout = 2 * time.Second
	}
	if cfg.JobUpsertWorkerPoolSize == 0 && !cfg.jobUpsertWorkerPoolSizeEnvSet {
		cfg.JobUpsertWorkerPoolSize = 8
	}
	if cfg.AccountingWorkerPoolSize == 0 && !cfg.accountingWorkerPoolSizeEnvSet {
		cfg.AccountingWorkerPoolSize = 8
	}
	if cfg.AccountingTimeout == 0 && !cfg.accountingTimeoutEnvSet {
		cfg.AccountingTimeout = 5 * time.Minute
	}
	if cfg.FailedJobRecoveryTimeout == 0 && !cfg.failedJobRecoveryTimeoutEnvSet {
		cfg.FailedJobRecoveryTimeout = 30 * time.Second
	}
	if cfg.BootstrapFailureLookback == 0 && !cfg.bootstrapFailureLookbackEnvSet {
		cfg.BootstrapFailureLookback = 30 * time.Minute
	}
	if cfg.AdviceLoopTimeout == 0 {
		cfg.AdviceLoopTimeout = cfg.PollInterval
	}
	if cfg.AdviceItemTimeout == 0 {
		cfg.AdviceItemTimeout = cfg.SlurmExecTimeout
	}
	if cfg.AdviceMaxAge == 0 && !cfg.adviceMaxAgeEnvSet {
		cfg.AdviceMaxAge = 5 * time.Minute
	}
	if cfg.AdviceRequeueHoldDelay == 0 {
		cfg.AdviceRequeueHoldDelay = time.Second
	}
	if cfg.AuditTrailDestination == "" {
		cfg.AuditTrailDestination = "logging"
	}
	if cfg.AuditTrailMaxBatchSize == 0 && !cfg.auditTrailMaxBatchSizeEnvSet {
		cfg.AuditTrailMaxBatchSize = 100
	}
	if cfg.AuditTrailBufferSize == 0 && !cfg.auditTrailBufferSizeEnvSet {
		cfg.AuditTrailBufferSize = 1000
	}
	if cfg.AuditTrailFlushInterval == 0 && !cfg.auditTrailFlushIntervalEnvSet {
		cfg.AuditTrailFlushInterval = 30 * time.Second
	}
	milestones := &cfg.MilestoneReporting
	if milestones.QueueCapacity == 0 && !cfg.milestoneQueueCapacityEnvSet {
		milestones.QueueCapacity = 256
	}
	if milestones.RPCTimeout == 0 && !cfg.milestoneRPCTimeoutEnvSet {
		milestones.RPCTimeout = 2 * time.Second
	}
	if milestones.MaxAttempts == 0 && !cfg.milestoneMaxAttemptsEnvSet {
		milestones.MaxAttempts = 3
	}
	if milestones.RetryDelay == 0 && !cfg.milestoneRetryDelayEnvSet {
		milestones.RetryDelay = 100 * time.Millisecond
	}
	if milestones.ShutdownTimeout == 0 && !cfg.milestoneShutdownTimeoutEnvSet {
		milestones.ShutdownTimeout = 2 * time.Second
	}
	if cfg.Environment == "" {
		cfg.Environment = "unspecified"
	}
	if cfg.Tracing.Protocol == "" {
		cfg.Tracing.Protocol = "http/protobuf"
	}
	cfg.Tracing.Protocol = strings.ToLower(strings.TrimSpace(cfg.Tracing.Protocol))
	if !cfg.metricsEnabledEnvSet {
		cfg.MetricsEnabled = true
	}
	if cfg.MetricsAddr == "" && !cfg.metricsAddrEnvSet {
		cfg.MetricsAddr = ":9090"
	}
	if cfg.LogLinks.ScanInterval == 0 && !cfg.LogLinks.scanIntervalEnvSet {
		cfg.LogLinks.ScanInterval = time.Minute
	}
	if cfg.LogLinks.CleanupInterval == 0 && !cfg.LogLinks.cleanupIntervalEnvSet {
		cfg.LogLinks.CleanupInterval = time.Hour
	}
	if cfg.LogLinks.Retention == 0 && !cfg.LogLinks.retentionEnvSet {
		cfg.LogLinks.Retention = 24 * time.Hour
	}
	if cfg.LogLinks.ScanTimeout == 0 && !cfg.LogLinks.scanTimeoutEnvSet {
		cfg.LogLinks.ScanTimeout = 5 * time.Second
	}
	if cfg.LogLinks.MaxPathLength == 0 && !cfg.LogLinks.maxPathLengthEnvSet {
		cfg.LogLinks.MaxPathLength = 4096
	}
	if cfg.LogLinks.MaxPathsPerSync == 0 && !cfg.LogLinks.maxPathsPerSyncEnvSet {
		cfg.LogLinks.MaxPathsPerSync = 4096
	}
	if cfg.LogLinks.MaxPatterns == 0 && !cfg.LogLinks.maxPatternsEnvSet {
		cfg.LogLinks.MaxPatterns = 32
	}
	if cfg.LogLinks.MaxScanDirectories == 0 && !cfg.LogLinks.maxScanDirectoriesEnvSet {
		cfg.LogLinks.MaxScanDirectories = 64
	}
	if cfg.LogLinks.MaxEntriesPerDirectory == 0 && !cfg.LogLinks.maxEntriesPerDirectoryEnvSet {
		cfg.LogLinks.MaxEntriesPerDirectory = 4096
	}
	if cfg.LogLinks.MaxMatchesPerScan == 0 && !cfg.LogLinks.maxMatchesPerScanEnvSet {
		cfg.LogLinks.MaxMatchesPerScan = 512
	}
	if cfg.LogLinks.MaxOwnedLinks == 0 && !cfg.LogLinks.maxOwnedLinksEnvSet {
		cfg.LogLinks.MaxOwnedLinks = 16384
	}
	return cfg
}

func (cfg Config) Validate() error {
	if err := cfg.GatewayTLS.Validate(); err != nil {
		return err
	}
	if err := cfg.OTLP.Validate(); err != nil {
		return err
	}
	if cfg.HeimdallEndpoint == "" {
		return errors.New("heimdall endpoint is required")
	}
	if cfg.ServiceAccountTokenPath != "" && strings.TrimSpace(cfg.ServiceAccountTokenPath) == "" {
		return errors.New("service account token path must not be empty when configured")
	}
	if cfg.PollInterval <= 0 {
		return errors.Newf("poll interval must be positive: %s", cfg.PollInterval)
	}
	if cfg.SlurmExecTimeout <= 0 {
		return errors.Newf("slurm exec timeout must be positive: %s", cfg.SlurmExecTimeout)
	}
	if cfg.LogMetadataRefreshInterval <= 0 {
		return errors.Newf(
			"log metadata refresh interval must be positive: %s",
			cfg.LogMetadataRefreshInterval,
		)
	}
	if cfg.LogMetadataEnrichmentTimeout <= 0 {
		return errors.Newf(
			"log metadata enrichment timeout must be positive: %s",
			cfg.LogMetadataEnrichmentTimeout,
		)
	}
	if cfg.JobUpsertWorkerPoolSize <= 0 {
		return errors.Newf(
			"job upsert worker pool size must be positive: %d",
			cfg.JobUpsertWorkerPoolSize,
		)
	}
	if cfg.AccountingWorkerPoolSize <= 0 {
		return errors.Newf("accounting worker pool size must be positive: %d", cfg.AccountingWorkerPoolSize)
	}
	if cfg.AccountingTimeout <= 0 {
		return errors.Newf("accounting timeout must be positive: %s", cfg.AccountingTimeout)
	}
	if cfg.FailedJobRecoveryTimeout <= 0 {
		return errors.Newf("failed-job recovery timeout must be positive: %s", cfg.FailedJobRecoveryTimeout)
	}
	if cfg.BootstrapFailureLookback <= 0 {
		return errors.Newf("bootstrap failure lookback must be positive: %s", cfg.BootstrapFailureLookback)
	}
	if cfg.AdviceLoopTimeout <= 0 {
		return errors.Newf("advice loop timeout must be positive: %s", cfg.AdviceLoopTimeout)
	}
	if cfg.AdviceItemTimeout <= 0 {
		return errors.Newf("advice item timeout must be positive: %s", cfg.AdviceItemTimeout)
	}
	if cfg.AdviceMaxAge <= 0 {
		return errors.Newf("advice max age must be positive: %s", cfg.AdviceMaxAge)
	}
	if cfg.AdviceRequeueHoldDelay <= 0 {
		return errors.Newf("advice requeue hold delay must be positive: %s", cfg.AdviceRequeueHoldDelay)
	}
	if !validLogLevel(cfg.LogLevel) {
		return errors.Newf("unsupported log level %q", cfg.LogLevel)
	}
	if cfg.MetricsEnabled && strings.TrimSpace(cfg.MetricsAddr) == "" {
		return errors.New("metrics address is required when metrics are enabled")
	}
	if cfg.AuditTrailDestination != "logging" && cfg.AuditTrailDestination != "gateway" {
		return errors.New("audit trail destination must be logging or gateway")
	}
	if cfg.AuditTrailMaxBatchSize <= 0 {
		return errors.Newf(
			"audit trail max batch size must be positive: %d",
			cfg.AuditTrailMaxBatchSize,
		)
	}
	if cfg.AuditTrailDestination == "gateway" &&
		cfg.AuditTrailMaxBatchSize > audit.MaxGatewayBatchSize {
		return errors.Newf(
			"audit trail max batch size for gateway destination must not exceed %d: %d",
			audit.MaxGatewayBatchSize,
			cfg.AuditTrailMaxBatchSize,
		)
	}
	if cfg.AuditTrailBufferSize <= 0 {
		return errors.Newf(
			"audit trail buffer size must be positive: %d",
			cfg.AuditTrailBufferSize,
		)
	}
	if cfg.AuditTrailFlushInterval <= 0 {
		return errors.Newf(
			"audit trail flush interval must be positive: %s",
			cfg.AuditTrailFlushInterval,
		)
	}
	if err := cfg.MilestoneReporting.validate(); err != nil {
		return err
	}
	if err := cfg.LogLinks.validate(); err != nil {
		return err
	}

	switch cfg.Mode {
	case ModeIgnore, ModeObserve:
		return nil
	case ModeAct:
		if !cfg.Safety.AcknowledgeSchedulerActions {
			return errors.New("act mode requires explicit scheduler action acknowledgement")
		}

		return nil
	default:
		return errors.Newf("unsupported mode %q", cfg.Mode)
	}
}

func (cfg LogLinksConfig) validate() error {
	if !cfg.Enabled {
		return nil
	}
	if strings.TrimSpace(cfg.Destination) == "" {
		return errors.New("log links destination is required when log links are enabled")
	}
	if len(cfg.SourceRoots) == 0 {
		return errors.New("at least one log links source root is required when log links are enabled")
	}
	for _, root := range cfg.SourceRoots {
		if strings.TrimSpace(root) == "" {
			return errors.New("log links source roots must not contain empty values")
		}
	}

	positiveDurations := []struct {
		name  string
		value time.Duration
	}{
		{name: "scan interval", value: cfg.ScanInterval},
		{name: "cleanup interval", value: cfg.CleanupInterval},
		{name: "retention", value: cfg.Retention},
		{name: "scan timeout", value: cfg.ScanTimeout},
	}
	for _, setting := range positiveDurations {
		if setting.value <= 0 {
			return errors.Newf("log links %s must be positive: %s", setting.name, setting.value)
		}
	}

	positiveLimits := []struct {
		name  string
		value int
	}{
		{name: "max path length", value: cfg.MaxPathLength},
		{name: "max paths per sync", value: cfg.MaxPathsPerSync},
		{name: "max patterns", value: cfg.MaxPatterns},
		{name: "max scan directories", value: cfg.MaxScanDirectories},
		{name: "max entries per directory", value: cfg.MaxEntriesPerDirectory},
		{name: "max matches per scan", value: cfg.MaxMatchesPerScan},
		{name: "max owned links", value: cfg.MaxOwnedLinks},
	}
	for _, setting := range positiveLimits {
		if setting.value <= 0 {
			return errors.Newf("log links %s must be positive: %d", setting.name, setting.value)
		}
	}

	return nil
}
