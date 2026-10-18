// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"encoding/json"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"go.uber.org/zap"
)

var now = time.Now

const (
	slurmNoValueUnixTime  = uint64(0xfffffffe)
	slurmInfiniteUnixTime = uint64(0xffffffff)
)

func mapJobState(rawState string) cluster.JobState {
	switch strings.ToUpper(rawState) {
	case "PENDING", "CONFIGURING":
		return cluster.JobStatePending
	case "RUNNING", "COMPLETING", "RESIZING", "SUSPENDED":
		return cluster.JobStateRunning
	case "STOPPED":
		return cluster.JobStateStopped
	case "COMPLETED":
		return cluster.JobStateCompleted
	case "CANCELLED":
		return cluster.JobStateCancelled
	case "REQUEUED":
		return cluster.JobStateRequeued
	case "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "TIMEOUT", "BOOT_FAIL", "DEADLINE", "PREEMPTED", "REVOKED", "SPECIAL_EXIT":
		return cluster.JobStateFailed
	default:
		return cluster.JobStateUnspecified
	}
}

func setUnixTime(value int, target **time.Time, metadata map[string]any, key string) {
	if !isConcreteSlurmUnixTime(value) {
		return
	}
	converted := time.Unix(int64(value), 0)
	*target = &converted
	metadata[key] = converted.Format(time.RFC3339)
}

func isConcreteSlurmUnixTime(value int) bool {
	if value <= 0 {
		return false
	}
	seconds := uint64(value)
	return seconds != slurmNoValueUnixTime && seconds != slurmInfiniteUnixTime
}

func resolveWorkflowIdentity(comment, jobName, schedulerJobID string) string {
	spec := commentSpec(comment)
	if spec != nil {
		if workflowID, ok := stringValue(spec, "workflow_id"); ok && validWorkflowIdentity(workflowID) {
			return workflowID
		}
		if workflowName, ok := stringValue(spec, "workflow_name"); ok && validWorkflowIdentity(workflowName) {
			return workflowName
		}
	}

	if validWorkflowIdentity(jobName) {
		return jobName
	}
	return schedulerJobID
}

func validWorkflowIdentity(value string) bool {
	return value != "" && utf8.ValidString(value) && utf8.RuneCountInString(value) <= 256
}

func recoveryConfigFromComment(comment string) *cluster.RecoveryConfig {
	spec := commentSpec(comment)
	if spec == nil {
		return nil
	}

	config := &cluster.RecoveryConfig{}
	if environment, ok := optionalStringValue(spec, "environment"); ok {
		config.Environment = &environment
	}
	if mode, ok := stringValue(spec, "auto_resume_mode"); ok {
		switch strings.ToLower(mode) {
		case string(cluster.AutoResumeModeRequeue):
			config.AutoResumeMode = cluster.AutoResumeModeRequeue
		case string(cluster.AutoResumeModeSingletonDependency):
			config.AutoResumeMode = cluster.AutoResumeModeSingletonDependency
		}
	}
	if maxRequeues, ok := int32Value(spec, "max_requeue_times"); ok && maxRequeues > 0 {
		config.MaxRequeueTimes = &maxRequeues
	}
	if value, ok := boolValue(spec, "requeue_on_completion"); ok {
		config.RequeueOnCompletion = &value
	}
	if value, ok := boolValue(spec, "log_file_paths_shared_across_attempts"); ok {
		config.LogFilePathsSharedAcrossAttempts = &value
	}
	if values, ok := spec["anomaly_detection_config"].(map[string]any); ok {
		anomalyConfig := &cluster.AnomalyDetectionConfig{}
		if value, present := boolValue(values, "log_based_crash_enabled"); present {
			anomalyConfig.LogBasedCrashEnabled = &value
		}
		if value, present := boolValue(values, "log_based_timeout_enabled"); present {
			anomalyConfig.LogBasedTimeoutEnabled = &value
		}
		if value, present := int32Value(values, "log_based_timeout_threshold_pretraining"); present && value > 0 {
			anomalyConfig.LogBasedTimeoutThresholdPretraining = &value
		}
		if value, present := int32Value(values, "log_based_timeout_threshold_trainingloop"); present && value > 0 {
			anomalyConfig.LogBasedTimeoutThresholdTrainingloop = &value
		}
		if anomalyConfig.LogBasedCrashEnabled != nil ||
			anomalyConfig.LogBasedTimeoutEnabled != nil ||
			anomalyConfig.LogBasedTimeoutThresholdPretraining != nil ||
			anomalyConfig.LogBasedTimeoutThresholdTrainingloop != nil {
			config.AnomalyDetection = anomalyConfig
		}
	}
	config.DAGDependencies, config.DAGAction = dagPolicyFromSpec(spec)
	if config.AutoResumeMode == cluster.AutoResumeModeUnset &&
		config.MaxRequeueTimes == nil &&
		config.RequeueOnCompletion == nil &&
		config.LogFilePathsSharedAcrossAttempts == nil &&
		config.AnomalyDetection == nil &&
		len(config.DAGDependencies) == 0 &&
		config.DAGAction == cluster.DAGActionUnset &&
		config.Environment == nil {
		return nil
	}
	return config
}

const invalidRecoveryCapWarningLimit = 1024

type invalidRecoveryCapKey struct {
	attempt cluster.JobAttemptRef
	raw     float64
}

// invalidRecoveryCapWarnings bounds process-local warning history. Scheduler
// reads can share this cache; it never gates publication or recovery decisions.
type invalidRecoveryCapWarnings struct {
	sync.Mutex
	seen  map[invalidRecoveryCapKey]struct{}
	order []invalidRecoveryCapKey
	next  int
}

func (s *slurmCluster) warnInvalidRecoveryCaps(jobs []cluster.JobExecutionRecord) {
	if s.logger == nil {
		return
	}
	for _, job := range jobs {
		raw, ok := commentSpec(job.Comment)["max_requeue_times"].(float64)
		if !ok || raw > 0 {
			continue
		}
		if job.RecoveryConfig != nil && job.RecoveryConfig.MaxRequeueTimes != nil {
			continue
		}
		key := invalidRecoveryCapKey{attempt: jobAttemptRef(job), raw: raw}
		warnings := &s.invalidRecoveryCaps
		warnings.Lock()
		if _, found := warnings.seen[key]; found {
			warnings.Unlock()
			continue
		}
		if warnings.seen == nil {
			warnings.seen = make(map[invalidRecoveryCapKey]struct{})
		}
		if len(warnings.order) < invalidRecoveryCapWarningLimit {
			warnings.order = append(warnings.order, key)
		} else {
			delete(warnings.seen, warnings.order[warnings.next])
			warnings.order[warnings.next] = key
			warnings.next = (warnings.next + 1) % invalidRecoveryCapWarningLimit
		}
		warnings.seen[key] = struct{}{}
		warnings.Unlock()
		s.logger.Warn("ignoring nonpositive job max_requeue_times; using system retry limit",
			zap.String("job_id", job.JobID),
			zap.Int32("attempt_index", job.AttemptIndex),
			zap.Float64("raw_max_requeue_times", raw),
		)
	}
}

func dagPolicyFromSpec(spec map[string]any) ([]cluster.DAGDependency, cluster.DAGAction) {
	action := cluster.DAGActionUnset
	if rawAction, present := spec["dag_action"]; present {
		value, ok := rawAction.(string)
		if !ok || value != string(cluster.DAGActionRelease) {
			return nil, cluster.DAGActionUnset
		}
		action = cluster.DAGActionRelease
	}

	rawDependencies, present := spec["dag_dependencies"]
	if !present {
		return nil, action
	}
	dependencies, ok := rawDependencies.([]any)
	if !ok {
		return nil, cluster.DAGActionUnset
	}

	parsed := make([]cluster.DAGDependency, 0, len(dependencies))
	for _, rawDependency := range dependencies {
		dependency, ok := dagDependencyFromValue(rawDependency)
		if !ok {
			return nil, cluster.DAGActionUnset
		}
		parsed = append(parsed, dependency)
	}
	return parsed, action
}

func dagDependencyFromValue(value any) (cluster.DAGDependency, bool) {
	step, ok := value.(map[string]any)
	if !ok {
		return cluster.DAGDependency{}, false
	}
	target, ok := step["target"].(map[string]any)
	if !ok {
		return cluster.DAGDependency{}, false
	}

	targetType, ok := target["type"].(string)
	if !ok {
		return cluster.DAGDependency{}, false
	}
	dependency := cluster.DAGDependency{Trigger: cluster.DAGTriggerConditionCompleted}
	switch cluster.DAGTargetType(targetType) {
	case cluster.DAGTargetTypeID:
		identifier, ok := target["id"].(string)
		if !ok {
			return cluster.DAGDependency{}, false
		}
		dependency.TargetType = cluster.DAGTargetTypeID
		dependency.TargetIdentifier = identifier
	case cluster.DAGTargetTypeName:
		identifier, ok := target["name"].(string)
		if !ok {
			return cluster.DAGDependency{}, false
		}
		dependency.TargetType = cluster.DAGTargetTypeName
		dependency.TargetIdentifier = identifier
	default:
		return cluster.DAGDependency{}, false
	}

	if rawTrigger, present := step["trigger"]; present {
		trigger, ok := rawTrigger.(string)
		if !ok || trigger != string(cluster.DAGTriggerConditionCompleted) {
			return cluster.DAGDependency{}, false
		}
	}
	return dependency, true
}

func dagChildFromComment(comment string) bool {
	spec := commentSpec(comment)
	if spec == nil {
		return false
	}
	dependencies, ok := spec["dag_dependencies"].([]any)
	return ok && len(dependencies) > 0
}

func jobModeRequestFromComment(comment string) (bool, cluster.JobModeRequest) {
	spec := commentSpec(comment)
	if spec == nil {
		return false, cluster.JobModeRequest{}
	}

	rawMode, specified := spec["mode"]
	if !specified {
		return true, cluster.JobModeRequest{Valid: true}
	}
	mode, ok := rawMode.(string)
	if !ok {
		return true, cluster.JobModeRequest{Specified: true}
	}

	request := cluster.JobModeRequest{
		Mode:      cluster.JobMode(strings.ToLower(mode)),
		Specified: true,
	}
	switch request.Mode {
	case cluster.JobModeIgnore, cluster.JobModeObserve, cluster.JobModeAct:
		request.Valid = true
	}

	return true, request
}

func jobEnvironmentRequestFromComment(comment string) cluster.JobEnvironmentRequest {
	spec := commentSpec(comment)
	if spec == nil {
		return cluster.JobEnvironmentRequest{}
	}

	rawEnvironment, specified := spec["environment"]
	if !specified || rawEnvironment == nil {
		return cluster.JobEnvironmentRequest{Valid: true}
	}
	environment, ok := rawEnvironment.(string)
	if !ok {
		return cluster.JobEnvironmentRequest{Specified: true}
	}

	return cluster.JobEnvironmentRequest{
		Environment: environment,
		Specified:   true,
		Valid:       true,
	}
}

func commentSpec(comment string) map[string]any {
	if comment == "" {
		return nil
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(comment), &decoded); err != nil {
		return nil
	}
	if spec, ok := decoded["APS"].(map[string]any); ok {
		return spec
	}
	if spec, ok := decoded["HEIMDALL"].(map[string]any); ok {
		return spec
	}
	return nil
}

func stringValue(values map[string]any, key string) (string, bool) {
	value, ok := values[key].(string)
	if !ok || value == "" {
		return "", false
	}
	return value, true
}

func optionalStringValue(values map[string]any, key string) (string, bool) {
	value, ok := values[key].(string)
	return value, ok
}

func int32Value(values map[string]any, key string) (int32, bool) {
	switch value := values[key].(type) {
	case float64:
		return int32(value), true
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return 0, false
		}
		return int32(parsed), true
	case int:
		return int32(value), true
	default:
		return 0, false
	}
}

func boolValue(values map[string]any, key string) (bool, bool) {
	value, ok := values[key].(bool)
	return value, ok
}
