// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"time"

	"github.com/cockroachdb/errors"
)

const (
	SchedulerSlurm        = "slurm"
	ManagedBySlurmAdvisor = "slurm-advisor-controller"
	AutoResumeModeUnset   = AutoResumeMode("")
	JobStateUnspecified   = JobState("")
)

var (
	ErrEmptyNodeUpdate   = errors.New("node update is empty")
	ErrInvalidNodeCount  = errors.New("node update has invalid node count")
	ErrNodeUpdateOverlap = errors.New("node update has overlapping inclusions and exclusions")
)

type JobState string

const (
	JobStatePending   JobState = "pending"
	JobStateRunning   JobState = "running"
	JobStateStopped   JobState = "stopped"
	JobStateCompleted JobState = "completed"
	JobStateFailed    JobState = "failed"
	JobStateRequeued  JobState = "requeued"
	JobStateCancelled JobState = "cancelled"
	JobStateDeleted   JobState = "deleted"
)

type AutoResumeMode string

const (
	AutoResumeModeRequeue             AutoResumeMode = "requeue"
	AutoResumeModeSingletonDependency AutoResumeMode = "singleton_dependency"
)

type RecoveryConfig struct {
	AutoResumeMode                   AutoResumeMode
	MaxRequeueTimes                  *int32
	RequeueOnCompletion              *bool
	LogFilePathsSharedAcrossAttempts *bool
	AnomalyDetection                 *AnomalyDetectionConfig
	DAGDependencies                  []DAGDependency
	DAGAction                        DAGAction
	Environment                      *string
}

type AnomalyDetectionConfig struct {
	LogBasedCrashEnabled                 *bool
	LogBasedTimeoutEnabled               *bool
	LogBasedTimeoutThresholdPretraining  *int32
	LogBasedTimeoutThresholdTrainingloop *int32
}

type DAGAction string

const (
	DAGActionUnset   = DAGAction("")
	DAGActionRelease = DAGAction("release")
)

type DAGTriggerCondition string

const DAGTriggerConditionCompleted = DAGTriggerCondition("completed")

type DAGTargetType string

const (
	DAGTargetTypeID   = DAGTargetType("id")
	DAGTargetTypeName = DAGTargetType("name")
)

type DAGDependency struct {
	TargetType       DAGTargetType
	TargetIdentifier string
	Trigger          DAGTriggerCondition
}

// JobMode is the scheduler-neutral policy capability requested for one job.
type JobMode string

const (
	JobModeIgnore  JobMode = "ignore"
	JobModeObserve JobMode = "observe"
	JobModeAct     JobMode = "act"
)

// JobModeRequest preserves whether a valid Heimdall comment contained a mode
// field separately from whether that field contained a supported value.
type JobModeRequest struct {
	Mode      JobMode
	Specified bool
	Valid     bool
}

// JobEnvironmentRequest preserves both protobuf-style presence and validity
// for the optional APS environment string. Empty strings are valid and retain
// presence even though routing treats them like an absent value.
type JobEnvironmentRequest struct {
	Environment string
	Specified   bool
	Valid       bool
}

type NodeConstraints struct {
	Inclusions []string
	Exclusions []string
}

type JobAttemptRef struct {
	JobID        string
	AttemptIndex int32
}

// JobExecutionRecord is the scheduler-neutral job model consumed by Advisor reconciliation.
type JobExecutionRecord struct {
	JobID              string
	RawJobID           string
	ArrayJobID         string
	ArrayTaskID        string
	AttemptIndex       int32
	JobName            string
	WorkflowID         string
	State              JobState
	RawState           string
	Reason             string
	Held               bool
	HeimdallManaged    bool
	ModeRequest        JobModeRequest
	EnvironmentRequest JobEnvironmentRequest
	SubmitTime         *time.Time
	StartTime          *time.Time
	EndTime            *time.Time
	Cluster            string
	Partition          string
	Reservation        string
	// User is the submitting username resolved by the scheduler, or empty if unknown.
	User              string
	Comment           string
	Command           string
	Dependency        string
	DAGChild          bool
	TotalTaskCount    int
	NodeCount         int
	AllocatedNodes    []string
	FailedNodes       []string
	NodeConstraints   NodeConstraints
	TotalCPUCount     float64
	TotalMemoryMB     float64
	TotalGPUCount     float64
	RecoveryConfig    *RecoveryConfig
	SchedulerMetadata map[string]any
	LastUpdatedAt     time.Time
}
