// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobpolicy"
)

// ModePolicy is the shared job policy used by SAC and watchdog.
type ModePolicy = jobpolicy.Policy

// ModeDecisionReason explains a shared job-policy decision.
type ModeDecisionReason = jobpolicy.Reason

// EffectiveModeDecision records requested and effective job modes.
type EffectiveModeDecision = jobpolicy.Decision

const (
	// ModeLabelInvalid preserves the shared policy decision label.
	ModeLabelInvalid = jobpolicy.ModeLabelInvalid
	// ModeReasonDefault preserves the shared policy decision label.
	ModeReasonDefault = jobpolicy.ModeReasonDefault
	// ModeReasonJobOverride preserves the shared policy decision label.
	ModeReasonJobOverride = jobpolicy.ModeReasonJobOverride
	// ModeReasonMissingComment preserves the shared policy decision label.
	ModeReasonMissingComment = jobpolicy.ModeReasonMissingComment
	// ModeReasonInvalidRequest preserves the shared policy decision label.
	ModeReasonInvalidRequest = jobpolicy.ModeReasonInvalidRequest
	// ModeReasonActionDenied preserves the shared policy decision label.
	ModeReasonActionDenied = jobpolicy.ModeReasonActionDenied
	// ModeReasonInvalidDefault preserves the shared policy decision label.
	ModeReasonInvalidDefault = jobpolicy.ModeReasonInvalidDefault
	// ModeReasonUserSelected preserves the shared policy decision label.
	ModeReasonUserSelected = jobpolicy.ModeReasonUserSelected
	// ModeReasonUserUnselected preserves the shared policy decision label.
	ModeReasonUserUnselected = jobpolicy.ModeReasonUserUnselected
)

// ResolveEffectiveMode applies shared job policy to a fresh scheduler record.
func ResolveEffectiveMode(policy ModePolicy, job cluster.JobExecutionRecord) EffectiveModeDecision {
	return jobpolicy.Resolve(policy, job)
}

func resolveSnapshotModes(snapshot *ControllerSnapshot, policy ModePolicy, observation *SyncObservation) {
	for _, key := range sortedSnapshotKeys(*snapshot) {
		if environmentAdmissionBlocks(*snapshot, key) {
			continue
		}
		job, ok := jobForModePolicy(*snapshot, key)
		if !ok {
			continue
		}
		decision := ResolveEffectiveMode(policy, job)
		snapshot.EffectiveModes[key] = decision
		observeModeDecision(observation, policy.DefaultMode, decision)
	}
}

func modeDecisionForJob(snapshot ControllerSnapshot, policy ModePolicy, job cluster.JobExecutionRecord) EffectiveModeDecision {
	if decision, ok := snapshot.EffectiveModes[jobAttemptRef(job)]; ok {
		return decision
	}
	return ResolveEffectiveMode(policy, job)
}
