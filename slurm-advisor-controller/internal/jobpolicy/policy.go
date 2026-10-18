// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package jobpolicy resolves the job action policy shared by SAC and watchdog.
package jobpolicy

import (
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/useradmission"
)

// Policy selects a job mode without bypassing operator action acknowledgement.
type Policy struct {
	// DefaultMode applies when neither the job nor the rollout list selects a mode.
	DefaultMode cluster.JobMode
	// AllowAct is the independent operator acknowledgement ceiling.
	AllowAct bool
	// UserAllowlist temporarily selects act for listed users and observe for others.
	// An empty list leaves the default unchanged; explicit job mode always wins.
	UserAllowlist []string
}

// Reason identifies why a job received its effective mode.
type Reason string

const (
	// ModeReasonUserSelected records selection by the temporary rollout list.
	ModeReasonUserSelected Reason = "user_selected"
	// ModeReasonUserUnselected records passive monitoring outside the rollout list.
	ModeReasonUserUnselected Reason = "user_unselected"
	// ModeLabelInvalid identifies the invalid requested-mode label.
	ModeLabelInvalid cluster.JobMode = "invalid"
	// ModeReasonDefault identifies the controller default selection.
	ModeReasonDefault Reason = "default"
	// ModeReasonJobOverride identifies the explicit job selection.
	ModeReasonJobOverride Reason = "job_override"
	// ModeReasonMissingComment identifies the absence of a valid onboarding specification.
	ModeReasonMissingComment Reason = "missing_comment"
	// ModeReasonInvalidRequest identifies the invalid explicit job request.
	ModeReasonInvalidRequest Reason = "invalid_request"
	// ModeReasonActionDenied identifies the operator acknowledgement ceiling.
	ModeReasonActionDenied Reason = "action_denied"
	// ModeReasonInvalidDefault identifies the invalid controller default.
	ModeReasonInvalidDefault Reason = "invalid_default"
)

// Decision records requested and effective modes with their selection reason.
type Decision struct {
	// Mode is the final mode after applying the acknowledgement ceiling.
	Mode cluster.JobMode
	// RequestedMode is the selected mode before the acknowledgement ceiling.
	RequestedMode cluster.JobMode
	// Reason explains the effective decision.
	Reason Reason
}

// Resolve honors explicit job mode, then the temporary username selection,
// then the default. Invalid explicit requests fail closed to observe.
func Resolve(policy Policy, job cluster.JobExecutionRecord) Decision {
	if job.HeimdallManaged && job.ModeRequest.Specified {
		if !job.ModeRequest.Valid {
			return Decision{
				Mode:          cluster.JobModeObserve,
				RequestedMode: ModeLabelInvalid,
				Reason:        ModeReasonInvalidRequest,
			}
		}
		return applyActionCeiling(policy, job.ModeRequest.Mode, ModeReasonJobOverride)
	}

	if job.HeimdallManaged && len(policy.UserAllowlist) > 0 {
		if useradmission.Allowlist(policy.UserAllowlist).Admits(job.User) {
			return applyActionCeiling(policy, cluster.JobModeAct, ModeReasonUserSelected)
		}
		return applyActionCeiling(policy, cluster.JobModeObserve, ModeReasonUserUnselected)
	}

	if !job.HeimdallManaged && policy.DefaultMode == cluster.JobModeAct {
		return Decision{
			Mode:          cluster.JobModeObserve,
			RequestedMode: cluster.JobModeAct,
			Reason:        ModeReasonMissingComment,
		}
	}

	switch policy.DefaultMode {
	case cluster.JobModeIgnore, cluster.JobModeObserve, cluster.JobModeAct:
		return applyActionCeiling(policy, policy.DefaultMode, ModeReasonDefault)
	default:
		return Decision{
			Mode:          cluster.JobModeObserve,
			RequestedMode: ModeLabelInvalid,
			Reason:        ModeReasonInvalidDefault,
		}
	}
}

func applyActionCeiling(policy Policy, requested cluster.JobMode, reason Reason) Decision {
	if requested == cluster.JobModeAct && !policy.AllowAct {
		return Decision{
			Mode:          cluster.JobModeObserve,
			RequestedMode: requested,
			Reason:        ModeReasonActionDenied,
		}
	}
	return Decision{
		Mode:          requested,
		RequestedMode: requested,
		Reason:        reason,
	}
}
