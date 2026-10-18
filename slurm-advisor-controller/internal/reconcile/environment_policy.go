// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	admission "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/environment"
)

const (
	EnvironmentUnspecified = admission.Unspecified
	EnvironmentProduction  = admission.Production
)

type EnvironmentAdmissionReason = admission.AdmissionReason

const (
	EnvironmentAdmissionMatched               = admission.AdmissionMatched
	EnvironmentAdmissionControllerUnspecified = admission.AdmissionControllerUnspecified
	EnvironmentAdmissionProductionDefault     = admission.AdmissionProductionDefault
	EnvironmentAdmissionMismatch              = admission.AdmissionMismatch
	EnvironmentAdmissionNotOnboarded          = admission.AdmissionNotOnboarded
	EnvironmentAdmissionInvalid               = admission.AdmissionInvalid
)

type EnvironmentAdmissionDecision = admission.AdmissionDecision

func environmentAdmission(controllerEnvironment string, job cluster.JobExecutionRecord) EnvironmentAdmissionDecision {
	return admission.AdmitLiteral(controllerEnvironment, job)
}

// heimdallJobEnvironmentAdmission filters discovery using the stored APS value,
// not the publishing backend's scheduler metadata. Missing config or environment
// means undeclared, including for legacy records. This only admits a candidate:
// fresh Slurm data must still establish onboarding, routing, mode, and state.
func heimdallJobEnvironmentAdmission(controllerEnvironment string, job *pb.Job) EnvironmentAdmissionDecision {
	environment := job.GetRecoveryConfig().GetEnvironment()
	return environmentAdmission(controllerEnvironment, cluster.JobExecutionRecord{
		HeimdallManaged: true,
		EnvironmentRequest: cluster.JobEnvironmentRequest{
			Environment: environment,
			Specified:   environment != "",
			Valid:       true,
		},
	})
}

func (c *Controller) controllerEnvironment() string {
	if c.Options.Environment != "" {
		return c.Options.Environment
	}
	if c.Environment != "" {
		return c.Environment
	}
	return EnvironmentUnspecified
}

func (c *Controller) admitJob(snapshot *ControllerSnapshot, job cluster.JobExecutionRecord) bool {
	key := jobAttemptRef(job)
	if !validJobKey(key) {
		return false
	}
	decision := environmentAdmission(c.controllerEnvironment(), job)
	snapshot.EnvironmentAdmissions[key] = decision
	if !decision.Admitted {
		latest, exists := snapshot.LatestRejectedEnvironmentAttempt[key.JobID]
		if !exists || key.AttemptIndex > latest {
			snapshot.LatestRejectedEnvironmentAttempt[key.JobID] = key.AttemptIndex
		}
	}
	return decision.Admitted
}

// environmentAdmissionBlocks reports whether this attempt is at or behind an
// environment-rejected attempt of the same scheduler job. A newer rejected
// attempt must prevent stale advice or accounting data from mutating the
// currently routed-away Slurm allocation.
func environmentAdmissionBlocks(snapshot ControllerSnapshot, key cluster.JobAttemptRef) bool {
	latestRejectedAttempt, exists := snapshot.LatestRejectedEnvironmentAttempt[key.JobID]
	return exists && latestRejectedAttempt >= key.AttemptIndex
}
