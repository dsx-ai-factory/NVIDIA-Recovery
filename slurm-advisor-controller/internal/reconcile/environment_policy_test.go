// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

func TestEnvironmentAdmissionStringSemantics(t *testing.T) {
	validAbsent := cluster.JobExecutionRecord{HeimdallManaged: true, EnvironmentRequest: cluster.JobEnvironmentRequest{Valid: true}}
	validEmpty := cluster.JobExecutionRecord{HeimdallManaged: true, EnvironmentRequest: cluster.JobEnvironmentRequest{Specified: true, Valid: true}}
	jobEnvironment := func(value string) cluster.JobExecutionRecord {
		return cluster.JobExecutionRecord{HeimdallManaged: true, EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: value, Specified: true, Valid: true}}
	}

	tests := []struct {
		name       string
		controller string
		job        cluster.JobExecutionRecord
		wantAdmit  bool
		wantReason EnvironmentAdmissionReason
	}{
		{name: "unspecified accepts absent", controller: "unspecified", job: validAbsent, wantAdmit: true, wantReason: EnvironmentAdmissionControllerUnspecified},
		{name: "unspecified accepts arbitrary", controller: "unspecified", job: jobEnvironment("QA/Canary 01"), wantAdmit: true, wantReason: EnvironmentAdmissionControllerUnspecified},
		{name: "prod defaults absent", controller: "prod", job: validAbsent, wantAdmit: true, wantReason: EnvironmentAdmissionProductionDefault},
		{name: "prod defaults empty", controller: "prod", job: validEmpty, wantAdmit: true, wantReason: EnvironmentAdmissionProductionDefault},
		{name: "prod exact match", controller: "prod", job: jobEnvironment("prod"), wantAdmit: true, wantReason: EnvironmentAdmissionMatched},
		{name: "arbitrary exact match", controller: "QA/Canary 01", job: jobEnvironment("QA/Canary 01"), wantAdmit: true, wantReason: EnvironmentAdmissionMatched},
		{name: "comparison is case sensitive", controller: "Prod", job: jobEnvironment("prod"), wantReason: EnvironmentAdmissionMismatch},
		{name: "comparison does not trim", controller: "prod", job: jobEnvironment(" prod "), wantReason: EnvironmentAdmissionMismatch},
		{name: "non-prod does not accept absent", controller: "dev", job: validAbsent, wantReason: EnvironmentAdmissionMismatch},
		{name: "missing APS is not onboarded", controller: "unspecified", job: cluster.JobExecutionRecord{}, wantReason: EnvironmentAdmissionNotOnboarded},
		{name: "non-string environment is invalid", controller: "unspecified", job: cluster.JobExecutionRecord{HeimdallManaged: true, EnvironmentRequest: cluster.JobEnvironmentRequest{Specified: true}}, wantReason: EnvironmentAdmissionInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := environmentAdmission(tt.controller, tt.job)
			if got.Admitted != tt.wantAdmit || got.Reason != tt.wantReason {
				t.Fatalf("environmentAdmission() = %#v, want admitted=%t reason=%q", got, tt.wantAdmit, tt.wantReason)
			}
		})
	}
}

func TestAdmitJobIndexesLatestRejectedAttemptByJobID(t *testing.T) {
	controller := Controller{Options: ControllerOptions{Environment: "blue"}}
	snapshot := newControllerSnapshot(time.Time{})

	for _, job := range []cluster.JobExecutionRecord{
		{JobID: "same-job", AttemptIndex: 3, HeimdallManaged: true, EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "green", Specified: true, Valid: true}},
		{JobID: "same-job", AttemptIndex: 1, HeimdallManaged: true, EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "green", Specified: true, Valid: true}},
		{JobID: "other-job", AttemptIndex: 7, HeimdallManaged: true, EnvironmentRequest: cluster.JobEnvironmentRequest{Environment: "blue", Specified: true, Valid: true}},
	} {
		controller.admitJob(&snapshot, job)
	}

	if got := snapshot.LatestRejectedEnvironmentAttempt["same-job"]; got != 3 {
		t.Fatalf("latest rejected attempt = %d, want 3", got)
	}
	if _, exists := snapshot.LatestRejectedEnvironmentAttempt["other-job"]; exists {
		t.Fatal("admitted job was indexed as environment-rejected")
	}

	for _, test := range []struct {
		key     cluster.JobAttemptRef
		blocked bool
	}{
		{key: cluster.JobAttemptRef{JobID: "same-job", AttemptIndex: 2}, blocked: true},
		{key: cluster.JobAttemptRef{JobID: "same-job", AttemptIndex: 3}, blocked: true},
		{key: cluster.JobAttemptRef{JobID: "same-job", AttemptIndex: 4}, blocked: false},
		{key: cluster.JobAttemptRef{JobID: "other-job", AttemptIndex: 7}, blocked: false},
	} {
		if got := environmentAdmissionBlocks(snapshot, test.key); got != test.blocked {
			t.Errorf("environmentAdmissionBlocks(%#v) = %t, want %t", test.key, got, test.blocked)
		}
	}
}
