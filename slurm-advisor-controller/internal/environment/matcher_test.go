// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package environment

import (
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

func TestMatcherPreservesLiteralControllerSemantics(t *testing.T) {
	tests := []struct {
		name       string
		selector   string
		job        cluster.JobExecutionRecord
		admitted   bool
		wantReason AdmissionReason
	}{
		{name: "unspecified accepts absent", selector: Unspecified, job: managedEnvironment(false, "", true), admitted: true, wantReason: AdmissionControllerUnspecified},
		{name: "unspecified accepts arbitrary", selector: Unspecified, job: managedEnvironment(true, "QA/Canary 01", true), admitted: true, wantReason: AdmissionControllerUnspecified},
		{name: "prod defaults absent", selector: Production, job: managedEnvironment(false, "", true), admitted: true, wantReason: AdmissionProductionDefault},
		{name: "prod defaults empty", selector: Production, job: managedEnvironment(true, "", true), admitted: true, wantReason: AdmissionProductionDefault},
		{name: "prod exact", selector: Production, job: managedEnvironment(true, Production, true), admitted: true, wantReason: AdmissionMatched},
		{name: "literal exact", selector: "QA/Canary 01", job: managedEnvironment(true, "QA/Canary 01", true), admitted: true, wantReason: AdmissionMatched},
		{name: "case sensitive", selector: "Prod", job: managedEnvironment(true, "prod", true), wantReason: AdmissionMismatch},
		{name: "does not trim", selector: "prod", job: managedEnvironment(true, " prod ", true), wantReason: AdmissionMismatch},
		{name: "literal rejects absent", selector: "dev", job: managedEnvironment(false, "", true), wantReason: AdmissionMismatch},
		{name: "not onboarded", selector: Unspecified, job: cluster.JobExecutionRecord{}, wantReason: AdmissionNotOnboarded},
		{name: "invalid environment", selector: Unspecified, job: managedEnvironment(true, "", false), wantReason: AdmissionInvalid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher, err := NewMatcher(tt.selector)
			if err != nil {
				t.Fatalf("NewMatcher() failed: %v", err)
			}
			got := matcher.Admit(tt.job)
			if got.Admitted != tt.admitted || got.Reason != tt.wantReason {
				t.Fatalf("Admit() = %#v, want admitted=%t reason=%q", got, tt.admitted, tt.wantReason)
			}
		})
	}
}

func TestMatcherSupportsMinimalCaseSensitiveWildcard(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		job      cluster.JobExecutionRecord
		admitted bool
		reason   AdmissionReason
	}{
		{name: "star matches absent", pattern: "*", job: managedEnvironment(false, "", true), admitted: true, reason: AdmissionMatched},
		{name: "star matches empty", pattern: "*", job: managedEnvironment(true, "", true), admitted: true, reason: AdmissionMatched},
		{name: "star matches arbitrary", pattern: "*", job: managedEnvironment(true, "customer/a", true), admitted: true, reason: AdmissionMatched},
		{name: "prefix matches", pattern: "dev-*", job: managedEnvironment(true, "dev-blue", true), admitted: true, reason: AdmissionMatched},
		{name: "prefix requires hyphen", pattern: "dev-*", job: managedEnvironment(true, "dev", true), reason: AdmissionMismatch},
		{name: "wildcard is case sensitive", pattern: "dev-*", job: managedEnvironment(true, "DEV-blue", true), reason: AdmissionMismatch},
		{name: "wildcard matches slash", pattern: "customer/*", job: managedEnvironment(true, "customer/a/b", true), admitted: true, reason: AdmissionMatched},
		{name: "repeated stars", pattern: "dev-**-blue", job: managedEnvironment(true, "dev-east-blue", true), admitted: true, reason: AdmissionMatched},
		{name: "middle wildcard can be empty", pattern: "dev-*-blue", job: managedEnvironment(true, "dev--blue", true), admitted: true, reason: AdmissionMatched},
		{name: "wildcard rejects malformed environment", pattern: "*", job: managedEnvironment(true, "", false), reason: AdmissionInvalid},
		{name: "wildcard rejects missing spec", pattern: "*", job: cluster.JobExecutionRecord{}, reason: AdmissionNotOnboarded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher, err := NewMatcher(tt.pattern)
			if err != nil {
				t.Fatalf("NewMatcher() failed: %v", err)
			}
			got := matcher.Admit(tt.job)
			if got.Admitted != tt.admitted || got.Reason != tt.reason {
				t.Fatalf("Admit() = %#v, want admitted=%t reason=%q", got, tt.admitted, tt.reason)
			}
		})
	}
}

func TestNewMatcherRejectsEmptyPattern(t *testing.T) {
	if _, err := NewMatcher(""); err == nil {
		t.Fatal("NewMatcher() error = nil, want empty-pattern error")
	}
}

func managedEnvironment(specified bool, value string, valid bool) cluster.JobExecutionRecord {
	return cluster.JobExecutionRecord{
		HeimdallManaged: true,
		EnvironmentRequest: cluster.JobEnvironmentRequest{
			Environment: value,
			Specified:   specified,
			Valid:       valid,
		},
	}
}
