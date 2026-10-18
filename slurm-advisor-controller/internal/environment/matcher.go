// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package environment

import (
	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

const (
	Unspecified = "unspecified"
	Production  = "prod"
)

type AdmissionReason string

const (
	AdmissionMatched               AdmissionReason = "matched"
	AdmissionControllerUnspecified AdmissionReason = "controller_unspecified"
	AdmissionProductionDefault     AdmissionReason = "prod_default"
	AdmissionMismatch              AdmissionReason = "environment_mismatch"
	AdmissionNotOnboarded          AdmissionReason = "not_onboarded"
	AdmissionInvalid               AdmissionReason = "invalid_environment"
)

var AdmissionReasons = [...]AdmissionReason{
	AdmissionMatched,
	AdmissionControllerUnspecified,
	AdmissionProductionDefault,
	AdmissionMismatch,
	AdmissionNotOnboarded,
	AdmissionInvalid,
}

type AdmissionDecision struct {
	Admitted bool
	Reason   AdmissionReason
}

type Matcher struct {
	pattern     string
	hasWildcard bool
}

func NewMatcher(pattern string) (Matcher, error) {
	if pattern == "" {
		return Matcher{}, errors.New("environment pattern must not be empty")
	}
	return Matcher{pattern: pattern, hasWildcard: containsWildcard(pattern)}, nil
}

func AdmitLiteral(selector string, job cluster.JobExecutionRecord) AdmissionDecision {
	return admitLiteral(selector, job.HeimdallManaged, job.EnvironmentRequest)
}

func (m Matcher) Admit(job cluster.JobExecutionRecord) AdmissionDecision {
	if !m.hasWildcard {
		return AdmitLiteral(m.pattern, job)
	}
	if !job.HeimdallManaged {
		return AdmissionDecision{Reason: AdmissionNotOnboarded}
	}
	request := job.EnvironmentRequest
	if request.Specified && !request.Valid {
		return AdmissionDecision{Reason: AdmissionInvalid}
	}
	if wildcardMatch(m.pattern, request.Environment) {
		return AdmissionDecision{Admitted: true, Reason: AdmissionMatched}
	}
	return AdmissionDecision{Reason: AdmissionMismatch}
}

func admitLiteral(selector string, managed bool, request cluster.JobEnvironmentRequest) AdmissionDecision {
	if !managed {
		return AdmissionDecision{Reason: AdmissionNotOnboarded}
	}
	if request.Specified && !request.Valid {
		return AdmissionDecision{Reason: AdmissionInvalid}
	}

	if selector == "" || selector == Unspecified {
		return AdmissionDecision{Admitted: true, Reason: AdmissionControllerUnspecified}
	}
	if !request.Specified || request.Environment == "" {
		if selector == Production {
			return AdmissionDecision{Admitted: true, Reason: AdmissionProductionDefault}
		}
		return AdmissionDecision{Reason: AdmissionMismatch}
	}
	if request.Environment == selector {
		return AdmissionDecision{Admitted: true, Reason: AdmissionMatched}
	}
	return AdmissionDecision{Reason: AdmissionMismatch}
}

func containsWildcard(pattern string) bool {
	for i := range len(pattern) {
		if pattern[i] == '*' {
			return true
		}
	}
	return false
}

// wildcardMatch implements the ADR 0015 string matcher. '*' matches zero or
// more bytes, including '/', and every other byte is literal. UTF-8 strings
// retain exact matching because non-wildcard bytes are compared unchanged.
func wildcardMatch(pattern, value string) bool {
	patternIndex := 0
	valueIndex := 0
	starIndex := -1
	starValueIndex := 0

	for valueIndex < len(value) {
		switch {
		case patternIndex < len(pattern) && pattern[patternIndex] == value[valueIndex]:
			patternIndex++
			valueIndex++
		case patternIndex < len(pattern) && pattern[patternIndex] == '*':
			starIndex = patternIndex
			patternIndex++
			starValueIndex = valueIndex
		case starIndex >= 0:
			patternIndex = starIndex + 1
			starValueIndex++
			valueIndex = starValueIndex
		default:
			return false
		}
	}
	for patternIndex < len(pattern) && pattern[patternIndex] == '*' {
		patternIndex++
	}
	return patternIndex == len(pattern)
}
