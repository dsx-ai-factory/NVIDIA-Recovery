// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"math"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

type accountingIdentity struct {
	jobID              string
	submitted, started int64
}

type accountingIdentities struct {
	mu       sync.RWMutex
	attempts map[accountingIdentity]int32
}

// SetAccountingIdentities replaces timestamp evidence used to identify array
// attempts in older accounting output. Evidence identifies an attempt only;
// accounting must still supply fresh scheduler state before any action.
// The controller owns replacement; asynchronous log readers share a lock with it.
func (s *slurmCluster) SetAccountingIdentities(jobs []cluster.JobExecutionRecord) {
	attempts := make(map[accountingIdentity]int32)
	for _, job := range jobs {
		if !strings.Contains(job.JobID, "_") || job.SubmitTime == nil || job.StartTime == nil || job.AttemptIndex < 0 {
			continue
		}
		identity := accountingIdentity{job.JobID, job.SubmitTime.Unix(), job.StartTime.Unix()}
		if identity.submitted <= 0 || identity.started <= 0 {
			continue
		}
		if previous, ok := attempts[identity]; ok && previous != job.AttemptIndex {
			// Conflicting anchors cannot safely assign either attempt.
			attempts[identity] = -1
		} else {
			attempts[identity] = job.AttemptIndex
		}
	}
	s.accountingIdentities.mu.Lock()
	s.accountingIdentities.attempts = attempts
	s.accountingIdentities.mu.Unlock()
}

var errUnconfirmedAccountingAttempt = errors.New("accounting attempt identity is unconfirmed")

type sacctAttemptResolver func(SacctJob, int32) (int32, error)

// resolveAccountingAttempt uses Slurm's explicit count when available and an
// exact timestamp anchor otherwise. Ordinal positions are unsafe for arrays:
// retained accounting can omit earlier attempts or return siblings out of order.
func (s *slurmCluster) resolveAccountingAttempt(job SacctJob, ordinal int32) (int32, error) {
	jobID, arrayID, _, include, err := jobIdentityFromSacct(job)
	if err != nil {
		return 0, err
	}
	if !include || arrayID == "" {
		return ordinal, nil
	}
	s.accountingIdentities.mu.RLock()
	anchor, found := s.accountingIdentities.attempts[accountingIdentity{jobID, int64(job.Time.Submission), int64(job.Time.Start)}]
	s.accountingIdentities.mu.RUnlock()
	if job.RestartCount != nil {
		count := *job.RestartCount
		if count < 0 || count > math.MaxInt32 || (found && anchor != int32(count)) {
			return 0, errors.Newf("conflicting or invalid accounting attempt identity for Slurm task %s", jobID)
		}
		return int32(count), nil
	}
	if found {
		if anchor < 0 {
			return 0, errors.Newf("conflicting accounting timestamp anchors for Slurm task %s", jobID)
		}
		return anchor, nil
	}
	return 0, errors.Wrapf(errUnconfirmedAccountingAttempt, "cannot confirm accounting attempt for Slurm task %s: restart_cnt or matching submission/start timestamps required", jobID)
}

func resolveSacctAttempt(job SacctJob, ordinal int32, resolvers []sacctAttemptResolver) (int32, error) {
	if len(resolvers) == 0 {
		return (&slurmCluster{}).resolveAccountingAttempt(job, ordinal)
	}
	return resolvers[0](job, ordinal)
}

// resolvedSacctAttempt keeps duplicate allocation rows together without using
// row positions as task retry counts.
type resolvedSacctAttempt struct {
	index int32
	rows  []SacctJob
}

func resolveSacctAttempts(jobs []SacctJob, resolvers []sacctAttemptResolver, requested map[int32]struct{}) ([]resolvedSacctAttempt, error) {
	var resolved []resolvedSacctAttempt
	var errs []error
	var unresolved []SacctJob
	var unresolvedErrors []error
	seen := make(map[int32]struct{})
	for ordinal, rows := range groupSacctAttempts(jobs) {
		index, err := resolveSacctAttempt(rows[0], int32(ordinal), resolvers)
		if err != nil {
			if errors.Is(err, errUnconfirmedAccountingAttempt) {
				// Inspect every duplicate: a malformed explicit counter must
				// not be hidden behind an earlier duplicate missing evidence.
				for _, row := range rows[1:] {
					if _, duplicateErr := resolveSacctAttempt(row, int32(ordinal), resolvers); duplicateErr == nil || !errors.Is(duplicateErr, errUnconfirmedAccountingAttempt) {
						return nil, errors.New("conflicting duplicate accounting attempt evidence")
					}
				}
				unresolved = append(unresolved, rows[0])
				unresolvedErrors = append(unresolvedErrors, err)
			} else {
				errs = append(errs, err)
			}
			continue
		}
		for _, duplicate := range rows[1:] {
			other, err := resolveSacctAttempt(duplicate, int32(ordinal), resolvers)
			if err != nil {
				return nil, err
			}
			if other != index {
				return nil, errors.New("conflicting duplicate accounting attempt identity")
			}
		}
		if _, duplicate := seen[index]; duplicate {
			return nil, errors.New("distinct accounting allocations resolve to the same task attempt")
		}
		seen[index] = struct{}{}
		resolved = append(resolved, resolvedSacctAttempt{index, rows})
	}
	for index, row := range unresolved {
		if !olderThanRequestedAttempts(row, resolved, requested) {
			errs = append(errs, unresolvedErrors[index])
		}
	}
	return resolved, errors.Join(errs...)
}

// olderThanRequestedAttempts excludes only historical extras which cannot be
// the requested attempt or a later successor. Unknown newer rows remain a
// safety error even when an older requested source is otherwise confirmed.
func olderThanRequestedAttempts(row SacctJob, resolved []resolvedSacctAttempt, requested map[int32]struct{}) bool {
	if len(requested) == 0 || row.Time.Submission <= 0 || row.Time.Start <= 0 {
		return false
	}
	matched := 0
	for _, attempt := range resolved {
		if _, wanted := requested[attempt.index]; !wanted {
			continue
		}
		target := attempt.rows[0]
		if target.Time.Submission <= 0 || target.Time.Start <= 0 || row.Time.Submission > target.Time.Submission || row.Time.Start >= target.Time.Start {
			return false
		}
		matched++
	}
	return matched == len(requested)
}
