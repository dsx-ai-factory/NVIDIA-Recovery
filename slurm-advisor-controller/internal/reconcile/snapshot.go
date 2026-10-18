// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package reconcile

import (
	"sort"
	"strings"
	"time"

	pb "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

type ControllerSnapshot struct {
	// ObservedAt is the timestamp shared by all source reads in one reconcile tick.
	ObservedAt time.Time

	// JobKeys is the union of job-attempt candidates from admitted Slurm and
	// Heimdall jobs, owned Advice with recommendations, and the seen store.
	JobKeys map[cluster.JobAttemptRef]struct{}

	// ActiveJobs are JobExecutionRecord metadata sourced from squeue.
	ActiveJobs map[cluster.JobAttemptRef]cluster.JobExecutionRecord

	// AccountingJobs are JobExecutionRecord metadata sourced from sacct
	AccountingJobs map[cluster.JobAttemptRef]cluster.JobExecutionRecord

	// HeimdallJobs contains environment-admitted bootstrap candidates.
	HeimdallJobs map[cluster.JobAttemptRef]*pb.Job

	// Advice contains Heimdall advice grouped by target Slurm job attempt.
	Advice map[cluster.JobAttemptRef][]*pb.Advice

	// EffectiveModes contains the one policy decision shared by all phases for
	// each current Slurm job attempt.
	EffectiveModes map[cluster.JobAttemptRef]EffectiveModeDecision

	// EnvironmentAdmissions contains the pre-mode routing decision for every
	// valid Slurm job attempt observed during this tick.
	EnvironmentAdmissions map[cluster.JobAttemptRef]EnvironmentAdmissionDecision

	// LatestRejectedEnvironmentAttempt indexes the newest environment-rejected
	// attempt for each Slurm job ID so every reconciliation phase can suppress
	// stale work in constant time.
	LatestRejectedEnvironmentAttempt map[string]int32

	// SeenJobCount records how many local seen-job entries contributed keys to
	// this snapshot for observability.
	SeenJobCount int
}

func newControllerSnapshot(observedAt time.Time) ControllerSnapshot {
	return ControllerSnapshot{
		ObservedAt:                       observedAt,
		JobKeys:                          map[cluster.JobAttemptRef]struct{}{},
		ActiveJobs:                       map[cluster.JobAttemptRef]cluster.JobExecutionRecord{},
		AccountingJobs:                   map[cluster.JobAttemptRef]cluster.JobExecutionRecord{},
		HeimdallJobs:                     map[cluster.JobAttemptRef]*pb.Job{},
		Advice:                           map[cluster.JobAttemptRef][]*pb.Advice{},
		EffectiveModes:                   map[cluster.JobAttemptRef]EffectiveModeDecision{},
		EnvironmentAdmissions:            map[cluster.JobAttemptRef]EnvironmentAdmissionDecision{},
		LatestRejectedEnvironmentAttempt: map[string]int32{},
	}
}

func jobForModePolicy(snapshot ControllerSnapshot, key cluster.JobAttemptRef) (cluster.JobExecutionRecord, bool) {
	if job, ok := snapshot.ActiveJobs[key]; ok {
		return job, true
	}
	job, ok := snapshot.AccountingJobs[key]
	return job, ok
}

func addActiveJob(snapshot *ControllerSnapshot, job cluster.JobExecutionRecord) {
	key := jobAttemptRef(job)
	if !validJobKey(key) {
		return
	}
	snapshot.ActiveJobs[key] = job
	addJobKey(snapshot, key)
}

func addAccountingJob(snapshot *ControllerSnapshot, job cluster.JobExecutionRecord) {
	key := jobAttemptRef(job)
	if !validJobKey(key) {
		return
	}
	snapshot.AccountingJobs[key] = job
	addJobKey(snapshot, key)
}

func addHeimdallJob(snapshot *ControllerSnapshot, job *pb.Job) {
	key := jobAttemptRefFromHeimdall(job)
	if !validJobKey(key) {
		return
	}
	snapshot.HeimdallJobs[key] = job
	addJobKey(snapshot, key)
}

func addAdvice(snapshot *ControllerSnapshot, advice *pb.Advice) {
	key := jobAttemptRefFromAdvice(advice)
	if !validJobKey(key) {
		return
	}
	snapshot.Advice[key] = append(snapshot.Advice[key], advice)
}

func addJobKey(snapshot *ControllerSnapshot, key cluster.JobAttemptRef) {
	if !validJobKey(key) {
		return
	}
	snapshot.JobKeys[key] = struct{}{}
}

func jobAttemptRef(job cluster.JobExecutionRecord) cluster.JobAttemptRef {
	return cluster.JobAttemptRef{JobID: job.JobID, AttemptIndex: job.AttemptIndex}
}

func jobAttemptRefFromHeimdall(job *pb.Job) cluster.JobAttemptRef {
	if job == nil {
		return cluster.JobAttemptRef{}
	}
	return cluster.JobAttemptRef{JobID: job.GetJobId(), AttemptIndex: job.GetAttemptIndex()}
}

func jobAttemptRefFromAdvice(advice *pb.Advice) cluster.JobAttemptRef {
	if advice == nil {
		return cluster.JobAttemptRef{}
	}
	return cluster.JobAttemptRef{JobID: advice.GetJobId(), AttemptIndex: advice.GetAttemptIndex()}
}

func validJobKey(key cluster.JobAttemptRef) bool {
	return key.JobID != ""
}

func accountingRequestKeys(snapshot ControllerSnapshot) []cluster.JobAttemptRef {
	requests := map[cluster.JobAttemptRef]struct{}{}
	for key := range snapshot.JobKeys {
		if environmentAdmissionBlocks(snapshot, key) {
			continue
		}
		if _, ok := snapshot.ActiveJobs[key]; !ok {
			requests[key] = struct{}{}
			continue
		}
		if activeJobRequiresAccounting(snapshot.ActiveJobs[key]) {
			requests[key] = struct{}{}
		}
	}

	out := make([]cluster.JobAttemptRef, 0, len(requests))
	for key := range requests {
		out = append(out, key)
	}
	sortJobKeys(out)

	return out
}

func activeJobRequiresAccounting(job cluster.JobExecutionRecord) bool {
	rawState := strings.ToUpper(job.RawState)
	reason := strings.ToUpper(job.Reason)
	return strings.Contains(rawState, "NODE_FAIL") ||
		strings.Contains(reason, "NODE_FAIL") ||
		(isFinalJobState(job.State) && job.EndTime == nil)
}

func sortedSnapshotKeys(snapshot ControllerSnapshot) []cluster.JobAttemptRef {
	keys := make([]cluster.JobAttemptRef, 0, len(snapshot.JobKeys))
	for key := range snapshot.JobKeys {
		keys = append(keys, key)
	}
	sortJobKeys(keys)

	return keys
}

func sortJobKeys(keys []cluster.JobAttemptRef) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].JobID == keys[j].JobID {
			return keys[i].AttemptIndex < keys[j].AttemptIndex
		}
		return keys[i].JobID < keys[j].JobID
	})
}

func sortedSnapshotAdvice(snapshot ControllerSnapshot) []*pb.Advice {
	keys := make([]cluster.JobAttemptRef, 0, len(snapshot.Advice))
	for key := range snapshot.Advice {
		keys = append(keys, key)
	}
	sortJobKeys(keys)

	out := []*pb.Advice{}
	for _, key := range keys {
		items := append([]*pb.Advice(nil), snapshot.Advice[key]...)
		sort.Slice(items, func(i, j int) bool {
			return items[i].GetHeimdallJobId() < items[j].GetHeimdallJobId()
		})
		out = append(out, items...)
	}

	return out
}

func snapshotAdviceCount(snapshot ControllerSnapshot) int {
	count := 0
	for _, items := range snapshot.Advice {
		count += len(items)
	}
	return count
}

func (snapshot ControllerSnapshot) EnvironmentAdmissionCount(reason EnvironmentAdmissionReason) int {
	count := 0
	for _, decision := range snapshot.EnvironmentAdmissions {
		if decision.Reason == reason {
			count++
		}
	}
	return count
}

func latestJobsByIDFromSnapshot(snapshot ControllerSnapshot) map[string]cluster.JobExecutionRecord {
	out := make(map[string]cluster.JobExecutionRecord, len(snapshot.ActiveJobs)+len(snapshot.AccountingJobs))
	for _, job := range snapshot.AccountingJobs {
		addLatestJob(out, job)
	}
	for _, job := range snapshot.ActiveJobs {
		addLatestJob(out, job)
	}

	return out
}

func addLatestJob(jobsByID map[string]cluster.JobExecutionRecord, job cluster.JobExecutionRecord) {
	current, ok := jobsByID[job.JobID]
	if !ok || job.AttemptIndex > current.AttemptIndex {
		jobsByID[job.JobID] = job
	}
}
