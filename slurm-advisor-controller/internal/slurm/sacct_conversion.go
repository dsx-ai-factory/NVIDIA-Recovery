// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

type SacctData struct {
	Jobs []SacctJob `json:"jobs"`
}

type SacctJob struct {
	// databaseIndex joins legacy text steps to allocations while reusing the
	// JSON attempt grouping algorithm. JSON discovery does not need this field.
	databaseIndex   uint64
	AllocationNodes int `json:"allocation_nodes"`
	JobID           int `json:"job_id"`
	Array           struct {
		JobID  int         `json:"job_id"`
		TaskID numberField `json:"task_id"`
	} `json:"array"`
	Name    string `json:"name"`
	Cluster string `json:"cluster"`
	State   struct {
		Current slurmStateList `json:"current"`
		Reason  string         `json:"reason"`
	} `json:"state"`
	Partition   string `json:"partition"`
	Reservation struct {
		Name string `json:"name"`
	} `json:"reservation"`
	User       string `json:"user"`
	Nodes      string `json:"nodes"`
	FailedNode string `json:"failed_node"`
	Time       struct {
		Submission int `json:"submission"`
		Start      int `json:"start"`
		End        int `json:"end"`
	} `json:"time"`
	Comment struct {
		Job string `json:"job"`
	} `json:"comment"`
	TRES           sacctTRES   `json:"tres"`
	StdoutExpanded string      `json:"stdout_expanded"`
	Steps          []SacctStep `json:"steps"`
}

type sacctTRES struct {
	Allocated *[]sacctResource `json:"allocated"`
	Requested json.RawMessage  `json:"requested"`
}

type sacctResource struct {
	Type  string  `json:"type"`
	Name  string  `json:"name"`
	Count float64 `json:"count"`
}

func (tres sacctTRES) effectiveResources() ([]sacctResource, error) {
	// Match the inherited persistence behavior: an allocation, when present,
	// is authoritative for the whole resource set. Requested resources are a
	// fallback for accounting rows that do not carry an allocation.
	if tres.Allocated != nil {
		return *tres.Allocated, nil
	}

	raw := bytes.TrimSpace(tres.Requested)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil
	}
	var direct []sacctResource
	if err := json.Unmarshal(raw, &direct); err == nil {
		return direct, nil
	}
	var summarized struct {
		Total []sacctResource `json:"total"`
	}
	if err := json.Unmarshal(raw, &summarized); err != nil {
		return nil, errors.Wrap(err, "parse sacct requested TRES")
	}
	return summarized.Total, nil
}

func sacctResourceValue(resources []sacctResource, resourceType, name string) float64 {
	for _, resource := range resources {
		if resource.Type == resourceType && (name == "" || resource.Name == name) {
			return resource.Count
		}
	}
	return 0
}

type SacctStep struct {
	Step struct {
		ID struct {
			JobID  int    `json:"job_id"`
			StepID string `json:"step_id"`
		} `json:"id"`
		Name           string `json:"name"`
		StdoutExpanded string `json:"stdout_expanded"`
	} `json:"step"`
}

func parseSacctOutput(data []byte) (*SacctData, error) {
	var sacctData SacctData
	if err := json.Unmarshal(data, &sacctData); err != nil {
		return nil, errors.Wrap(&sacctDecodeError{err: err}, "failed to unmarshal sacct output")
	}
	return &sacctData, nil
}

// sacctDecodeError distinguishes JSON decoding failures from later job
// conversion failures so raw-output diagnostics are limited to failed decodes.
type sacctDecodeError struct {
	err error
}

func (e *sacctDecodeError) Error() string { return e.err.Error() }

func (e *sacctDecodeError) Unwrap() error { return e.err }

func observedJobsFromSacct(data []byte) ([]cluster.JobExecutionRecord, error) {
	parsed, err := parseSacctOutput(data)
	if err != nil {
		return nil, err
	}

	grouped := map[string][]SacctJob{}
	groupOrder := []string{}
	for _, job := range parsed.Jobs {
		jobID, _, _, include := jobIdentityFromSacct(job)
		if !include {
			continue
		}
		if _, ok := grouped[jobID]; !ok {
			groupOrder = append(groupOrder, jobID)
		}
		grouped[jobID] = append(grouped[jobID], job)
	}

	jobs := make([]cluster.JobExecutionRecord, 0, len(parsed.Jobs))
	for _, jobID := range groupOrder {
		for attemptIndex, attempt := range groupSacctAttempts(grouped[jobID]) {
			job, err := observedJobFromSacct(attempt[0], int32(attemptIndex))
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, job)
		}
	}

	return jobs, nil
}

func observedJobFromSacct(jobData SacctJob, attemptIndex int32) (cluster.JobExecutionRecord, error) {
	jobID, arrayJobID, arrayTaskID, include := jobIdentityFromSacct(jobData)
	if !include {
		return cluster.JobExecutionRecord{}, errors.New("synthetic sacct array entry should have been filtered")
	}

	rawState := selectSlurmJobState(jobData.State.Current, true)
	config := recoveryConfigFromComment(jobData.Comment.Job)
	workflowID := resolveWorkflowIdentity(jobData.Comment.Job, jobData.Name, jobID)
	heimdallManaged, modeRequest := jobModeRequestFromComment(jobData.Comment.Job)
	environmentRequest := jobEnvironmentRequestFromComment(jobData.Comment.Job)
	dagChild := dagChildFromComment(jobData.Comment.Job)

	allocatedNodes, err := ExpandNodeList(jobData.Nodes)
	if err != nil {
		return cluster.JobExecutionRecord{}, errors.Wrapf(err, "expand sacct job %d nodes", jobData.JobID)
	}
	resources, err := jobData.TRES.effectiveResources()
	if err != nil {
		return cluster.JobExecutionRecord{}, errors.Wrapf(err, "parse sacct job %d resources", jobData.JobID)
	}

	rawJobID := strconv.Itoa(jobData.JobID)
	job := cluster.JobExecutionRecord{
		JobID:              jobID,
		RawJobID:           rawJobID,
		ArrayJobID:         arrayJobID,
		ArrayTaskID:        arrayTaskID,
		AttemptIndex:       attemptIndex,
		JobName:            jobData.Name,
		WorkflowID:         workflowID,
		State:              mapJobState(rawState),
		RawState:           rawState,
		Reason:             jobData.State.Reason,
		HeimdallManaged:    heimdallManaged,
		DAGChild:           dagChild,
		ModeRequest:        modeRequest,
		EnvironmentRequest: environmentRequest,
		Cluster:            jobData.Cluster,
		Partition:          jobData.Partition,
		Reservation:        jobData.Reservation.Name,
		User:               jobData.User,
		Comment:            jobData.Comment.Job,
		NodeCount:          jobData.AllocationNodes,
		AllocatedNodes:     allocatedNodes,
		TotalCPUCount:      sacctResourceValue(resources, "cpu", ""),
		TotalMemoryMB:      sacctResourceValue(resources, "mem", ""),
		TotalGPUCount:      sacctResourceValue(resources, "gres", "gpu"),
		RecoveryConfig:     config,
		LastUpdatedAt:      now(),
		SchedulerMetadata: map[string]any{
			"scheduler":           cluster.SchedulerSlurm,
			"managed_by":          cluster.ManagedBySlurmAdvisor,
			"slurm_job_id":        jobID,
			"slurm_raw_job_id":    rawJobID,
			"slurm_attempt_index": attemptIndex,
			"slurm_state":         rawState,
		},
	}
	if arrayJobID != "" {
		job.SchedulerMetadata["slurm_array_job_id"] = arrayJobID
	}
	if arrayTaskID != "" {
		job.SchedulerMetadata["slurm_array_task_id"] = arrayTaskID
	}
	// Accounting reasons for requeue-managed jobs can describe an older
	// scheduler transition. Keep the reason on the internal record for
	// reconciliation, but omit it from metadata so storage retains the value
	// last observed through squeue.
	if jobData.State.Reason != "" && (config == nil || config.AutoResumeMode != cluster.AutoResumeModeRequeue) {
		job.SchedulerMetadata["slurm_reason"] = jobData.State.Reason
	}
	if jobData.Partition != "" {
		job.SchedulerMetadata["slurm_partition"] = jobData.Partition
	}
	if jobData.FailedNode != "" {
		job.FailedNodes = []string{jobData.FailedNode}
		job.SchedulerMetadata["slurm_failed_nodes"] = job.FailedNodes
	}
	setUnixTime(jobData.Time.Submission, &job.SubmitTime, job.SchedulerMetadata, "slurm_submit_time")
	setUnixTime(jobData.Time.Start, &job.StartTime, job.SchedulerMetadata, "slurm_start_time")
	setUnixTime(jobData.Time.End, &job.EndTime, job.SchedulerMetadata, "slurm_end_time")

	return job, nil
}

func jobIdentityFromSacct(job SacctJob) (jobID, arrayJobID, arrayTaskID string, include bool) {
	rawJobID := strconv.Itoa(job.JobID)
	if job.Array.JobID <= 0 {
		return rawJobID, "", "", true
	}

	arrayJobID = strconv.Itoa(job.Array.JobID)
	arrayTaskID = strconv.Itoa(job.Array.TaskID.Number)
	if !job.Array.TaskID.Set {
		return "", arrayJobID, arrayTaskID, false
	}

	return arrayJobID + "_" + arrayTaskID, arrayJobID, arrayTaskID, true
}

func groupSacctAttempts(rawJobs []SacctJob) [][]SacctJob {
	seen := map[string]struct{}{}
	groupIndex := map[string]int{}
	groups := make([][]SacctJob, 0, len(rawJobs))
	for i, raw := range rawJobs {
		key := strconv.Itoa(raw.Time.Submission) + "\x00" + strconv.Itoa(raw.Time.Start)
		if raw.Time.Submission == 0 && raw.Time.Start == 0 {
			key = "missing-time\x00" + strconv.Itoa(i)
		}
		if _, ok := seen[key]; ok {
			index := groupIndex[key]
			groups[index] = append(groups[index], raw)
			continue
		}
		seen[key] = struct{}{}
		groupIndex[key] = len(groups)
		groups = append(groups, []SacctJob{raw})
	}
	return groups
}
