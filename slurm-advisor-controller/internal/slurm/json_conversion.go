// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

type numberField struct {
	Set      bool `json:"set"`
	Infinite bool `json:"infinite"`
	Number   int  `json:"number"`
}

type timestampField struct {
	Set      bool
	Infinite bool
	Number   int
}

func (field *timestampField) UnmarshalJSON(data []byte) error {
	*field = timestampField{}
	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("null")) {
		return nil
	}

	var legacy int
	if err := json.Unmarshal(data, &legacy); err == nil {
		field.Set = true
		field.Number = legacy
		return nil
	}

	var wrapped struct {
		Set      json.RawMessage `json:"set"`
		Infinite json.RawMessage `json:"infinite"`
		Number   json.RawMessage `json:"number"`
	}
	if err := json.Unmarshal(data, &wrapped); err != nil {
		return err
	}
	if err := unmarshalTimestampMember(wrapped.Set, &field.Set, "set"); err != nil {
		return err
	}
	if err := unmarshalTimestampMember(wrapped.Infinite, &field.Infinite, "infinite"); err != nil {
		return err
	}
	if err := unmarshalTimestampMember(wrapped.Number, &field.Number, "number"); err != nil {
		return err
	}
	return nil
}

func unmarshalTimestampMember(data json.RawMessage, target any, name string) error {
	if data == nil {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return errors.Newf("timestamp field %q cannot be null", name)
	}
	return json.Unmarshal(data, target)
}

func (field timestampField) concreteSeconds() (int, bool) {
	if !field.Set || field.Infinite || !isConcreteSlurmUnixTime(field.Number) {
		return 0, false
	}
	return field.Number, true
}

type SlurmMeta struct {
	Plugins struct {
		DataParser        string `json:"data_parser"`
		AccountingStorage string `json:"accounting_storage"`
	} `json:"plugins"`
	Command []string `json:"command"`
	Slurm   struct {
		Version json.RawMessage `json:"version"`
		Release string          `json:"release"`
	} `json:"Slurm"`
}

type SlurmJob struct {
	ArrayJobID     numberField     `json:"array_job_id"`
	ArrayTaskID    numberField     `json:"array_task_id"`
	Cluster        string          `json:"cluster"`
	Command        string          `json:"command"`
	Comment        string          `json:"comment"`
	Dependency     string          `json:"dependency"`
	EndTime        timestampField  `json:"end_time"`
	ExcludedNodes  string          `json:"excluded_nodes"`
	FailedNode     string          `json:"failed_node"`
	Hold           bool            `json:"hold"`
	JobID          int             `json:"job_id"`
	JobState       slurmStateList  `json:"job_state"`
	Name           string          `json:"name"`
	Nodes          string          `json:"nodes"`
	NodeCount      numberField     `json:"node_count"`
	Partition      string          `json:"partition"`
	RequiredNodes  string          `json:"required_nodes"`
	RestartCnt     int             `json:"restart_cnt"`
	ResvName       string          `json:"resv_name"`
	StartTime      timestampField  `json:"start_time"`
	StateReason    string          `json:"state_reason"`
	StandardOutput string          `json:"standard_output"`
	StdoutExpanded string          `json:"stdout_expanded"`
	SubmitTime     timestampField  `json:"submit_time"`
	Tasks          numberField     `json:"tasks"`
	TresReqStr     string          `json:"tres_req_str"`
	TresAllocStr   string          `json:"tres_alloc_str"`
	MailUser       string          `json:"mail_user"`
	UserID         json.RawMessage `json:"user_id"`
	UserName       string          `json:"user_name"`
}

type SqueueData struct {
	Meta     SlurmMeta     `json:"meta"`
	Jobs     []SlurmJob    `json:"jobs"`
	Warnings []interface{} `json:"warnings"`
	Errors   []interface{} `json:"errors"`
}

func parseSqueueOutput(data []byte) (*SqueueData, error) {
	var squeueData SqueueData
	if err := json.Unmarshal(data, &squeueData); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal squeue output")
	}
	return &squeueData, nil
}

func observedJobsFromSqueue(data []byte) ([]cluster.JobExecutionRecord, error) {
	observations, err := observedJobsWithSourceFromSqueue(data)
	if err != nil {
		return nil, err
	}

	jobs := make([]cluster.JobExecutionRecord, 0, len(observations))
	for _, observation := range observations {
		jobs = append(jobs, observation.Job)
	}
	return jobs, nil
}

type squeueObservation struct {
	Job    cluster.JobExecutionRecord
	Source SlurmJob
}

func observedJobsWithSourceFromSqueue(data []byte) ([]squeueObservation, error) {
	parsed, err := parseSqueueOutput(data)
	if err != nil {
		return nil, err
	}

	observations := make([]squeueObservation, 0, len(parsed.Jobs))
	for _, jobData := range parsed.Jobs {
		job, include, err := observedJobFromSqueue(jobData)
		if err != nil {
			return nil, err
		}
		if include {
			observations = append(observations, squeueObservation{
				Job:    job,
				Source: jobData,
			})
		}
	}

	return observations, nil
}

func observedJobFromSqueue(jobData SlurmJob) (cluster.JobExecutionRecord, bool, error) {
	jobID, arrayJobID, arrayTaskID, include := jobIdentityFromSqueue(jobData)
	if !include {
		return cluster.JobExecutionRecord{}, false, nil
	}

	requestedResources, err := parseTresReqStr(jobData.TresReqStr)
	if err != nil {
		return cluster.JobExecutionRecord{}, false, errors.Wrapf(err, "failed to parse tres_req_str for job %d", jobData.JobID)
	}
	allocatedResources, err := parseTresReqStr(jobData.TresAllocStr)
	if err != nil {
		return cluster.JobExecutionRecord{}, false, errors.Wrapf(err, "failed to parse tres_alloc_str for job %d", jobData.JobID)
	}
	resources := requestedResources
	for key, value := range allocatedResources {
		resources[key] = value
	}

	config := recoveryConfigFromComment(jobData.Comment)
	workflowID := resolveWorkflowIdentity(jobData.Comment, jobData.Name, jobID)
	heimdallManaged, modeRequest := jobModeRequestFromComment(jobData.Comment)
	environmentRequest := jobEnvironmentRequestFromComment(jobData.Comment)
	dagChild := dagChildFromComment(jobData.Comment)

	allocatedNodes, err := ExpandNodeList(jobData.Nodes)
	if err != nil {
		return cluster.JobExecutionRecord{}, false, errors.Wrapf(err, "expand squeue job %d nodes", jobData.JobID)
	}
	inclusions, err := ExpandNodeList(jobData.RequiredNodes)
	if err != nil {
		return cluster.JobExecutionRecord{}, false, errors.Wrapf(err, "expand squeue job %d required_nodes", jobData.JobID)
	}
	exclusions, err := ExpandNodeList(jobData.ExcludedNodes)
	if err != nil {
		return cluster.JobExecutionRecord{}, false, errors.Wrapf(err, "expand squeue job %d excluded_nodes", jobData.JobID)
	}
	rawJobID := strconv.Itoa(jobData.JobID)
	// Slurm >=23.11 reports the base state and flags together while an epilog
	// runs (for example ["TIMEOUT", "COMPLETING"]). Prefer a final state in
	// the array so recovery can overlap the COMPLETING window. Scalar states
	// used by older Slurm retain their existing behavior.
	rawJobState := selectSlurmJobState(jobData.JobState, true)
	job := cluster.JobExecutionRecord{
		JobID:              jobID,
		RawJobID:           rawJobID,
		ArrayJobID:         arrayJobID,
		ArrayTaskID:        arrayTaskID,
		AttemptIndex:       int32(jobData.RestartCnt),
		JobName:            jobData.Name,
		WorkflowID:         workflowID,
		State:              mapJobState(rawJobState),
		RawState:           rawJobState,
		Reason:             jobData.StateReason,
		Held:               jobData.Hold,
		HeimdallManaged:    heimdallManaged,
		DAGChild:           dagChild,
		ModeRequest:        modeRequest,
		EnvironmentRequest: environmentRequest,
		Cluster:            jobData.Cluster,
		Partition:          jobData.Partition,
		Reservation:        jobData.ResvName,
		User:               jobData.UserName,
		Comment:            jobData.Comment,
		Command:            jobData.Command,
		Dependency:         jobData.Dependency,
		TotalTaskCount:     jobData.Tasks.Number,
		NodeCount:          jobData.NodeCount.Number,
		AllocatedNodes:     allocatedNodes,
		NodeConstraints:    cluster.NodeConstraints{Inclusions: inclusions, Exclusions: exclusions},
		TotalCPUCount:      resources["cpu"],
		TotalMemoryMB:      resources["mem"],
		TotalGPUCount:      resources["gpu"],
		RecoveryConfig:     config,
		LastUpdatedAt:      now(),
		SchedulerMetadata: map[string]any{
			"scheduler":           cluster.SchedulerSlurm,
			"managed_by":          cluster.ManagedBySlurmAdvisor,
			"slurm_job_id":        jobID,
			"slurm_raw_job_id":    rawJobID,
			"slurm_attempt_index": int32(jobData.RestartCnt),
			"slurm_state":         rawJobState,
		},
	}
	if arrayJobID != "" {
		job.SchedulerMetadata["slurm_array_job_id"] = arrayJobID
	}
	if arrayTaskID != "" {
		job.SchedulerMetadata["slurm_array_task_id"] = arrayTaskID
	}
	if jobData.StateReason != "" {
		job.SchedulerMetadata["slurm_reason"] = jobData.StateReason
	}
	if jobData.Partition != "" {
		job.SchedulerMetadata["slurm_partition"] = jobData.Partition
	}
	if seconds, ok := jobData.SubmitTime.concreteSeconds(); ok {
		setUnixTime(seconds, &job.SubmitTime, job.SchedulerMetadata, "slurm_submit_time")
	}
	if seconds, ok := jobData.StartTime.concreteSeconds(); ok {
		setUnixTime(seconds, &job.StartTime, job.SchedulerMetadata, "slurm_start_time")
	}
	if seconds, ok := jobData.EndTime.concreteSeconds(); ok {
		setUnixTime(seconds, &job.EndTime, job.SchedulerMetadata, "slurm_end_time")
	}
	if jobData.FailedNode != "" {
		job.FailedNodes = []string{jobData.FailedNode}
		job.SchedulerMetadata["slurm_failed_nodes"] = job.FailedNodes
	}

	return job, true, nil
}

func jobIdentityFromSqueue(job SlurmJob) (jobID, arrayJobID, arrayTaskID string, include bool) {
	rawJobID := strconv.Itoa(job.JobID)
	if !job.ArrayJobID.Set || job.ArrayJobID.Number <= 0 {
		return rawJobID, "", "", true
	}

	arrayJobID = strconv.Itoa(job.ArrayJobID.Number)
	arrayTaskID = strconv.Itoa(job.ArrayTaskID.Number)
	if !job.ArrayTaskID.Set {
		return "", arrayJobID, arrayTaskID, false
	}

	return arrayJobID + "_" + arrayTaskID, arrayJobID, arrayTaskID, true
}

func parseTresReqStr(tresReqStr string) (map[string]float64, error) {
	result := make(map[string]float64)
	if strings.TrimSpace(tresReqStr) == "" {
		return result, nil
	}

	pairs := strings.Split(tresReqStr, ",")
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			return nil, errors.Newf("invalid format in tres_req_str: %s", pair)
		}

		key := strings.TrimPrefix(kv[0], "gres/")
		value, err := parseTresValue(key, kv[1])
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse value for %s", key)
		}
		result[key] = value
	}

	return result, nil
}

func parseTresValue(key, rawValue string) (float64, error) {
	numericValue := rawValue
	multiplier := 1.0
	if len(rawValue) > 0 {
		if unitMultiplier, ok := tresUnitMultiplier(key, rawValue[len(rawValue)-1]); ok {
			numericValue = rawValue[:len(rawValue)-1]
			multiplier = unitMultiplier
		}
	}

	value, err := strconv.ParseFloat(numericValue, 64)
	if err != nil {
		return 0, err
	}
	return value * multiplier, nil
}

func tresUnitMultiplier(key string, suffix byte) (float64, bool) {
	const (
		kibi = 1024.0
		mebi = kibi * kibi
		gibi = mebi * kibi
		tebi = gibi * kibi
		pebi = tebi * kibi
	)

	if key == "mem" {
		// Slurm's base unit for memory TRES values is MiB, so binary
		// suffixes scale relative to MiB rather than to a unitless count.
		switch suffix {
		case 'k', 'K':
			return 1 / kibi, true
		case 'm', 'M':
			return 1, true
		case 'g', 'G':
			return kibi, true
		case 't', 'T':
			return mebi, true
		case 'p', 'P':
			return gibi, true
		default:
			return 0, false
		}
	}

	// Count-based TRES suffixes scale the raw count by powers of 1024.
	switch suffix {
	case 'k', 'K':
		return kibi, true
	case 'm', 'M':
		return mebi, true
	case 'g', 'G':
		return gibi, true
	case 't', 'T':
		return tebi, true
	case 'p', 'P':
		return pebi, true
	default:
		return 0, false
	}
}
