// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

// ValidateMetadataIdentity checks Slurm task metadata against the opaque public
// job ID. Missing optional identity is allowed; supplied contradictory fields
// must not authorize publication, bootstrap discovery, or scheduler actions.
func ValidateMetadataIdentity(jobID string, metadata map[string]any) error {
	read := func(key string) (string, error) {
		value, present := metadata[key]
		if !present {
			return "", nil
		}
		text, ok := value.(string)
		if !ok || text == "" {
			return "", errors.Newf("invalid Slurm identity metadata %s", key)
		}
		return text, nil
	}
	canonical, err := read("slurm_job_id")
	if err != nil {
		return err
	}
	if canonical != "" && canonical != jobID {
		return errors.New("conflicting Slurm job identity metadata")
	}
	parent, err := read("slurm_array_job_id")
	if err != nil {
		return err
	}
	task, err := read("slurm_array_task_id")
	if err != nil {
		return err
	}
	expectedParent, expectedTask, array := strings.Cut(jobID, "_")
	if !array && parent == "" && task == "" {
		return nil
	}
	if !array || !canonicalDecimal(expectedParent, false) || !canonicalDecimal(expectedTask, true) {
		return errors.New("invalid canonical Slurm array task identity")
	}
	if (parent != "" && parent != expectedParent) || (task != "" && task != expectedTask) {
		return errors.New("conflicting Slurm array task identity metadata")
	}
	raw, err := read("slurm_raw_job_id")
	if err != nil {
		return err
	}
	if raw != "" && !canonicalDecimal(raw, false) {
		return errors.New("invalid raw Slurm task identity")
	}
	return nil
}

func canonicalDecimal(value string, zero bool) bool {
	number, err := strconv.ParseUint(value, 10, 32)
	return err == nil && (zero || number > 0) && number < uint64(^uint32(0))-1 && strconv.FormatUint(number, 10) == value
}

// ValidateRecordIdentity verifies scheduler observations before their identity
// can be published or used for an action.
func ValidateRecordIdentity(job cluster.JobExecutionRecord) error {
	metadata := make(map[string]any, len(job.SchedulerMetadata)+4)
	for key, value := range job.SchedulerMetadata {
		metadata[key] = value
	}
	for key, value := range map[string]string{"slurm_job_id": job.JobID, "slurm_raw_job_id": job.RawJobID, "slurm_array_job_id": job.ArrayJobID, "slurm_array_task_id": job.ArrayTaskID} {
		if value == "" {
			continue
		}
		if previous, exists := metadata[key]; exists && previous != value {
			return errors.Newf("conflicting Slurm observation identity %s", key)
		}
		metadata[key] = value
	}
	if job.AttemptIndex < 0 {
		return errors.New("negative Slurm attempt index")
	}
	return ValidateMetadataIdentity(job.JobID, metadata)
}
