// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package jobidentity validates Slurm identity at scheduler adapter boundaries.
package jobidentity

import (
	"strconv"

	"github.com/cockroachdb/errors"
)

// Validate checks supplied scheduler identity without inferring missing metadata
// or translating raw job IDs into array task IDs.
func Validate(jobID string, metadata map[string]any) error {
	if jobID == "" {
		return errors.New("missing job identity")
	}
	if value, present := metadata["slurm_job_id"]; present && value != jobID {
		return errors.New("Slurm job metadata conflicts with job ID")
	}
	arrayValue, hasArray := metadata["slurm_array_job_id"]
	taskValue, hasTask := metadata["slurm_array_task_id"]
	if !hasArray && !hasTask {
		return nil
	}
	arrayID, arrayOK := arrayValue.(string)
	taskID, taskOK := taskValue.(string)
	if !arrayOK || !taskOK || jobID != arrayID+"_"+taskID {
		return errors.New("Slurm array metadata conflicts with canonical job ID")
	}
	for pos, value := range []string{arrayID, taskID} {
		number, err := strconv.ParseUint(value, 10, 32)
		// The array ID must be positive; task zero is a valid array member.
		if err != nil || number >= SlurmNoValue32 || (pos == 0 && number == 0) || strconv.FormatUint(number, 10) != value {
			return errors.New("invalid Slurm array identity")
		}
	}
	return nil
}
