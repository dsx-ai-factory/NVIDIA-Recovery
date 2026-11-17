// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package jobidentity

import "testing"

func TestValidate(t *testing.T) {
	tests := []struct {
		name     string
		jobID    string
		metadata map[string]any
		valid    bool
	}{
		{name: "optional metadata", jobID: "4200_0", valid: true},
		{name: "standalone", jobID: "4200", valid: true},
		{name: "task zero", jobID: "4200_0", metadata: map[string]any{"slurm_job_id": "4200_0", "slurm_array_job_id": "4200", "slurm_array_task_id": "0", "slurm_raw_job_id": "4200"}, valid: true},
		{name: "sibling conflict", jobID: "4200_0", metadata: map[string]any{"slurm_job_id": "4200_1"}},
		{name: "wrong type", jobID: "4200", metadata: map[string]any{"slurm_job_id": 4200}},
		{name: "partial pair", jobID: "4200_0", metadata: map[string]any{"slurm_array_job_id": "4200"}},
		{name: "bare parent", jobID: "4200", metadata: map[string]any{"slurm_array_job_id": "4200", "slurm_array_task_id": "0"}},
		{name: "sentinel", jobID: "4200_4294967294", metadata: map[string]any{"slurm_array_job_id": "4200", "slurm_array_task_id": "4294967294"}},
		{name: "noncanonical", jobID: "4200_00", metadata: map[string]any{"slurm_array_job_id": "4200", "slurm_array_task_id": "00"}},
		{name: "missing job"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := Validate(test.jobID, test.metadata); (err == nil) != test.valid {
				t.Fatalf("Validate() = %v, valid = %v", err, test.valid)
			}
		})
	}
}
