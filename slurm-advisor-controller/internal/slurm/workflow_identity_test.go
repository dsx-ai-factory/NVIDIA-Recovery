// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"strings"
	"testing"
)

func TestResolveWorkflowIdentity(t *testing.T) {
	tests := []struct {
		name           string
		comment        string
		jobName        string
		schedulerJobID string
		want           string
	}{
		{
			name:           "canonical override wins",
			comment:        `{"APS":{"workflow_id":"experiment-42","workflow_name":"legacy"}}`,
			jobName:        "train",
			schedulerJobID: "123",
			want:           "experiment-42",
		},
		{
			name:           "deprecated alias remains compatible",
			comment:        `{"APS":{"workflow_name":"legacy"}}`,
			jobName:        "train",
			schedulerJobID: "123",
			want:           "legacy",
		},
		{
			name:           "oversized override falls through to job name",
			comment:        `{"APS":{"workflow_id":"` + strings.Repeat("界", 257) + `"}}`,
			jobName:        "train",
			schedulerJobID: "123",
			want:           "train",
		},
		{
			name:           "invalid canonical override falls through to deprecated alias",
			comment:        `{"APS":{"workflow_id":"` + strings.Repeat("界", 257) + `","workflow_name":"legacy"}}`,
			jobName:        "train",
			schedulerJobID: "123",
			want:           "legacy",
		},
		{
			name:           "empty job name falls back to stable scheduler ID",
			schedulerJobID: "123",
			want:           "123",
		},
		{
			name:           "oversized job name falls back to stable scheduler ID",
			jobName:        strings.Repeat("界", 257),
			schedulerJobID: "123",
			want:           "123",
		},
		{
			name:           "256 Unicode scalars are preserved",
			comment:        `{"APS":{"workflow_id":"` + strings.Repeat("界", 256) + `"}}`,
			jobName:        "train",
			schedulerJobID: "123",
			want:           strings.Repeat("界", 256),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := resolveWorkflowIdentity(test.comment, test.jobName, test.schedulerJobID); got != test.want {
				t.Fatalf("workflow ID = %q, want %q", got, test.want)
			}
		})
	}
}
