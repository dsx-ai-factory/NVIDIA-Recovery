// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"testing"
)

func TestParseTresReqStrNormalizesSlurmUnits(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  map[string]float64
	}{
		{
			name:  "memory without suffix is already MiB",
			input: "cpu=4,mem=8000,gres/gpu=2",
			want:  map[string]float64{"cpu": 4, "mem": 8000, "gpu": 2},
		},
		{
			name:  "memory kibibytes",
			input: "mem=8388608K",
			want:  map[string]float64{"mem": 8192},
		},
		{
			name:  "memory mebibytes",
			input: "mem=8192M",
			want:  map[string]float64{"mem": 8192},
		},
		{
			name:  "memory gibibytes",
			input: "mem=8G",
			want:  map[string]float64{"mem": 8192},
		},
		{
			name:  "memory tebibytes",
			input: "mem=8T",
			want:  map[string]float64{"mem": 8388608},
		},
		{
			name:  "memory pebibytes",
			input: "mem=1P",
			want:  map[string]float64{"mem": 1073741824},
		},
		{
			name:  "lowercase fractional memory",
			input: "mem=1.5t",
			want:  map[string]float64{"mem": 1572864},
		},
		{
			name:  "generic TRES count suffix",
			input: "gres/license=2K",
			want:  map[string]float64{"license": 2048},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTresReqStr(tt.input)
			if err != nil {
				t.Fatalf("parseTresReqStr(%q) failed: %v", tt.input, err)
			}
			if !maps.Equal(got, tt.want) {
				t.Fatalf("parseTresReqStr(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseTresReqStrRejectsInvalidUnit(t *testing.T) {
	_, err := parseTresReqStr("mem=8X")
	if err == nil {
		t.Fatal("parseTresReqStr(mem=8X) succeeded, want error")
	}
	if !strings.Contains(err.Error(), "failed to parse value for mem") {
		t.Fatalf("parseTresReqStr(mem=8X) error = %q, want memory value context", err)
	}
}

func TestGetJobsRequiresSubmittingUsername(t *testing.T) {
	tests := []struct {
		name string
		job  SlurmJob
		want string
	}{
		{name: "user name", job: SlurmJob{UserName: "alice", MailUser: "mail-alice", UserID: []byte(`1000`)}, want: "alice"},
		{name: "mail user", job: SlurmJob{MailUser: "mail-alice", UserID: []byte(`1000`)}, want: ""},
		{name: "numeric user ID", job: SlurmJob{UserID: []byte(`1000`)}, want: ""},
		{name: "string user ID", job: SlurmJob{UserID: []byte(`"1000"`)}, want: ""},
		{name: "missing", job: SlurmJob{}, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.job.JobID = 12345
			payload, err := json.Marshal(map[string]any{"jobs": []SlurmJob{tt.job}})
			if err != nil {
				t.Fatal(err)
			}
			executor := &recordingExecutor{results: map[string][]byte{"squeue\x00--json": payload}}
			scheduler := &slurmCluster{roCommands: executor}
			jobs, err := scheduler.GetJobs(context.Background())
			if err != nil {
				t.Fatalf("GetJobs() failed: %v", err)
			}
			if len(jobs) != 1 || jobs[0].User != tt.want {
				t.Fatalf("GetJobs() = %#v, want one job with user %q", jobs, tt.want)
			}
		})
	}
}
