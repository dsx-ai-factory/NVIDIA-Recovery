// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestSlurmStateListDecodesSupportedShapes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  slurmStateList
	}{
		{name: "missing", input: `{}`},
		{name: "null", input: `{"state":null}`},
		{name: "scalar", input: `{"state":"RUNNING"}`, want: slurmStateList{"RUNNING"}},
		{name: "empty scalar", input: `{"state":""}`, want: slurmStateList{""}},
		{
			name:  "ordered list",
			input: `{"state":["RUNNING","COMPLETING"]}`,
			want:  slurmStateList{"RUNNING", "COMPLETING"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var payload struct {
				State slurmStateList `json:"state"`
			}
			if err := json.Unmarshal([]byte(tt.input), &payload); err != nil {
				t.Fatalf("json.Unmarshal() failed: %v", err)
			}
			if !slices.Equal(payload.State, tt.want) {
				t.Fatalf("state = %#v, want %#v", payload.State, tt.want)
			}
		})
	}
}

func TestSlurmStateListUnmarshalResetsReceiver(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{name: "null", input: `null`},
		{name: "malformed array", input: `["RUNNING",""]`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			states := slurmStateList{"STALE"}
			err := json.Unmarshal([]byte(tt.input), &states)
			if tt.wantErr && err == nil {
				t.Fatal("json.Unmarshal() error = nil, want rejection")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("json.Unmarshal() failed: %v", err)
			}
			if states != nil {
				t.Fatalf("state = %#v, want nil after decoding %s", states, tt.input)
			}
		})
	}
}

func TestSlurmStateListRejectsMalformedValues(t *testing.T) {
	for _, tt := range []struct {
		name      string
		input     string
		wantError string
	}{
		{name: "empty array", input: `{"state":[]}`},
		{name: "empty first element", input: `{"state":[""]}`},
		{
			name:      "empty trailing element",
			input:     `{"state":["RUNNING",""]}`,
			wantError: "job state array element 1 cannot be empty",
		},
		{name: "null first element", input: `{"state":[null]}`},
		{name: "null trailing element", input: `{"state":["RUNNING",null]}`},
		{name: "numeric array element", input: `{"state":[1]}`},
		{
			name:      "numeric scalar",
			input:     `{"state":1}`,
			wantError: "job state must be a string or array of strings",
		},
		{
			name:      "object",
			input:     `{"state":{}}`,
			wantError: "job state must be a string or array of strings",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var payload struct {
				State slurmStateList `json:"state"`
			}
			err := json.Unmarshal([]byte(tt.input), &payload)
			if err == nil {
				t.Fatalf("json.Unmarshal(%s) error = nil, want rejection", tt.input)
			}
			if tt.wantError != "" && !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("json.Unmarshal(%s) error = %q, want substring %q", tt.input, err, tt.wantError)
			}
		})
	}
}

func TestSelectSlurmJobStateMatchesPythonContract(t *testing.T) {
	tests := []struct {
		name        string
		states      slurmStateList
		chooseFinal bool
		want        string
	}{
		{name: "squeue selects first", states: slurmStateList{"PENDING", "REQUEUED"}, want: "PENDING"},
		{
			name:        "sacct selects first final",
			states:      slurmStateList{"PENDING", "REQUEUED"},
			chooseFinal: true,
			want:        "REQUEUED",
		},
		{
			name:        "sacct preserves final input order",
			states:      slurmStateList{"FAILED", "NODE_FAIL"},
			chooseFinal: true,
			want:        "FAILED",
		},
		{
			name:        "sacct falls back to first",
			states:      slurmStateList{"PENDING", "RUNNING"},
			chooseFinal: true,
			want:        "PENDING",
		},
		{
			name:        "special exit is not Python final",
			states:      slurmStateList{"PENDING", "SPECIAL_EXIT"},
			chooseFinal: true,
			want:        "PENDING",
		},
		{
			name:        "revoked is not Python final",
			states:      slurmStateList{"PENDING", "REVOKED"},
			chooseFinal: true,
			want:        "PENDING",
		},
		{
			name:        "final membership is case sensitive",
			states:      slurmStateList{"pending", "failed"},
			chooseFinal: true,
			want:        "pending",
		},
		{name: "missing remains empty", chooseFinal: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := selectSlurmJobState(tt.states, tt.chooseFinal); got != tt.want {
				t.Fatalf("selectSlurmJobState() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSelectSlurmJobStateRecognizesEveryPythonFinalState(t *testing.T) {
	finalStates := []string{
		"BOOT_FAIL", "CANCELLED", "COMPLETED", "DEADLINE", "FAILED",
		"NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "STOPPED", "SUSPENDED",
		"TIMEOUT", "REQUEUED",
	}
	for _, finalState := range finalStates {
		t.Run(finalState, func(t *testing.T) {
			states := slurmStateList{"PENDING", finalState}
			if got := selectSlurmJobState(states, true); got != finalState {
				t.Fatalf("selectSlurmJobState() = %q, want %q", got, finalState)
			}
		})
	}
}
