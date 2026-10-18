// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//go:build harness

package scenarios

import (
	"encoding/json"
	"testing"
)

func TestSqueueJobStateAcceptsScalarAndArrayShapes(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  squeueJobState
	}{
		{name: "legacy scalar", value: `"RUNNING"`, want: "RUNNING"},
		{name: "singleton array", value: `["RUNNING"]`, want: "RUNNING"},
		{name: "base state plus flag", value: `["RUNNING","COMPLETING"]`, want: "RUNNING"},
		{name: "null", value: `null`},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got squeueJobState
			if err := json.Unmarshal([]byte(test.value), &got); err != nil {
				t.Fatalf("json.Unmarshal() failed: %v", err)
			}
			if got != test.want {
				t.Fatalf("state = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSqueueJobStateRejectsMalformedArrays(t *testing.T) {
	for _, value := range []string{
		`[]`,
		`[""]`,
		`["RUNNING",""]`,
		`[1]`,
	} {
		t.Run(value, func(t *testing.T) {
			var state squeueJobState
			if err := json.Unmarshal([]byte(value), &state); err == nil {
				t.Fatalf("json.Unmarshal(%s) error = nil", value)
			}
		})
	}
}
