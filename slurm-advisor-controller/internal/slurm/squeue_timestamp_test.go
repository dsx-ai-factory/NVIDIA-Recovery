// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"strings"
	"testing"
	"time"
)

func TestObservedJobsFromSqueueConvertsFlaggedTimestamps(t *testing.T) {
	jobs, err := observedJobsFromSqueue([]byte(`{
		"jobs": [{
			"job_id": 12345,
			"submit_time": {"set": true, "infinite": false, "number": 1},
			"start_time": {"set": true, "infinite": false, "number": 2},
			"end_time": {"set": true, "infinite": false, "number": 3}
		}]
	}`))
	if err != nil {
		t.Fatalf("observedJobsFromSqueue() failed: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("len(jobs) = %d, want 1", len(jobs))
	}

	job := jobs[0]
	assertObservedTimestamp(t, job.SubmitTime, job.SchedulerMetadata, "slurm_submit_time", 1, true)
	assertObservedTimestamp(t, job.StartTime, job.SchedulerMetadata, "slurm_start_time", 2, true)
	assertObservedTimestamp(t, job.EndTime, job.SchedulerMetadata, "slurm_end_time", 3, true)
}

func TestObservedJobsFromSqueueHonorsTimestampFlags(t *testing.T) {
	tests := []struct {
		name    string
		endTime string
	}{
		{name: "unset", endTime: `{"set": false, "infinite": false, "number": 4}`},
		{name: "unset infinite", endTime: `{"set": false, "infinite": true, "number": 4}`},
		{name: "infinite", endTime: `{"set": true, "infinite": true, "number": 4}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs, err := observedJobsFromSqueue([]byte(`{"jobs":[{"job_id":12345,"end_time":` + tt.endTime + `}]}`))
			if err != nil {
				t.Fatalf("observedJobsFromSqueue() failed: %v", err)
			}
			if len(jobs) != 1 {
				t.Fatalf("len(jobs) = %d, want 1", len(jobs))
			}

			assertObservedTimestamp(t, jobs[0].EndTime, jobs[0].SchedulerMetadata, "slurm_end_time", 0, false)
		})
	}
}

func TestObservedJobsFromSqueuePreservesLegacyTimestampSemantics(t *testing.T) {
	tests := []struct {
		name        string
		endTimeJSON string
		wantUnix    int64
		wantPresent bool
	}{
		{name: "positive integer", endTimeJSON: `,"end_time":4`, wantUnix: 4, wantPresent: true},
		{name: "zero", endTimeJSON: `,"end_time":0`},
		{name: "negative", endTimeJSON: `,"end_time":-1`},
		{name: "Slurm no value sentinel", endTimeJSON: `,"end_time":4294967294`},
		{name: "Slurm infinite sentinel", endTimeJSON: `,"end_time":4294967295`},
		{name: "null", endTimeJSON: `,"end_time":null`},
		{name: "missing"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs, err := observedJobsFromSqueue([]byte(`{"jobs":[{"job_id":12345` + tt.endTimeJSON + `}]}`))
			if err != nil {
				t.Fatalf("observedJobsFromSqueue() failed: %v", err)
			}
			if len(jobs) != 1 {
				t.Fatalf("len(jobs) = %d, want 1", len(jobs))
			}

			assertObservedTimestamp(t, jobs[0].EndTime, jobs[0].SchedulerMetadata, "slurm_end_time", tt.wantUnix, tt.wantPresent)
		})
	}
}

func TestObservedJobsFromSqueueRejectsMalformedTimestamps(t *testing.T) {
	tests := []struct {
		name    string
		endTime string
	}{
		{name: "string", endTime: `"tomorrow"`},
		{name: "array", endTime: `[]`},
		{name: "boolean", endTime: `true`},
		{name: "fractional number", endTime: `1.5`},
		{name: "wrong object field type", endTime: `{"set":"true","number":1}`},
		{name: "null set", endTime: `{"set":null,"infinite":false,"number":1}`},
		{name: "null infinite", endTime: `{"set":true,"infinite":null,"number":1}`},
		{name: "null number", endTime: `{"set":true,"infinite":false,"number":null}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs, err := observedJobsFromSqueue([]byte(`{"jobs":[{"job_id":12345,"end_time":` + tt.endTime + `}]}`))
			if err == nil {
				t.Fatalf("observedJobsFromSqueue() error = nil, jobs = %#v, want malformed timestamp error", jobs)
			}
			if !strings.Contains(err.Error(), "failed to unmarshal squeue output") {
				t.Fatalf("observedJobsFromSqueue() error = %q, want parse context", err)
			}
		})
	}
}

func assertObservedTimestamp(
	t *testing.T,
	got *time.Time,
	metadata map[string]any,
	key string,
	wantUnix int64,
	wantPresent bool,
) {
	t.Helper()

	if !wantPresent {
		if got != nil {
			t.Fatalf("timestamp = %v, want nil", got)
		}
		if value, ok := metadata[key]; ok {
			t.Fatalf("metadata[%q] = %#v, want absent", key, value)
		}
		return
	}

	if got == nil {
		t.Fatal("timestamp = nil, want present")
	}
	if got.Unix() != wantUnix {
		t.Fatalf("timestamp Unix() = %d, want %d", got.Unix(), wantUnix)
	}
	wantMetadata := time.Unix(wantUnix, 0).Format(time.RFC3339)
	gotMetadata, ok := metadata[key].(string)
	if !ok {
		t.Fatalf("metadata[%q] = %#v, want RFC3339 string %q", key, metadata[key], wantMetadata)
	}
	if gotMetadata != wantMetadata {
		t.Fatalf("metadata[%q] = %q, want %q", key, gotMetadata, wantMetadata)
	}
}
