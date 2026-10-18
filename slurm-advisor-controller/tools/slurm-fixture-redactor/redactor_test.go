// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRedactJSONPreservesShapeAndRedactsIdentifiers(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"meta": {
			"command": ["squeue", "--json"],
			"Slurm": {"release": "24.11.0"}
		},
		"jobs": [{
			"job_id": 123456,
			"array_job_id": {"set": true, "number": 123400},
			"array_task_id": {"set": true, "number": 7},
			"job_state": "RUNNING",
			"state_reason": "None",
			"cluster": "prod-cluster-west",
			"partition": "priority-lab",
			"user_name": "alice",
			"account": "customer-a",
			"nodes": "dgx-a[001-003]",
			"excluded_nodes": "dgx-b004,dgx-b[006-007]",
			"submit_time": 1722495600,
			"command": "/home/alice/train/run.sh --data /lustre/customer-a/dataset",
			"comment": "{\"APS\":{\"workflow_id\":\"example-lab-pretrain\",\"auto_resume_mode\":\"requeue\",\"max_requeue_times\":3,\"checkpoint_path\":\"/lustre/customer-a/checkpoints/run42\"}}",
			"unknown_future_field": {"host": "dgx-c009", "state": "COMPLETING"}
		}]
	}`)

	redacted, err := RedactJSON(raw)
	if err != nil {
		t.Fatalf("RedactJSON failed: %v", err)
	}
	assertNoRawTokens(t, redacted)

	decoded := decodeWithNumbers(t, redacted)
	jobs := decoded["jobs"].([]any)
	job := jobs[0].(map[string]any)
	if _, ok := job["job_id"].(json.Number); !ok {
		t.Fatalf("job_id = %T, want json.Number", job["job_id"])
	}
	if got := job["job_state"]; got != "RUNNING" {
		t.Fatalf("job_state = %v, want RUNNING", got)
	}
	if got := job["submit_time"]; got != json.Number("1722495600") {
		t.Fatalf("submit_time = %v, want original numeric timestamp", got)
	}
	if got := job["nodes"]; got != "node[001-003]" {
		t.Fatalf("nodes = %q, want node-list shape preserved", got)
	}
	if got := job["excluded_nodes"]; got != "node004,node[006-007]" {
		t.Fatalf("excluded_nodes = %q, want node-list shape preserved", got)
	}
	if _, ok := job["unknown_future_field"].(map[string]any); !ok {
		t.Fatalf("unknown_future_field = %T, want nested object preserved", job["unknown_future_field"])
	}

	comment, ok := job["comment"].(string)
	if !ok {
		t.Fatalf("comment = %T, want string", job["comment"])
	}
	var commentJSON map[string]map[string]any
	if err := json.Unmarshal([]byte(comment), &commentJSON); err != nil {
		t.Fatalf("redacted comment is not JSON: %v", err)
	}
	aps := commentJSON["APS"]
	if got := aps["auto_resume_mode"]; got != "requeue" {
		t.Fatalf("auto_resume_mode = %v, want enum preserved", got)
	}
	if got := aps["workflow_id"]; got != "workflow-redacted" {
		t.Fatalf("workflow_id = %v, want redacted workflow", got)
	}
	if got := aps["checkpoint_path"]; got != "/redacted/path" {
		t.Fatalf("checkpoint_path = %v, want redacted path", got)
	}
}

func TestRedactJSONIsDeterministic(t *testing.T) {
	t.Parallel()

	raw := []byte(`{"jobs":[{"user_name":"alice","cluster":"prod-cluster-west","nodes":"dgx-a001","job_state":"PENDING","job_id":123456}]}`)
	first, err := RedactJSON(raw)
	if err != nil {
		t.Fatalf("first RedactJSON failed: %v", err)
	}
	second, err := RedactJSON(raw)
	if err != nil {
		t.Fatalf("second RedactJSON failed: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("RedactJSON is not deterministic:\nfirst:\n%s\nsecond:\n%s", first, second)
	}
}

func TestRunWritesRedactedOutput(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	input := filepath.Join(dir, "squeue.json")
	output := filepath.Join(dir, "redacted.json")
	if err := os.WriteFile(input, []byte(`{"jobs":[{"job_id":123456,"user_name":"alice","cluster":"prod-cluster-west","nodes":"dgx-a001"}]}`), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	if err := run([]string{"-input", input, "-output", output}); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	redacted, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	assertNoRawTokens(t, redacted)
	if !bytes.HasSuffix(redacted, []byte("\n")) {
		t.Fatalf("output should end with newline: %q", redacted)
	}
}

func decodeWithNumbers(t *testing.T, data []byte) map[string]any {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var decoded map[string]any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatalf("decode redacted JSON: %v\n%s", err, data)
	}
	return decoded
}

func assertNoRawTokens(t *testing.T, data []byte) {
	t.Helper()

	lower := strings.ToLower(string(data))
	for _, token := range []string{
		"alice",
		"customer-a",
		"dgx",
		"example-lab",
		"prod-cluster",
		"priority-lab",
		"/home/",
		"/lustre/",
	} {
		if strings.Contains(lower, token) {
			t.Fatalf("redacted output still contains %q:\n%s", token, data)
		}
	}
}
