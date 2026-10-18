// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestFinalizedJobsSeparatesStreamsAndCapturesDiagnostics(t *testing.T) {
	// Exercise the real executor and JSON log encoder: stderr must not break
	// valid stdout, while failed decodes must preserve both streams byte-for-byte.
	for _, test := range []struct {
		name             string
		stdout           string
		stderr           string
		exitCode         string
		wantJobs         int
		wantError        bool
		wantLevel        string
		wantSyntaxOffset int64
	}{
		{
			name:      "stderr before valid JSON does not break job status",
			stdout:    "{\"jobs\":[{\"job_id\":123,\"state\":{\"current\":\"COMPLETED\"}}]}\n",
			stderr:    "message from SSH\n\t\xff",
			wantJobs:  1,
			wantLevel: "warn",
		},
		{
			name:      "malformed stdout preserves both streams and stdout offset",
			stdout:    " \nm\t\"\\\x00\xff" + strings.Repeat("payload", 4096) + "\n\n",
			stderr:    "warning before response\n\xff stderr_quoted=\"not a field\"\n",
			wantError: true, wantLevel: "error", wantSyntaxOffset: 3,
		},
		{name: "incompatible JSON shape", stdout: `{"jobs":"unexpected"}`, wantError: true, wantLevel: "error"},
		{name: "custom field decode failure", stdout: `{"jobs":[{"job_id":123,"state":{"current":[]}}]}`, wantError: true, wantLevel: "error"},
		{name: "empty stdout never falls back to stderr JSON", stderr: `{"jobs":[]}`, wantError: true, wantLevel: "error"},
		{name: "successful empty response is not logged", stdout: "{\"jobs\":[]}\n"},
		{name: "command failure uses existing error path", stderr: "SSH connection failed\n", exitCode: "1", wantError: true},
		{name: "job conversion failure is not a decode failure", stdout: `{"jobs":[{"job_id":123,"nodes":"node[invalid]"}]}`, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			installSacctDiagnosticFixture(t, test.stdout, test.stderr, test.exitCode)
			var logs bytes.Buffer
			logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&logs), zap.WarnLevel))
			adapter := NewCluster(Options{Logger: logger})
			defer adapter.Close()
			jobs, err := adapter.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{{JobID: "123"}})
			if (err != nil) != test.wantError || len(jobs) != test.wantJobs {
				t.Fatalf("FinalizedJobs() returned %d jobs, %v; want %d jobs, error=%t", len(jobs), err, test.wantJobs, test.wantError)
			}
			if test.wantJobs == 1 && jobs[0].State != cluster.JobStateCompleted {
				t.Fatalf("stderr changed job state: %v", jobs[0].State)
			}
			if test.wantLevel == "" {
				if logs.Len() != 0 {
					t.Fatalf("unexpected diagnostic: %s", logs.String())
				}
				return
			}
			var entry struct {
				Level           string   `json:"level"`
				Message         string   `json:"msg"`
				Source          string   `json:"source"`
				JobID           string   `json:"job_id"`
				Command         []string `json:"command"`
				StdoutBytes     int      `json:"stdout_bytes"`
				StderrBytes     int      `json:"stderr_bytes"`
				JSONErrorOffset int64    `json:"json_error_offset"`
			}
			if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
				t.Fatalf("decode single diagnostic: %v", err)
			}
			if entry.Level != test.wantLevel || entry.JobID != "123" || entry.Source != "sacct_status" || strings.Join(entry.Command, " ") != "sacct -Xj 123 --duplicates --json" {
				t.Fatalf("diagnostic has wrong identity/level: %#v", entry)
			}
			header := requireSacctStreamsInMessage(t, entry.Message, test.stdout, test.stderr, test.wantLevel == "error")
			if !strings.Contains(header, `job_id="123"`) || !strings.Contains(header, `source="sacct_status"`) ||
				!strings.Contains(header, `command=["sacct" "-Xj" "123" "--duplicates" "--json"]`) ||
				!strings.Contains(header, "stdout_bytes="+strconv.Itoa(len(test.stdout))) ||
				!strings.Contains(header, "stderr_bytes="+strconv.Itoa(len(test.stderr))) ||
				entry.StdoutBytes != len(test.stdout) || entry.StderrBytes != len(test.stderr) {
				t.Fatalf("msg is missing diagnostic context: %s", header)
			}
			if test.wantSyntaxOffset != 0 {
				var syntaxErr *json.SyntaxError
				if !errors.As(err, &syntaxErr) || syntaxErr.Offset != test.wantSyntaxOffset || entry.JSONErrorOffset != test.wantSyntaxOffset {
					t.Fatalf("syntax error identity/offset lost: error=%v, logged offset=%d, want=%d", err, entry.JSONErrorOffset, test.wantSyntaxOffset)
				}
			}
			if entry.JSONErrorOffset != 0 && !strings.Contains(header, "json_error_offset="+strconv.FormatInt(entry.JSONErrorOffset, 10)) {
				t.Fatalf("msg is missing decoder offset: %s", header)
			}
		})
	}
}

// requireSacctStreamsInMessage checks the interface visible in message-only
// log viewers, including lossless recovery of quoted bytes and stream labels.
func requireSacctStreamsInMessage(t *testing.T, message, stdout, stderr string, wantStdout bool) string {
	t.Helper()
	header := ""
	remainder := message
	if wantStdout {
		var found bool
		header, remainder, found = strings.Cut(message, " stdout_quoted=")
		if !found {
			t.Fatal("msg is missing stdout capture")
		}
		quoted, err := strconv.QuotedPrefix(remainder)
		if err != nil {
			t.Fatalf("stdout quote: %v", err)
		}
		got, err := strconv.Unquote(quoted)
		if err != nil || got != stdout {
			t.Fatalf("stdout capture differs: got %d bytes, want %d, error=%v", len(got), len(stdout), err)
		}
		remainder = remainder[len(quoted):]
	} else if strings.Contains(message, " stdout_quoted=") {
		t.Fatal("successful decode logged raw stdout")
	}
	before, quoted, found := strings.Cut(remainder, " stderr_quoted=")
	if !found || (wantStdout && before != "") {
		t.Fatal("msg is missing separately labeled stderr")
	}
	if !wantStdout {
		header = before
	}
	got, err := strconv.Unquote(quoted)
	if err != nil || got != stderr {
		t.Fatalf("stderr capture differs: got %d bytes, want %d, error=%v", len(got), len(stderr), err)
	}
	return header
}

func TestLogMetadataCapturesFailedDecodeInput(t *testing.T) {
	// Public enrichment calls must retain usable status when metadata fails.
	// The finalized query serves two attempts, but must log its output only once.
	const malformed = "message from SSH\n\t\xff{\"jobs\":[]}\n"
	for _, source := range []string{"sacct_steps", "sacct_finalized"} {
		for _, response := range []struct {
			name      string
			result    commandResult
			wantLevel string
			wantPaths bool
		}{
			{name: "failed decode", result: commandResult{output: []byte(malformed), stderr: []byte("metadata warning\n")}, wantLevel: "error"},
			{name: "successful decode with stderr", result: commandResult{output: sacctLogPayload("/logs/step.out"), stderr: []byte("metadata warning\n")}, wantLevel: "warn", wantPaths: true},
			{name: "successful decode", result: commandResult{output: []byte(`{"jobs":[]}`)}},
			{name: "execution failure", result: commandResult{err: errors.New("accounting unavailable")}},
		} {
			t.Run(source+"/"+response.name, func(t *testing.T) {
				executor := newScriptedLogExecutor(map[string][]commandResult{
					commandKey([]string{"squeue", "--json"}): {{output: []byte(`{"jobs":[{
						"job_id":12345,"job_state":"RUNNING","stdout_expanded":"/logs/parent.out"
					}]}`)}},
					commandKey([]string{"sacct", "-Xj", "12345", "--duplicates", "--json"}): {{output: []byte(`{"jobs":[
						{"job_id":12345,"state":{"current":"COMPLETED"},"time":{"submission":10,"start":20,"end":30}},
						{"job_id":12345,"state":{"current":"COMPLETED"},"time":{"submission":40,"start":50,"end":60}}
					]}`)}},
					logSacctKey("12345"): {response.result, response.result},
				})
				var logs bytes.Buffer
				logger := zap.New(zapcore.NewCore(
					zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
					zapcore.AddSync(&logs), zap.WarnLevel,
				))
				adapter := newSlurmCluster(Options{EnableLogMetadata: true, Logger: logger})
				adapter.roCommands = executor
				t.Cleanup(adapter.Close)
				var jobs []cluster.JobExecutionRecord
				var err error
				wantJobs, wantState := 2, cluster.JobStateCompleted
				if source == "sacct_steps" {
					wantJobs, wantState = 1, cluster.JobStateRunning
					jobs, err = adapter.ActiveJobs(context.Background())
					if err == nil {
						jobs = adapter.EnrichActiveJobs(context.Background(), jobs)
						waitForLogMetadataIdle(t, adapter)
						jobs = adapter.EnrichActiveJobs(context.Background(), jobs)
					}
				} else {
					jobs, err = adapter.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{
						{JobID: "12345", AttemptIndex: 0},
						{JobID: "12345", AttemptIndex: 1},
					})
				}
				// Join workers before reading the JSON encoder's output buffer.
				adapter.Close()
				if err != nil || len(jobs) != wantJobs {
					t.Fatalf("job status unavailable after metadata query: jobs=%d, err=%v", len(jobs), err)
				}
				for _, job := range jobs {
					if job.State != wantState {
						t.Fatalf("job state=%v, want %v", job.State, wantState)
					}
				}
				wantCalls := 1
				if response.result.err != nil {
					wantCalls = 2
				}
				if got := executor.callCount(logSacctKey("12345")); got != wantCalls {
					t.Fatalf("metadata queries=%d, want %d", got, wantCalls)
				}
				if response.wantPaths {
					paths := LogFilePaths(jobs[0])
					if !slices.Contains(paths, "/logs/step.out") {
						t.Fatalf("stderr prevented path discovery: %v", paths)
					}
				}
				type diagnostic struct {
					Message string   `json:"msg"`
					Source  string   `json:"source"`
					Level   string   `json:"level"`
					Command []string `json:"command"`
				}
				var captures []diagnostic
				decoder := json.NewDecoder(&logs)
				for {
					var entry diagnostic
					if err := decoder.Decode(&entry); errors.Is(err, io.EOF) {
						break
					} else if err != nil {
						t.Fatalf("decode log: %v", err)
					}
					// Existing metadata failure warnings have no command capture.
					if len(entry.Command) != 0 {
						captures = append(captures, entry)
					}
				}
				if response.wantLevel == "" {
					if len(captures) != 0 {
						t.Fatalf("unexpected raw-output diagnostics: %d", len(captures))
					}
					return
				}
				if len(captures) != 1 {
					t.Fatalf("diagnostic captures=%d, want exactly one per query", len(captures))
				}
				entry := captures[0]
				header := requireSacctStreamsInMessage(t, entry.Message, string(response.result.output), string(response.result.stderr), response.wantLevel == "error")
				if entry.Source != source || entry.Level != response.wantLevel || !strings.Contains(header, "source="+strconv.Quote(source)) ||
					!strings.Contains(header, `job_id="12345"`) ||
					!strings.Contains(header, `command=["sacct" "-j" "12345" "--duplicates" "--json=v0.0.43"]`) ||
					!strings.Contains(header, "stdout_bytes="+strconv.Itoa(len(response.result.output))) ||
					!strings.Contains(header, "stderr_bytes="+strconv.Itoa(len(response.result.stderr))) {
					t.Fatalf("metadata diagnostic has incorrect context: %s", header)
				}
				if response.wantLevel == "error" && !strings.Contains(header, "json_error_offset=1") {
					t.Fatalf("missing stdout error offset: %s", header)
				}
			})
		}
	}
}

func TestFinalizedJobsDecodeFailureWithoutLogger(t *testing.T) {
	installSacctDiagnosticFixture(t, "malformed", "", "")
	adapter := NewCluster(Options{})
	defer adapter.Close()
	_, err := adapter.FinalizedJobs(context.Background(), []cluster.JobAttemptRef{{JobID: "123"}})
	var syntaxErr *json.SyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Fatalf("FinalizedJobs() lost syntax error without a logger: %v", err)
	}
}

func installSacctDiagnosticFixture(t *testing.T, stdout, stderr, exitCode string) {
	t.Helper()
	dir := t.TempDir()
	if exitCode == "" {
		exitCode = "0"
	}
	for name, data := range map[string]string{
		"stdout": stdout,
		"stderr": stderr,
		"sacct":  "#!/bin/sh\ncat \"$SACCT_DIAGNOSTIC_FIXTURE/stderr\" >&2\ncat \"$SACCT_DIAGNOSTIC_FIXTURE/stdout\"\nexit " + exitCode + "\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("SACCT_DIAGNOSTIC_FIXTURE", dir)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}
