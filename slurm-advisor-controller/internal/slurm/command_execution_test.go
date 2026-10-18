// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExecutorDryRunDoesNotExecuteCommand(t *testing.T) {
	t.Parallel()

	output, err := executor{dryRun: true}.Execute(exec.Command("definitely-not-a-slurm-test-command"))
	if err != nil {
		t.Fatalf("Execute() dry run returned error: %v", err)
	}
	if len(output.stdout) != 0 {
		t.Fatalf("Execute() dry run output = %q, want empty", output)
	}
}

func TestExecutorRunsSuccessfulCommand(t *testing.T) {
	t.Parallel()

	output, err := executor{}.Execute(exec.Command("echo", "Hello, World!"))
	if err != nil {
		t.Fatalf("Execute() returned error: %v", err)
	}
	if !strings.Contains(string(output.stdout), "Hello, World!") {
		t.Fatalf("Execute() output = %q, want greeting", output)
	}
}

func TestExecutorWrapsFailedCommand(t *testing.T) {
	t.Parallel()

	_, err := executor{}.Execute(exec.Command("definitely-not-a-slurm-test-command"))
	if err == nil {
		t.Fatal("Execute() succeeded, want error")
	}
	if !strings.Contains(err.Error(), `command "definitely-not-a-slurm-test-command" failed`) {
		t.Fatalf("Execute() error = %q, want command context", err.Error())
	}
	var execErr *exec.Error
	if !errors.As(err, &execErr) {
		t.Fatalf("Execute() error = %v, want wrapped *exec.Error", err)
	}
}

func TestExecutorPreservesStreamsAndExitError(t *testing.T) {
	t.Parallel()
	for _, exitCode := range []string{"0", "7"} {
		t.Run(exitCode, func(t *testing.T) {
			output, err := executor{}.Execute(exec.Command("sh", "-c",
				"printf 'response\\n'; printf 'diagnostic\\n' >&2; exit "+exitCode))
			if string(output.stdout) != "response\n" || string(output.stderr) != "diagnostic\n" {
				t.Fatalf("streams were lost or mixed: stdout=%q stderr=%q", output.stdout, output.stderr)
			}
			if exitCode == "0" {
				if err != nil {
					t.Fatalf("stderr turned a successful command into an error: %v", err)
				}
				return
			}
			var exitErr *exec.ExitError
			var commandErr *commandError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 7 || !errors.As(err, &commandErr) {
				t.Fatalf("process error identity lost: %v", err)
			}
			if string(commandErr.output.stdout) != "response\n" || string(commandErr.output.stderr) != "diagnostic\n" ||
				!strings.Contains(err.Error(), `stdout: "response\n"`) || !strings.Contains(err.Error(), `stderr: "diagnostic\n"`) {
				t.Fatalf("failed command lost stream diagnostics: %v", err)
			}
		})
	}
}

func TestSchedulerCommandTimeoutObservation(t *testing.T) {
	for _, test := range []struct {
		name        string
		script      string
		localLimit  time.Duration
		parentLimit time.Duration
		preCanceled bool
		dryRun      bool
		wantTimeout int
		wantError   bool
	}{
		{name: "command deadline", script: "exec sleep 5", localLimit: 80 * time.Millisecond, wantTimeout: 1, wantError: true},
		{name: "earlier phase deadline with custom cause", script: "exec sleep 5", localLimit: time.Second, parentLimit: 80 * time.Millisecond, wantTimeout: 1, wantError: true},
		{name: "successful command", script: "exit 0", localLimit: time.Second},
		{name: "ordinary failure", script: "exit 1", localLimit: time.Second, wantError: true},
		{name: "pre canceled", script: "exec sleep 5", localLimit: time.Second, preCanceled: true, wantError: true},
		{name: "pre expired", script: "exec sleep 5", localLimit: time.Second, parentLimit: -time.Second, wantError: true},
		{name: "dry run", script: "exec sleep 5", localLimit: time.Nanosecond, dryRun: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			installScontrolFixture(t, test.script)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.preCanceled {
				cancel()
			}
			if test.parentLimit != 0 {
				var cancelDeadline context.CancelFunc
				ctx, cancelDeadline = context.WithTimeoutCause(ctx, test.parentLimit, errors.New("phase exhausted"))
				defer cancelDeadline()
			}
			observer := &timeoutRecorder{}
			adapter := NewCluster(Options{Timeout: test.localLimit, DryRun: test.dryRun, CommandObserver: observer})
			defer adapter.Close()
			err := adapter.JobRequeueHold(ctx, "123")
			if (err != nil) != test.wantError {
				t.Fatalf("JobRequeueHold() error = %v, want error %t", err, test.wantError)
			}
			if got := observer.counts["scontrol"]; got != test.wantTimeout {
				t.Fatalf("scontrol timeouts = %d, want %d", got, test.wantTimeout)
			}
			if test.wantTimeout > 0 && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("execution error lost deadline identity: %v", err)
			}
		})
	}
}

func TestSchedulerCommandCancellationDoesNotCountTimeout(t *testing.T) {
	// The fixture's marker ensures cancellation occurs after command startup.
	started := filepath.Join(t.TempDir(), "started")
	t.Setenv("COMMAND_STARTED_FILE", started)
	installScontrolFixture(t, ": > \"$COMMAND_STARTED_FILE\"\nexec sleep 5")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer := &timeoutRecorder{}
	adapter := NewCluster(Options{Timeout: 3 * time.Second, CommandObserver: observer})
	defer adapter.Close()
	result := make(chan error, 1)
	go func() { result <- adapter.JobRequeueHold(ctx, "123") }()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(started); err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("command did not start")
		case <-ticker.C:
		}
	}
	cancel()
	if err := <-result; err == nil {
		t.Fatal("cancelled command succeeded")
	}
	if len(observer.counts) != 0 {
		t.Fatalf("manual cancellation counted as timeout: %v", observer.counts)
	}
}

func TestSchedulerCommandDeadlineBoundsDescendantPipes(t *testing.T) {
	installScontrolFixture(t, "sleep 2 &\nwait")
	adapter := NewCluster(Options{Timeout: 80 * time.Millisecond})
	defer adapter.Close()
	started := time.Now()
	err := adapter.JobRequeueHold(context.Background(), "123")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("JobRequeueHold() = %v, want deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("deadline waited for inherited output pipes: %v", elapsed)
	}
}

func installScontrolFixture(t *testing.T, script string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "scontrol"), []byte("#!/bin/sh\n"+script+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// timeoutRecorder accumulates observations after execution has joined the
// command's cancellation callback; these tests issue one command at a time.
type timeoutRecorder struct {
	counts map[string]int
}

func (r *timeoutRecorder) ObserveCommandTimeout(command string) {
	if r.counts == nil {
		r.counts = make(map[string]int)
	}
	r.counts[command]++
}
