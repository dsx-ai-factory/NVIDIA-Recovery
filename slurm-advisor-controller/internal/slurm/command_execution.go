// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"bytes"
	"context"
	"os/exec"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
)

type commandExecutor interface {
	Execute(cmd *exec.Cmd) (commandOutput, error)
}

// commandOutput keeps stderr diagnostics out of structured stdout responses.
type commandOutput struct {
	stdout []byte
	stderr []byte
}

type executor struct {
	dryRun bool
}

func (e executor) Execute(cmd *exec.Cmd) (commandOutput, error) {
	if e.dryRun {
		return commandOutput{}, nil
	}

	// Each pipe has its own buffer. Run joins both copy goroutines before we
	// read the buffers, including when cancellation or WaitDelay closes a pipe.
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	output := commandOutput{stdout: stdout.Bytes(), stderr: stderr.Bytes()}
	if err != nil {
		return output, &commandError{
			output: output,
			err:    errors.Wrapf(err, "command %q failed\nstdout: %q\nstderr: %q", cmd.String(), output.stdout, output.stderr),
		}
	}

	return output, nil
}

var _ commandExecutor = executor{}

// commandError keeps diagnostics separate from command arguments so adapter
// classification cannot mistake command text for a scheduler response.
type commandError struct {
	output commandOutput
	err    error
}

func (e *commandError) Error() string { return e.err.Error() }

func (e *commandError) Unwrap() error { return e.err }

// CommandObserver records scheduler commands terminated by execution deadlines.
// Implementations must support concurrent calls from accounting and metadata workers.
type CommandObserver interface {
	// ObserveCommandTimeout records one issued command interrupted by its own or
	// an enclosing operation's deadline. Command arguments are never included.
	ObserveCommandTimeout(command string)
}

// execute owns the command deadline and waits for execution and cancellation to
// finish before reporting a timeout. The cancellation callback tracks commands
// actually killed by a deadline, excluding failures before process start and
// ordinary caller cancellation. A successful command is never counted.
func (s *slurmCluster) execute(ctx context.Context, runner commandExecutor, name string, args ...string) (commandOutput, error) {
	if s.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, name, args...)
	// SSH wrappers can leave descendants holding output pipes after the local
	// process is killed. Bound pipe draining so they cannot defeat phase budgets.
	cmd.WaitDelay = 100 * time.Millisecond
	var deadlineKilled atomic.Bool
	cancelCommand := cmd.Cancel
	cmd.Cancel = func() error {
		err := cancelCommand()
		if err == nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			deadlineKilled.Store(true)
		}
		return err
	}
	output, err := runner.Execute(cmd)
	if err != nil && deadlineKilled.Load() {
		if s.commandObserver != nil {
			s.commandObserver.ObserveCommandTimeout(name)
		}
		// Keep the execution deadline inspectable alongside the process exit error.
		err = errors.Join(err, context.DeadlineExceeded)
	}
	return output, err
}
