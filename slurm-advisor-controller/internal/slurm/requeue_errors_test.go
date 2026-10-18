// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

// Exercise the public adapter and real command executor: classification must
// survive scontrol/SSH prefixes and preserve the original process exit error.
func TestJobRequeueHoldClassifiesCommandFailures(t *testing.T) {
	dir := t.TempDir()
	script := "#!/bin/sh\nprintf '%s\\n' \"$SAC_TEST_DIAGNOSTIC\" >&2\nexit 1\n"
	if err := os.WriteFile(filepath.Join(dir, "scontrol"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	for _, tt := range []struct {
		name       string
		diagnostic string
		permanent  bool
	}{
		{"purged job", "Invalid job id specified for job 12345", true},
		{"wrapped purged job", "scontrol: error: requeue job 12345: INVALID JOB ID SPECIFIED\nConnection closed", true},
		{"permission", "Access/permission denied for job 12345", true},
		{"disabled", "Requested operation is presently disabled", true},
		{"unsupported", "Requested operation not supported on this system", true},
		{"not batch", "Only batch jobs are accepted or processed", true},
		{"controller unavailable", "Unable to contact slurm controller (connect failure)", false},
		{"socket timeout", "Socket timed out on send/recv operation", false},
		{"rate limit", "Rate limit exceeded, please retry momentarily", false},
		{"state transition", "Job can not be altered now, try again later", false},
		{"standby", "Slurm backup controller in standby mode", false},
		{"SSH failure", "ssh: connect to host scheduler port 22: Connection refused", false},
		{"unknown", "Unexpected scheduler error", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SAC_TEST_DIAGNOSTIC", tt.diagnostic)
			adapter := NewCluster(Options{})
			defer adapter.Close()
			err := adapter.JobRequeueHold(context.Background(), "12345")
			if got := errors.Is(err, cluster.ErrJobRequeuePermanent); got != tt.permanent {
				t.Fatalf("permanent = %v, want %v: %v", got, tt.permanent, err)
			}
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
				t.Fatalf("error = %v, want original exit code 1", err)
			}
		})
	}

	t.Run("command arguments are not scheduler diagnostics", func(t *testing.T) {
		t.Setenv("SAC_TEST_DIAGNOSTIC", "Unknown failure")
		adapter := NewCluster(Options{})
		defer adapter.Close()
		err := adapter.JobRequeueHold(context.Background(), "Invalid job id specified")
		if err == nil || errors.Is(err, cluster.ErrJobRequeuePermanent) {
			t.Fatalf("error = %v, want unclassified command failure", err)
		}
	})
}
