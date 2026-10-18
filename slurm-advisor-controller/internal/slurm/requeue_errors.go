// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

func classifyRequeueError(err error) error {
	var commandErr *commandError
	if !errors.As(err, &commandErr) {
		return err
	}
	// scontrol exposes Slurm error text rather than a structured error code.
	// Diagnostics: https://github.com/SchedMD/slurm/blob/slurm-25-05-8-1/src/common/slurm_errno.c
	// Keep the terminal list narrow: transport failures, controller standby,
	// rate limiting, and "try again later" state transitions remain retryable.
	// Unknown diagnostics also get the bounded budget instead of losing work
	// immediately when a Slurm version introduces a new transient error.
	output := strings.ToLower(string(commandErr.output.stdout) + "\n" + string(commandErr.output.stderr))
	for _, diagnostic := range []string{
		"invalid job id specified",
		"access/permission denied",
		"requested operation is presently disabled",
		"requested operation not supported on this system",
		"only batch jobs are accepted or processed",
	} {
		if strings.Contains(output, diagnostic) {
			return errors.Join(cluster.ErrJobRequeuePermanent, err)
		}
	}
	return err
}
