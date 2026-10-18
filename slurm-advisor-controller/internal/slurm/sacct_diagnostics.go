// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
)

// logSacctOutput captures both streams after a failed sacct decode, or stderr
// alone when the command succeeded and stdout did not fail JSON decoding.
// The caller still reports the error at its owning boundary; this separate
// diagnostic owns only the otherwise-lost command output. args must be the
// arguments supplied to the executor, before any external wrapper rewrites.
func (s *slurmCluster) logSacctOutput(jobID, source string, args []string, output commandOutput, err error) {
	var decodeErr *sacctDecodeError
	decodeFailed := errors.As(err, &decodeErr)
	if s.logger == nil || (!decodeFailed && len(output.stderr) == 0) {
		return
	}

	command := append([]string{"sacct"}, args...)
	summary := "sacct command wrote to stderr"
	if decodeFailed {
		summary = "Captured sacct output after JSON decode failure"
	}
	message := fmt.Sprintf("%s: job_id=%q source=%q command=%q stdout_bytes=%d stderr_bytes=%d",
		summary, jobID, source, command, len(output.stdout), len(output.stderr))
	fields := []zap.Field{
		zap.String("job_id", jobID),
		zap.String("source", source),
		zap.Strings("command", command),
		zap.Int("stdout_bytes", len(output.stdout)),
		zap.Int("stderr_bytes", len(output.stderr)),
	}
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	switch {
	case decodeFailed && errors.As(decodeErr, &syntaxErr):
		fields = append(fields, zap.Int64("json_error_offset", syntaxErr.Offset))
		message += fmt.Sprintf(" json_error_offset=%d", syntaxErr.Offset)
	case decodeFailed && errors.As(decodeErr, &typeErr):
		fields = append(fields, zap.Int64("json_error_offset", typeErr.Offset))
		message += fmt.Sprintf(" json_error_offset=%d", typeErr.Offset)
	}
	// Put the complete diagnostic in msg for log viewers that display only that
	// field. Go quoting preserves invalid UTF-8 and control bytes that a JSON
	// encoder would otherwise replace. Do not trim or truncate this capture:
	// Remote shell startup can still contaminate stdout even with split streams.
	if decodeFailed {
		message += " stdout_quoted=" + strconv.Quote(string(output.stdout))
	}
	message += " stderr_quoted=" + strconv.Quote(string(output.stderr))
	if decodeFailed {
		s.logger.Error(message, fields...)
	} else {
		s.logger.Warn(message, fields...)
	}
}
