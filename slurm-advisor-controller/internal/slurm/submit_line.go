// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import "regexp"

// Match three parts: a start/whitespace boundary, an output flag, and its value.
// Accept --output=path, --output path, -opath, and -o path. The only capture
// group is a quoted string (allowing spaces) or a non-whitespace value.
// Shell escapes and application boundaries are not parsed by this flag scan.
var slurmSubmitOutputPattern = regexp.MustCompile(`(?:^|\s)(?:--output(?:=|\s+)|-o\s*)('[^']*'|"[^"]*"|[^\s]+)`)

// slurmSubmitOutputOptions scans the complete SubmitLine for output flags without
// interpreting unrelated options or distinguishing scheduler and application flags.
// Retained quotes preserve spaces in a filename; quoting already lost by Slurm
// is unrecoverable.
func slurmSubmitOutputOptions(submitLine string) []string {
	var output []string
	for _, match := range slurmSubmitOutputPattern.FindAllStringSubmatch(submitLine, -1) {
		value := match[1]
		if len(value) >= 2 && (value[0] == '\'' || value[0] == '"') && value[len(value)-1] == value[0] {
			value = value[1 : len(value)-1]
		}
		if value != "" {
			output = append(output, value)
		}
	}
	return output
}
