// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"slices"
	"testing"
)

// The whole-line scan needs no knowledge of unrelated option arity and keeps
// every output candidate, including application flags after the executable or --.
func TestSlurmSubmitOutputOptions(t *testing.T) {
	for _, test := range []struct {
		name, line string
		want       []string
	}{
		{"disable status", "srun --disable-status --output=/logs/step.out train", []string{"/logs/step.out"}},
		{"unknown separate value", "srun --vendor-option value -o /logs/step.out train", []string{"/logs/step.out"}},
		{"unknown attached value", "srun --vendor-option=value -o /logs/step.out train", []string{"/logs/step.out"}},
		{"unknown short option", "srun -Z value -o /logs/step.out train", []string{"/logs/step.out"}},
		{"application output", "srun train --output=/application/model", []string{"/application/model"}},
		{"after double dash", "srun -- train -o /application/model", []string{"/application/model"}},
		{"scheduler and application", "srun -o /logs/step.out train --output=/application/model", []string{"/logs/step.out", "/application/model"}},
		{"long separate output", "srun --output /logs/step.out train", []string{"/logs/step.out"}},
		{"short attached output", "srun -o/logs/step.out train", []string{"/logs/step.out"}},
		{"quoted filename", "srun -o '/logs/with spaces.out' train", []string{"/logs/with spaces.out"}},
		{"unrelated quoting", "srun --comment 'unmatched -o /logs/step.out train", []string{"/logs/step.out"}},
		{"empty output", "srun --output= --output='' -o \"\" train", nil},
		{"missing output", "srun --output", nil},
		{"other options", "srun --error=/logs/stderr.out --output-format=json train", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := slurmSubmitOutputOptions(test.line)
			if !slices.Equal(got, test.want) {
				t.Fatalf("output = %v; want %v", got, test.want)
			}
		})
	}
}
