// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"bytes"
	"encoding/json"

	"github.com/cockroachdb/errors"
)

type slurmStateList []string

func (states *slurmStateList) UnmarshalJSON(data []byte) error {
	*states = nil
	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("null")) {
		return nil
	}

	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*states = slurmStateList{single}
		return nil
	}

	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return errors.Wrap(err, "job state must be a string or array of strings")
	}
	if len(many) == 0 {
		return errors.New("job state array cannot be empty")
	}
	for index, state := range many {
		if state == "" {
			return errors.Newf("job state array element %d cannot be empty", index)
		}
	}
	*states = slurmStateList(many)
	return nil
}

func selectSlurmJobState(states slurmStateList, chooseFinal bool) string {
	if chooseFinal {
		for _, state := range states {
			if isFinalSlurmState(state) {
				return state
			}
		}
	}
	if len(states) == 0 {
		return ""
	}
	return states[0]
}

func isFinalSlurmState(state string) bool {
	switch state {
	case "BOOT_FAIL",
		"CANCELLED",
		"COMPLETED",
		"DEADLINE",
		"FAILED",
		"NODE_FAIL",
		"OUT_OF_MEMORY",
		"PREEMPTED",
		"STOPPED",
		"SUSPENDED",
		"TIMEOUT",
		"REQUEUED":
		return true
	default:
		return false
	}
}
