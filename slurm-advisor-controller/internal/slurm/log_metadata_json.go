// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

const sacctLogMetadataJSONVersion = "v0.0.43"

// queryJSONLogPaths fetches accounting paths for all attempts of one job ID.
// Capture separated command streams once per response, before its paths are
// distributed across attempt caches.
func (s *slurmCluster) queryJSONLogPaths(
	ctx context.Context,
	jobID string,
	source string,
	runner logMetadataCommandRunner,
) (map[cluster.JobAttemptRef]sacctAttemptLogPaths, error) {
	args := []string{"-j", jobID, "--duplicates", "--json=" + sacctLogMetadataJSONVersion}
	output, err := runner(ctx, "sacct", args...)
	if err != nil {
		return nil, err
	}
	discovered, err := logPathsFromSacct(output.stdout)
	s.logSacctOutput(jobID, source, args, output, err)
	return discovered, err
}

// logPathsFromSacct extracts expanded paths using the same allocation
// ordering as status accounting, keeping parent and step completeness separate.
func logPathsFromSacct(data []byte) (map[cluster.JobAttemptRef]sacctAttemptLogPaths, error) {
	parsed, err := parseSacctOutput(data)
	if err != nil {
		return nil, err
	}

	grouped := sacctJobsByIdentity(parsed.Jobs)
	discovered := make(map[cluster.JobAttemptRef]sacctAttemptLogPaths)

	for jobID, jobs := range grouped {
		for attemptIndex, attempt := range groupSacctAttempts(jobs) {
			missingParent, missingSteps := false, false
			var rawParent []string
			var rawSteps []string
			for _, allocation := range attempt {
				if allocation.StdoutExpanded != "" {
					rawParent = append(rawParent, allocation.StdoutExpanded)
				} else {
					missingParent = true
				}

				for _, step := range allocation.Steps {
					if !numericStepID(step.Step.ID.StepID) {
						continue
					}
					if step.Step.StdoutExpanded == "" {
						missingSteps = true
						continue
					}
					rawSteps = append(rawSteps, step.Step.StdoutExpanded)
				}
			}

			parent, invalidParentCount := normalizeDiscoveredPaths(rawParent)
			steps, invalidStepCount := normalizeDiscoveredPaths(rawSteps)

			discovered[cluster.JobAttemptRef{
				JobID:        jobID,
				AttemptIndex: int32(attemptIndex),
			}] = sacctAttemptLogPaths{
				parent:        parent,
				steps:         steps,
				missingParent: missingParent,
				missingSteps:  missingSteps,
				invalidParent: invalidParentCount,
				invalidSteps:  invalidStepCount,
			}
		}
	}
	return discovered, nil
}

// sacctJobsByIdentity preserves accounting row order within each canonical job
// ID, which groupSacctAttempts uses to assign attempt indices.
func sacctJobsByIdentity(jobs []SacctJob) map[string][]SacctJob {
	grouped := make(map[string][]SacctJob)
	for _, job := range jobs {
		jobID, _, _, include := jobIdentityFromSacct(job)
		if include {
			grouped[jobID] = append(grouped[jobID], job)
		}
	}
	return grouped
}

// UnmarshalJSON accepts both compound and structured Slurm step IDs.
func (step *SacctStep) UnmarshalJSON(data []byte) error {
	var decoded struct {
		Step struct {
			ID             json.RawMessage `json:"id"`
			Name           string          `json:"name"`
			StdoutExpanded string          `json:"stdout_expanded"`
		} `json:"step"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}

	step.Step.Name = decoded.Step.Name
	step.Step.StdoutExpanded = decoded.Step.StdoutExpanded
	if len(decoded.Step.ID) == 0 || string(decoded.Step.ID) == "null" {
		return nil
	}

	var compoundID string
	if err := json.Unmarshal(decoded.Step.ID, &compoundID); err == nil {
		step.Step.ID.StepID = compoundID
		if separator := strings.LastIndexByte(compoundID, '.'); separator >= 0 {
			step.Step.ID.StepID = compoundID[separator+1:]
			if jobID, err := strconv.Atoi(compoundID[:separator]); err == nil {
				step.Step.ID.JobID = jobID
			}
		}
		return nil
	}

	var legacy struct {
		JobID  int             `json:"job_id"`
		StepID json.RawMessage `json:"step_id"`
	}
	if err := json.Unmarshal(decoded.Step.ID, &legacy); err != nil {
		return errors.Wrap(err, "decode Slurm step ID")
	}
	step.Step.ID.JobID = legacy.JobID
	if len(legacy.StepID) == 0 || string(legacy.StepID) == "null" {
		return nil
	}
	if err := json.Unmarshal(legacy.StepID, &step.Step.ID.StepID); err == nil {
		return nil
	}
	var numeric json.Number
	if err := json.Unmarshal(legacy.StepID, &numeric); err != nil {
		return errors.Wrap(err, "decode Slurm step ID")
	}
	step.Step.ID.StepID = numeric.String()
	return nil
}
