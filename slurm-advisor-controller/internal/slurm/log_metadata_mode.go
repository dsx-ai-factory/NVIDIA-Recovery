// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"context"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
)

// DetectLogMetadataMode probes the local Slurm client version once and sets
// only the enricher's query mode. Failure retains JSON log-path discovery.
func (s *slurmCluster) DetectLogMetadataMode(ctx context.Context) error {
	if s.logMetadata == nil {
		return nil
	}
	s.logMetadataInit.Do(func() {
		probeCtx, cancel := s.logMetadata.context(ctx)
		defer cancel()
		output, err := s.execute(probeCtx, s.roCommands, "sacct", "--version")
		if err != nil {
			s.logMetadataInitErr = errors.Wrap(err, "detect Slurm log metadata mode")
			return
		}

		version, err := parseSlurmVersion(output.stdout)
		if err != nil {
			s.logMetadataInitErr = errors.Wrap(err, "parse Slurm version")
			return
		}
		legacy := !hostSlurmSupportsExpandedJSON(version)
		s.logMetadata.legacyMode.Store(legacy)
		mode := "json"
		if legacy {
			mode = "submit_line"
		}
		s.logMetadata.logger.Info("selected Slurm log metadata mode",
			zap.String("slurm_version", version.raw), zap.String("mode", mode))
	})
	return s.logMetadataInitErr
}

// hostSlurmSupportsExpandedJSON prefers native step stdout from Slurm 25.05
// onward. A later major release qualifies regardless of its minor number.
func hostSlurmSupportsExpandedJSON(version slurmVersion) bool {
	return version.major > 25 || (version.major == 25 && version.minor >= 5)
}

type slurmVersion struct {
	major int
	minor int
	raw   string
}

// parseSlurmVersion extracts the release series while preserving the raw client
// version for logging. Maintenance and vendor suffixes do not affect the mode.
func parseSlurmVersion(sacctVersionOutput []byte) (slurmVersion, error) {
	fields := strings.Fields(string(sacctVersionOutput))
	if len(fields) != 2 || fields[0] != "slurm" {
		return slurmVersion{}, errors.New("unrecognized Slurm version output")
	}

	parts := strings.SplitN(fields[1], ".", 3)
	if len(parts) != 3 || len(parts[2]) == 0 || parts[2][0] < '0' || parts[2][0] > '9' {
		return slurmVersion{}, errors.New("invalid Slurm release version")
	}

	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(parts[1])
	if majorErr != nil || minorErr != nil || major <= 0 || minor < 0 {
		return slurmVersion{}, errors.New("invalid Slurm release version")
	}

	return slurmVersion{
		major: major,
		minor: minor,
		raw:   fields[1],
	}, nil
}
