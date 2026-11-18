// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package loglinks

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
)

var (
	collectorJobID = regexp.MustCompile(`^[0-9]+(?:_[0-9]+)?$`)
	unsafePrefix   = regexp.MustCompile(`[^A-Za-z0-9-]+`)
	// apsTimestamp is the YY-MM-DD_time_HH-MM-SS part of an APS filename. Anything
	// may follow it, such as an NVRx cycle suffix. It must be present: a name that
	// stops matching the convention after "_date_" is not an APS name, and the
	// collector would take a later numeric suffix as its job ID.
	apsTimestamp = regexp.MustCompile(`^\d{2}-\d{2}-\d{2}_time_\d{2}-\d{2}-\d{2}`)
)

// Name plans a primary link's basename without accessing the filesystem. The
// same plan is used for publication and asynchronous materialization, so the
// observed collector filename matches the job's published path. Callers remain
// responsible for source-root and filesystem validation before creating a link.
// With renaming disabled it retains the existing basename convention. Otherwise
// existing APS names are preserved and other paths require exact attempt identity.
func Name(target Target, path string, renameEnabled bool) (name string, renamed bool, err error) {
	if !renameEnabled {
		return filepath.Base(path), false, nil
	}
	if !utf8.ValidString(path) || strings.IndexByte(path, 0) >= 0 ||
		!filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", false, errors.New("log link source path is invalid")
	}
	if !collectorJobID.MatchString(target.JobID) || target.AttemptIndex < 0 {
		return "", false, errors.New("log link job attempt identity is invalid")
	}
	base := filepath.Base(path)
	if base == "." || base == string(filepath.Separator) || isReservedName(base) {
		return "", false, errors.New("log link source basename is invalid")
	}
	if target.JobName != "" {
		for _, jobID := range []string{target.JobID, target.ArrayJobID} {
			apsPrefix := target.JobName + "_" + jobID + "_date_"
			if jobID != "" && strings.HasPrefix(base, apsPrefix) && apsTimestamp.MatchString(base[len(apsPrefix):]) {
				return base, false, nil
			}
		}
	}
	if target.SubmitTime.IsZero() || target.SubmitTime.Unix() <= 0 {
		return "", false, errors.New("generated log link requires a submission time")
	}
	stem := strings.TrimSuffix(base, filepath.Ext(base))
	prefix := strings.Trim(unsafePrefix.ReplaceAllString(stem, "-"), "-")
	if len(prefix) > 64 {
		prefix = strings.TrimRight(prefix[:64], "-")
	}
	if prefix == "" {
		prefix = "log"
	}
	digest := sha256.Sum256([]byte(path))
	stamp := target.SubmitTime.UTC().Format("06-01-02_time_15-04-05")
	name = fmt.Sprintf("%s-%x-a%d_%s_date_%s.log", prefix, digest[:8], target.AttemptIndex, target.JobID, stamp)
	if len(name) > 255 {
		return "", false, errors.New("generated log link name exceeds filesystem limit")
	}
	return name, true, nil
}
