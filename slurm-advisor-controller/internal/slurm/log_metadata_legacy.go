// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package slurm

import (
	"bufio"
	"bytes"
	"context"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

const (
	maxSacctSubmitOutputBytes = 8 << 20
	maxSacctSubmitLineBytes   = 128 << 10
)

const legacySacctLogFormat = "JobIDRaw,DBIndex,Submit,Start,WorkDir,JobID,JobName,SubmitLine"

type sacctSubmitRow struct {
	jobID      int
	stepID     string
	dbIndex    uint64
	submission int
	start      int
	workDir    string
	submitLine string
}

type legacySacctRow struct {
	sacctSubmitRow
	canonicalID string
	jobName     string
}

// queryLegacyLogPaths needs one text accounting response for all requested
// attempts. JobID supplies array identity, and JobName supplies %x expansion;
// --array avoids consolidated task ranges that cannot identify one attempt.
func queryLegacyLogPaths(
	ctx context.Context,
	jobID string,
	candidates []logRefreshCandidate,
	runner logMetadataCommandRunner,
) (map[cluster.JobAttemptRef]sacctAttemptLogPaths, error) {
	output, err := runner(ctx, "env", "SLURM_TIME_FORMAT=%s",
		"sacct", "--noheader", "--parsable2", "--duplicates", "--array", "-j", jobID,
		"-o", legacySacctLogFormat)
	if err != nil {
		return nil, errors.Wrap(err, "query legacy sacct log metadata")
	}
	return logPathsFromLegacySacct(output.stdout, candidates)
}

// logPathsFromLegacySacct validates the entire response's allocation identities
// before expanding requested sources. Allocation row order and timestamp
// grouping match the existing JSON attempt algorithm; step rows join by DBIndex
// even if they precede their allocation. Output errors remain source-local.
func logPathsFromLegacySacct(
	data []byte,
	candidates []logRefreshCandidate,
) (map[cluster.JobAttemptRef]sacctAttemptLogPaths, error) {
	rows, err := parseLegacySacctRows(data)
	if err != nil {
		return nil, err
	}
	allocations := make(map[string]legacySacctRow)
	grouped := make(map[string][]SacctJob)
	steps := make(map[string][]legacySacctRow)
	seenSteps := make(map[string]legacySacctRow)
	for _, row := range rows {
		key := sacctSubmitStepKey(row.jobID, row.dbIndex, row.stepID)
		if row.stepID != "" {
			if previous, ok := seenSteps[key]; ok {
				if previous != row {
					return nil, errors.New("conflicting legacy sacct step rows")
				}
				continue
			}
			seenSteps[key] = row
			allocationKey := sacctSubmitStepKey(row.jobID, row.dbIndex, "")
			steps[allocationKey] = append(steps[allocationKey], row)
			continue
		}
		if previous, ok := allocations[key]; ok {
			if previous != row {
				return nil, errors.New("ambiguous legacy sacct allocation identity")
			}
			continue
		}
		allocation, err := legacySacctAllocation(row)
		if err != nil {
			return nil, err
		}
		allocations[key] = row
		grouped[row.canonicalID] = append(grouped[row.canonicalID], allocation)
	}
	for key, stepRows := range steps {
		allocation, ok := allocations[key]
		if !ok {
			return nil, errors.New("legacy sacct step has no allocation")
		}
		for _, row := range stepRows {
			if row.canonicalID != allocation.canonicalID {
				return nil, errors.New("legacy sacct step has conflicting array identity")
			}
		}
	}

	results := make(map[cluster.JobAttemptRef]sacctAttemptLogPaths, len(candidates))
	requested := make(map[cluster.JobAttemptRef]logRefreshCandidate, len(candidates))
	for _, candidate := range candidates {
		requested[candidate.ref] = candidate
		results[candidate.ref] = sacctAttemptLogPaths{
			missingParent: candidate.parentDue, missingSteps: candidate.stepsDue,
		}
	}
	var outputErrors []error
	for jobID, jobs := range grouped {
		for attemptIndex, attempt := range groupSacctAttempts(jobs) {
			ref := cluster.JobAttemptRef{JobID: jobID, AttemptIndex: int32(attemptIndex)}
			candidate, wanted := requested[ref]
			if !wanted {
				continue
			}
			var result sacctAttemptLogPaths
			for _, allocation := range attempt {
				key := sacctSubmitStepKey(allocation.JobID, allocation.databaseIndex, "")
				row := allocations[key]
				if candidate.parentDue {
					output, err := sacctSubmitLogPaths(row.submitLine, "sbatch", row.workDir, allocation, "")
					result.parent = append(result.parent, output...)
					if err != nil {
						result.missingParent = true
						outputErrors = append(outputErrors, errors.Wrap(err, "parse legacy parent stdout"))
					}
				}
				if !candidate.stepsDue {
					continue
				}
				for _, step := range steps[key] {
					output, err := sacctSubmitLogPaths(step.submitLine, "srun", row.workDir, allocation, step.stepID)
					result.steps = append(result.steps, output...)
					if err != nil {
						result.missingSteps = true
						outputErrors = append(outputErrors, errors.Wrap(err, "parse legacy step stdout"))
					}
				}
			}
			result.parent, result.invalidParent = normalizeDiscoveredPaths(result.parent)
			result.steps, result.invalidSteps = normalizeDiscoveredPaths(result.steps)
			results[ref] = result
			delete(requested, ref)
		}
	}
	if len(requested) > 0 {
		outputErrors = append(outputErrors, errors.New("legacy sacct is missing a requested attempt"))
	}
	return results, errors.Join(outputErrors...)
}

// parseLegacySacctRows leaves SubmitLine last so pipes and embedded newlines in
// application arguments remain intact. The common parser supplies identical
// timestamp and ID checks; the scanner bounds each complete accounting record.
func parseLegacySacctRows(data []byte) ([]legacySacctRow, error) {
	if len(data) > maxSacctSubmitOutputBytes || !utf8.Valid(data) || bytes.ContainsRune(data, 0) {
		return nil, errors.New("invalid or oversized legacy sacct output")
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), maxSacctSubmitLineBytes)
	scanner.Split(scanLegacySacctRecord)
	var rows []legacySacctRow
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == "" {
			continue
		}
		fields := strings.SplitN(scanner.Text(), "|", 8)
		if len(fields) != 8 {
			return nil, errors.New("malformed legacy sacct row")
		}
		common := []string{fields[0], fields[1], fields[2], fields[3], fields[4], fields[7]}
		row, include, err := parseSacctSubmitRow(common)
		if err != nil {
			return nil, err
		}
		if !include {
			continue
		}
		canonicalID, formattedStepID, isStep := strings.Cut(strings.TrimSpace(fields[5]), ".")
		if row.stepID != formattedStepID || isStep != (row.stepID != "") {
			return nil, errors.New("legacy sacct step identifiers disagree")
		}
		rows = append(rows, legacySacctRow{row, canonicalID, fields[6]})
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "read legacy sacct rows")
	}
	return rows, nil
}

// scanLegacySacctRecord splits at accounting headers rather than every newline:
// Slurm prints embedded argument newlines verbatim in the final SubmitLine field.
func scanLegacySacctRecord(data []byte, atEOF bool) (int, []byte, error) {
	for start := 0; start < len(data); {
		newline := bytes.IndexByte(data[start:], '\n')
		if newline < 0 {
			break
		}
		next := start + newline + 1
		if isLegacySacctRecordStart(data[next:]) {
			return next, bytes.TrimSuffix(data[:next-1], []byte{'\r'}), nil
		}
		start = next
	}
	if atEOF && len(data) > 0 {
		return len(data), bytes.TrimSuffix(bytes.TrimSuffix(data, []byte{'\n'}), []byte{'\r'}), nil
	}
	return 0, nil, nil
}

// isLegacySacctRecordStart recognizes JobIDRaw followed by the field delimiter.
// Other metadata is checked by the row parser, so malformed headers still fail.
func isLegacySacctRecordStart(data []byte) bool {
	line, _, _ := bytes.Cut(data, []byte{'\n'})
	rawID, _, found := bytes.Cut(line, []byte{'|'})
	if !found {
		return false
	}
	jobID, _, _ := strings.Cut(strings.TrimSpace(string(rawID)), ".")
	number, err := strconv.Atoi(jobID)
	return err == nil && number > 0
}

// legacySacctAllocation derives the same canonical array identity and expansion
// values as accounting JSON. Unknown allocation times cannot safely identify an
// attempt and are left for a later refresh rather than guessed.
func legacySacctAllocation(row legacySacctRow) (SacctJob, error) {
	allocation := SacctJob{JobID: row.jobID, Name: row.jobName, databaseIndex: row.dbIndex}
	allocation.Time.Submission, allocation.Time.Start = row.submission, row.start
	if row.submission == 0 && row.start == 0 {
		return SacctJob{}, errors.New("legacy sacct allocation has no identity timestamps")
	}
	arrayID, taskID, array := strings.Cut(row.canonicalID, "_")
	if array {
		var err error
		allocation.Array.JobID, err = strconv.Atoi(arrayID)
		if err != nil || allocation.Array.JobID <= 0 {
			return SacctJob{}, errors.New("invalid legacy sacct array job ID")
		}
		task, err := strconv.Atoi(taskID)
		if err != nil || task < 0 || uint64(task) >= uint64(^uint32(0)) {
			return SacctJob{}, errors.New("invalid legacy sacct array task ID")
		}
		allocation.Array.TaskID = numberField{Set: true, Number: task}
	} else if row.canonicalID != strconv.Itoa(row.jobID) {
		return SacctJob{}, errors.New("legacy sacct allocation identifiers disagree")
	}
	return allocation, nil
}

func sacctSubmitStepKey(jobID int, dbIndex uint64, stepID string) string {
	return strconv.Itoa(jobID) + ":" + strconv.FormatUint(dbIndex, 10) + ":" + stepID
}

// parseSacctSubmitRow validates the identity columns of a legacy accounting
// row. Synthetic steps are excluded before inspecting their empty fields.
func parseSacctSubmitRow(fields []string) (sacctSubmitRow, bool, error) {
	rawID, stepID, isStep := strings.Cut(strings.TrimSpace(fields[0]), ".")
	if isStep && !numericStepID(stepID) {
		return sacctSubmitRow{}, false, nil
	}
	jobID, err := strconv.Atoi(rawID)
	if err != nil || jobID <= 0 {
		return sacctSubmitRow{}, false, errors.New("invalid sacct raw job ID")
	}
	dbIndex, err := strconv.ParseUint(strings.TrimSpace(fields[1]), 10, 64)
	if err != nil || dbIndex == 0 {
		return sacctSubmitRow{}, false, errors.New("invalid sacct DBIndex")
	}
	row := sacctSubmitRow{jobID: jobID, stepID: stepID, dbIndex: dbIndex,
		workDir: fields[4], submitLine: fields[5]}
	if !isStep {
		row.submission, err = sacctSubmitTimestamp(fields[2])
		if err != nil {
			return sacctSubmitRow{}, false, err
		}
		row.start, err = sacctSubmitTimestamp(fields[3])
		if err != nil {
			return sacctSubmitRow{}, false, err
		}
	}
	return row, true, nil
}

func sacctSubmitTimestamp(value string) (int, error) {
	value = strings.TrimSpace(value)
	// Slurm emits missing-time sentinels even with SLURM_TIME_FORMAT=%s.
	if value == "" || value == "Unknown" || value == "None" {
		return 0, nil
	}
	stamp, err := strconv.Atoi(value)
	if err != nil || stamp < 0 {
		return 0, errors.New("legacy sacct requires Unix timestamps")
	}
	return stamp, nil
}

// sacctSubmitLogPaths expands all output candidates found by the whole-line
// scan. An unresolved candidate leaves the source incomplete without discarding
// other usable paths from the same SubmitLine.
func sacctSubmitLogPaths(submitLine, command, workDir string, allocation SacctJob, stepID string) ([]string, error) {
	var output []string
	var errs []error
	for _, pattern := range slurmSubmitOutputOptions(submitLine) {
		value, err := expandSacctLogPath(pattern, command, workDir, allocation, stepID)
		if err != nil {
			errs = append(errs, err)
		} else if value != "" {
			output = append(output, value)
		}
	}
	return output, errors.Join(errs...)
}

// expandSacctLogPath applies stdout inheritance, supported filename patterns,
// and the recorded allocation directory to one discovered output candidate.
func expandSacctLogPath(pattern, command, workDir string, allocation SacctJob, stepID string) (string, error) {
	// Match srun's inherited-stdout prefix checks to avoid publishing files it
	// never creates. sbatch treats these values as filenames.
	if command == "srun" && (strings.HasPrefix(pattern, "-") || strings.EqualFold(pattern, "none") ||
		strings.HasPrefix(strings.ToLower(pattern), "all") || numericStepID(pattern)) {
		return "", nil
	}
	output, err := expandSacctOutputPattern(pattern, allocation, stepID)
	if err != nil {
		return "", err
	}
	if !path.IsAbs(output) {
		// srun opens ordinary stdout in the launcher's directory; --chdir
		// changes only the remote tasks. WorkDir already includes sbatch --chdir.
		if !path.IsAbs(workDir) {
			return "", errors.New("relative sacct log path has no absolute working directory")
		}
		output = path.Join(workDir, output)
	}
	return output, nil
}

// expandSacctOutputPattern resolves values supplied by accounting. Patterns
// needing task or node identity cannot be turned into a single file here.
func expandSacctOutputPattern(pattern string, allocation SacctJob, stepID string) (string, error) {
	if strings.Contains(pattern, "\\") {
		return "", errors.New("sacct output pattern contains unresolved escaping")
	}
	var output strings.Builder
	for index := 0; index < len(pattern); index++ {
		if pattern[index] != '%' {
			output.WriteByte(pattern[index])
			continue
		}
		index++
		widthStart := index
		width := 0
		for index < len(pattern) && pattern[index] >= '0' && pattern[index] <= '9' {
			width = min(10, width*10+int(pattern[index]-'0'))
			index++
		}
		if index == len(pattern) {
			return "", errors.New("incomplete sacct output pattern")
		}
		value := ""
		switch pattern[index] {
		case '%':
			if index != widthStart {
				return "", errors.New("invalid padding in sacct output pattern")
			}
			// Slurm defers %%t/%%n/%%N/%%u to task-level expansion.
			deferred := index + 1
			for deferred < len(pattern) && pattern[deferred] >= '0' && pattern[deferred] <= '9' {
				deferred++
			}
			if deferred < len(pattern) && strings.ContainsRune("tnNu", rune(pattern[deferred])) {
				return "", errors.New("sacct output pattern needs task-level expansion")
			}
			value = "%"
		case 'j':
			value = strconv.Itoa(allocation.JobID)
		case 'J':
			if stepID == "" {
				return "", errors.New("step ID required for sacct output pattern")
			}
			value = strconv.Itoa(allocation.JobID)
			value = strings.Repeat("0", max(0, width-len(value))) + value + "." + stepID
		case 's':
			value = stepID
		case 'x':
			value = allocation.Name
		case 'A':
			value = strconv.Itoa(allocation.JobID)
			if allocation.Array.JobID > 0 {
				value = strconv.Itoa(allocation.Array.JobID)
			}
		case 'a':
			value = "4294967294" // Slurm's non-array task sentinel.
			if allocation.Array.JobID > 0 && allocation.Array.TaskID.Set {
				value = strconv.Itoa(allocation.Array.TaskID.Number)
			}
		default:
			return "", errors.New("sacct output pattern needs unavailable task or node identity")
		}
		if value == "" {
			return "", errors.New("sacct output pattern needs missing accounting identity")
		}
		if strings.ContainsRune("jsaA", rune(pattern[index])) {
			value = strings.Repeat("0", max(0, width-len(value))) + value
		}
		output.WriteString(value)
	}
	return output.String(), nil
}
