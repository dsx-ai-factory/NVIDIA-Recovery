// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package loglinks

import (
	"bytes"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
)

const manifestVersion = 1

type linkOrigin string

const (
	linkOriginPrimary     linkOrigin = "primary"
	linkOriginApplication linkOrigin = "application"
)

type manifestFile struct {
	Version int              `json:"version"`
	Links   []manifestRecord `json:"links"`
}

type manifestRecord struct {
	Name         string `json:"name"`
	Target       string `json:"target"`
	JobID        string `json:"job_id"`
	AttemptIndex int32  `json:"attempt_index"`
	// An empty origin is accepted only for conservative recovery of manifests
	// written before origin tracking was introduced. It behaves like a primary
	// record until the exact target is observed again.
	Origin         linkOrigin `json:"origin,omitempty"`
	LastObservedAt time.Time  `json:"last_observed_at"`
}

func validateManifestPath(config Config) error {
	path := filepath.Join(config.Destination, manifestFilename)
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "inspect log link manifest")
	}
	if !info.Mode().IsRegular() {
		return errors.New("log link manifest path is not a regular file")
	}
	return nil
}

func recoverManifest(config Config) (map[string]manifestRecord, RecoveryReport) {
	owned := make(map[string]manifestRecord)
	path := filepath.Join(config.Destination, manifestFilename)
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return owned, RecoveryReport{}
	}
	if err != nil || !info.Mode().IsRegular() {
		return owned, RecoveryReport{ManifestError: true}
	}
	if info.Size() < 0 || info.Size() > manifestSizeLimit(config) {
		return owned, RecoveryReport{ManifestError: true}
	}

	file, err := os.Open(path)
	if err != nil {
		return owned, RecoveryReport{ManifestError: true}
	}
	defer func() { _ = file.Close() }()

	decoder := json.NewDecoder(io.LimitReader(file, manifestSizeLimit(config)+1))
	decoder.DisallowUnknownFields()
	decoded, report, err := decodeManifest(decoder, config)
	if err != nil {
		return owned, RecoveryReport{ManifestError: true}
	}
	return decoded, report
}

func decodeManifest(
	decoder *json.Decoder,
	config Config,
) (map[string]manifestRecord, RecoveryReport, error) {
	owned := make(map[string]manifestRecord)
	report := RecoveryReport{}
	opening, err := decoder.Token()
	if err != nil {
		return nil, report, errors.Wrap(err, "read manifest object")
	}
	if !isJSONDelimiter(opening, '{') {
		return nil, report, errors.New("manifest root is not an object")
	}

	var (
		versionSeen bool
		linksSeen   bool
		version     int
	)
	for decoder.More() {
		fieldToken, err := decoder.Token()
		if err != nil {
			return nil, report, errors.Wrap(err, "read manifest field")
		}
		field, ok := fieldToken.(string)
		if !ok {
			return nil, report, errors.New("manifest field name is not a string")
		}
		switch field {
		case "version":
			if versionSeen {
				return nil, report, errors.New("manifest version field is duplicated")
			}
			versionSeen = true
			if err := decoder.Decode(&version); err != nil {
				return nil, report, errors.Wrap(err, "decode manifest version")
			}
		case "links":
			if linksSeen {
				return nil, report, errors.New("manifest links field is duplicated")
			}
			linksSeen = true
			if err := decodeManifestLinks(decoder, config, owned, &report); err != nil {
				return nil, report, err
			}
		default:
			return nil, report, errors.New("manifest contains an unknown field")
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return nil, report, errors.Wrap(err, "close manifest object")
	}
	if !isJSONDelimiter(closing, '}') {
		return nil, report, errors.New("manifest object is not closed")
	}
	if !versionSeen || !linksSeen {
		return nil, report, errors.New("manifest is missing a required field")
	}
	if version != manifestVersion {
		return nil, report, errors.New("manifest version is unsupported")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, report, err
	}
	report.Owned = len(owned)
	return owned, report, nil
}

func decodeManifestLinks(
	decoder *json.Decoder,
	config Config,
	owned map[string]manifestRecord,
	report *RecoveryReport,
) error {
	opening, err := decoder.Token()
	if err != nil {
		return errors.Wrap(err, "read manifest links")
	}
	if !isJSONDelimiter(opening, '[') {
		return errors.New("manifest links is not an array")
	}

	duplicateNames := make(map[string]struct{}, min(config.MaxOwnedLinks, 1024))
	entryCount := 0
	for decoder.More() {
		var record manifestRecord
		if err := decoder.Decode(&record); err != nil {
			return errors.Wrap(err, "decode manifest link")
		}
		entryCount = saturatingIncrement(entryCount)
		if entryCount > config.MaxOwnedLinks {
			report.InvalidEntries = saturatingIncrement(report.InvalidEntries)
			continue
		}
		if err := validateManifestRecord(config, record); err != nil {
			report.InvalidEntries = saturatingIncrement(report.InvalidEntries)
			continue
		}
		if _, duplicate := duplicateNames[record.Name]; duplicate {
			if _, previouslyOwned := owned[record.Name]; previouslyOwned {
				delete(owned, record.Name)
				report.InvalidEntries = saturatingIncrement(report.InvalidEntries)
			}
			report.InvalidEntries = saturatingIncrement(report.InvalidEntries)
			continue
		}
		duplicateNames[record.Name] = struct{}{}
		owned[record.Name] = record
	}
	closing, err := decoder.Token()
	if err != nil {
		return errors.Wrap(err, "close manifest links")
	}
	if !isJSONDelimiter(closing, ']') {
		return errors.New("manifest links array is not closed")
	}
	return nil
}

func isJSONDelimiter(token json.Token, want rune) bool {
	delimiter, ok := token.(json.Delim)
	return ok && rune(delimiter) == want
}

func validateManifestRecord(config Config, record manifestRecord) error {
	if record.Name == "" || filepath.Base(record.Name) != record.Name || isReservedName(record.Name) {
		return errors.New("manifest entry name is invalid")
	}
	if len(record.Name) > config.MaxPathLength {
		return errors.New("manifest entry name is too long")
	}
	target, err := validateTargetPath(config, record.Target)
	if err != nil {
		return errors.Wrap(err, "validate manifest target")
	}
	if target != record.Target || filepath.Base(target) != record.Name {
		return errors.New("manifest entry target does not match its name")
	}
	if record.JobID == "" || len(record.JobID) > config.MaxPathLength || record.AttemptIndex < 0 {
		return errors.New("manifest entry job identity is invalid")
	}
	switch record.Origin {
	case "", linkOriginPrimary, linkOriginApplication:
	default:
		return errors.New("manifest entry origin is invalid")
	}
	if record.LastObservedAt.IsZero() {
		return errors.New("manifest entry observation time is empty")
	}

	linkPath := filepath.Join(config.Destination, record.Name)
	info, err := os.Lstat(linkPath)
	if err != nil {
		return errors.Wrap(err, "inspect manifest-owned link")
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return errors.New("manifest-owned entry is not a symbolic link")
	}
	linkTarget, err := os.Readlink(linkPath)
	if err != nil {
		return errors.Wrap(err, "read manifest-owned link")
	}
	if linkTarget != record.Target {
		return errors.New("manifest-owned link target does not match")
	}
	return nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("manifest contains trailing JSON")
		}
		return errors.Wrap(err, "read manifest trailer")
	}
	return nil
}

// writeManifest reports whether the atomic rename completed. A following
// directory-fsync error is still surfaced, but the caller must retain the
// renamed state rather than rolling back its in-memory ownership.
func writeManifest(config Config, records map[string]manifestRecord) (bool, error) {
	names := make([]string, 0, len(records))
	for name := range records {
		names = append(names, name)
	}
	sort.Strings(names)
	manifest := manifestFile{
		Version: manifestVersion,
		Links:   make([]manifestRecord, 0, len(names)),
	}
	for _, name := range names {
		manifest.Links = append(manifest.Links, records[name])
	}
	contents, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return false, errors.Wrap(err, "encode log link manifest")
	}
	contents = append(contents, '\n')
	if int64(len(contents)) > manifestSizeLimit(config) {
		return false, errors.New("encoded log link manifest exceeds size bound")
	}

	temporary, err := os.CreateTemp(config.Destination, internalPrefix+"manifest-*")
	if err != nil {
		return false, errors.Wrap(err, "create temporary log link manifest")
	}
	temporaryPath := temporary.Name()
	keepTemporary := true
	defer func() {
		_ = temporary.Close()
		if keepTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return false, errors.Wrap(err, "set temporary manifest permissions")
	}
	if _, err := io.Copy(temporary, bytes.NewReader(contents)); err != nil {
		return false, errors.Wrap(err, "write temporary log link manifest")
	}
	if err := temporary.Sync(); err != nil {
		return false, errors.Wrap(err, "fsync temporary log link manifest")
	}
	if err := temporary.Close(); err != nil {
		return false, errors.Wrap(err, "close temporary log link manifest")
	}

	manifestPath := filepath.Join(config.Destination, manifestFilename)
	if info, err := os.Lstat(manifestPath); err == nil && !info.Mode().IsRegular() {
		return false, errors.New("log link manifest path is not a regular file")
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, errors.Wrap(err, "inspect existing log link manifest")
	}
	if err := os.Rename(temporaryPath, manifestPath); err != nil {
		return false, errors.Wrap(err, "atomically replace log link manifest")
	}
	keepTemporary = false
	if err := syncDirectory(config.Destination); err != nil {
		return true, errors.Wrap(err, "fsync log link manifest directory")
	}
	return true, nil
}

func manifestSizeLimit(config Config) int64 {
	const (
		minimum = int64(1 << 20)
		maximum = int64(64 << 20)
	)
	perRecord := int64(config.MaxPathLength)*2 + 1024
	if perRecord <= 0 {
		return maximum
	}
	if int64(config.MaxOwnedLinks) > maximum/perRecord {
		return maximum
	}
	size := int64(config.MaxOwnedLinks)*perRecord + 1024
	if size < minimum {
		return minimum
	}
	if size > maximum {
		return maximum
	}
	return size
}
