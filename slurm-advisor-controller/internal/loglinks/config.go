// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package loglinks

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	maxConfiguredPathLength = 1 << 20
	maxConfiguredCount      = 1_000_000
)

type patternTemplate struct {
	raw         string
	placeholder bool
	compiled    *regexp.Regexp
}

func normalizeConfig(config Config) (Config, []patternTemplate, error) {
	if err := validatePositiveDuration("scan interval", config.ScanInterval); err != nil {
		return Config{}, nil, err
	}
	if err := validatePositiveDuration("cleanup interval", config.CleanupInterval); err != nil {
		return Config{}, nil, err
	}
	if err := validatePositiveDuration("retention", config.Retention); err != nil {
		return Config{}, nil, err
	}
	if err := validatePositiveDuration("scan timeout", config.ScanTimeout); err != nil {
		return Config{}, nil, err
	}
	for name, value := range map[string]int{
		"maximum path length":           config.MaxPathLength,
		"maximum paths per sync":        config.MaxPathsPerSync,
		"maximum patterns":              config.MaxPatterns,
		"maximum scan directories":      config.MaxScanDirectories,
		"maximum entries per directory": config.MaxEntriesPerDirectory,
		"maximum matches per scan":      config.MaxMatchesPerScan,
		"maximum owned links":           config.MaxOwnedLinks,
	} {
		if value <= 0 {
			return Config{}, nil, errors.Newf("%s must be positive", name)
		}
		if value > maxConfiguredCount {
			return Config{}, nil, errors.Newf("%s exceeds the supported bound", name)
		}
	}
	if config.MaxPathLength > maxConfiguredPathLength {
		return Config{}, nil, errors.New("maximum path length exceeds the supported bound")
	}
	if len(config.SourceRoots) == 0 {
		return Config{}, nil, errors.New("at least one source root is required")
	}
	if len(config.SourceRoots) > config.MaxPathsPerSync {
		return Config{}, nil, errors.New("source root count exceeds the configured path bound")
	}
	if len(config.ApplicationPatterns) > config.MaxPatterns {
		return Config{}, nil, errors.New("application pattern count exceeds the configured bound")
	}

	destination, err := normalizeConfiguredAbsolutePath(
		"destination",
		config.Destination,
		config.MaxPathLength,
	)
	if err != nil {
		return Config{}, nil, err
	}
	if err := validateExistingDirectoryBoundary(destination); err != nil {
		return Config{}, nil, errors.Wrap(err, "validate destination")
	}
	config.Destination = destination

	roots := make([]string, 0, len(config.SourceRoots))
	seenRoots := make(map[string]struct{}, len(config.SourceRoots))
	for _, configuredRoot := range config.SourceRoots {
		root, err := normalizeConfiguredAbsolutePath(
			"source root",
			configuredRoot,
			config.MaxPathLength,
		)
		if err != nil {
			return Config{}, nil, err
		}
		if err := validateExistingDirectoryBoundary(root); err != nil {
			return Config{}, nil, errors.Wrap(err, "validate source root")
		}
		if pathWithin(destination, root) || pathWithin(root, destination) {
			return Config{}, nil, errors.New("source root and destination must not overlap")
		}
		if _, seen := seenRoots[root]; seen {
			continue
		}
		seenRoots[root] = struct{}{}
		roots = append(roots, root)
	}
	config.SourceRoots = roots

	patterns := make([]patternTemplate, 0, len(config.ApplicationPatterns))
	for _, pattern := range config.ApplicationPatterns {
		if pattern == "" {
			return Config{}, nil, errors.New("application pattern is empty")
		}
		if len(pattern) > config.MaxPathLength || strings.IndexByte(pattern, 0) >= 0 {
			return Config{}, nil, errors.New("application pattern is invalid")
		}
		hasPlaceholder := strings.Contains(pattern, "{job_id}")
		validationPattern := strings.ReplaceAll(pattern, "{job_id}", "0")
		compiled, err := regexp.Compile(validationPattern)
		if err != nil {
			return Config{}, nil, errors.Wrap(err, "compile application pattern")
		}
		patterns = append(patterns, patternTemplate{
			raw:         pattern,
			placeholder: hasPlaceholder,
			compiled:    compiled,
		})
	}
	return config, patterns, nil
}

func validatePositiveDuration(name string, value time.Duration) error {
	if value <= 0 {
		return errors.Newf("%s must be positive", name)
	}
	return nil
}

func normalizeAbsolutePath(name, value string, maxLength int) (string, error) {
	if value == "" {
		return "", errors.Newf("%s is empty", name)
	}
	if len(value) > maxLength {
		return "", errors.Newf("%s exceeds the configured path length", name)
	}
	if strings.IndexByte(value, 0) >= 0 {
		return "", errors.Newf("%s contains a NUL byte", name)
	}
	if !filepath.IsAbs(value) {
		return "", errors.Newf("%s must be absolute", name)
	}
	normalized := filepath.Clean(value)
	if len(normalized) > maxLength {
		return "", errors.Newf("%s exceeds the configured path length", name)
	}
	return normalized, nil
}

func normalizeConfiguredAbsolutePath(name, value string, maxLength int) (string, error) {
	normalized, err := normalizeAbsolutePath(name, value, maxLength)
	if err != nil {
		return "", err
	}
	if normalized != value {
		return "", errors.Newf("%s must already be normalized", name)
	}
	return normalized, nil
}

func validateExistingDirectoryBoundary(path string) error {
	components := pathComponents(path)
	for _, component := range components {
		info, err := os.Lstat(component)
		if err != nil {
			return errors.Wrap(err, "inspect directory boundary")
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("directory boundary contains a symbolic link")
		}
		if !info.IsDir() {
			return errors.New("directory boundary contains a non-directory")
		}
	}
	return nil
}

func pathComponents(path string) []string {
	components := make([]string, 0, 8)
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		components = append(components, current)
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	for left, right := 0, len(components)-1; left < right; left, right = left+1, right-1 {
		components[left], components[right] = components[right], components[left]
	}
	return components
}

func validateTargetPath(config Config, value string) (string, error) {
	target, err := normalizeConfiguredAbsolutePath("target", value, config.MaxPathLength)
	if err != nil {
		return "", err
	}
	name := filepath.Base(target)
	if name == "" || name == "." || name == ".." || isReservedName(name) {
		return "", errors.New("target basename is invalid")
	}
	if pathWithin(config.Destination, target) {
		return "", errors.New("target is inside the destination")
	}

	underSourceRoot := false
	for _, root := range config.SourceRoots {
		if pathWithin(root, target) {
			underSourceRoot = true
			break
		}
	}
	if !underSourceRoot {
		return "", errors.New("target is outside configured source roots")
	}
	if err := validateExistingTargetAncestors(target); err != nil {
		return "", err
	}
	return target, nil
}

func validateExistingTargetAncestors(target string) error {
	components := pathComponents(target)
	for index, component := range components {
		info, err := os.Lstat(component)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "inspect target boundary")
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("target boundary contains a symbolic link")
		}
		if index < len(components)-1 && !info.IsDir() {
			return errors.New("target boundary contains a non-directory")
		}
		if index == len(components)-1 && info.IsDir() {
			return errors.New("target names a directory")
		}
	}
	return nil
}

func pathWithin(root, target string) bool {
	relative, err := filepath.Rel(root, target)
	if err != nil {
		return false
	}
	return relative == "." ||
		(relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func isReservedName(name string) bool {
	return name == manifestFilename || strings.HasPrefix(name, internalPrefix)
}

func probeDestination(destination string) error {
	file, err := os.CreateTemp(destination, internalPrefix+"probe-*")
	if err != nil {
		return errors.Wrap(err, "create destination probe")
	}
	probeBase := file.Name()
	if closeErr := file.Close(); closeErr != nil {
		_ = os.Remove(probeBase)
		return errors.Wrap(closeErr, "close destination probe")
	}
	if err := os.Remove(probeBase); err != nil {
		return errors.Wrap(err, "prepare destination symlink probe")
	}

	linkPath := probeBase + ".link"
	renamedPath := probeBase + ".renamed"
	defer func() {
		_ = os.Remove(linkPath)
		_ = os.Remove(renamedPath)
	}()
	if err := os.Symlink(destination, linkPath); err != nil {
		return errors.Wrap(err, "create destination symlink probe")
	}
	if err := os.Rename(linkPath, renamedPath); err != nil {
		return errors.Wrap(err, "rename destination symlink probe")
	}
	info, err := os.Lstat(renamedPath)
	if err != nil {
		return errors.Wrap(err, "inspect destination symlink probe")
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return errors.New("destination symlink probe changed type")
	}
	target, err := os.Readlink(renamedPath)
	if err != nil {
		return errors.Wrap(err, "read destination symlink probe")
	}
	if target != destination {
		return errors.New("destination symlink probe changed target")
	}
	if err := syncDirectory(destination); err != nil {
		return errors.Wrap(err, "sync destination probe")
	}
	return nil
}

func syncDirectory(path string) (returnErr error) {
	directory, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, "open directory")
	}
	defer func() {
		returnErr = errors.CombineErrors(
			returnErr,
			errors.Wrap(directory.Close(), "close directory"),
		)
	}()
	if err := directory.Sync(); err != nil {
		return errors.Wrap(err, "fsync directory")
	}
	return nil
}
