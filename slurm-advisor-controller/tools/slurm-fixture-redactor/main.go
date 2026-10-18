// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("slurm-fixture-redactor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	inputPath := fs.String("input", "", "raw Slurm JSON input path, or stdin when empty")
	outputPath := fs.String("output", "", "redacted JSON output path, or stdout when empty")
	if err := fs.Parse(args); err != nil {
		return err
	}

	input, err := readInput(*inputPath)
	if err != nil {
		return err
	}
	redacted, err := RedactJSON(input)
	if err != nil {
		return err
	}
	return writeOutput(*outputPath, redacted)
}

func readInput(path string) ([]byte, error) {
	if path == "" {
		return io.ReadAll(os.Stdin)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrapf(err, "read input %q", path)
	}
	return data, nil
}

func writeOutput(path string, data []byte) error {
	if path == "" {
		_, err := os.Stdout.Write(data)
		return err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return errors.Wrapf(err, "write output %q", path)
	}
	return nil
}

func RedactJSON(input []byte) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, errors.Wrap(err, "decode Slurm JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("decode Slurm JSON: trailing JSON values")
	}

	redacted := redactValue(value, nil)
	output, err := json.MarshalIndent(redacted, "", "  ")
	if err != nil {
		return nil, errors.Wrap(err, "encode redacted Slurm JSON")
	}
	return append(output, '\n'), nil
}

func redactValue(value any, path []string) any {
	switch typed := value.(type) {
	case map[string]any:
		redacted := make(map[string]any, len(typed))
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			redacted[key] = redactValueForKey(key, typed[key], append(path, key))
		}
		return redacted
	case []any:
		redacted := make([]any, len(typed))
		for i, item := range typed {
			redacted[i] = redactValue(item, path)
		}
		return redacted
	default:
		return typed
	}
}

func redactValueForKey(key string, value any, path []string) any {
	switch typed := value.(type) {
	case string:
		return redactStringForKey(key, typed, path)
	default:
		return redactValue(typed, path)
	}
}

func redactStringForKey(key string, value string, path []string) string {
	if decoded, ok := decodeJSONString(value); ok {
		redacted := redactValue(decoded, append(path, "json"))
		encoded, err := json.Marshal(redacted)
		if err == nil {
			return string(encoded)
		}
	}

	normalized := normalizeKey(key)
	switch {
	case isNodeKey(normalized):
		return redactNodeList(value)
	case isUserKey(normalized):
		return "user-redacted"
	case isAccountKey(normalized):
		return "account-redacted"
	case strings.Contains(normalized, "cluster"):
		return "cluster-redacted"
	case strings.Contains(normalized, "partition"):
		return "partition-redacted"
	case strings.Contains(normalized, "reservation") || normalized == "resv_name":
		return "reservation-redacted"
	case isWorkflowKey(normalized):
		return "workflow-redacted"
	case isPathKey(normalized):
		return "/redacted/path"
	case normalized == "command" || normalized == "submit_line":
		return "/redacted/command"
	default:
		return redactPathFragments(value)
	}
}

func decodeJSONString(value string) (any, bool) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[") {
		return nil, false
	}
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, false
	}
	return decoded, true
}

func normalizeKey(key string) string {
	return strings.ToLower(strings.ReplaceAll(key, "-", "_"))
}

func isNodeKey(key string) bool {
	switch key {
	case "node", "nodes", "nodelist", "node_list", "required_nodes", "excluded_nodes", "failed_node", "batch_host", "submit_host", "hostname", "host":
		return true
	default:
		return strings.HasSuffix(key, "_node") || strings.HasSuffix(key, "_nodes") || strings.HasSuffix(key, "_host")
	}
}

func isUserKey(key string) bool {
	return key == "user" || key == "username" || key == "user_name" || strings.HasSuffix(key, "_user")
}

func isAccountKey(key string) bool {
	return key == "account" || key == "account_name" || key == "qos" || key == "wckey" || strings.Contains(key, "customer") || strings.Contains(key, "project")
}

func isWorkflowKey(key string) bool {
	return key == "name" || key == "job_name" || key == "workflow" || key == "workflow_id" || key == "workflow_name" || strings.HasSuffix(key, "_workflow")
}

func isPathKey(key string) bool {
	return strings.Contains(key, "path") || strings.Contains(key, "dir") || key == "cwd" || key == "workdir" || key == "work_dir"
}

func redactPathFragments(value string) string {
	if !strings.Contains(value, "/") {
		return value
	}
	fields := strings.Fields(value)
	for i, field := range fields {
		if strings.HasPrefix(field, "/") {
			fields[i] = "/redacted/path"
		}
	}
	return strings.Join(fields, " ")
}

func redactNodeList(value string) string {
	if strings.TrimSpace(value) == "" || value == "None assigned" {
		return value
	}

	parts := splitNodeList(value)
	redacted := make([]string, 0, len(parts))
	for _, part := range parts {
		redacted = append(redacted, redactNodeToken(part))
	}
	return strings.Join(redacted, ",")
}

func splitNodeList(value string) []string {
	var parts []string
	var current strings.Builder
	bracketDepth := 0
	for _, char := range value {
		switch char {
		case '[':
			bracketDepth++
			current.WriteRune(char)
		case ']':
			bracketDepth--
			current.WriteRune(char)
		case ',':
			if bracketDepth == 0 {
				parts = append(parts, strings.TrimSpace(current.String()))
				current.Reset()
				continue
			}
			current.WriteRune(char)
		default:
			current.WriteRune(char)
		}
	}
	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}
	return parts
}

var trailingNodeShape = regexp.MustCompile(`([0-9]+.*)$`)

func redactNodeToken(value string) string {
	if value == "" {
		return value
	}
	if bracket := strings.Index(value, "["); bracket >= 0 {
		return "node" + value[bracket:]
	}
	if match := trailingNodeShape.FindStringSubmatch(value); len(match) == 2 {
		return "node" + match[1]
	}
	return "node-redacted"
}
