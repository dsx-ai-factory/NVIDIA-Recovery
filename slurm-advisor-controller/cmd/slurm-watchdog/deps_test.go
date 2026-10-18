// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDependencyCheckRejectsForbiddenClosurePackages(t *testing.T) {
	for name, dependency := range map[string]string{
		"API Gateway server": "github.com/dsx-ai-factory/nvidia-recovery/api-gateway/internal/server",
		"internal Heimdall":  "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/heimdall",
		"Advisor":            "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/reconcile",
	} {
		t.Run(name, func(t *testing.T) {
			depsFile := writeDependencyFixture(t, dependency+"\n")
			importsFile := writeDependencyFixture(t, "context\n")
			output, err := exec.Command(dependencyCheckScript(t), "--deps-file", depsFile, "--imports-file", importsFile).CombinedOutput()
			if err == nil {
				t.Fatalf("dependency check succeeded for forbidden package %q", dependency)
			}
			if !strings.Contains(string(output), dependency) {
				t.Fatalf("dependency check output %q does not name forbidden package %q", output, dependency)
			}
		})
	}
}

func TestDependencyCheckRejectsSchedulerCoreTransportImports(t *testing.T) {
	for _, dependency := range []string{
		"google.golang.org/protobuf/proto",
		"google.golang.org/grpc",
		"github.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1",
		"github.com/gogo/protobuf/proto",
	} {
		t.Run(dependency, func(t *testing.T) {
			depsFile := writeDependencyFixture(t, "context\n")
			importsFile := writeDependencyFixture(t, dependency+"\n")
			output, err := exec.Command(dependencyCheckScript(t), "--deps-file", depsFile, "--imports-file", importsFile).CombinedOutput()
			if err == nil {
				t.Fatalf("dependency check succeeded for direct protobuf import %q", dependency)
			}
			if !strings.Contains(string(output), dependency) {
				t.Fatalf("dependency check output %q does not name direct protobuf import %q", output, dependency)
			}
		})
	}
}

func TestDependencyCheckAllowsAuditTransportAtProcessBoundary(t *testing.T) {
	depsFile := writeDependencyFixture(t, "google.golang.org/grpc\ngithub.com/dsx-ai-factory/nvidia-recovery/api-gateway/gen/go/heimdall/v1\n")
	importsFile := writeDependencyFixture(t, "context\n")
	if output, err := exec.Command(dependencyCheckScript(t), "--deps-file", depsFile, "--imports-file", importsFile).CombinedOutput(); err != nil {
		t.Fatalf("audit transport dependency check failed: %v\n%s", err, output)
	}
}

func TestStandaloneWatchdogDependencyClosurePasses(t *testing.T) {
	cmd := exec.Command(dependencyCheckScript(t))
	cmd.Dir = filepath.Join("..", "..")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dependency check failed: %v\n%s", err, output)
	}
}

func dependencyCheckScript(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "..", "scripts", "ci", "check-slurm-watchdog-deps.sh"))
	if err != nil {
		t.Fatalf("resolve dependency check script: %v", err)
	}
	return path
}

func writeDependencyFixture(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dependencies.txt")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write dependency fixture: %v", err)
	}
	return path
}
