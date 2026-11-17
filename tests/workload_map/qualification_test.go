// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func nativeWorkerSpec(t *testing.T, object *unstructured.Unstructured) map[string]any {
	t.Helper()
	jobs := object.Object["spec"].(map[string]any)["replicatedJobs"].([]any)
	return jobs[0].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
}

func TestInspectRetainsEvidenceWhenScaleExtractionFails(t *testing.T) {
	object := fixture(t, strings.Replace(jobsetFixture, `"parallelism": 3,`, "", 1))
	result := inspectFixture(t, object)
	if result.CapabilityErrors["components.replicatedjob.scale"] == "" {
		t.Fatal("missing parallelism must expose the upstream scale error")
	}
	if len(result.CapabilityErrors) != 1 || result.InputIdentitySource != "input_metadata" {
		t.Fatalf("unexpected failure scope: %#v", result)
	}
	if result.Components["replicatedjob"]["workers"].Replicas != nil {
		t.Fatal("failed scale must not masquerade as a zero or default")
	}
	if result.Components["replicatedjob"]["workers"].PodLabels["user.example/team"] != "research" || !slices.Contains(result.Statuses, "Running") {
		t.Fatal("scale failure discarded successful template/status evidence")
	}
}

func TestInspectRetainsScaleWhenTemplateExtractionFails(t *testing.T) {
	object := fixture(t, jobsetFixture)
	nativeWorkerSpec(t, object)["containers"] = "invalid"
	result := inspectFixture(t, object)
	if result.CapabilityErrors["components.replicatedjob.templates"] == "" || len(result.CapabilityErrors) != 1 {
		t.Fatalf("unexpected failure scope: %#v", result.CapabilityErrors)
	}
	if replicas := result.Components["replicatedjob"]["workers"].Replicas; replicas == nil || *replicas != 6 {
		t.Fatal("template failure discarded independent scale")
	}
}

func TestInspectRetainsComponentsWhenStatusExtractionFails(t *testing.T) {
	object := fixture(t, jobsetFixture)
	object.Object["status"] = map[string]any{"conditions": "invalid"}
	result := inspectFixture(t, object)
	if result.CapabilityErrors["status"] == "" || len(result.CapabilityErrors) != 1 {
		t.Fatalf("unexpected failure scope: %#v", result.CapabilityErrors)
	}
	if replicas := result.Components["replicatedjob"]["workers"].Replicas; replicas == nil || *replicas != 6 {
		t.Fatal("status failure discarded independent scale")
	}
	if len(result.Statuses) != 0 {
		t.Fatal("failed status extraction must not synthesize a successful match")
	}
}

func TestJobSetStatusSetsExposeOverlapAndStaleConditions(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		want         []string
	}{
		{"pending", `{}`, []string{"Undefined"}},
		{"active without readiness", `{"replicatedJobsStatus":[{"name":"workers","active":2,"ready":0,"failed":0}]}`, []string{"Initializing", "Running"}},
		{"ready", `{"replicatedJobsStatus":[{"name":"workers","active":2,"ready":2,"failed":0}]}`, []string{"Running"}},
		{"suspended and drained", `{"conditions":[{"type":"Suspended","status":"True"}]}`, []string{"Suspended"}},
		{"stale suspended condition", `{"conditions":[{"type":"Suspended","status":"True"}],"replicatedJobsStatus":[{"name":"workers","active":2,"ready":2,"failed":0}]}`, []string{"Running", "Suspended"}},
		{"failed with active pods", `{"conditions":[{"type":"Failed","status":"True"}],"replicatedJobsStatus":[{"name":"workers","active":2,"ready":0,"failed":1}]}`, []string{"Failed"}},
		{"false terminal condition", `{"conditions":[{"type":"Failed","status":"False"}]}`, []string{"Undefined"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object := fixture(t, jobsetFixture)
			var native map[string]any
			if err := json.Unmarshal([]byte(tc.status), &native); err != nil {
				t.Fatal(err)
			}
			object.Object["status"] = native
			result := inspectFixture(t, object)
			if len(result.CapabilityErrors) != 0 || !reflect.DeepEqual(result.Statuses, tc.want) {
				t.Fatalf("status=%v errors=%v, want %v", result.Statuses, result.CapabilityErrors, tc.want)
			}
		})
	}
}

func TestTerminatingWorkloadRefusesBothTransitions(t *testing.T) {
	object := fixture(t, jobsetFixture)
	object.Object["metadata"].(map[string]any)["deletionTimestamp"] = "2026-11-16T12:00:00Z"
	for _, operation := range []string{"suspend", "resume"} {
		if _, err := evaluate(context.Background(), object, operation, ""); err == nil {
			t.Fatalf("accepted %s while deleting", operation)
		}
	}
}

func TestPodClassificationDoesNotEstablishParentOwnership(t *testing.T) {
	workload := fixture(t, jobsetFixture)
	pods := []any{
		map[string]any{"metadata": map[string]any{"name": "owned", "uid": "owned-uid", "labels": map[string]any{"jobset.sigs.k8s.io/replicatedjob-name": "workers", "jobset.sigs.k8s.io/jobset-name": "training"}}},
		map[string]any{"metadata": map[string]any{"name": "foreign", "uid": "foreign-uid", "labels": map[string]any{"jobset.sigs.k8s.io/replicatedjob-name": "workers", "jobset.sigs.k8s.io/jobset-name": "unrelated"}}},
		map[string]any{"metadata": map[string]any{"name": "unknown", "uid": "unknown-uid", "labels": map[string]any{"jobset.sigs.k8s.io/replicatedjob-name": "unknown"}}},
	}
	result := classifyFixtures(t, workload, pods)
	if result.Scope != "caller_supplied_candidates" || len(result.Pods) != 3 {
		t.Fatalf("unexpected report: %#v", result)
	}
	for _, item := range result.Pods[:2] {
		if item.Component != "replicatedjob" || item.InstanceID != "workers" || item.Error != "" {
			t.Fatalf("unexpected classification: %#v", item)
		}
	}
	if result.Pods[2].Error == "" {
		t.Fatal("unknown replicated job must not silently match")
	}
}

func TestPyTorchPodClassificationUsesReplicaRole(t *testing.T) {
	pods := []any{}
	for _, role := range []string{"master", "worker", "unrecognized"} {
		pods = append(pods, map[string]any{"metadata": map[string]any{"name": role, "labels": map[string]any{"training.kubeflow.org/replica-type": role}}})
	}
	result := classifyFixtures(t, fixture(t, pytorchFixture), pods)
	for i, want := range []string{"master", "worker"} {
		if result.Pods[i].Component != want || result.Pods[i].InstanceID != "" || result.Pods[i].Error != "" {
			t.Fatalf("unexpected role classification: %#v", result.Pods[i])
		}
	}
	if result.Pods[2].Error == "" {
		t.Fatal("unrecognized Pod role silently matched")
	}
}

func classifyFixtures(t *testing.T, workload *unstructured.Unstructured, pods []any) associationReport {
	t.Helper()
	data, err := json.Marshal(map[string]any{"workload": workload, "pods": pods})
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"--operation", "associate-pods"}, bytes.NewReader(data), &output); err != nil {
		t.Fatal(err)
	}
	var result associationReport
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}
