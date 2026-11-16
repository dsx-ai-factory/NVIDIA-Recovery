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

func TestInstrumentationReportsRepresentationChanges(t *testing.T) {
	object := fixture(t, jobsetFixture)
	spec := nativeWorkerSpec(t, object)
	spec["hostNetwork"] = false
	container := spec["containers"].([]any)[0].(map[string]any)
	requests := container["resources"].(map[string]any)["requests"].(map[string]any)
	requests["nvidia.com/gpu"] = int64(8)
	requests["cpu"] = "0.5"
	data, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"--operation", "instrument", "--marker", "run-1", "--report"}, bytes.NewReader(data), &output); err != nil {
		t.Fatal(err)
	}
	var result instrumentationReport
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	base := "/spec/replicatedJobs/0/template/spec/template"
	for _, path := range []string{
		base + "/spec/hostNetwork",
		base + "/spec/containers/0/resources/requests/cpu",
		base + "/spec/containers/0/resources/requests/nvidia.com~1gpu",
	} {
		if !slices.Contains(result.Changes, fieldChange{path, "normalization"}) {
			t.Fatalf("normalization not reported for %s: %#v", path, result.Changes)
		}
	}
	updatedSpec := nativeWorkerSpec(t, result.Object)
	updatedRequests := updatedSpec["containers"].([]any)[0].(map[string]any)["resources"].(map[string]any)["requests"].(map[string]any)
	if updatedRequests["cpu"] != "500m" || updatedRequests["nvidia.com/gpu"] != "8" {
		t.Fatalf("unexpected SDK quantity serialization: %#v", updatedRequests)
	}
	if _, exists := updatedSpec["hostNetwork"]; exists {
		t.Fatal("expected the pinned typed serializer to omit explicit false")
	}
	if !slices.Contains(result.Changes, fieldChange{base + "/metadata/labels/qualification.nvidia.com~1marker", "marker"}) {
		t.Fatal("requested marker change not reported")
	}
}

func TestInstrumentationReportsEmptyEnvironmentOmissionAndRejectsNonemptyLoss(t *testing.T) {
	before := fixture(t, jobsetFixture)
	container := nativeWorkerSpec(t, before)["containers"].([]any)[0].(map[string]any)
	container["env"] = []any{}
	after := transformFixture(t, before, "instrument")
	changes, err := checkInstrumentation(before, after, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	path := "/spec/replicatedJobs/0/template/spec/template/spec/containers/0/env"
	if !slices.Contains(changes, fieldChange{path, "normalization"}) {
		t.Fatalf("empty environment omission not reported: %#v", changes)
	}
	if _, exists := nativeWorkerSpec(t, after)["containers"].([]any)[0].(map[string]any)["env"]; exists {
		t.Fatal("expected typed serialization to omit the empty environment")
	}

	before = fixture(t, jobsetFixture)
	after = transformFixture(t, before, "instrument")
	delete(nativeWorkerSpec(t, after)["containers"].([]any)[0].(map[string]any), "env")
	if _, err := checkInstrumentation(before, after, "run-1"); err == nil {
		t.Fatal("accepted removal of the user's nonempty environment")
	}
}

func TestInstrumentationRejectsUnexpectedMutations(t *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"new placement constraint":    func(spec map[string]any) { spec["nodeSelector"] = map[string]any{"pool": "other"} },
		"new empty scheduling object": func(spec map[string]any) { spec["nodeSelector"] = map[string]any{} },
		"new toleration":              func(spec map[string]any) { spec["tolerations"] = []any{map[string]any{"operator": "Exists"}} },
		"changed quantity": func(spec map[string]any) {
			spec["containers"].([]any)[0].(map[string]any)["resources"].(map[string]any)["requests"].(map[string]any)["cpu"] = "200m"
		},
		"changed placement": func(spec map[string]any) { spec["schedulerName"] = "other" },
		"removed true":      func(spec map[string]any) { delete(spec, "hostNetwork") },
	} {
		t.Run(name, func(t *testing.T) {
			before := fixture(t, jobsetFixture)
			nativeWorkerSpec(t, before)["hostNetwork"] = true
			after := transformFixture(t, before, "instrument")
			mutate(nativeWorkerSpec(t, after))
			if _, err := checkInstrumentation(before, after, "run-1"); err == nil {
				t.Fatal("accepted an unrequested template mutation")
			}
		})
	}
}

func TestInstrumentationReportsOnlyExplicitTypedDefaults(t *testing.T) {
	before := fixture(t, pytorchFixture)
	after := transformFixture(t, before, "instrument")
	changes, err := checkInstrumentation(before, after, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"Master", "Worker"} {
		path := "/spec/pytorchReplicaSpecs/" + role + "/template/spec/containers/0/resources"
		// The current Kubernetes type serializes an absent resource struct as {}.
		if !slices.Contains(changes, fieldChange{path, "default_added"}) {
			t.Fatalf("unreported typed resource default: %#v", changes)
		}
	}
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
