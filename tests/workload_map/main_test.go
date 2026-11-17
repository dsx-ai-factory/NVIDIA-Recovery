// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const jobsetFixture = `{
  "apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet",
  "metadata": {"name": "training", "namespace": "qualification", "uid": "parent-uid", "resourceVersion": "17"},
  "spec": {"replicatedJobs": [
    {"name": "workers", "replicas": 2, "template": {"spec": {"parallelism": 3, "template": {
      "metadata": {"labels": {"user.example/team": "research"}, "annotations": {"user.example/note": "preserve"}},
      "spec": {"restartPolicy": "Never", "schedulerName": "default-scheduler",
        "affinity": {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [
          {"matchExpressions": [{"key": "user.example/pool", "operator": "In", "values": ["training"]}]},
          {"matchExpressions": [{"key": "user.example/pool", "operator": "In", "values": ["backup"]}]}
        ]}}},
        "containers": [{"name": "worker", "image": "busybox:1.37.0", "env": [{"name": "TOKEN", "value": "private-fixture-value"}],
          "resources": {"requests": {"cpu": "100m", "memory": "32Mi"}}}]
      }
    }}}},
    {"name": "coordinator", "replicas": 1, "template": {"spec": {"parallelism": 1, "template": {
      "metadata": {"labels": {"user.example/role": "coordinator"}},
      "spec": {"containers": [{"name": "coordinator", "image": "busybox:1.37.0"}]}
    }}}}
  ]},
  "status": {"conditions": [], "replicatedJobsStatus": [{"name": "workers", "active": 2, "ready": 2, "failed": 0}]}
}`

const pytorchFixture = `{
  "apiVersion": "kubeflow.org/v1", "kind": "PyTorchJob",
  "metadata": {"name": "distributed-training", "namespace": "qualification", "uid": "pytorch-uid"},
  "spec": {"runPolicy": {"cleanPodPolicy": "None"}, "pytorchReplicaSpecs": {
    "Master": {"replicas": 1, "template": {"metadata": {"labels": {"user.example/role": "master"}}, "spec": {"containers": [{"name": "pytorch", "image": "example.invalid/trainer:fixture"}]}}},
    "Worker": {"replicas": 4, "template": {"metadata": {"annotations": {"user.example/note": "preserve"}}, "spec": {"containers": [{"name": "pytorch", "image": "example.invalid/trainer:fixture"}]}}}
  }},
  "status": {"conditions": [{"type": "Running", "status": "True"}]}
}`

func fixture(t *testing.T, value string) *unstructured.Unstructured {
	t.Helper()
	object := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(value), object); err != nil {
		t.Fatal(err)
	}
	return object
}

func inspectFixture(t *testing.T, object *unstructured.Unstructured) report {
	t.Helper()
	value, err := evaluate(context.Background(), object, "inspect", "")
	if err != nil {
		t.Fatal(err)
	}
	return value.(report)
}

func transformFixture(t *testing.T, object *unstructured.Unstructured, operation string) *unstructured.Unstructured {
	t.Helper()
	value, err := evaluate(context.Background(), object, operation, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	return value.(*unstructured.Unstructured)
}

func TestExtractsJobSetComponentsAndScale(t *testing.T) {
	object := fixture(t, jobsetFixture)
	result := inspectFixture(t, object)
	if result.Name != "training" || result.Namespace != "qualification" || result.UID != "parent-uid" {
		t.Fatalf("identity changed: %#v", result)
	}
	if result.DefinitionName != "jobset-x-k8s-io-jobset-v1alpha2" || len(result.DefinitionSHA256) != 64 {
		t.Fatalf("missing pinned definition provenance: %#v", result)
	}
	instances := result.Components["replicatedjob"]
	if len(result.Components) != 1 || len(instances) != 2 {
		t.Fatalf("incorrect component partition: %#v", result.Components)
	}
	for name, expected := range map[string]int32{"workers": 6, "coordinator": 1} {
		if instances[name].Replicas == nil || *instances[name].Replicas != expected {
			t.Fatalf("%s replicas = %v, want %d", name, instances[name].Replicas, expected)
		}
	}
	if !result.SuspendSupported || !slices.Contains(result.Statuses, "Running") {
		t.Fatalf("unexpected lifecycle interpretation: %#v", result)
	}
	var output bytes.Buffer
	if err := run(nil, strings.NewReader(jobsetFixture), &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "private-fixture-value") || strings.Contains(output.String(), "containers") {
		t.Fatal("inspection leaked container configuration")
	}
}

func TestExtractsPyTorchRoles(t *testing.T) {
	result := inspectFixture(t, fixture(t, pytorchFixture))
	for role, expected := range map[string]int32{"master": 1, "worker": 4} {
		instances := result.Components[role]
		if len(instances) != 1 || instances[""].Replicas == nil || *instances[""].Replicas != expected {
			t.Fatalf("incorrect %s scale: %#v", role, instances)
		}
	}
	if len(result.Components) != 2 || !slices.Contains(result.Statuses, "Running") {
		t.Fatalf("unexpected PyTorch interpretation: %#v", result)
	}
}

func TestInstrumentationUpdatesRecoveryFixtureTemplates(t *testing.T) {
	// These locations and expected user values come from the submitted native
	// fixtures, independently of SDK extraction. These assertions cover the
	// representative Recovery fixtures, not arbitrary template round trips.
	for _, tc := range []struct {
		name, input string
		paths       [][]string
	}{
		{"JobSet", jobsetFixture, [][]string{
			{"spec", "replicatedJobs", "0", "template", "spec", "template"},
			{"spec", "replicatedJobs", "1", "template", "spec", "template"},
		}},
		{"PyTorchJob", pytorchFixture, [][]string{
			{"spec", "pytorchReplicaSpecs", "Master", "template"},
			{"spec", "pytorchReplicaSpecs", "Worker", "template"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object := fixture(t, tc.input)
			before := object.DeepCopy()
			updated := transformFixture(t, object, "instrument")
			if !reflect.DeepEqual(before.Object, object.Object) {
				t.Fatal("probe modified its input object")
			}
			if !reflect.DeepEqual(before.Object["metadata"], updated.Object["metadata"]) {
				t.Fatal("instrumentation changed workload identity or metadata")
			}
			for _, path := range tc.paths {
				assertNativeValue(t, updated.Object, append(path, "metadata", "labels", markerKey), "run-1")
				// A native expected expression tests the admission contract,
				// including templates that initially have no affinity.
				index := "0"
				if tc.name == "JobSet" && path[2] == "0" {
					index = "1"
				}
				assertNativeValue(t, updated.Object, append(path, "spec", "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms", "0", "matchExpressions", index), decodeValue(t, `{"key":"recovery.nvidia.com/exclude-4e65d3fbe8ad6535681b021b30785b12","operator":"DoesNotExist"}`))
			}
			if tc.name == "JobSet" {
				worker := tc.paths[0]
				coordinator := tc.paths[1]
				assertNativeValue(t, updated.Object, append(worker, "metadata", "labels", "user.example/team"), "research")
				assertNativeValue(t, updated.Object, append(worker, "metadata", "annotations", "user.example/note"), "preserve")
				assertNativeValue(t, updated.Object, append(worker, "spec", "schedulerName"), "default-scheduler")
				assertNativeValue(t, updated.Object, append(worker, "spec", "restartPolicy"), "Never")
				assertNativeValue(t, updated.Object, append(worker, "spec", "affinity"), decodeValue(t, `{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"user.example/pool","operator":"In","values":["training"]},{"key":"recovery.nvidia.com/exclude-4e65d3fbe8ad6535681b021b30785b12","operator":"DoesNotExist"}]},{"matchExpressions":[{"key":"user.example/pool","operator":"In","values":["backup"]},{"key":"recovery.nvidia.com/exclude-4e65d3fbe8ad6535681b021b30785b12","operator":"DoesNotExist"}]}]}}}`))
				assertNativeValue(t, updated.Object, append(worker, "spec", "containers", "0", "env"), decodeValue(t, `[{"name":"TOKEN","value":"private-fixture-value"}]`))
				assertNativeValue(t, updated.Object, append(worker, "spec", "containers", "0", "resources"), decodeValue(t, `{"requests":{"cpu":"100m","memory":"32Mi"}}`))
				assertNativeValue(t, updated.Object, append(coordinator, "metadata", "labels", "user.example/role"), "coordinator")
				for _, path := range tc.paths {
					assertNativeValue(t, updated.Object, append(path, "spec", "containers", "0", "image"), "busybox:1.37.0")
				}
			} else {
				assertNativeValue(t, updated.Object, append(tc.paths[0], "metadata", "labels", "user.example/role"), "master")
				assertNativeValue(t, updated.Object, append(tc.paths[1], "metadata", "annotations", "user.example/note"), "preserve")
				assertNativeValue(t, updated.Object, []string{"spec", "runPolicy", "cleanPodPolicy"}, "None")
				for _, path := range tc.paths {
					assertNativeValue(t, updated.Object, append(path, "spec", "containers", "0", "image"), "example.invalid/trainer:fixture")
				}
			}
			// Reapplying the same marker is safe; changing its ownership is rejected.
			again := transformFixture(t, updated, "instrument")
			if !reflect.DeepEqual(updated.Object, again.Object) {
				t.Fatal("repeated admission changed the instrumented workload")
			}
			if _, err := evaluate(context.Background(), updated, "instrument", "another-run"); err == nil {
				t.Fatal("accepted conflicting qualification marker")
			}
		})
	}
}

// assertNativeValue follows an independently specified native object path.
// Numeric path segments index arrays; no SDK extraction is used here.
func assertNativeValue(t *testing.T, object map[string]any, path []string, expected any) {
	t.Helper()
	var current any = object
	for _, segment := range path {
		switch value := current.(type) {
		case map[string]any:
			var exists bool
			current, exists = value[segment]
			if !exists {
				t.Fatalf("missing native field %v", path)
			}
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(value) {
				t.Fatalf("invalid native array path %v", path)
			}
			current = value[index]
		default:
			t.Fatalf("cannot traverse native path %v", path)
		}
	}
	if !reflect.DeepEqual(current, expected) {
		t.Fatalf("native field %v = %#v, want %#v", path, current, expected)
	}
}

func decodeValue(t *testing.T, input string) any {
	t.Helper()
	var value any
	if err := json.Unmarshal([]byte(input), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestSuspendResumeUseNativeFieldsAndPreserveIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		path        []string
	}{
		{"JobSet", jobsetFixture, []string{"spec", "suspend"}},
		{"PyTorchJob", pytorchFixture, []string{"spec", "runPolicy", "suspend"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			object := fixture(t, tc.input)
			for _, operation := range []string{"suspend", "resume"} {
				expected := object.DeepCopy()
				if err := unstructured.SetNestedField(expected.Object, operation == "suspend", tc.path...); err != nil {
					t.Fatal(err)
				}
				updated := transformFixture(t, object, operation)
				if !reflect.DeepEqual(expected.Object, updated.Object) {
					t.Fatalf("%s changed more than the native suspend field\nwant: %v\ngot: %v", operation, expected.Object, updated.Object)
				}
				object = updated
			}
		})
	}
}

func TestTerminalConditionsRefuseLifecycleMutation(t *testing.T) {
	for _, tc := range []struct{ input, condition string }{
		{jobsetFixture, "Completed"}, {jobsetFixture, "Failed"},
		{pytorchFixture, "Succeeded"}, {pytorchFixture, "Failed"},
	} {
		object := fixture(t, tc.input)
		conditions := []any{map[string]any{"type": tc.condition, "status": "True"}}
		if err := unstructured.SetNestedSlice(object.Object, conditions, "status", "conditions"); err != nil {
			t.Fatal(err)
		}
		result := inspectFixture(t, object)
		want := "Completed"
		if tc.condition == "Failed" {
			want = "Failed"
		}
		if !slices.Contains(result.Statuses, want) {
			t.Fatalf("terminal condition %s missing from %v", tc.condition, result.Statuses)
		}
		for _, op := range []string{"suspend", "resume"} {
			if _, err := evaluate(context.Background(), object, op, ""); err == nil {
				t.Fatalf("accepted %s on terminal %s", op, tc.condition)
			}
		}
	}
	if err := requireActiveSuspend(fixture(t, jobsetFixture), false); err == nil {
		t.Fatal("missing suspend definition was treated as a supported no-op")
	}
}

func TestInvalidInputProducesNoResult(t *testing.T) {
	for _, input := range []string{
		`{`, `null`, `[]`, `{}`, `{"kind":"JobSet"}`, jobsetFixture + `{}`,
		`{"apiVersion":"example.invalid/v1","kind":"Unknown"}`,
	} {
		var output bytes.Buffer
		if err := run(nil, strings.NewReader(input), &output); err == nil {
			t.Fatalf("accepted invalid workload %q", input)
		}
		if output.Len() != 0 {
			t.Fatal("emitted partial result on invalid input")
		}
	}
	for _, args := range [][]string{{"--operation", "unknown"}, {"--operation", "instrument"}, {"--operation", "instrument", "--marker", "bad/label"}, {"unexpected"}} {
		if err := run(args, strings.NewReader(jobsetFixture), &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted invalid arguments %v", args)
		}
	}
}
