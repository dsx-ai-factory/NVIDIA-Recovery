// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	quantity "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type fieldChange struct {
	// Path is a JSON Pointer; values are omitted to avoid copying user secrets.
	Path string `json:"path"`
	// Kind distinguishes the requested marker, representation changes, and defaults.
	Kind string `json:"kind"`
}

type instrumentationReport struct {
	// Object is the complete transformed workload, including its original configuration.
	Object *unstructured.Unstructured `json:"object"`
	// Changes records every accepted changed leaf or empty object path.
	Changes []fieldChange `json:"changes"`
}

// checkInstrumentation is an independent native-path oracle for the two qualified
// kinds. It accepts only the marker and explicit, semantically understood typed
// serialization changes. Unknown fields, added scheduling constraints, and changed
// quantities fail closed. Normalizations remain observable because they can cause
// GitOps drift even when runtime meaning is unchanged.
func checkInstrumentation(before, after *unstructured.Unstructured, marker string) ([]fieldChange, error) {
	templates, err := nativeTemplatePaths(before)
	if err != nil {
		return nil, err
	}
	changes := []fieldChange{}
	var compare func(any, any, string) error
	compare = func(original, updated any, path string) error {
		if reflect.DeepEqual(original, updated) {
			return nil
		}
		switch original := original.(type) {
		case map[string]any:
			other, ok := updated.(map[string]any)
			if !ok {
				return errors.Newf("instrumentation changed object at %s", path)
			}
			for key, value := range original {
				child := path + "/" + pointerSegment(key)
				next, exists := other[key]
				if !exists {
					if typedOmission(value, relativeTemplatePath(child, templates)) {
						changes = append(changes, fieldChange{child, "normalization"})
						continue
					}
					return errors.Newf("instrumentation removed field at %s", child)
				}
				if err := compare(value, next, child); err != nil {
					return err
				}
			}
			for key, value := range other {
				if _, exists := original[key]; !exists {
					if err := recordAdded(value, path+"/"+pointerSegment(key), templates, marker, &changes); err != nil {
						return err
					}
				}
			}
		case []any:
			other, ok := updated.([]any)
			if !ok || len(original) != len(other) {
				return errors.Newf("instrumentation changed array at %s", path)
			}
			for i, value := range original {
				if err := compare(value, other[i], path+"/"+strconv.Itoa(i)); err != nil {
					return err
				}
			}
		default:
			if quantityPath(relativeTemplatePath(path, templates)) && equalQuantity(original, updated) {
				changes = append(changes, fieldChange{path, "normalization"})
				return nil
			}
			return errors.Newf("instrumentation changed field at %s", path)
		}
		return nil
	}
	if err := compare(before.Object, after.Object, ""); err != nil {
		return nil, err
	}
	for _, template := range templates {
		path := append(strings.Split(strings.TrimPrefix(template, "/"), "/"), "metadata", "labels", pointerSegment(markerKey))
		if value, ok := pointerValue(after.Object, path); !ok || value != marker {
			return nil, errors.Newf("instrumentation did not mark native template %s", template)
		}
	}
	sort.Slice(changes, func(i, j int) bool { return changes[i].Path < changes[j].Path })
	return changes, nil
}

func nativeTemplatePaths(object *unstructured.Unstructured) ([]string, error) {
	switch object.GetAPIVersion() + "/" + object.GetKind() {
	case "jobset.x-k8s.io/v1alpha2/JobSet":
		jobs, _, err := unstructured.NestedSlice(object.Object, "spec", "replicatedJobs")
		if err != nil {
			return nil, err
		}
		paths := make([]string, len(jobs))
		for i := range jobs {
			paths[i] = fmt.Sprintf("/spec/replicatedJobs/%d/template/spec/template", i)
		}
		return paths, nil
	case "kubeflow.org/v1/PyTorchJob":
		roles, _, err := unstructured.NestedMap(object.Object, "spec", "pytorchReplicaSpecs")
		if err != nil {
			return nil, err
		}
		paths := make([]string, 0, len(roles))
		for role := range roles {
			paths = append(paths, "/spec/pytorchReplicaSpecs/"+pointerSegment(role)+"/template")
		}
		return paths, nil
	default:
		return nil, errors.New("instrumentation preservation is qualified only for JobSet v1alpha2 and PyTorchJob v1")
	}
}

// recordAdded walks new objects so a marker's enclosing maps are accepted only
// when every introduced leaf is explicitly allowed. Empty containers.resources
// and metadata.creationTimestamp are typed Kubernetes serialization defaults.
func recordAdded(value any, path string, templates []string, marker string, changes *[]fieldChange) error {
	relative := relativeTemplatePath(path, templates)
	if relative == "/metadata/labels/"+pointerSegment(markerKey) && value == marker {
		*changes = append(*changes, fieldChange{path, "marker"})
		return nil
	}
	if relative == "/metadata/creationTimestamp" && value == nil {
		*changes = append(*changes, fieldChange{path, "default_added"})
		return nil
	}
	if fields, ok := value.(map[string]any); ok {
		if len(fields) == 0 && containerField(relative, "resources") {
			*changes = append(*changes, fieldChange{path, "default_added"})
			return nil
		}
		if len(fields) > 0 {
			for key, child := range fields {
				if err := recordAdded(child, path+"/"+pointerSegment(key), templates, marker, changes); err != nil {
					return err
				}
			}
			return nil
		}
	}
	return errors.Newf("instrumentation added unexpected field at %s", path)
}

func relativeTemplatePath(path string, templates []string) string {
	for _, template := range templates {
		if strings.HasPrefix(path, template+"/") {
			return strings.TrimPrefix(path, template)
		}
	}
	return ""
}

// typedOmission accepts only observed omissions with unchanged runtime meaning.
// The launcher submits env: []; the typed serializer omits that empty list. A
// nonempty environment list remains user configuration and must never disappear.
func typedOmission(value any, path string) bool {
	if value == false && typedFalsePath(path) {
		return true
	}
	items, ok := value.([]any)
	return ok && len(items) == 0 && containerField(path, "env")
}

func typedFalsePath(path string) bool {
	switch path {
	case "/spec/hostNetwork", "/spec/hostPID", "/spec/hostIPC":
		return true
	default:
		return containerField(path, "stdin") || containerField(path, "stdinOnce") || containerField(path, "tty")
	}
}

func containerField(path, field string) bool {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) != 4 || parts[0] != "spec" || parts[3] != field {
		return false
	}
	if parts[1] != "containers" && parts[1] != "initContainers" && parts[1] != "ephemeralContainers" {
		return false
	}
	_, err := strconv.Atoi(parts[2])
	return err == nil
}

func quantityPath(path string) bool {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 6 && containerField("/"+strings.Join(parts[:4], "/"), "resources") {
		return parts[4] == "requests" || parts[4] == "limits"
	}
	return len(parts) == 4 && parts[0] == "spec" && parts[1] == "resources" && (parts[2] == "requests" || parts[2] == "limits")
}

func equalQuantity(before, after any) bool {
	left, err := quantity.ParseQuantity(fmt.Sprint(before))
	if err != nil {
		return false
	}
	right, err := quantity.ParseQuantity(fmt.Sprint(after))
	return err == nil && left.Cmp(right) == 0
}

func pointerSegment(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}

func pointerValue(value any, segments []string) (any, bool) {
	for _, segment := range segments {
		key := strings.ReplaceAll(strings.ReplaceAll(segment, "~1", "/"), "~0", "~")
		switch current := value.(type) {
		case map[string]any:
			var ok bool
			value, ok = current[key]
			if !ok {
				return nil, false
			}
		case []any:
			index, err := strconv.Atoi(key)
			if err != nil || index < 0 || index >= len(current) {
				return nil, false
			}
			value = current[index]
		default:
			return nil, false
		}
	}
	return value, true
}
