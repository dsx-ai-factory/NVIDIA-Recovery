// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Command workload-map-probe inspects or transforms one JSON workload offline.
// Transformations are returned to stdout; this command never accesses a cluster.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime/debug"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/catalog"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
)

const sdkModule = "github.com/dsx-ai-factory/workload-map/karta/sdk"
const markerKey = "qualification.nvidia.com/marker"

type instanceReport struct {
	// Replicas is the SDK's normalized Pod count, or null if unavailable.
	Replicas *int32 `json:"replicas"`
	// PodLabels contains template labels, excluding environment and container configuration.
	PodLabels map[string]string `json:"pod_labels"`
}

type report struct {
	// SDKVersion identifies the dependency actually linked into this executable.
	SDKVersion string `json:"sdk_version"`
	// DefinitionName identifies the built-in definition selected for this workload.
	DefinitionName string `json:"definition_name"`
	// DefinitionSHA256 fingerprints the complete JSON-encoded definition.
	DefinitionSHA256 string `json:"definition_sha256"`
	// APIVersion is the input object's Kubernetes API group and version.
	APIVersion string `json:"api_version"`
	// Kind is the input object's Kubernetes kind.
	Kind string `json:"kind"`
	// Namespace is the input object's namespace.
	Namespace string `json:"namespace"`
	// Name is the input object's name.
	Name string `json:"name"`
	// UID is the input object's immutable Kubernetes identity, if assigned.
	UID string `json:"uid"`
	// SuspendSupported records the presence of an SDK lifecycle mapping, not live qualification.
	SuspendSupported bool `json:"suspend_supported"`
	// Statuses contains all matched SDK states; Running does not establish Pod readiness.
	Statuses []string `json:"statuses"`
	// Components groups template observations by SDK component and instance identifier.
	Components map[string]map[string]instanceReport `json:"components"`
}

// linkedSDKVersion reports unknown for test binaries or unavailable build info
// instead of claiming that a source constant proves the linked dependency.
func linkedSDKVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dependency := range info.Deps {
		if dependency.Path == sdkModule {
			if dependency.Replace != nil {
				return "replaced (not the upstream release)"
			}
			return dependency.Version
		}
	}
	return "unknown"
}

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run bounds input and parses exactly one object before doing any SDK work.
func run(args []string, input io.Reader, output io.Writer) error {
	flags := flag.NewFlagSet("workload-map-probe", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	operation := flags.String("operation", "inspect", "inspect, instrument, suspend, or resume")
	marker := flags.String("marker", "", "qualification Pod label value for instrument")
	if err := flags.Parse(args); err != nil {
		return errors.Wrap(err, "parse flags")
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	data, err := io.ReadAll(io.LimitReader(input, (4<<20)+1))
	if err != nil {
		return errors.Wrap(err, "read workload")
	}
	if len(data) > 4<<20 {
		return errors.New("workload exceeds 4 MiB input limit")
	}
	// UnmarshalJSON on Unstructured preserves integer values needed by Kubernetes.
	object := &unstructured.Unstructured{}
	if err := json.Unmarshal(data, object); err != nil {
		return errors.Wrap(err, "decode one workload JSON object")
	}
	result, err := evaluate(context.Background(), object, *operation, *marker)
	if err != nil {
		return err
	}
	return errors.Wrap(json.NewEncoder(output).Encode(result), "encode result")
}

func evaluate(ctx context.Context, object *unstructured.Unstructured, operation, marker string) (any, error) {
	gvk := object.GroupVersionKind()
	if gvk.Version == "" || gvk.Kind == "" {
		return nil, errors.New("workload requires apiVersion and kind")
	}
	definition, err := catalog.Get(gvk)
	if err != nil {
		return nil, errors.Wrap(err, "resolve upstream definition")
	}
	factory := resource.NewComponentFactoryFromObject(definition, object.DeepCopy())
	root, err := factory.GetRootComponent()
	if err != nil {
		return nil, errors.Wrap(err, "resolve root component")
	}
	components, err := factory.GetChildComponents()
	if err != nil {
		return nil, errors.Wrap(err, "resolve child components")
	}
	if operation == "inspect" {
		definitionJSON, err := json.Marshal(definition)
		if err != nil {
			return nil, errors.Wrap(err, "encode definition provenance")
		}
		result := report{
			SDKVersion: linkedSDKVersion(), DefinitionName: definition.Name,
			DefinitionSHA256: fmt.Sprintf("%x", sha256.Sum256(definitionJSON)),
			APIVersion:       object.GetAPIVersion(), Kind: object.GetKind(),
			Namespace: object.GetNamespace(), Name: object.GetName(), UID: string(object.GetUID()),
			SuspendSupported: root.HasSuspendDefinition(), Statuses: []string{},
			Components: map[string]map[string]instanceReport{},
		}
		status, err := root.GetStatus(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "extract workload status")
		}
		if status != nil {
			for _, matched := range status.MatchedStatuses {
				result.Statuses = append(result.Statuses, string(matched))
			}
		}
		sort.Strings(result.Statuses)
		for _, component := range components {
			instances, err := component.GetExtractedInstances(ctx)
			if err != nil {
				return nil, errors.Wrapf(err, "extract component %s", component.Name())
			}
			result.Components[component.Name()] = map[string]instanceReport{}
			for id, instance := range instances {
				item := instanceReport{PodLabels: map[string]string{}}
				if instance.Scale != nil {
					item.Replicas = instance.Scale.Replicas
				}
				if instance.PodTemplateSpec != nil && instance.PodTemplateSpec.Labels != nil {
					item.PodLabels = instance.PodTemplateSpec.Labels
				}
				result.Components[component.Name()][id] = item
			}
		}
		return result, nil
	}
	switch operation {
	case "instrument":
		if marker == "" || len(validation.IsValidLabelValue(marker)) != 0 {
			return nil, errors.New("instrument requires a nonempty valid --marker label value")
		}
		templateCount := 0
		for _, component := range components {
			templates, err := component.GetPodTemplateSpec(ctx)
			if err != nil {
				return nil, errors.Wrapf(err, "read templates for %s", component.Name())
			}
			for id, template := range templates {
				if old, exists := template.Labels[markerKey]; exists && old != marker {
					return nil, errors.New("refusing to replace an existing qualification marker")
				}
				if template.Labels == nil {
					template.Labels = map[string]string{}
				}
				template.Labels[markerKey] = marker
				templates[id] = template
				templateCount++
			}
			if len(templates) != 0 {
				if err := component.UpdatePodTemplateSpec(ctx, templates); err != nil {
					return nil, errors.Wrapf(err, "instrument templates for %s", component.Name())
				}
			}
		}
		if templateCount == 0 {
			return nil, errors.New("definition exposes no Pod templates")
		}
	case "suspend", "resume":
		if err := requireActiveSuspend(object, root.HasSuspendDefinition()); err != nil {
			return nil, err
		}
		if operation == "suspend" {
			err = root.Suspend(ctx)
		} else {
			err = root.Resume(ctx)
		}
		if err != nil {
			return nil, errors.Wrap(err, operation+" through Workload-Map")
		}
	default:
		return nil, errors.Newf("unknown operation %q", operation)
	}
	updated, err := factory.GetResource()
	if err != nil {
		return nil, errors.Wrap(err, "read transformed workload")
	}
	// The SDK's jq engine uses Go numeric types that differ from Unstructured's
	// int64 values. Compare the JSON contract after Kubernetes decoding, not
	// incidental in-memory number types.
	encoded, err := json.Marshal(updated)
	if err != nil {
		return nil, errors.Wrap(err, "encode transformed workload")
	}
	candidate := &unstructured.Unstructured{}
	if err := json.Unmarshal(encoded, candidate); err != nil {
		return nil, errors.Wrap(err, "decode transformed workload")
	}
	// Typed template round trips can discard fields unknown to the SDK's pinned
	// Kubernetes types. Refuse that loss rather than emit a destructive mutation.
	if operation == "instrument" {
		if !preserves(object.Object, candidate.Object) {
			return nil, errors.New("template round trip removed or changed existing fields")
		}
	}
	return candidate, nil
}

func requireActiveSuspend(object *unstructured.Unstructured, supported bool) error {
	gvk := object.GroupVersionKind()
	qualifiedKind := (gvk.Group == "jobset.x-k8s.io" && gvk.Version == "v1alpha2" && gvk.Kind == "JobSet") ||
		(gvk.Group == "kubeflow.org" && gvk.Version == "v1" && gvk.Kind == "PyTorchJob")
	if !qualifiedKind {
		return errors.New("lifecycle transformations are restricted to JobSet v1alpha2 and PyTorchJob v1")
	}
	if !supported {
		return errors.New("definition does not support suspend/resume")
	}
	if object.GetDeletionTimestamp() != nil {
		return errors.New("refusing to transform a terminating workload")
	}
	conditions, _, err := unstructured.NestedSlice(object.Object, "status", "conditions")
	if err != nil {
		return errors.Wrap(err, "read native terminal conditions")
	}
	for _, condition := range conditions {
		fields, ok := condition.(map[string]any)
		if !ok {
			return errors.New("invalid native condition")
		}
		if fields["status"] == "True" && (fields["type"] == "Completed" || fields["type"] == "Succeeded" || fields["type"] == "Failed") {
			return errors.New("terminal workload recovery is outside the qualification contract")
		}
	}
	return nil
}

// preserves permits additions but rejects changes to any field supplied by the
// caller. This intentionally fails closed for unknown fields and lossy types.
func preserves(before, after any) bool {
	switch original := before.(type) {
	case map[string]any:
		updated, ok := after.(map[string]any)
		if !ok {
			return false
		}
		for key, value := range original {
			other, exists := updated[key]
			if !exists || !preserves(value, other) {
				return false
			}
		}
		return true
	case []any:
		updated, ok := after.([]any)
		if !ok || len(original) != len(updated) {
			return false
		}
		for i, value := range original {
			if !preserves(value, updated[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(before, after)
	}
}
