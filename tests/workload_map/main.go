// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Command workload-map-probe inspects or transforms one JSON workload offline.
// Transformations are returned to stdout; this command never accesses a cluster.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/catalog"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
)

const sdkModule = "github.com/dsx-ai-factory/workload-map/karta/sdk"
const markerKey = "qualification.nvidia.com/marker"

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
	operation := flags.String("operation", "inspect", "inspect, instrument, suspend, resume, or associate-pods")
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
	if *operation == "associate-pods" {
		result, err := associatePods(context.Background(), data)
		if err != nil {
			return err
		}
		return errors.Wrap(json.NewEncoder(output).Encode(result), "encode Pod classifications")
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
		return inspect(ctx, object, definition, root, components)
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
