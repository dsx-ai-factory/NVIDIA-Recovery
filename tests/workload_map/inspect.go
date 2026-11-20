// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/api/runai/v1alpha1"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type instanceReport struct {
	// Replicas is the SDK's normalized Pod count, or null if unavailable.
	Replicas *int32 `json:"replicas"`
	// PodLabels contains template labels, excluding container configuration.
	PodLabels map[string]string `json:"pod_labels"`
}

type report struct {
	// SDKVersion identifies the dependency linked into this executable.
	SDKVersion string `json:"sdk_version"`
	// DefinitionName identifies the selected built-in definition.
	DefinitionName string `json:"definition_name"`
	// DefinitionSHA256 fingerprints the complete JSON-encoded definition.
	DefinitionSHA256 string `json:"definition_sha256"`
	// InputIdentitySource distinguishes input metadata from SDK extraction.
	InputIdentitySource string `json:"input_identity_source"`
	// APIVersion is the input object's Kubernetes API group and version.
	APIVersion string `json:"api_version"`
	// Kind is the input object's Kubernetes kind.
	Kind string `json:"kind"`
	// Namespace is echoed from the input object.
	Namespace string `json:"namespace"`
	// Name is echoed from the input object.
	Name string `json:"name"`
	// UID is echoed from the input object, if assigned.
	UID string `json:"uid"`
	// SuspendSupported describes a definition, not live qualification.
	SuspendSupported bool `json:"suspend_supported"`
	// Statuses contains all SDK matches; Running does not establish readiness.
	Statuses []string `json:"statuses"`
	// Components groups observations by SDK component and instance identifier.
	Components map[string]map[string]instanceReport `json:"components"`
	// CapabilityErrors preserves extraction failures alongside usable evidence.
	CapabilityErrors map[string]string `json:"capability_errors"`
	// PodSelectors contains SDK classification rules, not workload ownership rules.
	PodSelectors map[string]*v1alpha1.PodSelector `json:"pod_selectors"`
}

// inspect extracts independent capabilities separately: an upstream scale error
// must not discard successful status, instance, or template observations.
func inspect(ctx context.Context, object *unstructured.Unstructured, definition *v1alpha1.Karta, root *resource.Component, components []*resource.Component) (report, error) {
	definitionJSON, err := json.Marshal(definition)
	if err != nil {
		return report{}, errors.Wrap(err, "encode definition provenance")
	}
	result := report{
		SDKVersion: linkedSDKVersion(), DefinitionName: definition.Name,
		DefinitionSHA256:    fmt.Sprintf("%x", sha256.Sum256(definitionJSON)),
		InputIdentitySource: "input_metadata", APIVersion: object.GetAPIVersion(), Kind: object.GetKind(),
		Namespace: object.GetNamespace(), Name: object.GetName(), UID: string(object.GetUID()),
		SuspendSupported: root.HasSuspendDefinition(), Statuses: []string{},
		Components: map[string]map[string]instanceReport{}, CapabilityErrors: map[string]string{},
		PodSelectors: map[string]*v1alpha1.PodSelector{},
	}
	status, err := root.GetStatus(ctx)
	if err != nil {
		result.CapabilityErrors["status"] = err.Error()
	} else if status != nil {
		for _, matched := range status.MatchedStatuses {
			result.Statuses = append(result.Statuses, string(matched))
		}
	}
	sort.Strings(result.Statuses)
	for _, component := range components {
		name := component.Name()
		prefix := "components." + name + "."
		result.PodSelectors[name] = component.GetPodSelector()
		instances, err := component.GetInstanceIds(ctx)
		if err != nil {
			result.CapabilityErrors[prefix+"instances"] = err.Error()
			continue
		}
		templates, err := component.GetPodTemplateSpec(ctx)
		if err != nil {
			result.CapabilityErrors[prefix+"templates"] = err.Error()
		}
		scales, err := component.GetScale(ctx)
		if err != nil {
			result.CapabilityErrors[prefix+"scale"] = err.Error()
		}
		result.Components[name] = map[string]instanceReport{}
		for _, id := range instances {
			item := instanceReport{PodLabels: map[string]string{}}
			if scale, ok := scales[id]; ok {
				item.Replicas = scale.Replicas
			}
			if template, ok := templates[id]; ok && template.Labels != nil {
				item.PodLabels = template.Labels
			}
			result.Components[name][id] = item
		}
	}
	return result, nil
}
