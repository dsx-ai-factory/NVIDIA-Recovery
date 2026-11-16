// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/errors"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/catalog"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/instructions"
	"github.com/dsx-ai-factory/workload-map/karta/sdk/pkg/resource"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type podClassification struct {
	// UID is echoed from the candidate Pod to correlate independent evidence.
	UID string `json:"uid"`
	// Name is echoed from the candidate Pod.
	Name string `json:"name"`
	// Component is inferred by the SDK, not by native controller labels here.
	Component string `json:"component"`
	// InstanceID is inferred by the SDK; empty means an unnamed single instance.
	InstanceID string `json:"instance_id"`
	// Error records a classification failure without discarding other Pods.
	Error string `json:"error,omitempty"`
}

type associationReport struct {
	// Scope makes explicit that SDK classification does not establish ownership.
	Scope string `json:"scope"`
	// Pods retains one classification result per input candidate, in input order.
	Pods []podClassification `json:"pods"`
}

// associatePods exercises SDK classification on independently scoped candidates.
// In particular, JobSet's component label cannot distinguish different parents;
// callers must establish workload ownership before passing candidate Pods.
func associatePods(ctx context.Context, data []byte) (associationReport, error) {
	var input struct {
		Workload *unstructured.Unstructured `json:"workload"`
		Pods     []corev1.Pod               `json:"pods"`
	}
	if err := json.Unmarshal(data, &input); err != nil {
		return associationReport{}, errors.Wrap(err, "decode workload and Pod candidates")
	}
	if input.Workload == nil || input.Workload.GetKind() == "" || input.Workload.GetAPIVersion() == "" || input.Pods == nil {
		return associationReport{}, errors.New("associate-pods requires workload and pods array")
	}
	definition, err := catalog.Get(input.Workload.GroupVersionKind())
	if err != nil {
		return associationReport{}, errors.Wrap(err, "resolve upstream definition")
	}
	summary, err := instructions.NewStructureSummary(definition)
	if err != nil {
		return associationReport{}, errors.Wrap(err, "summarize upstream definition")
	}
	factory := resource.NewComponentFactoryFromObject(definition, input.Workload)
	result := associationReport{Scope: "caller_supplied_candidates", Pods: []podClassification{}}
	for i := range input.Pods {
		pod := &input.Pods[i]
		item := podClassification{UID: string(pod.UID), Name: pod.Name}
		querier := resource.NewPodQuerier(pod)
		component, err := instructions.InferPodComponent(ctx, querier, summary)
		if err != nil {
			item.Error = err.Error()
		} else {
			item.Component = component
			instance, err := instructions.InferPodComponentInstance(ctx, querier, component, factory)
			if err != nil {
				item.Error = err.Error()
			} else if instance != nil {
				item.InstanceID = *instance
			}
		}
		result.Pods = append(result.Pods, item)
	}
	return result, nil
}
