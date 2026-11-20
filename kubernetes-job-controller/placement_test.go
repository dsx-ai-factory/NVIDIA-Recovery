// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestJobSetPlacementHandlesEitherInformerOrder(t *testing.T) {
	workload := object(jobSetKind.GroupVersion().String(), jobSetKind.Kind, "training", "root", firstJobID)
	parent := object("batch/v1", "Job", "worker", "batch", "")
	addOwner(parent, workload)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: parent.GetNamespace(), UID: "worker", Labels: map[string]string{jobIDLabel: firstJobID},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "batch/v1", Kind: "Job", UID: parent.GetUID(), Controller: new(true)},
			},
		},
		Spec: corev1.PodSpec{NodeName: "node-z"},
	}
	earlier := pod.DeepCopy()
	earlier.UID, earlier.Spec.NodeName = "other-worker", "node-a"

	index := newPlacementIndex(jobSetKind)
	index.updatePod(nil, pod)
	if got := index.allocatedNodes(workload, firstJobID, 0); len(got) != 0 {
		t.Fatalf("placed Pod without a verified parent: %v", got)
	}
	index.updateJob(nil, parent)
	index.updatePod(nil, earlier)
	if got := index.allocatedNodes(workload, firstJobID, 0); !slices.Equal(got, []string{"node-a", "node-z"}) {
		t.Fatalf("nodes after both informer streams are ready: %v", got)
	}
	index.updateJob(parent, nil)
	replacement := parent.DeepCopy()
	replacement.SetUID("replacement-batch-uid")
	index.updateJob(nil, replacement)
	if got := index.allocatedNodes(workload, firstJobID, 0); len(got) != 0 {
		t.Fatalf("a replacement parent must not inherit stale Pod placements: %v", got)
	}
}
