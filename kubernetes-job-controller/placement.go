// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"maps"
	"slices"
	"strconv"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ownerKey struct {
	namespace string
	uid       types.UID
}

type placementKey struct {
	ownerKey
	id      string
	attempt string
}

type podPlacement struct {
	placementKey
	node     string
	childJob bool
}

type membershipChanges map[placementKey]struct{}

// placementIndex consumes ordered events within each informer. The mutex
// serializes the independent Pod and Job streams and lets reconcile take a
// consistent snapshot. Pending child placements survive either arrival order.
type placementIndex struct {
	mu       sync.Mutex
	kind     schema.GroupVersionKind
	nodes    map[placementKey]map[string]int
	jobs     map[ownerKey]types.UID
	pods     map[types.UID]podPlacement
	children map[ownerKey]map[types.UID]struct{}
}

func newPlacementIndex(kind schema.GroupVersionKind) *placementIndex {
	return &placementIndex{
		kind: kind, nodes: make(map[placementKey]map[string]int),
		jobs: make(map[ownerKey]types.UID), pods: make(map[types.UID]podPlacement),
		children: make(map[ownerKey]map[types.UID]struct{}),
	}
}

func (p *placementIndex) placementForPod(obj client.Object) podPlacement {
	if obj == nil {
		return podPlacement{}
	}
	pod := obj.(*corev1.Pod)
	owner := metav1.GetControllerOfNoCopy(pod)
	if owner == nil || owner.UID == "" || pod.Spec.NodeName == "" || pod.Labels[jobIDLabel] == "" {
		return podPlacement{}
	}
	childJob := p.kind == jobSetKind && owner.Kind == batchJobKind.Kind && owner.APIVersion == batchJobKind.GroupVersion().String()
	if !childJob && !ownerOfKind(owner, p.kind) {
		return podPlacement{}
	}
	attempt := pod.Labels[attemptLabel]
	if attempt == "" {
		attempt = "0"
	}
	return podPlacement{
		placementKey: placementKey{ownerKey{pod.Namespace, owner.UID}, pod.Labels[jobIDLabel], attempt},
		node:         pod.Spec.NodeName, childJob: childJob,
	}
}

func (p *placementIndex) updatePod(old, current client.Object) membershipChanges {
	after := p.placementForPod(current)
	uid := objectUID(current)
	p.mu.Lock()
	defer p.mu.Unlock()
	var changes membershipChanges
	if previousUID := objectUID(old); previousUID != uid {
		p.setPod(previousUID, podPlacement{}, &changes)
	}
	p.setPod(uid, after, &changes)
	return changes
}

func objectUID(obj client.Object) types.UID {
	if obj == nil {
		return ""
	}
	return obj.GetUID()
}

func (p *placementIndex) setPod(uid types.UID, after podPlacement, changes *membershipChanges) {
	before := p.pods[uid]
	if before == after {
		return
	}
	if before.node != "" {
		p.changePod(before, uid, -1, changes)
		delete(p.pods, uid)
	}
	if after.node != "" {
		p.pods[uid] = after
		p.changePod(after, uid, 1, changes)
	}
}

func (p *placementIndex) changePod(pod podPlacement, uid types.UID, delta int, changes *membershipChanges) {
	key := pod.placementKey
	if pod.childJob {
		children := p.children[key.ownerKey]
		if delta > 0 {
			if children == nil {
				children = make(map[types.UID]struct{})
				p.children[key.ownerKey] = children
			}
			children[uid] = struct{}{}
		} else {
			delete(children, uid)
			if len(children) == 0 {
				delete(p.children, key.ownerKey)
			}
		}
		key.uid = p.jobs[key.ownerKey]
	}
	p.adjust(key, pod.node, delta, changes)
}

// Job owner changes revisit only that Job's tracked children. Ordinary Job
// resource-version and status updates return without touching the index.
func (p *placementIndex) updateJob(old, current client.Object) membershipChanges {
	before, _ := jobSetOwner(old)
	after, root := jobSetOwner(current)
	p.mu.Lock()
	defer p.mu.Unlock()
	var changes membershipChanges
	if before != after {
		p.setJob(before, "", &changes)
	}
	p.setJob(after, root, &changes)
	return changes
}

func jobSetOwner(obj client.Object) (ownerKey, types.UID) {
	if obj == nil {
		return ownerKey{}, ""
	}
	key := ownerKey{obj.GetNamespace(), obj.GetUID()}
	owner := metav1.GetControllerOfNoCopy(obj)
	if !ownerOfKind(owner, jobSetKind) {
		return key, ""
	}
	return key, owner.UID
}

func (p *placementIndex) setJob(job ownerKey, root types.UID, changes *membershipChanges) {
	previous := p.jobs[job]
	if previous == root {
		return
	}
	if root == "" {
		delete(p.jobs, job)
	} else {
		p.jobs[job] = root
	}
	for uid := range p.children[job] {
		pod := p.pods[uid]
		key := pod.placementKey
		key.uid = previous
		p.adjust(key, pod.node, -1, changes)
		key.uid = root
		p.adjust(key, pod.node, 1, changes)
	}
}

func (p *placementIndex) adjust(key placementKey, node string, delta int, changes *membershipChanges) {
	if key.uid == "" {
		return
	}
	counts := p.nodes[key]
	previous := counts[node]
	current := previous + delta
	switch {
	case previous == 0 && current > 0:
		if counts == nil {
			counts = make(map[string]int)
			p.nodes[key] = counts
		}
		counts[node] = current
	case previous > 0 && current == 0:
		delete(counts, node)
		if len(counts) == 0 {
			delete(p.nodes, key)
		}
	case current > 0:
		counts[node] = current
	default:
		return
	}
	if previous == 0 || current == 0 {
		if *changes == nil {
			*changes = make(membershipChanges)
		}
		(*changes)[key] = struct{}{}
	}
}

func (p *placementIndex) allocatedNodes(workload client.Object, id string, attempt int32) []string {
	key := placementKey{ownerKey{workload.GetNamespace(), workload.GetUID()}, id, strconv.FormatInt(int64(attempt), 10)}
	p.mu.Lock()
	nodes := slices.Collect(maps.Keys(p.nodes[key]))
	p.mu.Unlock()
	// This list describes membership. Kubernetes has no common contract
	// that maps a Pod's placement to global training ranks.
	slices.Sort(nodes)
	return nodes
}
