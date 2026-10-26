// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"reflect"
	"sync"

	heimdallclient "github.com/dsx-ai-factory/nvidia-recovery/advice-operator/internal/client"
)

// AdviceCache provides thread-safe storage for API advice data.
// The syncer populates this cache from the Heimdall API, and the reconciler
// reads from it to determine the desired state for each Advice CRD.
type AdviceCache struct {
	mu sync.RWMutex
	// adviceByID maps HeimdallID to APIAdvice
	adviceByID map[string]*heimdallclient.APIAdvice
	// adviceByJobID maps JobID to a list of HeimdallIDs (for lookup by job)
	adviceByJobID map[string][]string
	// allIDs stores all HeimdallIDs currently in the cache
	allIDs map[string]struct{}
}

// NewAdviceCache creates a new AdviceCache.
func NewAdviceCache() *AdviceCache {
	return &AdviceCache{
		adviceByID:    make(map[string]*heimdallclient.APIAdvice),
		adviceByJobID: make(map[string][]string),
		allIDs:        make(map[string]struct{}),
	}
}

// RemovedAdvice contains info about an advice item that was removed from the API.
// This is used to emit events for deletion without needing to query the cluster.
type RemovedAdvice struct {
	ID        string
	Namespace string
}

// Update replaces the entire cache with new advice data from the API.
// Returns lists of IDs that were added, updated, and removed items with namespace info.
func (c *AdviceCache) Update(advice []heimdallclient.APIAdvice) (added, updated []string, removed []RemovedAdvice) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Build new cache
	newByID := make(map[string]*heimdallclient.APIAdvice)
	newByJobID := make(map[string][]string)
	newAllIDs := make(map[string]struct{})

	for i := range advice {
		adv := &advice[i]
		id := adv.HeimdallID
		newByID[id] = adv
		newAllIDs[id] = struct{}{}
		newByJobID[adv.JobID] = append(newByJobID[adv.JobID], id)

		// Check if this is new or updated
		if existing, exists := c.adviceByID[id]; exists {
			if adviceNamespace(existing) != adviceNamespace(adv) {
				removed = append(removed, RemovedAdvice{ID: id, Namespace: adviceNamespace(existing)})
			}
			// Only mark as updated if content actually changed
			if !adviceEqual(existing, adv) {
				updated = append(updated, id)
			}
		} else {
			added = append(added, id)
		}
	}

	// Find removed items - capture namespace before clearing
	for id, oldAdvice := range c.adviceByID {
		if _, exists := newAllIDs[id]; !exists {
			removed = append(removed, RemovedAdvice{ID: id, Namespace: adviceNamespace(oldAdvice)})
		}
	}

	// Replace cache
	c.adviceByID = newByID
	c.adviceByJobID = newByJobID
	c.allIDs = newAllIDs

	return added, updated, removed
}

// adviceEqual compares two APIAdvice structs for equality.
// Returns true if they are semantically equal (same content).
func adviceEqual(a, b *heimdallclient.APIAdvice) bool {
	if a.JobID != b.JobID || a.AttemptIndex != b.AttemptIndex {
		return false
	}

	if !reflect.DeepEqual(a.SchedulerMetadata, b.SchedulerMetadata) {
		return false
	}

	// Compare advice state
	if (a.Advice.State == nil) != (b.Advice.State == nil) {
		return false
	}
	if a.Advice.State != nil && b.Advice.State != nil && *a.Advice.State != *b.Advice.State {
		return false
	}

	// Compare node constraints
	if (a.Advice.NodeConstraints == nil) != (b.Advice.NodeConstraints == nil) {
		return false
	}
	if a.Advice.NodeConstraints != nil && b.Advice.NodeConstraints != nil {
		if !stringSlicesEqual(a.Advice.NodeConstraints.Exclusions, b.Advice.NodeConstraints.Exclusions) {
			return false
		}
		if !stringSlicesEqual(a.Advice.NodeConstraints.Inclusions, b.Advice.NodeConstraints.Inclusions) {
			return false
		}
	}

	if !reflect.DeepEqual(a.Issues, b.Issues) {
		return false
	}

	return true
}

func adviceNamespace(advice *heimdallclient.APIAdvice) string {
	if advice == nil {
		return "default"
	}

	namespace := advice.GetNamespace()
	if namespace == "" {
		return "default"
	}

	return namespace
}

// stringSlicesEqual compares two string slices for equality.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Get retrieves advice by HeimdallID.
// Returns nil if not found.
func (c *AdviceCache) Get(id string) *heimdallclient.APIAdvice {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.adviceByID[id]
}

// GetByJobID retrieves all advice for a given JobID.
func (c *AdviceCache) GetByJobID(jobID string) []*heimdallclient.APIAdvice {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ids := c.adviceByJobID[jobID]
	result := make([]*heimdallclient.APIAdvice, 0, len(ids))
	for _, id := range ids {
		if adv := c.adviceByID[id]; adv != nil {
			result = append(result, adv)
		}
	}
	return result
}

// List returns all advice in the cache.
func (c *AdviceCache) List() []*heimdallclient.APIAdvice {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]*heimdallclient.APIAdvice, 0, len(c.adviceByID))
	for _, adv := range c.adviceByID {
		result = append(result, adv)
	}
	return result
}

// AllIDs returns all HeimdallIDs in the cache.
func (c *AdviceCache) AllIDs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	result := make([]string, 0, len(c.allIDs))
	for id := range c.allIDs {
		result = append(result, id)
	}
	return result
}

// Exists checks if an advice with the given ID exists in the cache.
func (c *AdviceCache) Exists(id string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.allIDs[id]
	return exists
}
