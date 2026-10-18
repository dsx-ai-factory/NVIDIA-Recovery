// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"context"
	"sort"
	"sync"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/cluster"
)

type SeenJobRecord = cluster.JobAttemptRef

type SeenJobStore interface {
	ListSeenJobs(ctx context.Context) ([]SeenJobRecord, error)
	MarkSeen(ctx context.Context, record SeenJobRecord) error
	Forget(ctx context.Context, record SeenJobRecord) error
	CountSeenJobs() int
}

type MemorySeenJobStore struct {
	mu      sync.Mutex
	records map[SeenJobRecord]struct{}
}

func NewMemorySeenJobStore(records ...SeenJobRecord) *MemorySeenJobStore {
	store := &MemorySeenJobStore{
		records: map[SeenJobRecord]struct{}{},
	}
	for _, record := range records {
		store.records[record] = struct{}{}
	}

	return store
}

func (s *MemorySeenJobStore) ListSeenJobs(context.Context) ([]SeenJobRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]SeenJobRecord, 0, len(s.records))
	for record := range s.records {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].JobID == out[j].JobID {
			return out[i].AttemptIndex < out[j].AttemptIndex
		}
		return out[i].JobID < out[j].JobID
	})

	return out, nil
}

func (s *MemorySeenJobStore) MarkSeen(_ context.Context, record SeenJobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.records[record] = struct{}{}

	return nil
}

func (s *MemorySeenJobStore) Forget(_ context.Context, record SeenJobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.records, record)

	return nil
}

func (s *MemorySeenJobStore) CountSeenJobs() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.records)
}
