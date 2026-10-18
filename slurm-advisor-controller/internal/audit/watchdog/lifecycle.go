// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package watchdog projects watchdog scheduler releases into audit events.
package watchdog

import (
	"context"
	"sync"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/jobidentity"
	watchdogengine "github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/watchdog"
	"github.com/google/uuid"
)

// Observer projects watchdog release results into audit events.
type Observer struct {
	sink  audit.Sink
	now   func() time.Time
	newID func() string
}

// NewObserver constructs a watchdog audit lifecycle observer.
func NewObserver(sink audit.Sink) *Observer {
	if sink == nil {
		sink = audit.NopSink{}
	}
	return &Observer{
		sink:  sink,
		now:   time.Now,
		newID: uuid.NewString,
	}
}

// StartReleaseScope preserves the supplied context and begins one watchdog release scope.
func (observer *Observer) StartReleaseScope(
	ctx context.Context,
	attributes watchdogengine.ReleaseAttributes,
) (context.Context, watchdogengine.ReleaseScope) {
	return ctx, &releaseAuditScope{
		observer:       observer,
		attributes:     attributes,
		idempotencyKey: observer.newID(),
	}
}

type releaseAuditScope struct {
	once           sync.Once
	observer       *Observer
	attributes     watchdogengine.ReleaseAttributes
	idempotencyKey string
}

func (scope *releaseAuditScope) EndReleaseScope(result watchdogengine.ReleaseResult) {
	scope.once.Do(func() {
		producedAt := result.CompletedAt
		if producedAt.IsZero() {
			producedAt = scope.observer.now()
		}
		errorMessage := ""
		if result.Err != nil {
			errorMessage = result.Err.Error()
		}
		scope.observer.sink.Add(audit.WatchdogReleaseCompletedEvent{
			HeimdallJobID: jobidentity.HeimdallJobID(
				scope.attributes.JobID,
				scope.attributes.AttemptIndex,
			),
			JobID:          scope.attributes.JobID,
			AttemptIndex:   scope.attributes.AttemptIndex,
			Success:        result.Err == nil,
			Error:          errorMessage,
			ProducedAt:     producedAt,
			IdempotencyKey: scope.idempotencyKey,
		}.ToProducerEvent())
	})
}

var _ watchdogengine.ReleaseLifecycleObserver = (*Observer)(nil)
