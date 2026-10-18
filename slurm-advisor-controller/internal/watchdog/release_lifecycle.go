// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package watchdog

import (
	"context"
	"time"
)

// ReleaseAttributes identify one watchdog scheduler release command.
type ReleaseAttributes struct {
	// JobID identifies the scheduler job.
	JobID string
	// AttemptIndex identifies the held scheduler attempt.
	AttemptIndex int32
}

// ReleaseResult describes a returned watchdog scheduler release command.
type ReleaseResult struct {
	// CompletedAt records when the scheduler command returned.
	CompletedAt time.Time
	// Err is the scheduler command error when present.
	Err error
}

// ReleaseScope receives the result of one watchdog scheduler release command.
type ReleaseScope interface {
	// EndReleaseScope completes the watchdog release lifecycle scope.
	EndReleaseScope(ReleaseResult)
}

// ReleaseLifecycleObserver observes watchdog scheduler releases independently of delivery.
type ReleaseLifecycleObserver interface {
	// StartReleaseScope starts one watchdog release lifecycle scope.
	StartReleaseScope(context.Context, ReleaseAttributes) (context.Context, ReleaseScope)
}

// NopReleaseLifecycleObserver preserves contexts and discards release results.
type NopReleaseLifecycleObserver struct{}

// StartReleaseScope returns the input context and a no-op scope.
func (NopReleaseLifecycleObserver) StartReleaseScope(
	ctx context.Context,
	_ ReleaseAttributes,
) (context.Context, ReleaseScope) {
	return ctx, nopReleaseScope{}
}

type nopReleaseScope struct{}

func (nopReleaseScope) EndReleaseScope(ReleaseResult) {}
