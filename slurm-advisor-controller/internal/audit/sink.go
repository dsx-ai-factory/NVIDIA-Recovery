// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package audit

// MaxGatewayBatchSize is the largest event batch accepted by Notifications.
const MaxGatewayBatchSize = 1000

// Sink accepts audit events without exposing their delivery implementation.
type Sink interface {
	// Add enqueues events for asynchronous delivery.
	Add(...ProducerEvent)
}

// NopSink discards audit events.
type NopSink struct{}

// Add discards events without blocking.
func (NopSink) Add(...ProducerEvent) {}
