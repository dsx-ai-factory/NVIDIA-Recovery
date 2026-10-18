// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package destinations

import (
	"context"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"go.uber.org/zap"
)

// Logging writes audit event metadata to the application log.
// Payload is intentionally omitted because it may contain arbitrary data.
type Logging struct {
	logger *zap.Logger
}

// NewLogging constructs a logging audit destination.
func NewLogging(logger *zap.Logger) *Logging {
	return &Logging{logger: logger}
}

// Deliver logs every event and reports the whole batch as delivered. Logging
// is the default destination, so it also honors ctx: a stuck log sink (e.g. a
// blocked disk or remote syslog core) must not hold up Stop's deadline.
func (d *Logging) Deliver(ctx context.Context, events []audit.ProducerEvent) (int, error) {
	for i, event := range events {
		select {
		case <-ctx.Done():
			return i, ctx.Err()
		default:
		}

		d.logger.Info("sent audit event",
			zap.String("heimdall_job_id", event.HeimdallJobID),
			zap.String("event_producer", string(event.EventProducer)),
			zap.String("event_type", string(event.EventType)),
			zap.String("event_level", string(event.EventLevel)),
			zap.Int64("event_produced_at", event.EventProducedAt),
			zap.String("idempotency_key", event.IdempotencyKey),
		)
	}

	return len(events), nil
}

var _ audit.EventDestination[audit.ProducerEvent] = (*Logging)(nil)
