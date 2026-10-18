// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package producer wires the asynchronous audit buffer to a delivery destination.
package producer

import (
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/buffer"
	"go.uber.org/zap"
)

// Options configures one process-local audit producer.
type Options struct {
	// Destination delivers buffered events using the transport owned by the process.
	Destination audit.EventDestination[audit.ProducerEvent]
	// MaxBatchSize limits events sent in one delivery attempt.
	MaxBatchSize int
	// BufferSize bounds events waiting behind the active delivery batch.
	BufferSize int
	// FlushInterval bounds how long a partial batch waits before delivery.
	FlushInterval time.Duration
	// Logger records delivery and drop outcomes without event payloads.
	Logger *zap.Logger
}

// Start returns a best-effort sink and a stop function that drains queued events.
func Start(options Options) (audit.Sink, func()) {
	logger := options.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	eventBuffer := buffer.NewInMemory(
		buffer.WithMaxBatchSize[audit.ProducerEvent](options.MaxBatchSize),
		buffer.WithBufferSize[audit.ProducerEvent](options.BufferSize),
		buffer.WithFlushInterval[audit.ProducerEvent](options.FlushInterval),
		buffer.WithEventDestination(options.Destination),
		buffer.WithLogger[audit.ProducerEvent](logger),
	)
	eventBuffer.Start()
	return eventBuffer, func() { eventBuffer.Stop(buffer.DefaultStopTimeout) }
}
