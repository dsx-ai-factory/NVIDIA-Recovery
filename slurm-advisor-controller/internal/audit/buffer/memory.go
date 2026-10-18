// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"context"
	"time"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit"
	"go.uber.org/zap"
)

const (
	defaultBufferSize    = 1000
	defaultFlushInterval = 30 * time.Second
	defaultMaxBatchSize  = 100

	// DefaultStopTimeout bounds how long Stop waits for a blocked flush
	// before canceling in-flight delivery and draining the remainder.
	DefaultStopTimeout = 5 * time.Second
)

type noOpEventDestination[T any] struct{}

func (noOpEventDestination[T]) Deliver(_ context.Context, items []T) (int, error) {
	return len(items), nil
}

// InMemory maintains a bounded queue for the delivery events. Events are sent
// in batches. This is **best-effort** only, and events are dropped when the
// queue is full.
//
// Call Start before Producers start producing the events. Ensure all event
// producers stop producing events before calling Stop. Once Stop is called,
// this cannot be reused.
type InMemory[T any] struct {
	maxBatchSize  int
	bufferSize    int
	flushInterval time.Duration

	receiveCh     chan T
	flusherDoneCh chan struct{}

	eventDestination audit.EventDestination[T]
	logger           *zap.Logger

	// runCtx is an internal run context that will be cancelled with a
	// deadline, after Stop(deadline) is called.
	runCtx    context.Context
	cancelRun context.CancelFunc
}

// Option configures an InMemory buffer before it starts.
type Option[T any] func(*InMemory[T])

// WithMaxBatchSize configures the maximum number of events in one flush.
func WithMaxBatchSize[T any](maxBatchSize int) Option[T] {
	return func(b *InMemory[T]) {
		b.maxBatchSize = maxBatchSize
	}
}

// WithFlushInterval configures how often a nonempty partial batch is flushed.
func WithFlushInterval[T any](flushInterval time.Duration) Option[T] {
	return func(b *InMemory[T]) {
		b.flushInterval = flushInterval
	}
}

// WithBufferSize configures the number of events that may wait behind the
// worker's current batch.
func WithBufferSize[T any](bufferSize int) Option[T] {
	return func(b *InMemory[T]) {
		b.bufferSize = bufferSize
	}
}

// WithEventDestination configures the synchronous batch destination.
func WithEventDestination[T any](destination audit.EventDestination[T]) Option[T] {
	return func(b *InMemory[T]) {
		b.eventDestination = destination
	}
}

// WithLogger configures structured logging for terminal event drops.
func WithLogger[T any](logger *zap.Logger) Option[T] {
	return func(b *InMemory[T]) {
		b.logger = logger
	}
}

// NewInMemory constructs a not-yet-started in-memory buffer.
func NewInMemory[T any](opts ...Option[T]) *InMemory[T] {
	b := &InMemory[T]{
		maxBatchSize:     defaultMaxBatchSize,
		bufferSize:       defaultBufferSize,
		flushInterval:    defaultFlushInterval,
		eventDestination: noOpEventDestination[T]{},
		logger:           zap.NewNop(),
	}

	for _, opt := range opts {
		opt(b)
	}

	b.validate()
	return b
}

func (b *InMemory[T]) validate() {
	if b.maxBatchSize <= 0 {
		panic("buffer: max batch size must be positive")
	}
	if b.bufferSize <= 0 {
		panic("buffer: buffer size must be positive")
	}
	if b.flushInterval <= 0 {
		panic("buffer: flush interval must be positive")
	}
	if b.eventDestination == nil {
		panic("buffer: nil event destination")
	}
	if b.logger == nil {
		panic("buffer: nil logger")
	}
}

// Start starts the delivery worker. An InMemory buffer cannot be restarted.
func (b *InMemory[T]) Start() {
	b.receiveCh = make(chan T, b.bufferSize)
	b.flusherDoneCh = make(chan struct{})

	b.runCtx, b.cancelRun = context.WithCancel(context.Background())

	go b.startSender()
}

// Stop closes the receive channel and waits up to timeout for the flusher to
// drain gracefully. If it has not finished by then, Stop cancels the run
// context so in-flight and remaining delivery can observe cancellation, then
// waits up to timeout again for the flusher to exit. A destination that
// ignores ctx entirely can still exceed that second window; Stop then gives
// up and returns rather than blocking the caller forever, logging that the
// flusher was abandoned. The flusher goroutine is not leaked in the sense
// that matters here: it terminates independently whenever the destination
// finally returns, and does not block process exit either way.
func (b *InMemory[T]) Stop(timeout time.Duration) {
	close(b.receiveCh)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-b.flusherDoneCh:
		b.cancelRun()
		return
	case <-timer.C:
	}

	b.cancelRun()

	forceTimer := time.NewTimer(timeout)
	defer forceTimer.Stop()

	select {
	case <-b.flusherDoneCh:
	case <-forceTimer.C:
		b.logger.Warn("audit buffer flusher did not exit within the stop deadline; abandoning it")
	}
}

func (b *InMemory[T]) startSender() {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()
	defer close(b.flusherDoneCh)

	batch := make([]T, 0, b.maxBatchSize)

	for {
		select {
		case <-ticker.C:
			b.sendIfNeeded(b.runCtx, batch)
			batch = make([]T, 0, b.maxBatchSize)

		case item, ok := <-b.receiveCh:
			if !ok {
				b.sendIfNeeded(b.runCtx, batch)
				return
			}

			batch = append(batch, item)
			if len(batch) == b.maxBatchSize {
				b.sendIfNeeded(b.runCtx, batch)
				batch = make([]T, 0, b.maxBatchSize)
				ticker.Reset(b.flushInterval)
			}
		}
	}
}

func (b *InMemory[T]) sendIfNeeded(ctx context.Context, items []T) {
	if len(items) == 0 {
		return
	}

	delivered, err := b.eventDestination.Deliver(ctx, items)
	dropped := len(items) - delivered

	if dropped > 0 || err != nil {
		b.logger.Warn("audit event batch was not fully delivered",
			zap.Int("batch_size", len(items)),
			zap.Int("delivered_count", delivered),
			zap.Int("dropped_count", dropped),
			zap.Error(err),
		)
	}
}

// Add events to the processing queue. If it is blocked, **all remaining items**
// are dropped. Event producers MUST NOT call Add after calling Stop.
func (b *InMemory[T]) Add(items ...T) {
	if len(items) == 0 {
		return
	}

	dropped := 0
	for index, item := range items {
		select {
		case b.receiveCh <- item:
		default:
			dropped = len(items) - index
		}
		if dropped > 0 {
			break
		}
	}
	if dropped > 0 {
		b.logger.Warn("audit event buffer full; dropping events",
			zap.Int("event_count", dropped),
		)
	}
}
