// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package buffer_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
	zapobserver "go.uber.org/zap/zaptest/observer"

	"github.com/dsx-ai-factory/nvidia-recovery/slurm-advisor-controller/internal/audit/buffer"
)

const testWaitTimeout = time.Second

func TestInMemoryDropsEventsWhenBufferIsFull(t *testing.T) {
	logCore, observedLogs := zapobserver.New(zap.WarnLevel)
	recorder := newRecordingDestination[int]()
	destination := newFirstFlushBlockingDestination(recorder)
	defer destination.release()

	b := buffer.NewInMemory(
		buffer.WithMaxBatchSize[int](1),
		buffer.WithBufferSize[int](2),
		buffer.WithFlushInterval[int](time.Hour),
		buffer.WithEventDestination[int](destination),
		buffer.WithLogger[int](zap.New(logCore)),
	)
	b.Start()

	b.Add(1)
	assertBatch(t, receiveBatch(t, destination.startedCh), []int{1})

	// The worker is blocked flushing 1, so only 2 and 3 fit in the queue.
	b.Add(2, 3, 4, 5)
	destination.release()
	stopGracefully(t, b)

	if diff := cmp.Diff([][]int{{1}, {2}, {3}}, recorder.batchesSnapshot()); diff != "" {
		t.Errorf("flushed batches mismatch (-want +got):\n%s", diff)
	}
	if got := observedLogs.FilterMessage("audit event buffer full; dropping events").Len(); got != 1 {
		t.Errorf("buffer-full log count = %d, want 1", got)
	}
}

func TestInMemoryFlushesPartialBatchOnInterval(t *testing.T) {
	destination := newRecordingDestination[int]()
	b := buffer.NewInMemory(
		buffer.WithMaxBatchSize[int](4),
		buffer.WithBufferSize[int](4),
		buffer.WithFlushInterval[int](10*time.Millisecond),
		buffer.WithEventDestination[int](destination),
	)
	b.Start()

	b.Add(1)
	assertBatch(t, receiveBatch(t, destination.callsCh), []int{1})
	stopGracefully(t, b)

	if diff := cmp.Diff([][]int{{1}}, destination.batchesSnapshot()); diff != "" {
		t.Errorf("flushed batches mismatch (-want +got):\n%s", diff)
	}
}

func TestInMemoryFlushesWhenBatchIsFull(t *testing.T) {
	destination := newRecordingDestination[int]()
	b := buffer.NewInMemory(
		buffer.WithMaxBatchSize[int](3),
		buffer.WithBufferSize[int](3),
		buffer.WithFlushInterval[int](time.Hour),
		buffer.WithEventDestination[int](destination),
	)
	b.Start()

	b.Add(1, 2, 3)
	assertBatch(t, receiveBatch(t, destination.callsCh), []int{1, 2, 3})
	stopGracefully(t, b)

	if diff := cmp.Diff([][]int{{1, 2, 3}}, destination.batchesSnapshot()); diff != "" {
		t.Errorf("flushed batches mismatch (-want +got):\n%s", diff)
	}
}

func TestInMemoryFlushesRemainingEventsDuringGracefulShutdown(t *testing.T) {
	destination := newRecordingDestination[int]()
	b := buffer.NewInMemory(
		buffer.WithMaxBatchSize[int](4),
		buffer.WithBufferSize[int](4),
		buffer.WithFlushInterval[int](time.Hour),
		buffer.WithEventDestination[int](destination),
	)
	b.Start()

	b.Add(1, 2, 3)
	stopGracefully(t, b)

	if diff := cmp.Diff([][]int{{1, 2, 3}}, destination.batchesSnapshot()); diff != "" {
		t.Errorf("flushed batches mismatch (-want +got):\n%s", diff)
	}
}

func TestInMemoryStopWaitsForBlockedFlushAndDrainsRemainingEvents(t *testing.T) {
	recorder := newRecordingDestination[int]()
	destination := newFirstFlushBlockingDestination(recorder)
	defer destination.release()

	b := buffer.NewInMemory(
		buffer.WithMaxBatchSize[int](1),
		buffer.WithBufferSize[int](2),
		buffer.WithFlushInterval[int](time.Hour),
		buffer.WithEventDestination[int](destination),
	)
	b.Start()

	b.Add(1)
	assertBatch(t, receiveBatch(t, destination.startedCh), []int{1})

	// These events remain queued while the destination is stuck on 1.
	b.Add(2, 3)
	stoppedCh := make(chan struct{})
	go func() {
		b.Stop(testWaitTimeout)
		close(stoppedCh)
	}()

	select {
	case <-stoppedCh:
		t.Fatal("Stop() returned while the destination was blocked")
	case <-time.After(10 * time.Millisecond):
	}

	destination.release()
	waitForStop(t, stoppedCh)

	if diff := cmp.Diff([][]int{{1}, {2}, {3}}, recorder.batchesSnapshot()); diff != "" {
		t.Errorf("flushed batches mismatch (-want +got):\n%s", diff)
	}
}

func TestInMemoryStopBoundsWaitAndCancelsRunContext(t *testing.T) {
	destination := newCtxAwareDestination[int]()
	b := buffer.NewInMemory(
		buffer.WithMaxBatchSize[int](1),
		buffer.WithBufferSize[int](1),
		buffer.WithFlushInterval[int](time.Hour),
		buffer.WithEventDestination[int](destination),
	)
	b.Start()

	b.Add(1)
	assertBatch(t, receiveBatch(t, destination.startedCh), []int{1})

	// The destination never releases on its own; only Stop's deadline can
	// unblock it.
	const stopTimeout = 20 * time.Millisecond
	stoppedCh := make(chan struct{})
	start := time.Now()
	go func() {
		b.Stop(stopTimeout)
		close(stoppedCh)
	}()

	waitForStop(t, stoppedCh)
	if elapsed := time.Since(start); elapsed > testWaitTimeout {
		t.Fatalf("Stop() took %v, want it bounded near the %v deadline", elapsed, stopTimeout)
	}

	select {
	case <-destination.canceledCh:
	case <-time.After(testWaitTimeout):
		t.Fatal("Deliver's context was never canceled by Stop's deadline")
	}
}

func TestInMemoryStopAbandonsDestinationThatIgnoresCancellation(t *testing.T) {
	logCore, observedLogs := zapobserver.New(zap.WarnLevel)
	destination := newUncooperativeDestination[int]()
	b := buffer.NewInMemory(
		buffer.WithMaxBatchSize[int](1),
		buffer.WithBufferSize[int](1),
		buffer.WithFlushInterval[int](time.Hour),
		buffer.WithEventDestination[int](destination),
		buffer.WithLogger[int](zap.New(logCore)),
	)
	b.Start()

	b.Add(1)
	assertBatch(t, receiveBatch(t, destination.startedCh), []int{1})

	// The destination ignores ctx entirely, so cancellation cannot unblock
	// it; Stop must still return within a bounded time rather than hang the
	// caller forever.
	const stopTimeout = 20 * time.Millisecond
	stoppedCh := make(chan struct{})
	start := time.Now()
	go func() {
		b.Stop(stopTimeout)
		close(stoppedCh)
	}()

	waitForStop(t, stoppedCh)
	if elapsed := time.Since(start); elapsed > testWaitTimeout {
		t.Fatalf("Stop() took %v, want it bounded near %v", elapsed, 2*stopTimeout)
	}

	if got := observedLogs.FilterMessage("audit buffer flusher did not exit within the stop deadline; abandoning it").Len(); got != 1 {
		t.Errorf("abandonment log count = %d, want 1", got)
	}
}

func TestInMemoryRecordsPartialFlushAndContinues(t *testing.T) {
	logCore, observedLogs := zapobserver.New(zap.WarnLevel)
	recorder := newRecordingDestination[int]()
	destination := &partialFirstDestination[int]{
		destination: recorder,
		delivered:   2,
		err:         errors.New("flush failed"),
	}
	b := buffer.NewInMemory(
		buffer.WithMaxBatchSize[int](3),
		buffer.WithBufferSize[int](4),
		buffer.WithFlushInterval[int](time.Hour),
		buffer.WithEventDestination[int](destination),
		buffer.WithLogger[int](zap.New(logCore)),
	)
	b.Start()

	b.Add(1, 2, 3, 4)
	stopGracefully(t, b)

	if diff := cmp.Diff([][]int{{1, 2, 3}, {4}}, recorder.batchesSnapshot()); diff != "" {
		t.Errorf("flush attempts mismatch (-want +got):\n%s", diff)
	}
	if got := observedLogs.FilterMessage("audit event batch was not fully delivered").Len(); got != 1 {
		t.Errorf("incomplete-delivery log count = %d, want 1", got)
	}
}

func TestInMemoryAcceptsConcurrentProducers(t *testing.T) {
	const (
		numProducers      = 4
		eventsPerProducer = 10
		numEvents         = numProducers * eventsPerProducer
	)

	destination := newRecordingDestination[int]()
	b := buffer.NewInMemory(
		buffer.WithMaxBatchSize[int](7),
		buffer.WithBufferSize[int](numEvents),
		buffer.WithFlushInterval[int](time.Hour),
		buffer.WithEventDestination[int](destination),
	)
	b.Start()

	var producers sync.WaitGroup
	for producer := range numProducers {
		producers.Add(1)
		go func() {
			defer producers.Done()

			firstEvent := producer * eventsPerProducer
			events := make([]int, eventsPerProducer)
			for index := range events {
				events[index] = firstEvent + index
			}
			b.Add(events...)
		}()
	}
	producers.Wait()
	stopGracefully(t, b)

	gotEvents := make([]int, 0, numEvents)
	for _, batch := range destination.batchesSnapshot() {
		gotEvents = append(gotEvents, batch...)
	}
	slices.Sort(gotEvents)
	wantEvents := make([]int, numEvents)
	for index := range wantEvents {
		wantEvents[index] = index
	}
	if diff := cmp.Diff(wantEvents, gotEvents); diff != "" {
		t.Errorf("delivered events mismatch (-want +got):\n%s", diff)
	}
}

type recordingDestination[T any] struct {
	mu      sync.Mutex
	batches [][]T
	callsCh chan []T
}

func newRecordingDestination[T any]() *recordingDestination[T] {
	return &recordingDestination[T]{callsCh: make(chan []T, 16)}
}

func (d *recordingDestination[T]) Deliver(_ context.Context, items []T) (int, error) {
	batch := slices.Clone(items)
	d.mu.Lock()
	d.batches = append(d.batches, batch)
	d.mu.Unlock()
	d.callsCh <- batch
	return len(items), nil
}

func (d *recordingDestination[T]) batchesSnapshot() [][]T {
	d.mu.Lock()
	defer d.mu.Unlock()

	batches := make([][]T, len(d.batches))
	for index, batch := range d.batches {
		batches[index] = slices.Clone(batch)
	}
	return batches
}

type firstFlushBlockingDestination[T any] struct {
	destination *recordingDestination[T]
	startedCh   chan []T
	releaseCh   chan struct{}
	blockOnce   sync.Once
	releaseOnce sync.Once
}

func newFirstFlushBlockingDestination[T any](
	destination *recordingDestination[T],
) *firstFlushBlockingDestination[T] {
	return &firstFlushBlockingDestination[T]{
		destination: destination,
		startedCh:   make(chan []T, 1),
		releaseCh:   make(chan struct{}),
	}
}

func (d *firstFlushBlockingDestination[T]) Deliver(
	ctx context.Context,
	items []T,
) (int, error) {
	block := false
	d.blockOnce.Do(func() {
		block = true
	})
	if block {
		d.startedCh <- slices.Clone(items)
		<-d.releaseCh
	}
	return d.destination.Deliver(ctx, items)
}

func (d *firstFlushBlockingDestination[T]) release() {
	d.releaseOnce.Do(func() {
		close(d.releaseCh)
	})
}

// ctxAwareDestination blocks until its Deliver context is canceled and
// records that cancellation, modeling a destination that only unblocks in
// response to Stop's deadline rather than on its own.
type ctxAwareDestination[T any] struct {
	startedCh  chan []T
	canceledCh chan struct{}
}

func newCtxAwareDestination[T any]() *ctxAwareDestination[T] {
	return &ctxAwareDestination[T]{
		startedCh:  make(chan []T, 1),
		canceledCh: make(chan struct{}),
	}
}

func (d *ctxAwareDestination[T]) Deliver(ctx context.Context, items []T) (int, error) {
	d.startedCh <- slices.Clone(items)
	<-ctx.Done()
	close(d.canceledCh)
	return 0, ctx.Err()
}

// uncooperativeDestination ignores ctx entirely, modeling a delivery with no
// way to be interrupted (e.g. a blocking call with no I/O deadline of its
// own).
type uncooperativeDestination[T any] struct {
	startedCh    chan []T
	blockForever chan struct{}
}

func newUncooperativeDestination[T any]() *uncooperativeDestination[T] {
	return &uncooperativeDestination[T]{
		startedCh:    make(chan []T, 1),
		blockForever: make(chan struct{}),
	}
}

func (d *uncooperativeDestination[T]) Deliver(_ context.Context, items []T) (int, error) {
	d.startedCh <- slices.Clone(items)
	<-d.blockForever
	return 0, nil
}

type partialFirstDestination[T any] struct {
	destination *recordingDestination[T]
	delivered   int
	err         error
	once        sync.Once
}

func (d *partialFirstDestination[T]) Deliver(
	ctx context.Context,
	items []T,
) (int, error) {
	delivered, err := d.destination.Deliver(ctx, items)
	if err != nil {
		return delivered, err
	}

	partial := false
	d.once.Do(func() {
		partial = true
	})
	if partial {
		return d.delivered, d.err
	}
	return delivered, nil
}

func receiveBatch[T any](t *testing.T, batches <-chan []T) []T {
	t.Helper()

	select {
	case batch := <-batches:
		return batch
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for destination flush")
		return nil
	}
}

func stopGracefully[T any](t *testing.T, b *buffer.InMemory[T]) {
	t.Helper()

	stoppedCh := make(chan struct{})
	go func() {
		b.Stop(testWaitTimeout)
		close(stoppedCh)
	}()
	waitForStop(t, stoppedCh)
}

func waitForStop(t *testing.T, stoppedCh <-chan struct{}) {
	t.Helper()

	select {
	case <-stoppedCh:
	case <-time.After(testWaitTimeout):
		t.Fatal("timed out waiting for Stop")
	}
}

func assertBatch[T any](t *testing.T, got, want []T) {
	t.Helper()
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("batch mismatch (-want +got):\n%s", diff)
	}
}
