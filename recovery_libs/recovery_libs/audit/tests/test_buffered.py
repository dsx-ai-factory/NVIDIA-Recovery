# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Sequence

import pytest
from pydantic import ValidationError
from recovery_libs.audit import AuditEventBuffer, Config, DeliveryError, DeliverySender

_TEST_WAIT_TIMEOUT_SECONDS = 1.0
_LONG_FLUSH_INTERVAL_SECONDS = 60 * 60


class _RecordingSender[T]:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._batches: list[tuple[T, ...]] = []
        self.calls: queue.Queue[tuple[T, ...]] = queue.Queue()

    def __call__(self, items: Sequence[T], stop_event: threading.Event) -> int:
        del stop_event
        batch = tuple(items)
        with self._lock:
            self._batches.append(batch)
        self.calls.put(batch)
        return len(batch)

    def batches_snapshot(self) -> list[tuple[T, ...]]:
        with self._lock:
            return list(self._batches)


class _FirstSendBlockingSender[T]:
    def __init__(self, destination: _RecordingSender[T]) -> None:
        self._destination = destination
        self._lock = threading.Lock()
        self._should_block = True
        self._release = threading.Event()
        self.started: queue.Queue[tuple[T, ...]] = queue.Queue(maxsize=1)

    def __call__(self, items: Sequence[T], stop_event: threading.Event) -> int:
        batch = tuple(items)
        with self._lock:
            should_block = self._should_block
            self._should_block = False

        if should_block:
            self.started.put(batch)
            self._release.wait()

        return self._destination(batch, stop_event)

    def release(self) -> None:
        self._release.set()


class _StopEventAwareSender[T]:
    """Blocks until the buffer's stop_event fires, modeling a destination
    that only unblocks in response to a stop deadline rather than on its own.
    """

    def __init__(self) -> None:
        self.started: queue.Queue[tuple[T, ...]] = queue.Queue(maxsize=1)
        self.observed_stop_fired: queue.Queue[bool] = queue.Queue(maxsize=1)

    def __call__(self, items: Sequence[T], stop_event: threading.Event) -> int:
        batch = tuple(items)
        self.started.put(batch)
        fired = stop_event.wait(_TEST_WAIT_TIMEOUT_SECONDS)
        self.observed_stop_fired.put(fired)
        return 0


class _UncooperativeSender[T]:
    """Ignores stop_event entirely, modeling a sender with no way to be
    interrupted (e.g. a blocking call with no I/O timeout of its own).
    """

    def __init__(self) -> None:
        self.started: queue.Queue[tuple[T, ...]] = queue.Queue(maxsize=1)
        self._block_forever = threading.Event()

    def __call__(self, items: Sequence[T], stop_event: threading.Event) -> int:
        del stop_event
        self.started.put(tuple(items))
        self._block_forever.wait()
        return 0


class _PartialFirstSender[T]:
    def __init__(self, destination: _RecordingSender[T]) -> None:
        self._destination = destination
        self._lock = threading.Lock()
        self._should_fail = True

    def __call__(self, items: Sequence[T], stop_event: threading.Event) -> int:
        delivered_count = self._destination(items, stop_event)

        with self._lock:
            should_fail = self._should_fail
            self._should_fail = False

        if should_fail:
            raise DeliveryError("delivery failed", delivered_count=2)

        return delivered_count


type _StopRequest = tuple[
    threading.Thread,
    threading.Event,
    queue.Queue[BaseException],
]


def _request_stop[T](buffer: AuditEventBuffer[T], timeout_seconds: float | None = None) -> _StopRequest:
    stopped = threading.Event()
    errors: queue.Queue[BaseException] = queue.Queue()

    def stop() -> None:
        try:
            if timeout_seconds is None:
                buffer.stop()
            else:
                buffer.stop(timeout_seconds)
        except BaseException as error:  # noqa: BLE001
            errors.put(error)
        finally:
            stopped.set()

    thread = threading.Thread(target=stop, daemon=True)
    thread.start()
    return thread, stopped, errors


def _wait_for_stop(request: _StopRequest) -> None:
    thread, stopped, errors = request
    if not stopped.wait(_TEST_WAIT_TIMEOUT_SECONDS):
        pytest.fail("timed out waiting for AuditEventBuffer.stop()")

    thread.join(timeout=_TEST_WAIT_TIMEOUT_SECONDS)
    assert not thread.is_alive()
    if not errors.empty():
        raise errors.get_nowait()


def _stop_gracefully[T](buffer: AuditEventBuffer[T], *, timeout_seconds: float | None = None) -> None:
    _wait_for_stop(_request_stop(buffer, timeout_seconds))


def _receive_batch[T](batches: queue.Queue[tuple[T, ...]]) -> tuple[T, ...]:
    try:
        return batches.get(timeout=_TEST_WAIT_TIMEOUT_SECONDS)
    except queue.Empty:
        pytest.fail("timed out waiting for destination flush")


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("max_batch_size", 0),
        ("max_batch_size", -1),
        ("max_buffer_size", 0),
        ("max_buffer_size", -1),
        ("flush_interval_seconds", 0),
        ("flush_interval_seconds", -0.1),
    ],
)
def test_config_rejects_non_positive_values(field_name: str, invalid_value: float) -> None:
    sender: DeliverySender[int] = _RecordingSender[int]()
    values = {
        "max_batch_size": 4,
        "max_buffer_size": 4,
        "flush_interval_seconds": 1,
        "sender": sender,
    }
    values[field_name] = invalid_value

    with pytest.raises(ValidationError):
        Config[int](**values)


def test_config_rejects_non_callable_sender() -> None:
    with pytest.raises(ValidationError):
        Config[int](
            max_batch_size=4,
            max_buffer_size=4,
            flush_interval_seconds=1,
            sender=None,
        )


def test_drops_events_when_buffer_is_full(caplog: pytest.LogCaptureFixture) -> None:
    recorder = _RecordingSender[int]()
    sender = _FirstSendBlockingSender(recorder)
    buffer = AuditEventBuffer(
        Config(
            max_batch_size=1,
            max_buffer_size=2,
            flush_interval_seconds=_LONG_FLUSH_INTERVAL_SECONDS,
            sender=sender,
        )
    )
    buffer.start()

    try:
        buffer.add(1)
        assert _receive_batch(sender.started) == (1,)

        # The worker is blocked sending 1, so only 2 and 3 fit in the queue.
        with caplog.at_level(logging.WARNING):
            buffer.add(2, 3, 4, 5)
    finally:
        sender.release()

    _stop_gracefully(buffer)

    assert recorder.batches_snapshot() == [(1,), (2,), (3,)]
    matching_logs = [
        record for record in caplog.records if record.getMessage() == "Queue is full, dropping all remaining items."
    ]
    assert len(matching_logs) == 1


def test_flushes_partial_batch_on_interval() -> None:
    sender = _RecordingSender[int]()
    buffer = AuditEventBuffer(
        Config(
            max_batch_size=4,
            max_buffer_size=4,
            flush_interval_seconds=0.01,
            sender=sender,
        )
    )
    buffer.start()

    buffer.add(1)
    assert _receive_batch(sender.calls) == (1,)
    _stop_gracefully(buffer)

    assert sender.batches_snapshot() == [(1,)]


def test_flushes_when_batch_is_full() -> None:
    sender = _RecordingSender[int]()
    buffer = AuditEventBuffer(
        Config(
            max_batch_size=3,
            max_buffer_size=3,
            flush_interval_seconds=_LONG_FLUSH_INTERVAL_SECONDS,
            sender=sender,
        )
    )
    buffer.start()

    buffer.add(1, 2, 3)
    assert _receive_batch(sender.calls) == (1, 2, 3)
    _stop_gracefully(buffer)

    assert sender.batches_snapshot() == [(1, 2, 3)]


def test_flushes_remaining_events_during_graceful_shutdown() -> None:
    sender = _RecordingSender[int]()
    buffer = AuditEventBuffer(
        Config(
            max_batch_size=4,
            max_buffer_size=4,
            flush_interval_seconds=_LONG_FLUSH_INTERVAL_SECONDS,
            sender=sender,
        )
    )
    buffer.start()

    buffer.add(1, 2, 3)
    _stop_gracefully(buffer)

    assert sender.batches_snapshot() == [(1, 2, 3)]


def test_stop_waits_for_blocked_send_and_drains_remaining_events() -> None:
    recorder = _RecordingSender[int]()
    sender = _FirstSendBlockingSender(recorder)
    buffer = AuditEventBuffer(
        Config(
            max_batch_size=1,
            max_buffer_size=2,
            flush_interval_seconds=_LONG_FLUSH_INTERVAL_SECONDS,
            sender=sender,
        )
    )
    buffer.start()

    buffer.add(1)
    assert _receive_batch(sender.started) == (1,)

    # These events remain queued while the destination is stuck on 1.
    buffer.add(2, 3)
    stop_request = _request_stop(buffer)
    _, stopped, _ = stop_request

    try:
        assert not stopped.wait(0.01), "stop() returned while the destination was blocked"
    finally:
        sender.release()

    _wait_for_stop(stop_request)
    assert recorder.batches_snapshot() == [(1,), (2,), (3,)]


def test_stop_bounds_wait_when_queue_is_full() -> None:
    sender = _StopEventAwareSender[int]()
    buffer = AuditEventBuffer(
        Config(
            max_batch_size=1,
            max_buffer_size=1,
            flush_interval_seconds=_LONG_FLUSH_INTERVAL_SECONDS,
            sender=sender,
        )
    )
    buffer.start()

    buffer.add(1)
    assert _receive_batch(sender.started) == (1,)

    # The queue is now full and the worker is stuck sending 1, so stop()'s
    # own sentinel cannot be enqueued either.
    buffer.add(2)

    stop_timeout_seconds = 0.02
    start = time.monotonic()
    _stop_gracefully(buffer, timeout_seconds=stop_timeout_seconds)
    elapsed = time.monotonic() - start

    assert elapsed < _TEST_WAIT_TIMEOUT_SECONDS, f"stop() took {elapsed}s, want it bounded near {stop_timeout_seconds}s"

    try:
        fired = sender.observed_stop_fired.get(timeout=_TEST_WAIT_TIMEOUT_SECONDS)
    except queue.Empty:
        pytest.fail("sender never observed the stop_event fire")
    assert fired is True


def test_stop_bounds_wait_and_signals_stop_event() -> None:
    sender = _StopEventAwareSender[int]()
    buffer = AuditEventBuffer(
        Config(
            max_batch_size=1,
            max_buffer_size=1,
            flush_interval_seconds=_LONG_FLUSH_INTERVAL_SECONDS,
            sender=sender,
        )
    )
    buffer.start()

    buffer.add(1)
    assert _receive_batch(sender.started) == (1,)

    # The sender never returns on its own; only stop()'s deadline firing the
    # stop_event can unblock it.
    stop_timeout_seconds = 0.02
    start = time.monotonic()
    _stop_gracefully(buffer, timeout_seconds=stop_timeout_seconds)
    elapsed = time.monotonic() - start

    assert elapsed < _TEST_WAIT_TIMEOUT_SECONDS, f"stop() took {elapsed}s, want it bounded near {stop_timeout_seconds}s"

    try:
        fired = sender.observed_stop_fired.get(timeout=_TEST_WAIT_TIMEOUT_SECONDS)
    except queue.Empty:
        pytest.fail("sender never observed the stop_event fire")
    assert fired is True


def test_stop_abandons_a_sender_that_ignores_stop_event(caplog: pytest.LogCaptureFixture) -> None:
    sender = _UncooperativeSender[int]()
    buffer = AuditEventBuffer(
        Config(
            max_batch_size=1,
            max_buffer_size=1,
            flush_interval_seconds=_LONG_FLUSH_INTERVAL_SECONDS,
            sender=sender,
        )
    )
    buffer.start()

    buffer.add(1)
    assert _receive_batch(sender.started) == (1,)

    # The sender never returns, cooperatively or otherwise. stop() must
    # still return within a bounded time rather than hang the caller.
    stop_timeout_seconds = 0.02
    start = time.monotonic()
    with caplog.at_level(logging.WARNING, logger="recovery_libs.audit.buffered"):
        _stop_gracefully(buffer, timeout_seconds=stop_timeout_seconds)
    elapsed = time.monotonic() - start

    assert elapsed < _TEST_WAIT_TIMEOUT_SECONDS, (
        f"stop() took {elapsed}s, want it bounded near {2 * stop_timeout_seconds}s"
    )
    assert any(
        record.getMessage() == "audit buffer worker did not exit within the stop deadline; abandoning it"
        for record in caplog.records
    )


def test_records_partial_send_and_continues(caplog: pytest.LogCaptureFixture) -> None:
    recorder = _RecordingSender[int]()
    sender = _PartialFirstSender(recorder)
    buffer = AuditEventBuffer(
        Config(
            max_batch_size=3,
            max_buffer_size=4,
            flush_interval_seconds=_LONG_FLUSH_INTERVAL_SECONDS,
            sender=sender,
        )
    )
    buffer.start()

    with caplog.at_level(logging.WARNING, logger="recovery_libs.audit.buffered"):
        buffer.add(1, 2, 3, 4)
        _stop_gracefully(buffer)

    assert recorder.batches_snapshot() == [(1, 2, 3), (4,)]
    matching_logs = [
        record for record in caplog.records if record.getMessage() == "audit event batch was not fully delivered"
    ]
    assert len(matching_logs) == 1
    assert matching_logs[0].batch_size == 3
    assert matching_logs[0].delivered_count == 2
    assert matching_logs[0].dropped_count == 1


def test_drains_after_concurrent_producers_are_joined() -> None:
    num_producers = 4
    events_per_producer = 10
    num_events = num_producers * events_per_producer

    sender = _RecordingSender[int]()
    buffer = AuditEventBuffer(
        Config(
            max_batch_size=7,
            max_buffer_size=num_events,
            flush_interval_seconds=_LONG_FLUSH_INTERVAL_SECONDS,
            sender=sender,
        )
    )
    buffer.start()

    producers_ready = threading.Barrier(num_producers)

    def produce(producer: int) -> None:
        producers_ready.wait()
        first_event = producer * events_per_producer
        buffer.add(*range(first_event, first_event + events_per_producer))

    producers = [threading.Thread(target=produce, args=(producer,)) for producer in range(num_producers)]
    for producer in producers:
        producer.start()
    for producer in producers:
        producer.join(timeout=_TEST_WAIT_TIMEOUT_SECONDS)
        assert not producer.is_alive()

    _stop_gracefully(buffer)

    delivered_events = [event for batch in sender.batches_snapshot() for event in batch]
    assert sorted(delivered_events) == list(range(num_events))
