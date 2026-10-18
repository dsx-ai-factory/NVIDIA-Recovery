# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable, Sequence
from typing import Annotated, Protocol, cast

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


type DeliverySender[T] = Callable[[Sequence[T], threading.Event], int]

# Bounds how long stop() waits for a blocked delivery before signaling the
# sender's stop_event to interrupt in-flight retry backoff and draining.
DEFAULT_STOP_TIMEOUT_SECONDS = 5.0


class DeliveryError(Exception):
    delivered_count: int

    def __init__(self, message: str, *, delivered_count: int) -> None:
        super().__init__(message)
        self.delivered_count = delivered_count


class _QueueStopSentinel:
    pass


_queue_stop_sentinel = _QueueStopSentinel()


class AuditBuffer[T](Protocol):
    """Accept audit events and own their delivery lifecycle."""

    def add(self, *items: T) -> None:
        """Accept events for delivery."""
        ...

    def start(self) -> None:
        """Start event delivery."""
        ...

    def stop(self, timeout_seconds: float = DEFAULT_STOP_TIMEOUT_SECONDS) -> None:
        """Stop event delivery after producers have been quiesced."""
        ...


class Config[T](BaseModel, frozen=True):
    max_batch_size: Annotated[int, Field(gt=0, strict=True)]
    max_buffer_size: Annotated[int, Field(gt=0, strict=True)]
    flush_interval_seconds: Annotated[float, Field(gt=0)]
    sender: DeliverySender[T]


class AuditEventBuffer[T]:
    """Deliver buffered events after their producers have been quiesced.

    All calls to :meth:`add` must complete before :meth:`stop` is called.
    """

    q: queue.Queue[T | _QueueStopSentinel]
    flush_interval_seconds: float
    max_batch_size: int
    max_buffer_size: int
    sender: DeliverySender[T]
    _worker_thread: threading.Thread
    _stop_event: threading.Event

    def __init__(self, config: Config[T]):
        self.q = queue.Queue(maxsize=config.max_buffer_size)
        self.max_buffer_size = config.max_buffer_size
        self.flush_interval_seconds = config.flush_interval_seconds
        self.max_batch_size = config.max_batch_size
        self.sender = config.sender
        # daemon=True does not bound stop()'s own final join, which still
        # waits unconditionally on the caller's thread. It only guards the
        # case where stop() is never called at all (a skipped finally, a
        # test that forgets to stop the buffer): without it, a live worker
        # would otherwise block interpreter exit forever with nothing left
        # to time it out.
        self._worker_thread = threading.Thread(target=self._delivery_loop, daemon=True)
        self._stop_event = threading.Event()

    def add(self, *items: T) -> None:
        try:
            for item in items:
                self.q.put(item, block=False)
        except queue.Full:
            logger.warning("Queue is full, dropping all remaining items.")

    def start(self) -> None:
        self._worker_thread.start()

    def stop(self, timeout_seconds: float = DEFAULT_STOP_TIMEOUT_SECONDS) -> None:
        """Flush accepted events and stop the worker after producers exit.

        Waits up to timeout_seconds for the delivery loop to drain
        gracefully. If it has not finished by then, signals the sender's
        stop_event so any in-flight retry backoff is interrupted and the
        delivery loop stops picking up further batches, then waits up to
        timeout_seconds more for the worker to actually exit. If a sender
        ignores stop_event entirely (e.g. a blocking call with no I/O
        timeout of its own), stop() gives up and returns anyway rather than
        hanging the caller; the worker thread is a daemon, so an abandoned
        worker cannot block interpreter exit either.
        """

        deadline = time.monotonic() + timeout_seconds

        # The queue is bounded, so enqueuing the sentinel can itself block if
        # producers filled it while the worker is stuck inside sender(). Bound
        # that wait too, or the timeout above would never be reached.
        try:
            self.q.put(_queue_stop_sentinel, block=True, timeout=max(deadline - time.monotonic(), 0))
        except queue.Full:
            self._stop_event.set()

        self._worker_thread.join(max(deadline - time.monotonic(), 0))
        if self._worker_thread.is_alive():
            # Still running past the deadline: the sentinel may be stuck
            # behind a full queue, or sender() itself hasn't returned yet.
            # Force the loop to stop picking up further batches once it next
            # checks stop_event, then give it one more bounded window to
            # exit. If it still hasn't, abandon it rather than blocking the
            # caller forever; being a daemon thread means it will be
            # discarded, not leaked, whenever the process eventually exits.
            self._stop_event.set()
            self._worker_thread.join(timeout_seconds)
            if self._worker_thread.is_alive():
                logger.warning("audit buffer worker did not exit within the stop deadline; abandoning it")

    def _delivery_loop(self) -> None:
        batch: list[T] = []

        t = time.monotonic()

        while True:
            if self._stop_event.is_set():
                self._deliver_batch_if_needed(batch)
                return

            deadline = t + self.flush_interval_seconds
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                self._deliver_batch_if_needed(batch)
                t = time.monotonic()
                batch = []
                continue

            try:
                while True:
                    if self._stop_event.is_set():
                        self._deliver_batch_if_needed(batch)
                        return

                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break

                    item = self.q.get(block=True, timeout=remaining)

                    if item is _queue_stop_sentinel:
                        self._deliver_batch_if_needed(batch)
                        return

                    item = cast(T, item)
                    batch += [item]
                    if len(batch) == self.max_batch_size:
                        self._deliver_batch_if_needed(batch)
                        t = time.monotonic()
                        batch = []
                        break
            except queue.Empty:
                continue

    def _deliver_batch_if_needed(self, items: Sequence[T]) -> None:
        if not items:
            return

        batch = tuple(items)
        delivery_error: Exception | None = None
        try:
            delivered_count = self.sender(batch, self._stop_event)
        except DeliveryError as error:
            delivered_count = error.delivered_count
            delivery_error = error
        except Exception as error:
            logger.debug("audit event sender raised an unexpected error", exc_info=True)
            delivered_count = 0
            delivery_error = error

        dropped_count = len(batch) - delivered_count
        if dropped_count > 0 or delivery_error is not None:
            logger.warning(
                "audit event batch was not fully delivered",
                extra={
                    "batch_size": len(batch),
                    "delivered_count": delivered_count,
                    "dropped_count": dropped_count,
                    "delivery_error": repr(delivery_error) if delivery_error is not None else None,
                },
            )


class NoOpAuditBuffer[T]:
    """Discard audit events when audit delivery is disabled."""

    def add(self, *items: T) -> None:
        """Discard the supplied events."""

    def start(self) -> None:
        """Leave delivery disabled."""

    def stop(self, timeout_seconds: float = DEFAULT_STOP_TIMEOUT_SECONDS) -> None:
        """Leave delivery disabled."""
