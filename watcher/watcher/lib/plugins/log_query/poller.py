# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any

from watcher.lib.plugins.log_query.batcher import LogQueryBatcher
from watcher.lib.utils.constants import (
    LOG_QUERY_POLLER_CHUNK_SIZE_SECS,
    LOG_QUERY_POLLER_INITIAL_BACKOFF_SECS,
    LOG_QUERY_POLLER_ITERATION_BUDGET_SECS,
    LOG_QUERY_POLLER_MAX_BACKOFF_SECS,
)
from watcher.lib.utils.logging import logger
from watcher.lib.utils.metrics import (
    LOG_QUERY_POLLER_BACKLOG,
    LOG_QUERY_POLLER_CHUNK_DURATION,
    LOG_QUERY_POLLER_CHUNKS_PROCESSED,
    LOG_QUERY_POLLER_CURSOR_TIMESTAMP,
    LOG_QUERY_POLLER_ITERATION_BUDGET_EXCEEDED,
    LOG_QUERY_POLLER_ITERATIONS,
    LOG_QUERY_POLLER_RETRY_COUNT,
)


class LogQueryPoller:
    """
    Dedicated async poller for the selected log-query backend with internal backoff and chunking.

    Features:
    - Queries the selected backend in bounded time chunks to prevent large catch-up windows
    - Internal exponential backoff on failures
    - Only advances cursor on successful fetch
    - Per-query async locks to prevent concurrent cursor updates
    - Parallel outer query loop (asyncio.gather) with per-query wall-clock
      budget so a slow query cannot starve faster ones
    - Comprehensive metrics tracking
    """

    def __init__(
        self,
        log_query_batcher: LogQueryBatcher,
        workflow_callback: Any,
        query_configs: list[dict[str, Any]],
        poll_interval_secs: int = 15,
        chunk_size_secs: int = LOG_QUERY_POLLER_CHUNK_SIZE_SECS,
        iteration_budget_secs: int = LOG_QUERY_POLLER_ITERATION_BUDGET_SECS,
    ):
        """
        Initialize LogQueryPoller.

        Args:
            log_query_batcher (LogQueryBatcher): log-query batcher instance
            workflow_callback (Callable): Async function to call with query results
            query_configs (List[Dict[str, Any]]): List of query configurations from workflow
            poll_interval_secs (int): Interval between poll iterations
            chunk_size_secs (int): Maximum time window per query chunk
            iteration_budget_secs (int): Per-query wall-clock budget enforced
                via asyncio.wait_for. A query exceeding this budget is
                cancelled; its cursor remains at the last successful chunk so
                the next iteration resumes from there.
        """
        self.log_query_batcher = log_query_batcher
        self.workflow_callback = workflow_callback
        self.query_configs = query_configs
        self.poll_interval_secs = poll_interval_secs
        self.chunk_size_secs = chunk_size_secs
        self.iteration_budget_secs = iteration_budget_secs

        # Per-query state
        self.query_locks: dict[str, asyncio.Lock] = {}
        self.query_backoff: dict[str, float] = {}
        self.query_last_failure: dict[str, float] = {}

        # Cache labeled metrics to avoid .labels() overhead on every call
        self._backlog_metrics = {}
        self._cursor_metrics = {}
        self._retry_metrics = {}
        self._chunk_duration_metrics = {}
        self._chunks_processed_metrics = {}
        self._budget_exceeded_metrics = {}

        # Initialize locks and cached metrics for each query
        for config in query_configs:
            query_id = config.get("name", "NA")
            self.query_locks[query_id] = asyncio.Lock()
            self.query_backoff[query_id] = 0.0
            self.query_last_failure[query_id] = 0.0

            # Pre-create labeled metric instances
            self._backlog_metrics[query_id] = LOG_QUERY_POLLER_BACKLOG.labels(query=query_id)
            self._cursor_metrics[query_id] = LOG_QUERY_POLLER_CURSOR_TIMESTAMP.labels(query=query_id)
            self._retry_metrics[query_id] = LOG_QUERY_POLLER_RETRY_COUNT.labels(query=query_id)
            self._chunk_duration_metrics[query_id] = LOG_QUERY_POLLER_CHUNK_DURATION.labels(query=query_id)
            self._chunks_processed_metrics[query_id] = LOG_QUERY_POLLER_CHUNKS_PROCESSED.labels(query=query_id)
            self._budget_exceeded_metrics[query_id] = LOG_QUERY_POLLER_ITERATION_BUDGET_EXCEEDED.labels(query=query_id)

        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """
        Start the poller loop.

        Creates and starts the background polling task that continuously queries the selected backend.
        This is idempotent - calling start() multiple times has no effect if already running.
        """
        if self._running:
            logger.warning("[LogQueryPoller] Already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("[LogQueryPoller] Started")

    async def stop(self) -> None:
        """
        Stop the poller loop gracefully.

        Cancels the background polling task and waits for it to complete.
        This is idempotent - calling stop() multiple times has no effect if already stopped.
        """
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception as error:
                logger.warning(
                    f"[LogQueryPoller] Poller task failed during shutdown: {error}",
                    exc_info=True,
                    extra={"error": str(error)},
                )
        await self.log_query_batcher.close()
        logger.info("[LogQueryPoller] Stopped")

    async def _poll_loop(self) -> None:
        """
        Main polling loop that runs continuously until stopped.

        Executes poll iterations at regular intervals (poll_interval_secs) and handles:
        - Timing each iteration and sleeping for the remaining interval
        - Warning if iteration exceeds poll interval
        - Graceful cancellation on stop()
        - Recovery from unexpected errors with brief pause

        Metrics updated:
        - LOG_QUERY_POLLER_ITERATIONS: Incremented after each successful iteration
        """
        while self._running:
            try:
                iteration_start = time.time()
                await self._poll_iteration()
                iteration_duration = time.time() - iteration_start

                LOG_QUERY_POLLER_ITERATIONS.inc()

                # Sleep for remaining poll interval
                sleep_time = max(0, self.poll_interval_secs - iteration_duration)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                else:
                    logger.debug(
                        "[LogQueryPoller] Iteration took %.2fs, poll interval set to %ds",
                        iteration_duration,
                        self.poll_interval_secs,
                    )
            except asyncio.CancelledError:
                logger.info("[LogQueryPoller] Poll loop cancelled")
                break
            except Exception as e:
                logger.exception("[LogQueryPoller] Unexpected error in poll loop", extra={"error": str(e)})
                await asyncio.sleep(5)  # Brief pause before retrying

    async def _poll_iteration(self) -> None:
        """
        Execute one polling iteration across all configured queries in parallel.

        All queries run concurrently via ``asyncio.gather``; each is wrapped in
        ``asyncio.wait_for(..., timeout=iteration_budget_secs)`` so a slow query
        can never starve faster ones (the failure mode of the prior sequential
        design where ``crash`` was deferred behind a slow ``noprogress``). The
        per-query lock acquired inside ``_process_query_config`` still
        serializes consecutive iterations of the same query; only the outer
        fan-out is parallel.

        Correctness invariants preserved by this design:
        - Cursor advancement is still strictly monotonic per query — chunks
          within a single query remain sequential.
        - Each query writes a disjoint kvstore namespace (see workflow.py),
          so concurrent queries do not race on shared state.
        - ``EventBusPublisher.publish_message`` is synchronous (no ``await``
          on the publish path), so concurrent publish calls serialize on the
          event loop with no interleaved channel state.

        On per-query timeout the in-flight task is cancelled; the cursor
        remains at the last successfully committed chunk so the next iteration
        resumes from there with no data loss.

        Metrics updated:
        - LOG_QUERY_POLLER_ITERATION_BUDGET_EXCEEDED: incremented per slow/failed query
        """
        iteration_start = time.time()

        async def _run(config: dict[str, Any]) -> tuple[str, float, BaseException | None]:
            """Run a single query config under ``self.iteration_budget_secs`` as a per-query wall-clock timeout.

            Invokes :meth:`_process_query_config` inside :func:`asyncio.wait_for` so a slow query
            cannot starve sibling queries dispatched via :func:`asyncio.gather` in the caller.

            Args:
                config: The query configuration dict (must contain ``name`` for attribution; falls
                    back to ``"NA"``).

            Returns:
                Tuple of ``(query_id, elapsed_secs, error)``. ``error`` is ``None`` on success,
                an :class:`asyncio.TimeoutError` on budget exhaustion, or any other
                :class:`Exception` raised by ``_process_query_config``. ``BaseException``
                subclasses outside this set (e.g. ``KeyboardInterrupt``) are re-raised so they
                still abort the event loop.
            """
            query_id = config.get("name", "NA")
            t0 = time.time()
            try:
                await asyncio.wait_for(self._process_query_config(config), timeout=self.iteration_budget_secs)
                return query_id, time.time() - t0, None
            except BaseException as e:  # incl. asyncio.TimeoutError
                if not isinstance(e, (asyncio.TimeoutError, Exception)):
                    raise
                return query_id, time.time() - t0, e

        results = await asyncio.gather(*(_run(cfg) for cfg in self.query_configs))
        elapsed = time.time() - iteration_start

        slow = [(qid, dt, err) for qid, dt, err in results if err is not None]
        if slow:

            def _fmt(qid: str, dt: float, err: BaseException | None) -> str:
                if isinstance(err, asyncio.TimeoutError):
                    return f"{qid}=timeout@{dt:.1f}s"
                return f"{qid}=failed@{dt:.1f}s({type(err).__name__})"

            logger.warning(
                "[LogQueryPoller] [LOG_QUERY_DEGRADED] Iteration finished in %.2fs "
                "(per-query budget=%ds); slow/failed queries (%d/%d): %s",
                elapsed,
                self.iteration_budget_secs,
                len(slow),
                len(results),
                ", ".join(_fmt(qid, dt, err) for qid, dt, err in slow),
            )
            for qid, _, _ in slow:
                self._budget_exceeded_metrics[qid].inc()

    async def _process_query_config(self, config: dict[str, Any]) -> None:
        """
        Process a single query configuration with backoff enforcement and locking.

        This method acts as a gatekeeper before processing query chunks by:
        1. Checking if the query is in backoff state and skipping if so
        2. Acquiring a per-query async lock to prevent concurrent processing
        3. Delegating to _process_query_chunks() for actual work

        Backoff Enforcement:
        - Checks query_backoff[query_id] to see if query is in cooldown
        - Compares time since last failure against backoff delay
        - Skips processing if still within backoff period
        - Logs remaining backoff time for debugging

        Concurrency Control:
        - Acquires query-specific async lock before processing
        - Prevents multiple concurrent executions of the same query
        - Ensures cursor updates are serialized and safe

        Args:
            config: Query configuration dictionary from workflow
        """
        query_id = config.get("name", "NA")

        # Check if we need to backoff
        backoff_delay = self.query_backoff.get(query_id, 0.0)
        if backoff_delay > 0:
            last_failure = self.query_last_failure.get(query_id, 0.0)
            current_time = time.time()
            time_since_failure = current_time - last_failure
            if time_since_failure < backoff_delay:
                logger.debug(
                    "[LogQueryPoller] Query %s in backoff, waiting %.1fs more",
                    query_id,
                    backoff_delay - time_since_failure,
                )
                return

        # Acquire lock for this query
        async with self.query_locks[query_id]:
            await self._process_query_chunks(config, query_id)

    async def _process_query_chunks(self, config: dict[str, Any], query_id: str) -> None:
        """
        Process a single provider-native query in bounded time chunks with automatic backoff.

        This method handles the core polling logic for a query by:
        1. Computing the time range to query (from cursor to now, with optional query_delay)
        2. Splitting large time ranges into bounded chunks (default 60s per chunk)
        3. Processing each chunk sequentially via the workflow callback
        4. Advancing the cursor only after successful chunk processing
        5. Applying exponential backoff on failures

        Time Range Computation:
        - Start time: Retrieved from cursor (the shared exclusive end bound,
                      or legacy Loki's historical end + 1μs)
                      OR max_lookback seconds ago if no cursor exists
        - End time: Current time minus query_delay (if configured)
        - Query delay ensures data is fully ingested before querying

        Chunking Strategy:
        - Large time ranges are split into chunks of chunk_size_secs (default 60s)
        - Each chunk is processed independently via _process_single_chunk()
        - Cursor advances after each successful chunk, preventing data loss on partial failures
        - If a chunk fails, processing stops and the cursor remains at the last success point

        Backoff Behavior (Per Query):
        - Backoff is enforced per query, not globally - one failing query doesn't block others
        - On success: Backoff timer for this query is reset to 0, allowing immediate next execution
        - On failure: Exponential backoff applied to this query only (2s, 4s, 8s ... max 300s)
        - Backoff delay is checked in _process_query_config() before processing this query

        Metrics Updated:
        - LOG_QUERY_POLLER_BACKLOG: Time delta between cursor and current time
        - LOG_QUERY_POLLER_CURSOR_TIMESTAMP: Current cursor position timestamp
        - LOG_QUERY_POLLER_RETRY_COUNT: Incremented on chunk failures

        Args:
            config: Query configuration dictionary containing query definition and metadata
            query_id: Unique identifier for this query (from config['name'])

        """
        current_time = self.log_query_batcher._query_now()

        # Apply query_delay if configured
        query_delay = self.log_query_batcher.query_delay
        if query_delay:
            current_time = current_time - timedelta(seconds=query_delay)

        # Get cursor
        cursor_query = self.log_query_batcher.cursor_query_identity(config, query_id)
        last_execution_time = self.log_query_batcher._get_query_last_execution_time(
            query=cursor_query, query_id=query_id
        )

        if last_execution_time is not None:
            start_time = self.log_query_batcher._start_after_cursor(last_execution_time)
        else:
            start_time = current_time - timedelta(seconds=self.log_query_batcher.max_lookback)

        end_time = current_time

        # Calculate backlog and update metric
        backlog_seconds = (end_time - start_time).total_seconds()
        self._backlog_metrics[query_id].set(backlog_seconds)

        # Update cursor timestamp metric
        self._cursor_metrics[query_id].set(start_time.timestamp())

        logger.debug(
            "[LogQueryPoller] Query %s: backlog=%.1fs, start=%s, end=%s",
            query_id,
            backlog_seconds,
            start_time,
            end_time,
        )

        # Process in chunks
        chunk_start = start_time
        while chunk_start < end_time:
            chunk_end = min(chunk_start + timedelta(seconds=self.chunk_size_secs), end_time)

            try:
                await self._process_single_chunk(config, query_id, chunk_start, chunk_end)
            except Exception as e:
                logger.exception(
                    "[LogQueryPoller] Query %s chunk failed (start=%s, end=%s)",
                    query_id,
                    chunk_start,
                    chunk_end,
                    extra={"query_id": query_id, "error": str(e)},
                )

                # Apply exponential backoff
                current_backoff = self.query_backoff.get(query_id, 0.0)
                if current_backoff == 0.0:
                    new_backoff = LOG_QUERY_POLLER_INITIAL_BACKOFF_SECS
                else:
                    new_backoff = min(current_backoff * 2, LOG_QUERY_POLLER_MAX_BACKOFF_SECS)  # type: ignore[assignment]

                current_time = time.time()  # type: ignore[assignment]
                self.query_backoff[query_id] = new_backoff
                self.query_last_failure[query_id] = current_time  # type: ignore[assignment]

                self._retry_metrics[query_id].inc()

                logger.warning("[LogQueryPoller] Query %s entering backoff for %.1fs", query_id, new_backoff)

                # Stop processing chunks for this query this iteration
                break
            else:
                # Commit cursor ONLY after the chunk's workflow callback (incl. publish)
                # returned without raising. _store_query_last_execution_time is sync, so
                # no await sits between the awaited call above and this commit — an
                # asyncio.wait_for() cancellation injected into _process_single_chunk
                # raises CancelledError (BaseException), bypasses `except Exception:`,
                # and never reaches this line. Likewise any post-fetch publish failure
                # raised through _process_single_chunk skips the commit, so the next
                # iteration resumes from the last actually-published chunk_end.
                # Cursor key intentionally mirrors the read at the top of this method
                # (query=cursor_query, query_id=query_id) so reads and writes hit the same
                # kvstore slot; LogQueryBatcher.batch_query skips its own write when called
                # with explicit start/end (see `explicit_window` there).
                self.log_query_batcher._store_query_last_execution_time(
                    query=cursor_query,
                    query_id=query_id,
                    end_time=chunk_end,
                )
                self.query_backoff[query_id] = 0.0
                chunk_start = chunk_end

    async def _process_single_chunk(
        self, config: dict[str, Any], query_id: str, start_time: datetime, end_time: datetime
    ) -> None:
        """
        Process a single time chunk for a query.

        Executes the workflow callback for a bounded time range and tracks metrics.
        The callback is expected to:
        1. Query the selected backend for the specified time range via log_query_batcher.batch_query()
        2. Process the results through the workflow engine (including publish)

        Cursor advancement is intentionally NOT handled here or in batch_query for
        bounded-window calls — _process_query_chunks commits the cursor on the success
        path *after* this method returns, so a cancellation/timeout/publish failure
        thrown out of the callback cannot leave the cursor ahead of published data.

        Timing and Metrics:
        - Measures chunk processing duration (query + workflow execution)
        - Records chunk latency histogram for performance monitoring
        - Increments chunks processed counter on success

        Args:
            config: Query configuration dictionary
            query_id: Unique identifier for this query
            start_time: Start of the time window to query
            end_time: End of the time window to query

        Metrics updated:
        - LOG_QUERY_POLLER_CHUNK_DURATION: Processing duration for this chunk
        - LOG_QUERY_POLLER_CHUNKS_PROCESSED: Incremented on successful completion
        """
        chunk_start_time = time.time()

        # This calls the workflow callback with the query results
        # The callback should handle calling log_query_batcher.batch_query internally
        await self.workflow_callback(config, start_time, end_time)

        chunk_duration = time.time() - chunk_start_time

        self._chunk_duration_metrics[query_id].observe(chunk_duration)
        self._chunks_processed_metrics[query_id].inc()

        logger.debug(
            "[LogQueryPoller] Processed chunk for query %s in %.2fs (start=%s, end=%s)",
            query_id,
            chunk_duration,
            start_time,
            end_time,
        )
