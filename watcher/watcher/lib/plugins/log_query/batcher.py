# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import json
import os
import time
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from functools import wraps
from typing import Any

from recovery_libs.log_query import (
    AsyncLogQueryBackend,
    BackendName,
    LogRecord,
    NativeQuery,
    QueryDialect,
    QueryFailure,
    QueryFailureKind,
    QueryOrder,
    QueryRequest,
    QueryStats,
)

from watcher.lib.utils.constants import (
    DEFAULT_LOG_QUERY_CONNECT_TIMEOUT_SECS,
    DEFAULT_LOG_QUERY_TIMEOUT_SECS,
    HTTP_MAX_QUERY_WINDOW_SECS,
    LOG_QUERY_AUTH_RESTART_COOLDOWN_SECS,
)
from watcher.lib.utils.data import (
    LabelRemapper,
    apply_label_remapping,
    deduplicate_records,
    generate_sha256_signature,
    generate_uuid7,
    is_json,
)
from watcher.lib.utils.logging import logger
from watcher.lib.utils.metrics import (
    LOG_QUERY_API_EXCEPTIONS,
    LOG_QUERY_API_LATENCY,
    LOG_QUERY_EMPTY_RESPONSE,
    LOG_QUERY_ERROR_REASONS,
    LOG_QUERY_ERRORS,
    LOG_QUERY_LATENCY,
    LOG_QUERY_POLLER_AUTH_RESTARTS,
    LOG_QUERY_PROCESSING_TIME,
    LOG_QUERY_RECORDS,
    LOG_QUERY_RECORDS_DUPLICATE_DISCARDED,
    LOG_QUERY_RECORDS_RECEIVED,
    LOG_QUERY_REQUESTS,
    LOG_QUERY_WINDOW_CAPPED,
)
from watcher.models.config import LogQueryConfig

type StatsRecorder = Callable[[Sequence[QueryStats], bool, str], None]
"""Records one query's per-page statistics, whether it returned records, and its query label."""


def _describe_stats(stats: Sequence[QueryStats]) -> str:
    return (
        f"{len(stats)} pages, {sum(page.attempts for page in stats)} attempts, "
        f"{sum(page.response_bytes for page in stats)} bytes"
    )


def _close_soon(backend: AsyncLogQueryBackend) -> None:
    """Close a replaced backend without blocking the caller, or synchronously when no event loop is running."""
    try:
        asyncio.get_running_loop().create_task(backend.close())
    except RuntimeError:
        asyncio.run(backend.close())


def _datetime_to_epoch_ns(value: datetime) -> int:
    """Convert a Watcher datetime to UTC epoch nanoseconds without floats.

    Explicit naive values are interpreted as UTC. Shared polling creates
    aware UTC values, so wire bounds never depend on the process timezone.
    """

    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    else:
        value = value.astimezone(UTC)
    delta = value - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1_000


_CURSOR_NS_PREFIX = "v1:ns:"
# Match LokiLogFetcher.async_fetch_logs' established safety cap.
LOG_QUERY_COMPATIBILITY_MAX_PAGES = 5


def _count_log_query_api_exceptions(function: Any) -> Any:
    """Count failed log-query calls across providers."""

    @wraps(function)
    async def wrapper(self: "LogQueryBatcher", *args: Any, **kwargs: Any) -> Any:
        try:
            return await function(self, *args, **kwargs)
        except Exception:
            LOG_QUERY_API_EXCEPTIONS.inc()
            raise

    return wrapper


def _record_log_query_metrics(
    *, backend: str, query_id: str, started: float, outcome: str, record_count: int = 0
) -> None:
    """Record bounded backend-neutral metrics without raw queries or errors."""

    LOG_QUERY_REQUESTS.labels(backend=backend, query_id=query_id, outcome=outcome).inc()
    LOG_QUERY_LATENCY.labels(backend=backend, query_id=query_id).observe(time.time() - started)
    if record_count:
        LOG_QUERY_RECORDS.labels(backend=backend, query_id=query_id).inc(record_count)


_CANCELLED_REASON = QueryFailureKind.CANCELLED.value
_AUTH_REASONS = frozenset({QueryFailureKind.UNAUTHENTICATED.value, QueryFailureKind.FORBIDDEN_OR_TENANT.value})
_OTHER_REASON = "other"


def classify_query_failure(err: BaseException) -> str:
    """
    Name why a log query failed, as a bounded label value.

    Backend failures report their `QueryFailureKind`, whichever backend raised them.

    Args:
        err: Exception raised while querying logs.

    Returns:
        str: A `QueryFailureKind` value, or "other" for failures the watcher does not classify.
    """
    if isinstance(err, asyncio.CancelledError):
        return _CANCELLED_REASON
    if isinstance(err, QueryFailure):
        return err.kind.value
    return _OTHER_REASON


class LogQueryBatcher:
    """
    Retrieve and normalize events from the selected log-query backend.

    Attributes:
        backend: Shared log-query backend currently serving queries; replaced on restart.
        limit (int): The maximum number of results to retrieve per query.
            Default is 5000.
        last_execution_time (Union[None, datetime]): Timestamp of last
            execution. Default is None.
        max_lookback (int): Maximum lookback time in seconds for retrieving
            historical data. Default is 15.
        query_delay (Union[None, int], optional): configures delay for
            query [END] timestamp: now() - [QUERY_DELAY]
    """

    def __init__(
        self,
        backend_factory: Callable[[], AsyncLogQueryBackend],
        log_query_config: LogQueryConfig,
        state_store: dict[str, Any],
        max_lookback: int = 15,
        start_override: None | datetime = None,
        end_override: None | datetime = None,
        query_delay: None | int = None,
        query_timeout: None | int | tuple[int, int] = None,
        query_limit: int = 1000,
        label_remapper: LabelRemapper | None = None,
        restart_on_auth_failure: bool = False,
        stats_recorder: StatsRecorder | None = None,
    ):
        """
        Initialize LogQueryBatcher class.

        Args:
            backend_factory: Builds the backend to query; called again on every restart.
            log_query_config: Selected backend and provider settings used to build queries.
            state_store: Durable query cursor state.
            max_lookback (int): Maximum lookback time in seconds for
                retrieving historical data. Default is 15.
            start_override (Union[None, datetime], optional): Overrides start time for all queries. Useful for testing.
            end_override (Union[None, datetime], optional): Overrides end time for all queries. Useful for testing.
            query_delay (Union[None, int], optional): configures delay for query [END] timestamp: now() - [QUERY_DELAY]
            query_timeout (Union[None, int, tuple[int, int]], optional): Timeout for queries in seconds.
                If int, applies to all phases. If tuple, first value is connect timeout, second is read/write/pool timeout.
                Defaults to (DEFAULT_LOG_QUERY_CONNECT_TIMEOUT_SECS, DEFAULT_LOG_QUERY_TIMEOUT_SECS).
            query_limit (int): Maximum number of entries per request. Default is 1000.
            label_remapper: Pre-configured provider field remapper. If None,
                no remapping is applied.
            restart_on_auth_failure: Rebuild the backend after an authentication failure, for deployments
                whose credentials can rotate.
            stats_recorder: Records each query's statistics, for providers whose statistics Watcher exports as
                metrics.
        """
        self._backend_factory = backend_factory
        self.kvstore = state_store
        self.max_lookback = max_lookback
        self.start_override = start_override
        self.end_override = end_override
        self.query_delay = query_delay
        self.query_timeout = (
            query_timeout
            if query_timeout is not None
            else (DEFAULT_LOG_QUERY_CONNECT_TIMEOUT_SECS, DEFAULT_LOG_QUERY_TIMEOUT_SECS)
        )
        self.query_limit = query_limit
        self.label_remapper = label_remapper
        self.log_query_config = log_query_config
        self.restart_on_auth_failure = restart_on_auth_failure
        self.stats_recorder = stats_recorder
        self._last_auth_restart = float("-inf")
        self._active_queries: dict[int, int] = {}
        self._retired_backends: dict[int, AsyncLogQueryBackend] = {}
        self.backend: AsyncLogQueryBackend = backend_factory()
        if self.backend.name != log_query_config.backend:
            _close_soon(self.backend)
            raise ValueError(
                f"backend_factory built a {self.backend.name} backend, but log_query_config selects "
                f"{log_query_config.backend}"
            )

    @property
    def backend_name(self) -> BackendName:
        """Provider name of the current backend, used in metric labels, logs and cursor identity."""
        return self.backend.name

    @property
    def query_dialect(self) -> QueryDialect:
        """Native query language the current backend accepts."""
        return self.backend.dialect

    def restart_client(self) -> None:
        """
        Re-initialize the configured backend client after a connection issue.

        Queries run concurrently, so other queries may still be awaiting the previous backend. It closes now
        only when idle; otherwise the last query using it closes it, so a restart never cuts off a request in
        flight.
        """
        logger.info(f"[{self.backend_name}] Restarting log-query client")
        previous_backend = self.backend
        self.backend = self._backend_factory()
        if id(previous_backend) in self._active_queries:
            self._retired_backends[id(previous_backend)] = previous_backend
        else:
            _close_soon(previous_backend)
        logger.info(f"[{self.backend_name}] Log-query client restarted successfully")

    @asynccontextmanager
    async def _checked_out_backend(self) -> AsyncIterator[AsyncLogQueryBackend]:
        """Hold the current backend for one query, closing it afterwards if a restart retired it meanwhile."""
        backend = self.backend
        key = id(backend)
        self._active_queries[key] = self._active_queries.get(key, 0) + 1
        try:
            yield backend
        finally:
            self._active_queries[key] -= 1
            if self._active_queries[key] == 0:
                del self._active_queries[key]
                if (retired := self._retired_backends.pop(key, None)) is not None:
                    _close_soon(retired)

    async def close(self) -> None:
        """Release resources owned by the shared backend."""

        await self.backend.close()

    @_count_log_query_api_exceptions
    async def query_records(
        self,
        query: str,
        start: None | datetime = None,
        end: None | datetime = None,
        limit: None | int = None,
        query_id: str | None = None,
    ) -> tuple[list[LogRecord], list[QueryStats]]:
        """
        Retrieve one normalized request from the configured backend.

        Args:
            query (str): Query string.
            start (Union[None, datetime], optional): Start.
            end (Union[None, datetime], optional): End.
            limit (Optional(int)): number of max entries in response.
            query_id (Optional[str]): Query name for metric labels. Defaults to query string.

        Returns:
            Backend response records and each page's statistics.
        """
        # Keep metric labels bounded when callers do not provide a logical query ID.
        metric_label = query_id or "anonymous"
        stable_query_label = query_id or "anonymous"

        query_latency_start = time.time()
        try:
            if start is None or end is None:
                raise ValueError("shared log-query backend requires explicit start and end times")
            async with self._checked_out_backend() as backend:
                response, stats = await self._query_shared_backend(
                    backend,
                    query=query,
                    query_id=stable_query_label,
                    start=start,
                    end=end,
                    limit=limit,
                )
        except QueryFailure as e:
            query_latency_end = time.time()
            _record_log_query_metrics(
                backend=self.backend_name,
                query_id=stable_query_label,
                started=query_latency_start,
                outcome="failure",
            )
            logger.warning(f"[{self.backend_name}] Log-query request failed: {e}")
            logger.info(
                f"[LogQuery] (query='{query}', start='{start}', end='{end}', start_ts='{start.timestamp() if start else None}', end_ts='{end.timestamp() if end else None}', limit={limit}) query FAILED with execution time: {query_latency_end - query_latency_start:.2f} secs, error: {e}"
            )
            raise  # Re-raise to let LogQueryPoller handle retry with backoff
        except Exception:
            _record_log_query_metrics(
                backend=self.backend_name,
                query_id=stable_query_label,
                started=query_latency_start,
                outcome="failure",
            )
            raise

        query_latency_end = time.time()
        LOG_QUERY_API_LATENCY.labels(query=metric_label).observe(float(query_latency_end - query_latency_start))
        logger.debug(
            f"[LogQuery] (query='{query}', start='{start}', end='{end}', start_ts='{start.timestamp() if start else None}', end_ts='{end.timestamp() if end else None}', limit={limit}) query execution time: {query_latency_end - query_latency_start:.2f} secs, stats: {_describe_stats(stats)}"
        )
        if self.stats_recorder is not None:
            self.stats_recorder(stats, bool(response), metric_label)
        _record_log_query_metrics(
            backend=self.backend_name,
            query_id=stable_query_label,
            started=query_latency_start,
            outcome="success" if response else "empty",
            record_count=len(response),
        )
        return (response, stats)

    async def _query_shared_backend(
        self,
        backend: AsyncLogQueryBackend,
        *,
        query: str,
        query_id: str,
        start: datetime,
        end: datetime,
        limit: int | None,
    ) -> tuple[list[LogRecord], list[QueryStats]]:
        """Read all pages from the selected shared backend for one fixed window."""

        timeout = self.query_timeout[-1] if isinstance(self.query_timeout, tuple) else self.query_timeout
        native_query = NativeQuery(query_id=query_id, dialect=backend.dialect, text=query)
        page_token: str | None = None
        seen_tokens: set[str] = set()
        records: list[LogRecord] = []
        query_stats: list[QueryStats] = []

        for _ in range(LOG_QUERY_COMPATIBILITY_MAX_PAGES):
            page = await backend.query_page(
                QueryRequest(
                    query=native_query,
                    start_ns=_datetime_to_epoch_ns(start),
                    end_ns=_datetime_to_epoch_ns(end),
                    order=QueryOrder.FORWARD,
                    page_size=self.query_limit if limit is None else limit,
                    page_token=page_token,
                    timeout_seconds=float(timeout),
                )
            )
            records.extend(page.records)
            query_stats.append(page.stats)
            if page.complete:
                break
            if page.next_page_token in seen_tokens:
                raise QueryFailure(
                    QueryFailureKind.INCOMPLETE,
                    backend=backend.name,
                    query_id=query_id,
                    message="backend returned a repeated page token",
                    retryable=True,
                )
            page_token = page.next_page_token
            if page_token is None:
                raise QueryFailure(
                    QueryFailureKind.INCOMPLETE,
                    backend=backend.name,
                    query_id=query_id,
                    message="backend returned an incomplete page without a continuation token",
                    retryable=True,
                )
            seen_tokens.add(page_token)
        else:
            raise QueryFailure(
                QueryFailureKind.INCOMPLETE,
                backend=backend.name,
                query_id=query_id,
                message=f"query exceeded the {LOG_QUERY_COMPATIBILITY_MAX_PAGES}-page compatibility limit",
                retryable=True,
            )

        return records, query_stats

    async def batch_query(
        self,
        query: str,
        query_id: str | None = None,
        reset: bool = False,
        start: None | datetime = None,
        end: None | datetime = None,
        limit: None | int = None,
        ignore_query_delay: bool = False,
        log_empty_response: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Execute a request against the selected log-query backend.

        Args:
            query (str): Query string.
            query_id (str): Query ID. Used for deduping.
            reset (bool): If true, clears last_execution_time from kvstore
            start (Union[None, datetime], optional): Start.
            end (Union[None, datetime], optional): End.
            limit (Optional(int)): number of max entries in response.
            ignore_query_delay (bool): ignore the configured query delay.
            log_empty_response (bool): when True, logs available provider diagnostics at INFO level.

        Returns:
            List[Dict[str, Any]]: Query results
        """
        clock_start = time.time()
        results = []
        current_time = self._query_now()
        max_lookback = self.max_lookback
        start_time = self.start_override
        end_time = self.end_override

        # An "explicit window" means start/end were supplied by the caller (or fixed by
        # instance overrides). When set, this fetch is not cursor-driven and the cursor
        # MUST NOT be written from here — committing post-fetch would advance the cursor
        # past records that have not yet been processed/published downstream, leaking
        # data on asyncio.wait_for cancellations, publish errors, or any post-fetch raise.
        # The bounded-window caller (LogQueryPoller) owns the cursor write itself and only
        # commits after the chunk's workflow callback returns successfully.
        explicit_window = bool((start and end) or (self.start_override and self.end_override))

        if start and end:
            start_time = start
            end_time = end

        # check if query_delay has been set
        if self.query_delay and not ignore_query_delay:
            current_time = current_time - timedelta(seconds=self.query_delay)
            logger.debug(
                f"[LogQuery] Query delay is set to: {self.query_delay} secs, adjusting current_time to: {current_time}"
            )

        if reset:
            self.clear_query_last_execution_time(query=query, query_id=query_id)

        last_execution_time = self._get_query_last_execution_time(query=query, query_id=query_id)

        if not start_time:
            if last_execution_time is not None:
                start_time = self._start_after_cursor(last_execution_time)
            else:
                start_time = current_time - timedelta(seconds=max_lookback)
        else:
            logger.debug(f"[LogQuery] Start time override detected: {start_time}")

        if not end_time:
            end_time = current_time
        else:
            logger.debug(f"[LogQuery] End time override detected: {end_time}")

        start_time = self._normalize_query_datetime(start_time)
        end_time = self._normalize_query_datetime(end_time)

        # Keep metric labels bounded when callers do not provide a logical query ID.
        metric_label = query_id or "anonymous"

        # Defense-in-depth: cap (end - start) to HTTP_MAX_QUERY_WINDOW_SECS to keep a
        # stuck cursor from snowballing into a multi-day query that a backend may
        # reject, which would in turn prevent the cursor
        # from advancing — a self-reinforcing loop. The poller's chunking
        # (LOG_QUERY_POLLER_CHUNK_SIZE_SECS) is the primary mechanism; this kicks in
        # only when the cursor lag exceeds the cap.
        window_secs = (end_time - start_time).total_seconds()
        if window_secs > HTTP_MAX_QUERY_WINDOW_SECS:
            skipped_secs = window_secs - HTTP_MAX_QUERY_WINDOW_SECS
            start_time = end_time - timedelta(seconds=HTTP_MAX_QUERY_WINDOW_SECS)
            degraded_marker = f" [{self.backend_name.upper()}_DEGRADED]"
            logger.warning(
                "[LogQuery]%s Query window %.0fs exceeds HTTP_MAX_QUERY_WINDOW_SECS=%ds for "
                "query=%s; fast-forwarding start to %s and skipping %.0fs of data to break out of a "
                "stuck-cursor loop.",
                degraded_marker,
                window_secs,
                HTTP_MAX_QUERY_WINDOW_SECS,
                metric_label,
                start_time,
                skipped_secs,
            )
            LOG_QUERY_WINDOW_CAPPED.labels(query=metric_label).inc(skipped_secs)

        logger.debug(f"[LogQuery] Start timestamp: {start_time}")
        logger.debug(f"[LogQuery] End timestamp: {end_time}")

        try:
            response, stats = await self.query_records(
                query=query,
                start=start_time,
                end=end_time,
                limit=limit,
                query_id=query_id,
            )
        # note (@mmelo): broad exception catching to ensure failure counters and workflow state upstream is updated
        except (Exception, asyncio.CancelledError) as err:
            logger.error(
                f"[LogQuery] Failed to get a response from {self.backend_name} for query=({query_id or query})"
            )
            reason = classify_query_failure(err)
            logger.warning(
                "[LogQuery] Query failure",
                extra={
                    "log_query_backend": self.backend_name,
                    "log_query_failure_reason": reason,
                    "log_query_error_type": type(err).__name__,
                    "log_query_id": query_id or "anonymous",
                    "log_query_elapsed_secs": round(time.time() - clock_start, 2),
                    **({"log_query_status_code": err.http_status} if isinstance(err, QueryFailure) else {}),
                },
            )
            LOG_QUERY_ERRORS.labels(query=metric_label).inc()
            LOG_QUERY_ERROR_REASONS.labels(backend=self.backend_name, reason=reason).inc()
            self._handle_auth_error_and_restart(err)
            raise
        else:
            # Self-contained cursor-driven mode only: advance the cursor right after a
            # successful fetch because no downstream owner exists. See `explicit_window`
            # above for the bounded-window contract that intentionally skips this commit.
            if not explicit_window:
                self._store_query_last_execution_time(
                    query=query,
                    query_id=query_id,
                    end_time=end_time,
                )

        if response:
            results = self._parse_records_from_backend(response)
        else:
            LOG_QUERY_EMPTY_RESPONSE.labels(query=metric_label).inc()
            if log_empty_response:
                logger.info(
                    f"[LogQuery] Empty response from {self.backend_name} (query_id={query_id or query}, "
                    f"start={start_time}, end={end_time}, limit={limit}): stats={_describe_stats(stats)}"
                )

        # add UUID to all entries in results
        for entry in results:
            entry["attributes"]["uuid"] = generate_uuid7()

        clock_end = time.time()
        LOG_QUERY_PROCESSING_TIME.labels(query=metric_label).observe(float(clock_end - clock_start))
        return results

    def _parse_records_from_backend(self, records: list[LogRecord]) -> list[dict[str, Any]]:
        """Apply Watcher's existing consumer normalization to neutral backend records."""

        compatibility_response = [
            {
                "stream": dict(record.fields),
                "values": [[str(record.timestamp_ns), record.body]],
            }
            for record in records
        ]
        return self._parse_legacy_loki_response(compatibility_response)

    def _parse_legacy_loki_response(self, response: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Parse records from the legacy Loki response.

        Args:
            response (List[Dict[str, Any]]): The response object.

        Returns:
            list: A list of records parsed from the response,
                  with each record being a dictionary
                  containing the event data and timestamp.
        """
        records = []
        # Cache remapper reference to avoid repeated attribute lookup
        remapper = self.label_remapper

        raw_event_count = sum(len(entry.get("values", [])) for entry in response)
        LOG_QUERY_RECORDS_RECEIVED.inc(raw_event_count)

        for entry in response:
            stream_labels = entry.get("stream", {})

            for event in entry["values"]:
                ts, data_text = event
                if is_json(data_text):
                    data = json.loads(data_text)
                    if "body" not in data and "message" in data:
                        data["body"] = data["message"]
                    if "attributes" not in data:
                        data["attributes"] = dict(stream_labels)
                else:
                    data = {"body": data_text}
                    data["attributes"] = dict(stream_labels)

                data["timestamp"] = ts

                if remapper is not None:
                    apply_label_remapping(data["attributes"], stream_labels, remapper)

                # quick hack to convert "filename" to "log.file.name" and "log.file.path"
                if "filename" in data["attributes"] and "log.file.name" not in data["attributes"]:
                    data["attributes"]["log.file.path"] = data["attributes"]["filename"]
                    data["attributes"]["log.file.name"] = os.path.basename(data["attributes"]["filename"])
                    del data["attributes"]["filename"]

                if (
                    self._does_record_contain_jobid(data) or self._is_heartbeat_log(data)
                ) and self._does_record_contain_filename(data):
                    # Inject job_id for heartbeat logs to prevent downstream workflow issues
                    if self._is_heartbeat_log(data) and not self._does_record_contain_jobid(data):
                        data["attributes"]["job_id"] = "000000"

                    records.append(data)
                elif stream_labels.get("pod_owner", None) is not None:
                    job_name = stream_labels.get("pod_owner")
                    job_name = job_name.replace("Job/", "")
                    logger.debug(f"[LogQuery] Extracted job name: {job_name} from response: {response}")
                    data["attributes"] = {}
                    data["attributes"]["job_id"] = job_name
                    data["attributes"]["log.file.path"] = (
                        job_name  # This needs to match for hangs to be processed by job manager
                    )
                    data["attributes"]["log.file.name"] = job_name  # TODO find a way to track iteration number
                    records.append(data)
                else:
                    logger.warning(
                        f"[LogQuery] Event does not contain one of the following attrs [job_id, filename] , discarding it: {data}"
                    )

        pre_dedup_count = len(records)
        records = deduplicate_records(records)
        duplicates_removed = pre_dedup_count - len(records)
        if duplicates_removed:
            logger.info(f"[LogQuery] Deduplicated {duplicates_removed} records ({pre_dedup_count} -> {len(records)})")
            LOG_QUERY_RECORDS_DUPLICATE_DISCARDED.inc(duplicates_removed)

        return records

    def _does_record_contain_jobid(self, record: dict[str, Any]) -> bool:
        """
        Checks if record from a normalized log-query response contains [job_id].

        Args:
            record (Dict[str, Any]): Normalized log record.

        Returns:
            bool: does record contain [job_id]
        """
        return "attributes" in record and "job_id" in record["attributes"]

    def _does_record_contain_filename(self, record: dict[str, Any]) -> bool:
        """
        Checks if record from a normalized log-query response contains [filename].

        Args:
            record (Dict[str, Any]): Normalized log record.

        Returns:
            bool: does record contain [filename]
        """
        filename_attrs = ("filename", "log.file.name", "log_file_name")
        return "attributes" in record and any(attr in record["attributes"] for attr in filename_attrs)

    def _is_heartbeat_log(self, record: dict[str, Any]) -> bool:
        """
        Checks if record is a heartbeat application log.

        Args:
            record (Dict[str, Any]): Normalized log record.

        Returns:
            bool: is record a heartbeat log
        """
        body = record.get("body", "")
        return isinstance(body, str) and "heartbeat_application_logs" in body

    def _get_query_last_execution_time(self, query: str, query_id: str | None = None) -> int | None:
        """
        Retrieve the last execution time for a given query.

        Args:
            query (str): The query string.
            query_id (str, optional): An optional query ID. Defaults to None.

        Returns:
            Integer epoch nanoseconds; ``None`` if the cursor is absent or invalid.
        """

        query_signature = self._query_cursor_signature(query=query, query_id=query_id)
        try:
            raw_cursor = self.kvstore[query_signature]
            if isinstance(raw_cursor, bytes):
                raw_cursor = raw_cursor.decode("ascii")
            if not isinstance(raw_cursor, str) or not raw_cursor.startswith(_CURSOR_NS_PREFIX):
                raise ValueError("shared cursor is not a versioned nanosecond value")
            last_execution_time = int(raw_cursor.removeprefix(_CURSOR_NS_PREFIX))
            if last_execution_time < 0:
                raise ValueError("shared cursor timestamp must not be negative")
        except KeyError as err:
            logger.debug(f"[LogQuery] Unable to find last_execution_time query=({query}): {err}")
            return None
        except (UnicodeDecodeError, TypeError, ValueError) as err:
            logger.debug(f"[LogQuery] Incorrect format last_execution_time query=({query}): {err}")
            del self.kvstore[query_signature]
            return None

        logger.debug(f"[LogQuery] Found last_execution_time for query=({query}): {last_execution_time}")
        return last_execution_time

    def _store_query_last_execution_time(
        self,
        query: str,
        end_time: datetime | float,
        query_id: str | None = None,
    ) -> None:
        """
        Store the last execution time for a given query and optional query ID.

        Args:
            query (str): The query string.
            end_time: End of the query execution. Shared backends require a
                datetime or integer nanoseconds; legacy Loki accepts seconds.
            query_id (str, optional): An optional query ID. Defaults to None.
        """
        query_signature = self._query_cursor_signature(query=query, query_id=query_id)
        if isinstance(end_time, datetime):
            end_ns = _datetime_to_epoch_ns(end_time)
        elif isinstance(end_time, int) and not isinstance(end_time, bool):
            end_ns = end_time
        else:
            raise TypeError("shared log-query cursors require datetime or integer nanoseconds")
        if end_ns < 0:
            raise ValueError("shared cursor timestamp must not be negative")
        self.kvstore[query_signature] = f"{_CURSOR_NS_PREFIX}{end_ns}"

    def clear_query_last_execution_time(self, query, query_id: str | None = None) -> None:
        """
        Clear the stored last execution time for a given query and
        optional query ID.

        Args:
            query (str): The query string.
            query_id (str, optional): An optional query ID. Defaults to None.
        """
        logger.warning(f"[LogQuery] Clearing last_execution_time for query=({query})")
        query_signature = self._query_cursor_signature(query=query, query_id=query_id)
        del self.kvstore[query_signature]

    def _query_cursor_signature(self, *, query: str, query_id: str | None) -> str:
        """Namespace durable cursors by backend and provider-native query revision."""

        native_query = NativeQuery(query_id=query_id or query, dialect=self.query_dialect, text=query)
        return generate_sha256_signature(
            data=f"log-query:{self.backend_name}:{native_query.revision}:{query_id or query}"
        )

    def _start_after_cursor(self, cursor_timestamp: int) -> datetime:
        """Resume exactly at the committed end, which shared backends define as exclusive."""

        if not isinstance(cursor_timestamp, int) or isinstance(cursor_timestamp, bool):
            raise TypeError("shared log-query cursor must contain integer nanoseconds")
        seconds, nanoseconds = divmod(cursor_timestamp, 1_000_000_000)
        if nanoseconds % 1_000:
            raise ValueError("Watcher cannot resume a cursor below microsecond precision")
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
            seconds=seconds,
            microseconds=nanoseconds // 1_000,
        )

    def _query_now(self) -> datetime:
        """Return the current time in the UTC convention shared backends use."""

        return datetime.now(UTC)

    def _normalize_query_datetime(self, value: datetime) -> datetime:
        """Normalize query bounds to aware UTC."""

        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def cursor_query_identity(self, config: dict[str, Any], query_id: str) -> str:
        """Return stable cursor input including the active native query variant."""

        provider = self.log_query_config.selected_provider
        native = provider.queries.workflows.get(config.get("name", query_id)) if provider is not None else None
        prefix = provider.queries.query_prefix if provider is not None else ""
        return json.dumps(
            {"query_id": query_id, "prefix": prefix, "queries": native.to_json() if native is not None else None},
            sort_keys=True,
            separators=(",", ":"),
        )

    def _handle_auth_error_and_restart(self, error: BaseException) -> bool:
        """
        Restart the Loki client if the backend rejected the watcher's credentials.

        The failure reason is read from the classified error, not its message text, which
        never carries the HTTP status. Restarts are limited to one per cooldown: with
        credentials that stay bad every query fails, and each failure would otherwise
        restart the client again.

        Args:
            error: The exception raised by the failed query.

        A failed restart is logged and does not consume the cooldown, so the caller still
        sees the original query failure and the next auth failure tries again.

        Returns:
            bool: True if client was restarted, False otherwise
        """
        if classify_query_failure(error) not in _AUTH_REASONS:
            return False

        if not self.restart_on_auth_failure:
            logger.warning(
                f"[{self.backend_name}] Authentication or tenant error detected; "
                "configuration must be corrected before retry can succeed."
            )
            return False

        now = time.monotonic()
        since_restart = now - self._last_auth_restart
        if since_restart < LOG_QUERY_AUTH_RESTART_COOLDOWN_SECS:
            logger.warning(
                f"[{self.backend_name}] Authentication-related error detected. "
                f"Client restarted {since_restart:.0f}s ago; skipping restart."
            )
            return False

        logger.warning(f"[{self.backend_name}] Authentication-related error detected. Restarting client.")
        try:
            self.restart_client()
        except Exception:
            logger.exception(f"[{self.backend_name}] Failed to restart client; keeping the original query failure.")
            return False
        self._last_auth_restart = now
        LOG_QUERY_POLLER_AUTH_RESTARTS.inc()
        return True
