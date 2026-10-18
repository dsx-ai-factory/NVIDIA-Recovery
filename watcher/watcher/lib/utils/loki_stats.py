# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Loki query stats recording and performance monitoring.

Records raw stats fields from Loki's query_range API response as Prometheus
metrics and feeds observations to a performance monitor that detects four
degradation signals. See docs/LOKI.md for details.
"""

import time
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Flag, auto
from typing import Any

from prometheus_client import Gauge, Histogram
from recovery_libs.log_query import LokiQueryStats, QueryStats

from watcher.lib.utils.constants import (
    LOKI_PERF_MONITOR_EMPTY_THRESHOLD,
    LOKI_PERF_MONITOR_HIGH_QUEUE_THRESHOLD,
    LOKI_PERF_MONITOR_INTERVAL_SECS,
    LOKI_PERF_MONITOR_MAX_OBSERVATIONS,
    LOKI_PERF_MONITOR_QUEUE_TIME_RATIO,
    LOKI_PERF_MONITOR_WINDOW_SECS,
    LOKI_PERF_MONITOR_ZERO_SCAN_THRESHOLD,
)
from watcher.lib.utils.logging import logger
from watcher.lib.utils.metrics import LOG_QUERY_POLLER_ITERATION_BUDGET_EXCEEDED

LABELS_QUERY = ["query"]

# --- summary section ---
LOKI_STATS_BYTES_PROCESSED = Histogram(
    "watcher_loki_stats_bytes_processed",
    "Bytes processed by Loki (summary.totalBytesProcessed)",
    LABELS_QUERY,
    buckets=(0, 1024, 65536, 1048576, 16777216, 268435456, float("inf")),
)
LOKI_STATS_LINES_PROCESSED = Histogram(
    "watcher_loki_stats_lines_processed",
    "Lines processed by Loki (summary.totalLinesProcessed)",
    LABELS_QUERY,
    buckets=(0, 10, 100, 1000, 10000, 100000, 1000000, float("inf")),
)
LOKI_STATS_EXEC_TIME = Histogram(
    "watcher_loki_stats_exec_time_seconds",
    "Query execution time reported by Loki (summary.execTime)",
    LABELS_QUERY,
)
LOKI_STATS_QUEUE_TIME = Histogram(
    "watcher_loki_stats_queue_time_seconds",
    "Queue wait time reported by Loki (summary.queueTime)",
    LABELS_QUERY,
)
LOKI_STATS_ENTRIES_RETURNED = Histogram(
    "watcher_loki_stats_entries_returned",
    "Entries returned by Loki (summary.totalEntriesReturned)",
    LABELS_QUERY,
    buckets=(0, 1, 10, 50, 100, 500, 1000, 5000, float("inf")),
)

# --- ingester section ---
LOKI_STATS_INGESTER_CHUNKS_MATCHED = Histogram(
    "watcher_loki_stats_ingester_chunks_matched",
    "Chunks matched by ingesters (ingester.totalChunksMatched)",
    LABELS_QUERY,
    buckets=(0, 1, 5, 10, 50, 100, 500, float("inf")),
)
LOKI_STATS_INGESTER_LINES_SENT = Histogram(
    "watcher_loki_stats_ingester_lines_sent",
    "Lines sent by ingesters (ingester.totalLinesSent)",
    LABELS_QUERY,
    buckets=(0, 10, 100, 1000, 10000, 100000, float("inf")),
)
LOKI_STATS_INGESTER_REACHED = Histogram(
    "watcher_loki_stats_ingester_reached",
    "Number of ingesters reached (ingester.totalReached)",
    LABELS_QUERY,
    buckets=(0, 1, 2, 3, 5, 10, 20, float("inf")),
)

# --- store (querier) section ---
LOKI_STATS_STORE_CHUNKS_DOWNLOADED = Histogram(
    "watcher_loki_stats_store_chunks_downloaded",
    "Chunks downloaded from store (store.totalChunksDownloaded)",
    LABELS_QUERY,
    buckets=(0, 1, 5, 10, 50, 100, 500, float("inf")),
)

_STATS_FIELD_MAP: list[tuple[str, Histogram]] = [
    ("bytes_processed", LOKI_STATS_BYTES_PROCESSED),
    ("lines_processed", LOKI_STATS_LINES_PROCESSED),
    ("exec_time_seconds", LOKI_STATS_EXEC_TIME),
    ("queue_time_seconds", LOKI_STATS_QUEUE_TIME),
    ("entries_returned", LOKI_STATS_ENTRIES_RETURNED),
    ("ingester_chunks_matched", LOKI_STATS_INGESTER_CHUNKS_MATCHED),
    ("ingester_lines_sent", LOKI_STATS_INGESTER_LINES_SENT),
    ("ingesters_reached", LOKI_STATS_INGESTER_REACHED),
    ("store_chunks_downloaded", LOKI_STATS_STORE_CHUNKS_DOWNLOADED),
]

LOKI_DEGRADED = Gauge(
    "watcher_loki_degraded",
    "Whether Loki is in a degraded state (0=healthy, 1=degraded)",
)

_KVSTORE_PREFIX = "loki_perf_monitor_"
_KVSTORE_LAST_RUN = f"{_KVSTORE_PREFIX}last_run"
_KVSTORE_PREV_BUDGET_EXCEEDED = f"{_KVSTORE_PREFIX}prev_budget_exceeded"
_KVSTORE_PREV_BUDGET_EXCEEDED_BY_QUERY = f"{_KVSTORE_PREFIX}prev_budget_exceeded_by_query"
_KVSTORE_PREV_DEGRADED = f"{_KVSTORE_PREFIX}prev_degraded"


class DegradationSignal(Flag):
    NONE = 0
    CONSECUTIVE_EMPTY_RESPONSES = auto()
    ITERATION_BUDGET_EXCEEDED = auto()
    ZERO_SCAN_EMPTY = auto()
    HIGH_QUEUE_TIME = auto()


@dataclass
class StatsObservation:
    timestamp: float
    query_label: str
    has_results: bool
    bytes_processed: float | None
    lines_processed: float | None
    exec_time: float | None
    queue_time: float | None
    chunks_matched: float | None
    chunks_downloaded: float | None
    ingesters_reached: float | None


_observations: deque = deque(maxlen=LOKI_PERF_MONITOR_MAX_OBSERVATIONS)

_resolved_metrics: dict[str, list] = {}


def _resolve_metrics(query_label: str) -> list:
    """Return pre-resolved labeled metric children for *query_label*.

    Caches the ``.labels(query=...)`` call so the hot-path only pays
    for it once per distinct label.
    """
    cached = _resolved_metrics.get(query_label)
    if cached is not None:
        return cached
    resolved = [(field, metric.labels(query=query_label)) for field, metric in _STATS_FIELD_MAP]
    _resolved_metrics[query_label] = resolved
    return resolved


def _read_counter_total(counter) -> float:
    """Sum a Prometheus Counter across all label combinations."""
    total = 0.0
    for mf in counter.collect():
        for sample in mf.samples:
            if sample.name.endswith("_total"):
                total += sample.value
    return total


def _read_counter_by_label(counter, label: str) -> dict[str, float]:
    """Return per-label totals for a Prometheus Counter.

    Maps ``label_value`` -> cumulative ``*_total`` sample value. Used to
    attribute counter deltas (e.g. iteration budget exceeded events) back to
    the specific labeled child (e.g. the query that exceeded the budget).
    """
    result: dict[str, float] = {}
    for mf in counter.collect():
        for sample in mf.samples:
            if not sample.name.endswith("_total"):
                continue
            key = sample.labels.get(label, "")
            result[key] = result.get(key, 0.0) + sample.value
    return result


def _has_scan_stats(observation: StatsObservation) -> bool:
    return all(
        value is not None
        for value in (
            observation.bytes_processed,
            observation.lines_processed,
            observation.chunks_matched,
            observation.chunks_downloaded,
        )
    )


def _is_genuine_empty(observation: StatsObservation) -> bool:
    if not _has_scan_stats(observation):
        return False
    return (
        (observation.bytes_processed or 0) > 0
        and (observation.lines_processed or 0) > 0
        and ((observation.chunks_matched or 0) > 0 or (observation.chunks_downloaded or 0) > 0)
    )


def _is_suspicious_validation_empty(observation: StatsObservation) -> bool:
    return (
        not observation.has_results
        and observation.query_label.startswith("nooutput_val_")
        and _has_scan_stats(observation)
        and not _is_genuine_empty(observation)
    )


def _count_by_query(observations: list[StatsObservation]) -> dict[str, int]:
    """Tally observations grouped by ``query_label``.

    Used to attribute degradation signals back to specific queries in the
    INFO log emitted by ``LokiQueryPerformanceMonitor.evaluate``.
    """
    counts: dict[str, int] = {}
    for obs in observations:
        counts[obs.query_label] = counts.get(obs.query_label, 0) + 1
    return counts


def _format_count_map(counts: Mapping[str, float], top_n: int = 5) -> str:
    """Format a {label: count} map as ``label=N`` pairs, top contributors first."""
    if not counts:
        return "none"
    items = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    return ", ".join(f"{label}={int(count)}" for label, count in items)


class LokiQueryPerformanceMonitor:
    """Periodic evaluator of Loki query performance.

    Reads from the module-level ``_observations`` ring buffer (populated by
    ``record_query_stats``) and from Prometheus counters to detect four
    degradation signals:

    - ``CONSECUTIVE_EMPTY_RESPONSES`` — burst of empty responses within the
      evaluation window, indicating systemic query failures across jobs.
    - ``ITERATION_BUDGET_EXCEEDED`` — LogQueryPoller exceeded its iteration time
      budget since the last evaluation, indicating Loki is under heavy load.
    - ``ZERO_SCAN_EMPTY`` — queries returned empty without scanning any data
      (totalBytesProcessed=0, totalLinesProcessed=0, no chunks accessed),
      indicating querier-side short-circuit or index lookup failure.
    - ``HIGH_QUEUE_TIME`` — queries spent a disproportionate amount of time
      waiting in queue relative to execution time, indicating querier overload.
    """

    def __init__(
        self,
        kvstore: dict[str, Any],
        interval_secs: float = LOKI_PERF_MONITOR_INTERVAL_SECS,
        window_secs: float = LOKI_PERF_MONITOR_WINDOW_SECS,
        empty_threshold: int = LOKI_PERF_MONITOR_EMPTY_THRESHOLD,
        zero_scan_threshold: int = LOKI_PERF_MONITOR_ZERO_SCAN_THRESHOLD,
        high_queue_threshold: int = LOKI_PERF_MONITOR_HIGH_QUEUE_THRESHOLD,
        queue_time_ratio: float = LOKI_PERF_MONITOR_QUEUE_TIME_RATIO,
    ):
        self.kvstore = kvstore
        self.interval_secs = interval_secs
        self.window_secs = window_secs
        self.empty_threshold = empty_threshold
        self.zero_scan_threshold = zero_scan_threshold
        self.high_queue_threshold = high_queue_threshold
        self.queue_time_ratio = queue_time_ratio

    def evaluate(self) -> DegradationSignal:
        """Evaluate current Loki health and update the ``LOKI_DEGRADED`` gauge.

        Returns a ``DegradationSignal`` flag set containing all active signals
        (can be multiple simultaneously).
        """
        signals = DegradationSignal.NONE
        reasons: list[str] = []
        details: list[str] = []

        now = time.time()
        cutoff = now - self.window_secs
        recent = [o for o in _observations if o.timestamp >= cutoff]
        empty_recent = [o for o in recent if not o.has_results]
        suspicious_validation_empty = [o for o in empty_recent if _is_suspicious_validation_empty(o)]

        # --- Signal 1: consecutive empty responses ---
        if len(suspicious_validation_empty) >= self.empty_threshold:
            signals |= DegradationSignal.CONSECUTIVE_EMPTY_RESPONSES
            reasons.append(
                f"CONSECUTIVE_EMPTY_RESPONSES: {len(suspicious_validation_empty)} suspicious empty "
                f"validation responses in the last {self.window_secs:.0f}s "
                f"(threshold: {self.empty_threshold})"
            )
            by_query = _count_by_query(suspicious_validation_empty)
            details.append(f"CONSECUTIVE_EMPTY_RESPONSES per-query: {_format_count_map(by_query)}")

        # --- Signal 2: iteration budget exceeded ---
        current_by_query = _read_counter_by_label(LOG_QUERY_POLLER_ITERATION_BUDGET_EXCEEDED, "query")
        current_total = sum(current_by_query.values())
        prev_total = float(self.kvstore.get(_KVSTORE_PREV_BUDGET_EXCEEDED, 0.0))
        prev_by_query = self.kvstore.get(_KVSTORE_PREV_BUDGET_EXCEEDED_BY_QUERY) or {}
        self.kvstore[_KVSTORE_PREV_BUDGET_EXCEEDED] = current_total
        self.kvstore[_KVSTORE_PREV_BUDGET_EXCEEDED_BY_QUERY] = dict(current_by_query)
        delta = current_total - prev_total
        if delta > 0:
            signals |= DegradationSignal.ITERATION_BUDGET_EXCEEDED
            reasons.append(f"ITERATION_BUDGET_EXCEEDED: {int(delta)} budget exceeded events since last evaluation")
            per_query_deltas = {
                query: count - prev_by_query.get(query, 0.0)
                for query, count in current_by_query.items()
                if count - prev_by_query.get(query, 0.0) > 0
            }
            details.append(
                "ITERATION_BUDGET_EXCEEDED attributed to queries (counter deltas since last eval): "
                f"{_format_count_map(per_query_deltas)}. "
                "See preceding [LogQueryPoller] WARNING(s) for per-iteration timing breakdown."
            )

        # --- Signal 3: zero-scan empty — short-circuit) ---
        zero_scan = [
            o
            for o in empty_recent
            if _has_scan_stats(o)
            and o.bytes_processed == 0
            and o.lines_processed == 0
            and o.chunks_matched == 0
            and o.chunks_downloaded == 0
        ]
        if len(zero_scan) >= self.zero_scan_threshold:
            signals |= DegradationSignal.ZERO_SCAN_EMPTY
            reasons.append(
                f"ZERO_SCAN_EMPTY: {len(zero_scan)} queries returned empty without "
                f"scanning any data in the last {self.window_secs:.0f}s "
                f"(threshold: {self.zero_scan_threshold})"
            )
            details.append(f"ZERO_SCAN_EMPTY per-query: {_format_count_map(_count_by_query(zero_scan))}")

        # --- Signal 4: high queue time - partial degradation ---
        high_queue = [
            o
            for o in recent
            if o.exec_time is not None
            and o.queue_time is not None
            and (o.exec_time + o.queue_time) > 0
            and o.queue_time / (o.exec_time + o.queue_time) > self.queue_time_ratio
        ]
        if len(high_queue) >= self.high_queue_threshold:
            signals |= DegradationSignal.HIGH_QUEUE_TIME
            reasons.append(
                f"HIGH_QUEUE_TIME: {len(high_queue)} queries had queue time exceeding "
                f"{self.queue_time_ratio * 100:.0f}% of total time in the last {self.window_secs:.0f}s "
                f"(threshold: {self.high_queue_threshold})"
            )
            # Guard with default=0 in case high_queue_threshold is configured to 0 (which
            # would let this branch fire on an empty high_queue and make max(...) raise).
            max_ratio = max(
                (o.queue_time / (o.exec_time + o.queue_time) for o in high_queue),
                default=0,
            )
            details.append(
                f"HIGH_QUEUE_TIME per-query: {_format_count_map(_count_by_query(high_queue))} "
                f"(max queue ratio observed: {max_ratio * 100:.0f}%)"
            )

        was_degraded = self.kvstore.get(_KVSTORE_PREV_DEGRADED, False)
        LOKI_DEGRADED.set(1 if signals else 0)
        self.kvstore[_KVSTORE_PREV_DEGRADED] = bool(signals)

        if signals:
            logger.warning(
                f"[Loki] [LOKI_DEGRADED] Performance degradation detected — active signals: {' | '.join(reasons)}"
            )
            logger.info(
                "[Loki] [LOKI_DEGRADED] Gauge set to 1 — window=%.0fs, observations=%d (empty=%d). Why: %s",
                self.window_secs,
                len(recent),
                len(empty_recent),
                " || ".join(details) if details else "see WARNING above",
            )
        elif was_degraded:
            logger.info("[Loki] [LOKI_DEGRADED] Performance recovered — gauge set to 0, all signals cleared")

        return signals

    def evaluate_if_due(self) -> DegradationSignal | None:
        """Run ``evaluate`` only if ``interval_secs`` has elapsed since last run."""
        now = time.time()
        last_run = self.kvstore.get(_KVSTORE_LAST_RUN)
        if last_run is not None and (now - float(last_run)) < self.interval_secs:
            return None
        self.kvstore[_KVSTORE_LAST_RUN] = now
        return self.evaluate()


def record_query_stats(
    stats: Sequence[QueryStats],
    has_results: bool,
    query_label: str,
) -> None:
    """Record a query's Loki stats as histogram observations and feed the monitor.

    Uses the first page's statistics. Records histogram observations for every
    field Loki reported and appends an observation to the module-level ring
    buffer consumed by ``LokiQueryPerformanceMonitor``. Fields Loki omits are
    skipped, since the reported sections vary by Loki version.
    """
    loki = LokiQueryStats.from_query_stats(stats[0]) if stats else LokiQueryStats()
    logger.debug("[Loki] Query stats (query=%s): %s", query_label, loki)

    for field, child in _resolve_metrics(query_label):
        value = getattr(loki, field)
        if value is not None:
            child.observe(value)

    _observations.append(
        StatsObservation(
            timestamp=time.time(),
            query_label=query_label,
            has_results=has_results,
            bytes_processed=loki.bytes_processed,
            lines_processed=loki.lines_processed,
            exec_time=loki.exec_time_seconds,
            queue_time=loki.queue_time_seconds,
            chunks_matched=loki.ingester_chunks_matched,
            chunks_downloaded=loki.store_chunks_downloaded,
            ingesters_reached=loki.ingesters_reached,
        )
    )
