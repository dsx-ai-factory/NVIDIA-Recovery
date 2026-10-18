# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import time

import pytest
from recovery_libs.log_query import QueryStats
from watcher.lib.utils.loki_stats import (
    LOKI_DEGRADED,
    LOKI_STATS_BYTES_PROCESSED,
    LOKI_STATS_ENTRIES_RETURNED,
    LOKI_STATS_EXEC_TIME,
    LOKI_STATS_INGESTER_CHUNKS_MATCHED,
    LOKI_STATS_INGESTER_REACHED,
    LOKI_STATS_STORE_CHUNKS_DOWNLOADED,
    DegradationSignal,
    LokiQueryPerformanceMonitor,
    StatsObservation,
    _observations,
    record_query_stats,
)
from watcher.lib.utils.metrics import LOG_QUERY_POLLER_ITERATION_BUDGET_EXCEEDED


def _make_stats(summary=None, ingester=None, store=None):
    entry = {}
    if summary is not None:
        entry["summary"] = summary
    if ingester is not None:
        entry["ingester"] = ingester
    if store is not None:
        entry["store"] = store
    return [QueryStats(duration_seconds=0.01, attempts=1, response_bytes=100, returned_records=0, provider=entry)]


def _hsum(h, label):
    return h.labels(query=label)._sum._value


def _counter_total(counter):
    return sum(sample.value for mf in counter.collect() for sample in mf.samples if sample.name.endswith("_total"))


def _obs(**kw):
    defaults = {
        "timestamp": time.time(),
        "query_label": "q",
        "has_results": True,
        "bytes_processed": 100,
        "lines_processed": 10,
        "exec_time": 0.5,
        "queue_time": 0.01,
        "chunks_matched": 5,
        "chunks_downloaded": 3,
        "ingesters_reached": 3,
    }
    defaults.update(kw)
    return StatsObservation(**defaults)


SUMMARY = {
    "totalBytesProcessed": 5000,
    "totalLinesProcessed": 200,
    "execTime": 0.5,
    "queueTime": 0.01,
    "totalEntriesReturned": 42,
}
INGESTER = {"totalChunksMatched": 8, "totalLinesSent": 150, "totalReached": 3}
STORE = {"totalChunksDownloaded": 12}


@pytest.fixture(autouse=True)
def _clear_observations():
    _observations.clear()
    LOKI_DEGRADED.set(0)
    yield
    _observations.clear()
    LOKI_DEGRADED.set(0)


@pytest.fixture
def monitor():
    return LokiQueryPerformanceMonitor(
        kvstore={"loki_perf_monitor_prev_budget_exceeded": _counter_total(LOG_QUERY_POLLER_ITERATION_BUDGET_EXCEEDED)},
        empty_threshold=3,
        window_secs=300,
    )


@pytest.mark.parametrize(
    "metric, section, field, value",
    [
        (LOKI_STATS_BYTES_PROCESSED, "summary", "totalBytesProcessed", SUMMARY["totalBytesProcessed"]),
        (LOKI_STATS_EXEC_TIME, "summary", "execTime", SUMMARY["execTime"]),
        (LOKI_STATS_ENTRIES_RETURNED, "summary", "totalEntriesReturned", SUMMARY["totalEntriesReturned"]),
        (LOKI_STATS_INGESTER_CHUNKS_MATCHED, "ingester", "totalChunksMatched", INGESTER["totalChunksMatched"]),
        (LOKI_STATS_INGESTER_REACHED, "ingester", "totalReached", INGESTER["totalReached"]),
        (LOKI_STATS_STORE_CHUNKS_DOWNLOADED, "store", "totalChunksDownloaded", STORE["totalChunksDownloaded"]),
    ],
)
def test_records_stat_field(metric, section, field, value):
    label = f"test_{section}_{field}"
    before = _hsum(metric, label)
    record_query_stats(_make_stats(SUMMARY, INGESTER, STORE), has_results=True, query_label=label)
    assert _hsum(metric, label) == before + value


@pytest.mark.parametrize(
    "stats, metric",
    [
        (_make_stats(summary=SUMMARY), LOKI_STATS_INGESTER_CHUNKS_MATCHED),
        (_make_stats(summary={"totalBytesProcessed": 100}), LOKI_STATS_ENTRIES_RETURNED),
        ([], LOKI_STATS_BYTES_PROCESSED),
    ],
)
def test_missing_data_skips_histograms(stats, metric):
    label = "test_skip"
    before = _hsum(metric, label)
    record_query_stats(stats, has_results=False, query_label=label)
    assert _hsum(metric, label) == before


def test_record_appends_observation():
    record_query_stats(_make_stats(SUMMARY, INGESTER, STORE), has_results=True, query_label="q")
    assert len(_observations) == 1
    assert _observations[0].has_results is True
    assert _observations[0].bytes_processed == SUMMARY["totalBytesProcessed"]


def test_record_empty_stats_appends_zero_observation():
    record_query_stats([], has_results=False, query_label="q")
    assert len(_observations) == 1 and _observations[0].bytes_processed is None


def test_evaluate_healthy(monitor):
    _observations.extend([_obs() for _ in range(5)])
    assert monitor.evaluate() == DegradationSignal.NONE
    assert LOKI_DEGRADED._value._value == 0


def test_evaluate_consecutive_empty(monitor):
    _observations.extend(
        [
            _obs(
                query_label="nooutput_val_q",
                has_results=False,
                bytes_processed=0,
                chunks_matched=0,
                chunks_downloaded=0,
            )
            for _ in range(3)
        ]
    )
    result = monitor.evaluate()
    assert DegradationSignal.CONSECUTIVE_EMPTY_RESPONSES in result
    assert LOKI_DEGRADED._value._value == 1


def test_evaluate_genuine_empty_validation_is_healthy(monitor):
    _observations.extend([_obs(query_label="nooutput_val_q", has_results=False) for _ in range(3)])
    assert monitor.evaluate() == DegradationSignal.NONE


def test_evaluate_missing_stats_empty_is_healthy(monitor):
    _observations.extend(
        [
            _obs(
                query_label="nooutput_val_q",
                has_results=False,
                bytes_processed=None,
                lines_processed=None,
                chunks_matched=None,
                chunks_downloaded=None,
            )
            for _ in range(3)
        ]
    )
    assert monitor.evaluate() == DegradationSignal.NONE


def test_evaluate_zero_scan_empty(monitor):
    _observations.extend(
        [
            _obs(has_results=False, bytes_processed=0, lines_processed=0, chunks_matched=0, chunks_downloaded=0)
            for _ in range(3)
        ]
    )
    result = monitor.evaluate()
    assert DegradationSignal.ZERO_SCAN_EMPTY in result
    assert DegradationSignal.CONSECUTIVE_EMPTY_RESPONSES not in result


def test_evaluate_high_queue_time(monitor):
    _observations.extend([_obs(exec_time=0.1, queue_time=0.9) for _ in range(3)])
    assert DegradationSignal.HIGH_QUEUE_TIME in monitor.evaluate()


def test_evaluate_high_queue_time_below_threshold(monitor):
    _observations.extend([_obs(exec_time=0.1, queue_time=0.9) for _ in range(2)])
    assert DegradationSignal.HIGH_QUEUE_TIME not in monitor.evaluate()


def test_evaluate_zero_scan_below_threshold(monitor):
    _observations.extend(
        [
            _obs(has_results=False, bytes_processed=0, lines_processed=0, chunks_matched=0, chunks_downloaded=0)
            for _ in range(2)
        ]
    )
    assert DegradationSignal.ZERO_SCAN_EMPTY not in monitor.evaluate()


def test_evaluate_budget_exceeded(monitor):
    LOG_QUERY_POLLER_ITERATION_BUDGET_EXCEEDED.labels(query="test_budget").inc()
    assert DegradationSignal.ITERATION_BUDGET_EXCEEDED in monitor.evaluate()


def test_evaluate_budget_delta_only(monitor):
    LOG_QUERY_POLLER_ITERATION_BUDGET_EXCEEDED.labels(query="test_budget_delta").inc()
    monitor.evaluate()
    assert DegradationSignal.ITERATION_BUDGET_EXCEEDED not in monitor.evaluate()


def test_evaluate_window_boundary(monitor):
    old = _obs(has_results=False)
    old.timestamp = time.time() - 600
    _observations.extend([old] * 5)
    assert monitor.evaluate() == DegradationSignal.NONE


def test_evaluate_multiple_signals(monitor):
    _observations.extend(
        [
            _obs(
                query_label="nooutput_val_q",
                has_results=False,
                bytes_processed=0,
                lines_processed=0,
                chunks_matched=0,
                chunks_downloaded=0,
                exec_time=0.1,
                queue_time=0.9,
            )
            for _ in range(3)
        ]
    )
    result = monitor.evaluate()
    expected = (
        DegradationSignal.CONSECUTIVE_EMPTY_RESPONSES
        | DegradationSignal.ZERO_SCAN_EMPTY
        | DegradationSignal.HIGH_QUEUE_TIME
    )
    assert result & expected == expected


def test_evaluate_if_due_first_call(monitor):
    assert monitor.evaluate_if_due() is not None


def test_evaluate_if_due_throttles(monitor):
    monitor.evaluate_if_due()
    assert monitor.evaluate_if_due() is None


def test_evaluate_if_due_runs_after_interval(monitor):
    monitor.interval_secs = 0.01
    monitor.evaluate_if_due()
    time.sleep(0.02)
    assert monitor.evaluate_if_due() is not None
