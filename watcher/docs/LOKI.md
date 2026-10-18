# Loki Query Stats Monitoring

These statistics are provider-specific diagnostics. When Watcher selects
VictoriaLogs through `datasources.log_query`, backend-neutral query health and
request metrics remain available, but Watcher does not fabricate Loki scan,
queue, ingester, or store statistics. See
[`docs/architecture/log-query-backends.md`](../../docs/architecture/log-query-backends.md).

The watcher records raw stats fields from every Loki `query_range` response
as Prometheus histogram metrics and feeds them to a periodic performance
monitor that detects degradation. All metrics and the monitor are implemented
in `watcher.lib.utils.loki_stats`.

## Stats Field to Metric Mapping

Every successful `query_range` call records histogram observations for all
available fields. Missing sections or fields are silently skipped, so this
is safe across different Loki versions.

### summary

| Loki stats field | Prometheus metric |
|---|---|
| `summary.totalBytesProcessed` | `watcher_loki_stats_bytes_processed` |
| `summary.totalLinesProcessed` | `watcher_loki_stats_lines_processed` |
| `summary.execTime` | `watcher_loki_stats_exec_time_seconds` |
| `summary.queueTime` | `watcher_loki_stats_queue_time_seconds` |
| `summary.totalEntriesReturned` | `watcher_loki_stats_entries_returned` |

### ingester

| Loki stats field | Prometheus metric |
|---|---|
| `ingester.totalChunksMatched` | `watcher_loki_stats_ingester_chunks_matched` |
| `ingester.totalLinesSent` | `watcher_loki_stats_ingester_lines_sent` |
| `ingester.totalReached` | `watcher_loki_stats_ingester_reached` |

### store (querier)

| Loki stats field | Prometheus metric |
|---|---|
| `store.totalChunksDownloaded` | `watcher_loki_stats_store_chunks_downloaded` |

## Performance Monitor

`LokiQueryPerformanceMonitor` runs periodically from
`RulesEngine.run_maintenance_tasks` and evaluates four degradation signals
over a sliding window. State is stored in the workflow kvstore.

**Output metric:** `watcher_loki_degraded` (Gauge, 0=healthy, 1=degraded)

When degradation is detected, a WARNING log is emitted listing all active
signals and their counts, followed by an INFO log with per-signal "why"
details (per-query attribution, observation counts, etc.).

### Log filtering convention

All logs related to the `watcher_loki_degraded` gauge lifecycle are tagged
with the literal token **`[LOKI_DEGRADED]`** so they can be retrieved with
a single grep:

```bash
grep LOKI_DEGRADED watcher.log
```

Tagged log sites:

| Where | Level | Purpose |
|---|---|---|
| `LogQueryPoller._poll_iteration` (`plugins/log_query/poller.py`) | WARNING | Iteration budget exceeded (root cause for `ITERATION_BUDGET_EXCEEDED` signal) |
| `LogQueryPoller._poll_iteration` (`plugins/log_query/poller.py`) | INFO | Per-query timing breakdown + list of deferred queries |
| `LokiQueryPerformanceMonitor.evaluate` (`utils/loki_stats.py`) | WARNING | Degradation detected — active signals summary |
| `LokiQueryPerformanceMonitor.evaluate` (`utils/loki_stats.py`) | INFO | Per-signal "why" with per-query attribution |
| `LokiQueryPerformanceMonitor.evaluate` (`utils/loki_stats.py`) | INFO | Recovery — gauge set back to 0 |
| `_process_hangnooutput_anomalies` (`workflow.py`) | WARNING | Downstream suppression of a HANGNOOUTPUT anomaly while the gauge is 1 |
| `LogQueryBatcher.batch_query` (`plugins/log_query/batcher.py`) | WARNING | `HTTP_MAX_QUERY_WINDOW_SECS` safety cap tripped — cursor was lagging far enough that the query window would have been rejected by the backend |

New code that sets/clears the gauge, explains why a signal fired, or
changes behavior based on the gauge MUST include the `[LOKI_DEGRADED]`
token in its log message to preserve this filter.

### Signal 1: CONSECUTIVE_EMPTY_RESPONSES

A burst of suspicious empty **validation** responses within the evaluation
window, indicating systemic query failures across multiple jobs. Triggered
when the number of suspicious validation empties reaches
`LOKI_PERF_MONITOR_EMPTY_THRESHOLD` within
`LOKI_PERF_MONITOR_WINDOW_SECS`.

Normal "no data" empties do not contribute to this signal. Responses with
missing scan stats also do not contribute, to preserve compatibility across
different Loki versions.

This maps to the observed pattern where clusters of validation failures
occur simultaneously across different jobs (e.g. 13 failures in 6 minutes).

### Signal 2: ITERATION_BUDGET_EXCEEDED

The `LogQueryPoller` exceeded its iteration time budget since the last evaluation.
Detected by reading the delta of the
`watcher_log_query_poller_iteration_budget_exceeded_total` Prometheus counter.

This indicates Loki is under heavy load — iterations taking 2x or more of
the configured budget (default 120s) signal that queriers are struggling to
keep up. Fires on any delta > 0 since budget exceeded is already a rare event.

### Signal 3: ZERO_SCAN_EMPTY

Queries returned empty **without scanning any data** — a strong signal of
Loki-side short-circuit. Triggered when the number of zero-scan empties
reaches `LOKI_PERF_MONITOR_ZERO_SCAN_THRESHOLD` within the window.

A zero-scan empty is a response where all of:
- `summary.totalBytesProcessed` = 0
- `summary.totalLinesProcessed` = 0
- `ingester.totalChunksMatched` = 0
- `store.totalChunksDownloaded` = 0

Possible causes: querier failure to read chunks, index lookup failure,
ingesters unreachable.

Queries with missing scan stats are ignored for this signal rather than being
treated as zeroes.

### Signal 4: HIGH_QUEUE_TIME

Queries spent a disproportionate amount of time waiting in queue relative
to execution time. Triggered when the number of high-queue queries reaches
`LOKI_PERF_MONITOR_HIGH_QUEUE_THRESHOLD` within the window.

A query is considered "high queue" when
`queueTime / (execTime + queueTime)` exceeds `LOKI_PERF_MONITOR_QUEUE_TIME_RATIO`.

This indicates querier overload and often precedes full degradation.

## Configuration

All thresholds are defined as constants in `watcher.lib.utils.constants`
and passed as defaults to `LokiQueryPerformanceMonitor.__init__`.

| Constant | Default | Description |
|---|---|---|
| `LOKI_PERF_MONITOR_INTERVAL_SECS` | 60 | How often the monitor evaluates, in seconds. The monitor self-throttles inside `evaluate_if_due()`. |
| `LOKI_PERF_MONITOR_WINDOW_SECS` | 300 | Sliding window (seconds) over which observations are analyzed. Older observations are ignored. |
| `LOKI_PERF_MONITOR_EMPTY_THRESHOLD` | 5 | Minimum number of empty responses within the window to fire `CONSECUTIVE_EMPTY_RESPONSES`. |
| `LOKI_PERF_MONITOR_ZERO_SCAN_THRESHOLD` | 3 | Minimum number of zero-scan empty responses within the window to fire `ZERO_SCAN_EMPTY`. |
| `LOKI_PERF_MONITOR_HIGH_QUEUE_THRESHOLD` | 3 | Minimum number of high-queue-time queries within the window to fire `HIGH_QUEUE_TIME`. |
| `LOKI_PERF_MONITOR_QUEUE_TIME_RATIO` | 0.8 | Ratio of `queueTime / (execTime + queueTime)` above which a query is classified as "high queue". |
| `LOKI_PERF_MONITOR_MAX_OBSERVATIONS` | 1000 | Ring buffer capacity for recent query observations. Older entries are evicted automatically. |

## Degradation Patterns

The four signals map to three observable Loki behaviors:

### Normal "no data" (genuine empty)

Loki scanned data but nothing matched the pipeline filters
(e.g. `| job_id = "3328253"`). No signals fire.

- `summary.totalBytesProcessed` > 0
- `summary.totalLinesProcessed` > 0
- `ingester.totalChunksMatched` > 0 and/or `store.totalChunksDownloaded` > 0
- `summary.totalEntriesReturned` = 0

No operator action required.

### Degraded empty (suspected short-circuit)

Signals: `CONSECUTIVE_EMPTY_RESPONSES` + `ZERO_SCAN_EMPTY`

Loki returned empty without scanning any data across multiple queries.
Correlation signals:
- `watcher_log_query_poller_iteration_budget_exceeded_total` increasing
- `watcher_log_query_poller_backlog_seconds` growing
- `watcher_downstream_log_query_api_latency` spikes

### Partial degradation (timeout / overload)

Signals: `HIGH_QUEUE_TIME`, possibly `ITERATION_BUDGET_EXCEEDED`

Loki processed some data but queries spent most of their time in queue.
This state can precede full degradation. If `queueTime` is consistently
high across queries, Loki queriers are likely overloaded.
