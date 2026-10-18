# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Write a test run's spans to a Chrome trace file that Perfetto or chrome://tracing can open.

Pass `--trace-output PATH` or set `E2E_TRACE_OUTPUT`. Each test traced by the
run gets its own row, with its spans nested by time. Under pytest-xdist each
worker writes `PATH` with its name appended, and the controller combines them
into `PATH`, one process per worker, so one file always holds the whole run.
"""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult


class _CollectingExporter(SpanExporter):
    """Keep finished spans in memory until the run writes them out."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._spans: list[ReadableSpan] = []

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        with self._lock:
            self._spans.extend(spans)
        return SpanExportResult.SUCCESS

    def finished(self) -> tuple[ReadableSpan, ...]:
        with self._lock:
            return tuple(self._spans)


def chrome_trace(spans: Sequence[ReadableSpan]) -> dict[str, Any]:
    """Convert spans to Chrome trace events, one row per trace."""

    in_start_order = sorted(spans, key=lambda span: span.start_time or 0)
    trace_ids = dict.fromkeys(span.context.trace_id for span in in_start_order)
    rows = {trace_id: row for row, trace_id in enumerate(trace_ids)}
    events: list[dict[str, Any]] = []
    for span in spans:
        if span.start_time is None or span.end_time is None:
            continue
        events.append(
            {
                "name": span.name,
                "ph": "X",
                "ts": span.start_time / 1000,
                "dur": (span.end_time - span.start_time) / 1000,
                "pid": 1,
                "tid": rows[span.context.trace_id],
                "args": {key: str(value) for key, value in (span.attributes or {}).items()},
            }
        )
    roots = {span.context.trace_id: span.name for span in spans if span.parent is None}
    events.extend(
        {"name": "thread_name", "ph": "M", "pid": 1, "tid": row, "args": {"name": roots.get(trace_id, "trace")}}
        for trace_id, row in rows.items()
    )
    return {"traceEvents": events, "displayTimeUnit": "ms"}


def merge_traces(traces: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Combine Chrome traces into one, each as its own process named by its key, so their rows never collide."""

    events: list[dict[str, Any]] = []
    for pid, (name, document) in enumerate(traces.items(), start=1):
        events.append({"name": "process_name", "ph": "M", "pid": pid, "args": {"name": name}})
        events.extend({**event, "pid": pid} for event in document["traceEvents"])
    return {"traceEvents": events, "displayTimeUnit": "ms"}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.getgroup("trace").addoption(
        "--trace-output",
        default=os.environ.get("E2E_TRACE_OUTPUT"),
        help="write the run's spans to this Chrome trace file (default: $E2E_TRACE_OUTPUT)",
    )


def pytest_configure(config: pytest.Config) -> None:
    if not config.getoption("trace_output"):
        return
    if not os.environ.get("PYTEST_XDIST_WORKER"):
        for stale in _worker_traces(_trace_path(config)).values():
            stale.unlink()
    exporter = _CollectingExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    config.stash[_EXPORTER] = exporter


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    if _EXPORTER in terminalreporter.config.stash:
        terminalreporter.write_sep("-", f"trace: {_trace_path(terminalreporter.config)}")


def pytest_unconfigure(config: pytest.Config) -> None:
    exporter = config.stash.get(_EXPORTER, None)
    if exporter is None:
        return
    path = _trace_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    document = chrome_trace(exporter.finished())
    workers = {} if os.environ.get("PYTEST_XDIST_WORKER") else _worker_traces(path)
    if workers:
        document = merge_traces({name: json.loads(file.read_text()) for name, file in workers.items()})
    path.write_text(json.dumps(document))


def _trace_path(config: pytest.Config) -> Path:
    path = Path(config.getoption("trace_output"))
    if worker := os.environ.get("PYTEST_XDIST_WORKER"):
        path = path.with_name(f"{path.stem}-{worker}{path.suffix}")
    return path


def _worker_traces(path: Path) -> dict[str, Path]:
    """Each pytest-xdist worker's trace beside `path`, by worker name; the controller runs no tests of its own."""

    prefix = f"{path.stem}-"
    return {file.stem.removeprefix(prefix): file for file in sorted(path.parent.glob(f"{prefix}gw*{path.suffix}"))}


_EXPORTER = pytest.StashKey[_CollectingExporter]()
