# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from recovery_libs.testing.trace_export import merge_traces


def span(name: str, row: int) -> dict:
    return {"name": name, "ph": "X", "ts": 0, "dur": 1, "pid": 1, "tid": row, "args": {}}


def test_worker_traces_merge_into_one_file_without_sharing_rows() -> None:
    """Each worker numbers its rows from 0, so merged rows must stay apart by process, labelled by worker."""

    merged = merge_traces(
        {
            "gw0": {"traceEvents": [span("scenario 10", 0)]},
            "gw1": {"traceEvents": [span("scenario 20", 0)]},
        }
    )

    spans = {event["name"]: event["pid"] for event in merged["traceEvents"] if event["ph"] == "X"}
    processes = {
        event["pid"]: event["args"]["name"] for event in merged["traceEvents"] if event["name"] == "process_name"
    }
    assert spans["scenario 10"] != spans["scenario 20"]
    assert processes == {spans["scenario 10"]: "gw0", spans["scenario 20"]: "gw1"}
