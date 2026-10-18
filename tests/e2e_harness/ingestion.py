# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Exact-body qualification of logs collected from ordinary application and system files."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta

from e2e_harness.log_backend import LogRecord
from e2e_harness.recovery import SYSTEM_LOG_FORBIDDEN_LABELS, CausalEvidence


def evaluate_ingestion_records(
    records: list[LogRecord],
    *,
    messages: tuple[str, ...],
    submitted_at: str,
    job_id: str | None = None,
    node_name: str | None = None,
) -> CausalEvidence:
    """Require exact source-line counts, bodies, and identity for every observation."""

    start = datetime.fromisoformat(submitted_at) - timedelta(seconds=1)
    end = datetime.now(UTC) + timedelta(seconds=1)
    expected = Counter(messages)
    found: Counter[str] = Counter()
    for record in records:
        if record.message not in messages:
            return CausalEvidence(False, "ingestion changed a marker's exact message body")
        timestamp = datetime.fromisoformat(record.observed_at)
        if not start <= timestamp <= end:
            return CausalEvidence(False, "ingestion marker timestamp is outside the current run")
        labels = record.labels
        if labels.get("cluster") != "slurm":
            return CausalEvidence(False, "message content overwrote the collector cluster field")
        if job_id is not None:
            if (
                labels.get("appName") != "slurm-app-log"
                or labels.get("job_id") != job_id
                or not labels.get("filename", "").startswith("/home/log-links/")
            ):
                return CausalEvidence(False, "application source identity or filename was not preserved")
        elif (
            labels.get("component") != "journal"
            or labels.get("node_name") != node_name
            or SYSTEM_LOG_FORBIDDEN_LABELS.intersection(labels)
        ):
            return CausalEvidence(False, "system source identity was changed or workload fields were added")
        found[record.message] += 1
    if found != expected:
        return CausalEvidence(
            False, f"ingestion line counts differ: missing={dict(expected - found)!r}, extra={dict(found - expected)!r}"
        )
    return CausalEvidence(True, "source lines retained exact counts, bodies, identity, and current timestamps")
