# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Strict decoding of production collection evidence from either log backend."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit

LOG_BACKENDS = ("loki", "victorialogs")
QUERY_RECORD_LIMIT = 1000


@dataclass(frozen=True, slots=True)
class LogRecord:
    """One collected message, its collection labels, and its UTC timestamp."""

    labels: dict[str, str]
    message: str
    observed_at: str


def validate_log_backend(backend: str) -> str:
    """Reject misspelled providers before provisioning or querying a log store."""

    if backend not in LOG_BACKENDS:
        raise ValueError(f"invalid E2E_LOG_BACKEND={backend}; expected loki or victorialogs")
    return backend


def validate_log_backend_url(backend: str, url: str) -> None:
    """Require the configured store to match the local service-proxy evidence target."""

    validate_log_backend(backend)
    if not url:
        return
    service, port = ("loki-gateway", 80) if backend == "loki" else ("victorialogs", 9428)
    parsed = urlsplit(url)
    allowed_hosts = {f"{service}.monitoring{suffix}" for suffix in ("", ".svc", ".svc.cluster.local")}
    if (
        parsed.scheme != "http"
        or parsed.hostname not in allowed_hosts
        or (parsed.port or 80) != port
        or parsed.path not in ("", "/")
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            f"{backend} E2E evidence requires the local endpoint http://{service}.monitoring.svc.cluster.local:{port}"
        )


def _utc_timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("log timestamp must include a timezone")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def decode_log_response(response_text: str, backend: str) -> list[LogRecord]:
    """Decode complete Loki streams or VictoriaLogs NDJSON, rejecting malformed rows.

    Decode all rows before returning evidence so a valid marker cannot hide a
    malformed sibling or a backend/configuration mismatch.
    """

    validate_log_backend(backend)
    records: list[LogRecord] = []
    try:
        if backend == "loki":
            payload = json.loads(response_text)
            if not isinstance(payload, dict) or payload.get("status") != "success":
                raise ValueError("Loki response is not successful")
            data = payload.get("data")
            if not isinstance(data, dict) or data.get("resultType") != "streams":
                raise ValueError("Loki response has no stream data")
            results = data.get("result")
            if not isinstance(results, list):
                raise ValueError("Loki response has no result list")
            for result in results:
                if not isinstance(result, dict):
                    raise ValueError("Loki result is not an object")  # noqa: TRY004
                labels, values = result.get("stream"), result.get("values")
                if not isinstance(labels, dict) or not all(isinstance(v, str) for v in labels.values()):
                    raise ValueError("Loki stream labels must be strings")
                if not isinstance(values, list):
                    raise ValueError("Loki stream values must be a list")  # noqa: TRY004
                for value in values:
                    if (
                        not isinstance(value, list)
                        or len(value) != 2
                        or not isinstance(value[0], str)
                        or not value[0].isdigit()
                        or not isinstance(value[1], str)
                    ):
                        raise ValueError("Loki value must contain a nanosecond timestamp and a message")
                    seconds, nanoseconds = divmod(int(value[0]), 1_000_000_000)
                    observed = datetime.fromtimestamp(seconds, tz=UTC) + timedelta(microseconds=nanoseconds // 1000)
                    records.append(LogRecord(labels, value[1], observed.isoformat().replace("+00:00", "Z")))
        else:
            for line in response_text.splitlines():
                if not line.strip():
                    continue
                row = json.loads(line)
                if not isinstance(row, dict) or not all(isinstance(v, str) for v in row.values()):
                    raise ValueError("VictoriaLogs row fields must be strings")
                if "_time" not in row or "_msg" not in row:
                    raise ValueError("VictoriaLogs row is missing _time or _msg")
                observed_at = _utc_timestamp(row["_time"])
                labels = {
                    key: value for key, value in row.items() if key not in ("_time", "_msg", "_stream", "_stream_id")
                }
                records.append(LogRecord(labels, row["_msg"], observed_at))
    except (ValueError, TypeError, OverflowError, OSError) as error:
        raise ValueError(f"malformed {backend} response: {error}") from error
    if len(records) >= QUERY_RECORD_LIMIT:
        raise ValueError(f"{backend} query reached its record limit; evidence may be truncated")
    return records
