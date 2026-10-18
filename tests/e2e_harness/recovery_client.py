# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""HTTP access to the recovery stack, with one scoped API per service."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx
from recovery_libs.testing.http.retry import RetryingTransport, RetryPolicy
from recovery_libs.testing.http.tracing import TracedTransport, traced

QueryParams = Mapping[str, str | int]


@dataclass(frozen=True)
class ApiResponse:
    """A successful response body and where it came from."""

    url: str
    status_code: int
    text: str

    def json(self) -> Any:
        return json.loads(self.text)


class RecoveryApiError(Exception):
    """A request that did not return a successful status."""

    def __init__(self, url: str, status_code: int, text: str) -> None:
        super().__init__(f"{url} returned HTTP {status_code}: {text[:500]}")
        self.url = url
        self.status_code = status_code
        self.text = text


def _get(http: httpx.Client, path: str, params: QueryParams | None = None) -> ApiResponse:
    return _checked(http.get(path, params=params))


def _post(http: httpx.Client, path: str, payload: Mapping[str, object]) -> ApiResponse:
    return _checked(http.post(path, json=payload))


def _checked(response: httpx.Response) -> ApiResponse:
    if response.is_error:
        raise RecoveryApiError(str(response.url), response.status_code, response.text)
    return ApiResponse(str(response.url), response.status_code, response.text)


def _segment(value: str | int) -> str:
    return quote(str(value), safe="")


class JobManagerApi:
    """Job Manager reads (job status, Advice, attributions, DAG gates, and requeue policy) and its job ingress."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    @traced("job_manager.job_status")
    def job_status(self, *, job_id: str, attempt: int | None = None) -> ApiResponse:
        params = {} if attempt is None else {"job_attempt_index": attempt}
        return _get(self._http, f"/job_status/{_segment(job_id)}", params)

    @traced("job_manager.advice")
    def advice(self, *, job_id: str, attempt: int) -> ApiResponse:
        return _get(self._http, f"/advice/by-job/{_segment(job_id)}/{_segment(attempt)}")

    @traced("job_manager.jobs")
    def jobs(self, *, job_id: str) -> ApiResponse:
        return _get(self._http, "/jobs", {"job_id": job_id})

    @traced("job_manager.post_job")
    def post_job(self, status: Mapping[str, object]) -> ApiResponse:
        """Upsert a scheduler observation of a job attempt, as the scheduler adapter does."""

        return _post(self._http, "/jobs", status)

    @traced("job_manager.error_attributions")
    def error_attributions(self, *, job_id: str, attempt: int) -> ApiResponse:
        return _get(self._http, "/job_error_attributions", {"job_id": job_id, "job_attempt_index": attempt})

    @traced("job_manager.dag_dependencies")
    def dag_dependencies(self, *, dependent_job_id: str) -> ApiResponse:
        return _get(self._http, "/dag_dependencies", {"dependent_job_id": dependent_job_id})

    @traced("job_manager.requeue_config")
    def requeue_config(self) -> ApiResponse:
        return _get(self._http, "/config/requeue")


class NotificationsApi:
    """Notifications reads: recovery notifications and the internal audit query."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    @traced("notifications.notifications")
    def notifications(self, *, job_id: str, created_after: str) -> ApiResponse:
        return _get(self._http, "/notifications", {"job_id": job_id, "created_after": created_after})

    @traced("notifications.audit_events")
    def audit_events(
        self,
        *,
        heimdall_job_id: str,
        min_event_produced_at: int,
        limit: int,
        cursor: str = "",
    ) -> ApiResponse:
        params: dict[str, str | int] = {
            "heimdall_job_id": heimdall_job_id,
            "min_event_produced_at": min_event_produced_at,
            "limit": limit,
        }
        if cursor:
            params["cursor"] = cursor
        return _get(self._http, "/v1/audit/events", params)


class GatewayApi:
    """Public API Gateway reads."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    @traced("gateway.audit_events")
    def audit_events(
        self,
        *,
        job_id: str,
        limit: int,
        attempt: int | None = None,
        event_type: str | None = None,
        cursor: str = "",
    ) -> ApiResponse:
        params: dict[str, str | int] = {"jobId": job_id, "limit": limit}
        if attempt is not None:
            params["attemptIndex"] = attempt
        if event_type is not None:
            params["eventType"] = event_type
        if cursor:
            params["cursor"] = cursor
        return _get(self._http, "/v1/audit/events", params)


class ControllerApi:
    """Slurm Advisor Controller reads."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    @traced("controller.metrics")
    def metrics(self) -> ApiResponse:
        """The controller's Prometheus metrics in text exposition format."""

        return _get(self._http, "/metrics")


class LogsApi:
    """Collected log reads from the stack's log backend: Loki or VictoriaLogs."""

    QUERY_LIMIT = 1000

    def __init__(self, http: httpx.Client, *, backend: str) -> None:
        if backend not in ("loki", "victorialogs"):
            raise ValueError(f"unsupported log backend {backend!r}")
        self._http = http
        self.backend = backend

    @traced("logs.query")
    def query(self, *, labels: Mapping[str, str], contains: str, start: str, end: str) -> ApiResponse:
        """Records with exactly these labels whose message contains `contains`, between `start` and `end`."""

        if self.backend == "loki":
            selector = ",".join(f"{key}={json.dumps(value)}" for key, value in labels.items())
            params: dict[str, str | int] = {
                "query": f"{{{selector}}} |= {json.dumps(contains)}",
                "start": start,
                "end": end,
                "limit": self.QUERY_LIMIT,
                "direction": "forward",
            }
            return _get(self._http, "/loki/api/v1/query_range", params)
        selector = " AND ".join(f"{key}:={json.dumps(value)}" for key, value in labels.items())
        # A regex of the escaped text keeps LogQL's plain-substring meaning for any punctuation.
        params = {
            "query": f"{selector} AND _msg:~{json.dumps(re.escape(contains))}",
            "start": start,
            "end": end,
            "limit": self.QUERY_LIMIT,
        }
        return _get(self._http, "/select/logsql/query", params)


@dataclass(frozen=True)
class RecoveryClient:
    """One client for the recovery stack, with a scoped API per service."""

    job_manager: JobManagerApi
    notifications: NotificationsApi
    gateway: GatewayApi
    logs: LogsApi
    controller: ControllerApi


@dataclass(frozen=True)
class ServiceUrls:
    """Base URLs for the services the recovery client reads."""

    job_manager: str
    notifications: str
    gateway: str
    logs: str
    controller: str


def traced_http(
    base_url: str,
    *,
    timeout: float,
    transport: httpx.BaseTransport | None = None,
    retry: RetryPolicy = RetryPolicy(),
) -> httpx.Client:
    """Build an HTTP client that retries transient failures and traces each attempt."""

    return httpx.Client(
        base_url=base_url,
        timeout=timeout,
        transport=RetryingTransport(TracedTransport(transport or httpx.HTTPTransport()), retry),
    )


@contextmanager
def open_recovery_client(urls: ServiceUrls, *, log_backend: str, timeout: float = 30.0) -> Iterator[RecoveryClient]:
    """Yield a traced recovery client and close its connections on exit."""

    with (
        traced_http(urls.job_manager, timeout=timeout) as job_manager,
        traced_http(urls.notifications, timeout=timeout) as notifications,
        traced_http(urls.gateway, timeout=timeout) as gateway,
        traced_http(urls.logs, timeout=timeout) as logs,
        traced_http(urls.controller, timeout=timeout) as controller,
    ):
        yield RecoveryClient(
            JobManagerApi(job_manager),
            NotificationsApi(notifications),
            GatewayApi(gateway),
            LogsApi(logs, backend=log_backend),
            ControllerApi(controller),
        )
