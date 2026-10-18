# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Compose OpenTelemetry spans around E2E harness operations."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import ParamSpec, TypeVar

import httpx
from opentelemetry import propagate, trace

TRACER_NAME = "heimdall.e2e"

P = ParamSpec("P")
R = TypeVar("R")


def traced(name: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Run the decorated operation in a span named `name`.

    Keyword arguments with string or integer values become span attributes, so
    operations should take identities such as `job_id` and `attempt` as keywords.
    """

    def decorate(operation: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(operation)
        def run(*args: P.args, **kwargs: P.kwargs) -> R:
            attributes = {
                key: value
                for key, value in kwargs.items()
                if isinstance(value, (str, int)) and not isinstance(value, bool)
            }
            with trace.get_tracer(TRACER_NAME).start_as_current_span(name, attributes=attributes):
                return operation(*args, **kwargs)

        return run

    return decorate


class TracedTransport(httpx.BaseTransport):
    """Send each request through `inner` in a client span that carries W3C trace context."""

    def __init__(self, inner: httpx.BaseTransport) -> None:
        self._inner = inner

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        tracer = trace.get_tracer(TRACER_NAME)
        attributes = {"http.request.method": request.method, "url.full": str(request.url)}
        with tracer.start_as_current_span(
            f"{request.method} {request.url.path}", kind=trace.SpanKind.CLIENT, attributes=attributes
        ) as span:
            carrier: dict[str, str] = {}
            propagate.inject(carrier)
            response = self._inner.handle_request(
                httpx.Request(
                    request.method,
                    request.url,
                    headers=[*request.headers.multi_items(), *carrier.items()],
                    stream=request.stream,
                    extensions=request.extensions,
                )
            )
            span.set_attribute("http.response.status_code", response.status_code)
            if response.status_code >= 400:
                span.set_status(trace.Status(trace.StatusCode.ERROR))
            return response

    def close(self) -> None:
        self._inner.close()
