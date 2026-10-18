# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""FastAPI entrypoint for the attribution-adapter.

Exposes ``POST /v1/attribution`` (a single FAST or SLOW pass) and ``GET /healthz``.
Prometheus metrics are served on :8080 via ``start_http_server`` and the trace
provider is configured at startup. The W3C trace context from the inbound request
headers is extracted and used as the parent of the per-request handler span.

The ``AttributionService`` is built lazily through ``get_attribution_service`` so the
FastAPI app can be imported (e.g. by ``TestClient``) without a full ``Settings`` env,
and so tests can patch the getter (correction C13).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import uvicorn
from fastapi import FastAPI, Request
from opentelemetry import trace
from prometheus_client import start_http_server
from recovery_libs.schema.attribution_interface.request import WorkloadAttributionRequest
from recovery_libs.schema.attribution_interface.response import WorkloadAttributionResponse
from recovery_libs.tracing.recovery import recovery_context_from_headers, recovery_scope, recovery_span_attributes
from recovery_libs.tracing.traces import extract_trace_context, set_up_trace_provider

from attribution_adapter.identity import SERVICE_IDENTIFIER, _service_version
from attribution_adapter.settings.settings import Settings
from attribution_adapter.utils.logger import logger

if TYPE_CHECKING:
    from attribution_adapter.attribution_service import AttributionService

# Lazily-built singleton; see get_attribution_service.
_attribution_service: AttributionService | None = None


def close_attribution_service() -> None:
    """Close and release the process-wide AttributionService singleton."""
    global _attribution_service
    service = _attribution_service
    _attribution_service = None
    if service is not None:
        service.close()


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Release shared log-query clients when the application shuts down."""
    try:
        yield
    finally:
        close_attribution_service()


app = FastAPI(title="Attribution Adapter", lifespan=lifespan)
tracer = trace.get_tracer(__name__)


def get_attribution_service() -> AttributionService:
    """Lazily construct the singleton AttributionService.

    Building lazily (rather than at import time) keeps the FastAPI app importable without a
    full Settings environment and lets tests patch this getter (correction C13).

    Returns:
        AttributionService: The process-wide attribution service instance.
    """
    global _attribution_service
    if _attribution_service is None:
        settings = Settings()
        settings.validate_provider_url()
        try:
            from attribution_adapter.attribution_service import AttributionService
        except ModuleNotFoundError as exc:
            if exc.name == "nvidia_attribution_client" or (exc.name or "").startswith("nvidia_attribution_client."):
                raise RuntimeError(
                    "Attribution requires the NVIDIA Attribution client; build the adapter with "
                    "the pinned provider source. Public client publication remains a release prerequisite."
                ) from exc
            raise
        _attribution_service = AttributionService(settings)
    return _attribution_service


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Liveness/readiness probe."""
    return {"status": "ok"}


@app.post("/v1/attribution", response_model=WorkloadAttributionResponse)
def attribution(request: WorkloadAttributionRequest, http_request: Request) -> WorkloadAttributionResponse:
    """Run attribution for a single FAST or SLOW pass.

    Extracts the W3C trace context from the inbound request headers and starts the handler
    span as its child, then delegates to ``AttributionService.attribute``.

    Args:
        request (WorkloadAttributionRequest): The parsed attribution request body.
        http_request (Request): The raw request, used to read trace-propagation headers.

    Returns:
        WorkloadAttributionResponse: The attribution result.
    """
    headers = dict(http_request.headers)
    ctx = recovery_context_from_headers(extract_trace_context(headers), headers)
    with recovery_scope(ctx), tracer.start_as_current_span("job.recovery.attribution", context=ctx) as span:
        span.set_attribute("service.identifier", SERVICE_IDENTIFIER)
        span.set_attribute("service.version", _service_version())
        span.set_attribute("workload.id", request.workload_id)
        span.set_attribute("attribution.phase", request.phase.value)
        span.set_attribute("job.id", request.workload_id)
        span.set_attribute("job.attempt.source", request.workload_context.attempt_index)
        recovery_attributes = recovery_span_attributes()
        span.set_attributes(recovery_attributes)
        if recovery_attributes:
            span.set_attribute("recovery.slo.eligible", recovery_attributes["recovery.operation"] == "restart")
        if request.workload_context.anomaly_uuid:
            span.set_attribute("anomaly.id", request.workload_context.anomaly_uuid)
        logger.info(f"Received {request.phase.value} attribution request for workload {request.workload_id}")
        return get_attribution_service().attribute(request)


def main() -> None:
    """Entrypoint: set up tracing, start the metrics server, and run uvicorn."""
    # Running this provider workload is explicit enablement: fail before serving
    # if its client or configuration is missing. Import/schema export stays lazy.
    get_attribution_service()
    start_http_server(port=8080)
    set_up_trace_provider(service_name="nvidia-recovery-attribution-adapter")
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
