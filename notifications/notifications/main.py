# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from contextlib import AsyncExitStack, asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.routing import APIRoute
from prometheus_fastapi_instrumentator import Instrumentator, metrics

from notifications import database, middlewares, sending
from notifications.prometheus.metrics import NAMESPACE, SERVICE_CONFIG_INFO, SUBSYSTEM
from notifications.utils.config import Config

from .routers import audit_events, health, notifications, preferences


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with AsyncExitStack() as stack:
        # Set config_info with basic configuration values
        SERVICE_CONFIG_INFO.info(
            {
                "service": "notifications",
                **{
                    key: str(value)
                    for key, value in Config.as_dict().items()
                    if not ("TOKEN" in key or "PASSWORD" in key)
                },
            },
        )

        await stack.enter_async_context(sending.lifespan(app))
        await stack.enter_async_context(database.lifespan(app))
        yield


app = FastAPI(title="Notifications", description="NVIDIA Recovery Notifications", version="1.0.0", lifespan=lifespan)

# Set up enhanced Prometheus instrumentation
instrumentator = Instrumentator(
    should_group_status_codes=True,
    should_ignore_untemplated=True,
    excluded_handlers=["/healthz", "/readyz", "/metrics"],
    inprogress_name=f"{NAMESPACE}_{SUBSYSTEM}_http_requests_in_progress",
    inprogress_labels=True,
)

# Add extra metrics to the instrumentator
instrumentator.add(
    metrics.request_size(
        metric_name=f"{NAMESPACE}_{SUBSYSTEM}_request_size_bytes",
        should_include_handler=True,
        should_include_method=True,
        should_include_status=True,
    )
)

instrumentator.add(
    metrics.response_size(
        metric_name=f"{NAMESPACE}_{SUBSYSTEM}_response_size_bytes",
        should_include_handler=True,
        should_include_method=True,
        should_include_status=True,
    )
)

instrumentator.add(
    metrics.latency(
        metric_name=f"{NAMESPACE}_{SUBSYSTEM}_request_duration_seconds",
        should_include_handler=True,
        should_include_method=True,
        should_include_status=True,
    )
)

# Instrument and expose metrics endpoint
instrumentator.instrument(app).expose(app)


app.include_router(notifications.router)
app.include_router(preferences.router)
app.include_router(audit_events.router)
app.include_router(health.router)


def use_route_names_as_operation_ids(app: FastAPI) -> None:
    """Ensure OpenAPI operation ids are sane.

    Without this, any clients generated from the OpenAPI spec will have the safe-but-long operationId that FastAPI
    generates.
    """
    for route in app.routes:
        if isinstance(route, APIRoute):
            # Convert snake case to camelCase for consistency with other APIs.
            words = route.name.split("_")
            route.operation_id = words[0] + "".join(word.capitalize() for word in words[1:])


# ORDER MATTERS! This must happen after setting up all the routes.
use_route_names_as_operation_ids(app)


app.middleware("http")(middlewares.log_http_requests)


def main():
    uvicorn.run(app=app, host="0.0.0.0", port=8080)


if __name__ == "__main__":
    main()
