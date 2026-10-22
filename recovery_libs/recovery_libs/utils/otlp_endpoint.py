# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Per-signal OTLP endpoint resolution.

Services share one ``OTEL_EXPORTER_OTLP_ENDPOINT`` for traces and logs. Under
the OTLP exporter specification that value is a base URL: gRPC uses it as-is,
while HTTP exporters must send each signal to ``<base>/v1/<signal>``. The
OpenTelemetry Python HTTP exporters only append that path when they read the
environment themselves; an ``endpoint=`` constructor argument is used verbatim.
Because these services pass the endpoint explicitly (to add bearer-token
headers), they must append the signal path here.
"""

import os
from enum import StrEnum
from urllib.parse import urlsplit, urlunsplit

from recovery_libs.utils.types import OTLPProtocol


class OTLPSignal(StrEnum):
    """OTLP signals exported by the Python services.

    Attributes:
        TRACES: Trace spans, sent to ``/v1/traces`` over HTTP.
        LOGS: Log records, sent to ``/v1/logs`` over HTTP.
    """

    TRACES = "traces"
    LOGS = "logs"


def resolve_otlp_signal_endpoint(base_endpoint: str | None, protocol: OTLPProtocol, signal: OTLPSignal) -> str | None:
    """Return the URL an exporter should use for one signal.

    A signal-specific ``OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT`` is a complete URL
    and is returned verbatim. Otherwise the shared base endpoint is returned
    as-is for gRPC, and for HTTP it gets ``v1/<signal>`` appended to any
    existing base path, matching the Go controller's ``url.JoinPath`` behavior.

    Args:
        base_endpoint: The shared ``OTEL_EXPORTER_OTLP_ENDPOINT`` value, if any.
        protocol: The OTLP transport in use.
        signal: The signal being exported.

    Returns:
        The endpoint to pass to the exporter, or None when neither the
        signal-specific nor the shared endpoint is set.
    """
    if signal_endpoint := os.getenv(f"OTEL_EXPORTER_OTLP_{signal.value.upper()}_ENDPOINT", "").strip():
        return signal_endpoint

    base = (base_endpoint or "").strip()
    if not base or protocol == OTLPProtocol.GRPC:
        return base or None

    parts = urlsplit(base)
    return urlunsplit(parts._replace(path=f"{parts.path.rstrip('/')}/v1/{signal.value}"))
