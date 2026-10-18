# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Thin HTTP client that calls attribution-adapter for FAST/SLOW attribution passes.

Design §6 (request shape), §7.1, §10 (failure→empty), §12.1 (per-phase timeouts),
§12.2 (W3C trace context injection).

The client NEVER returns None and NEVER raises to the caller: any failure
(network error, non-2xx) returns a synthesised empty
WorkloadAttributionResponse with an informational AttributionError. Intentional
disablement returns an empty response without an error.
"""

import logging
import time
import uuid
from datetime import UTC, datetime

import requests
from opentelemetry import trace
from opentelemetry.util.types import AttributeValue
from recovery_libs.audit import AuditBuffer, AuditEventLevel, NoOpAuditBuffer, ProducerEvent
from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION
from recovery_libs.schema.attribution_interface.enums import AttributionErrorType, AttributionPhase
from recovery_libs.schema.attribution_interface.request import (
    FailureContext,
    WorkloadAttributionRequest,
    WorkloadContext,
    WorkloadScope,
)
from recovery_libs.schema.attribution_interface.response import (
    AttributionError,
    WorkloadAttributionResponse,
)
from recovery_libs.schema.job_manager.models import JobAnomaly, SlurmJobExecutionStatus
from recovery_libs.tracing.recovery import operation_correlation, recovery_headers, recovery_span_attributes
from recovery_libs.tracing.traces import populate_headers_with_span_context

from anomaly_detection_isolator.models.audit import (
    ADIAttributionRequestForwardedAuditEvent,
    ADIAttributionResponseReceivedAuditEvent,
    AttributionRequestForwardedPayload,
    AttributionResponseReceivedPayload,
)
from anomaly_detection_isolator.settings.settings import Settings

logger = logging.getLogger(__name__)


class AttributionClient:
    """HTTP client for calling attribution-adapter from the Policy Engine.

    Builds WorkloadAttributionRequest objects from internal job/anomaly models and
    POSTs them to attribution-adapter, returning a WorkloadAttributionResponse.
    Returns an empty response (never None, never raises) on any failure or when
    attribution is disabled (design §10, §D7).
    """

    def __init__(
        self,
        settings: Settings,
        audit_event_buffer: AuditBuffer[ProducerEvent] = NoOpAuditBuffer[ProducerEvent](),
    ) -> None:
        """Initialise with application settings.

        Args:
            settings: Application settings containing attribution configuration.
        """
        self.settings = settings
        self.audit_event_buffer = audit_event_buffer

    def build_fast_request(
        self, job_status: SlurmJobExecutionStatus, anomaly: JobAnomaly
    ) -> WorkloadAttributionRequest:
        """Build a FAST-phase WorkloadAttributionRequest from a job status and anomaly.

        The FAST scope covers the full job window: start_time → end_time.
        Either may be None (design C12 — schema allows Optional datetimes).

        Args:
            job_status: The Slurm job execution status fetched from Job Manager.
            anomaly: The job anomaly that triggered the attribution request.

        Returns:
            A fully-populated WorkloadAttributionRequest with phase=FAST.
        """
        scope = WorkloadScope(
            nodes=(job_status.allocated_node_list or []),
            ranks=[],
            start_time=job_status.start_time,
            end_time=job_status.end_time,
        )
        failure_context = FailureContext(
            detected_at=anomaly.timestamp,
            detection_signal=anomaly.anomaly_type,
        )
        workload_context = WorkloadContext(
            workflow_id=job_status.workflow_id,
            attempt_index=job_status.job_attempt_index,
            command=(job_status.command or ""),
            cluster=(job_status.cluster or ""),
            workload_name=(job_status.job_name or ""),
            state=(job_status.state or ""),
            node_count=job_status.node_count,
            total_gpu_count=job_status.total_gpu_count,
            job_end_time=job_status.end_time,
            anomaly_uuid=(anomaly.uuid or ""),
        )
        return WorkloadAttributionRequest(
            schema_version=ATTRIBUTION_SCHEMA_VERSION,
            workload_id=job_status.job_id,
            user_id=(job_status.user or ""),
            scope=scope,
            failure_context=failure_context,
            timeout_ms=self.settings.attribution_fast_http_timeout_seconds * 1000,
            workload_context=workload_context,
            continue_session=None,
            phase=AttributionPhase.FAST,
        )

    def build_slow_request(
        self,
        job_status: SlurmJobExecutionStatus,
        anomaly_type: str,
        anomaly_uuid: str,
        continue_session: str,
        epilog_end_time: datetime | None,
    ) -> WorkloadAttributionRequest:
        """Build a SLOW-phase WorkloadAttributionRequest for the epilog window.

        The SLOW scope is the epilog window: scope.start_time = job_status.end_time
        (the real job end = epilog window start), scope.end_time = epilog_end_time
        (epilog window end).  This matches the NV slow-path contract (design §7.1).

        Args:
            job_status: The Slurm job execution status fetched from Job Manager.
            anomaly_type: The anomaly type string (e.g. 'CRASH').
            anomaly_uuid: The UUID of the original anomaly.
            continue_session: Opaque continuation token from the FAST pass
                (attribution_session_id returned by attribution-adapter).
            epilog_end_time: The end of the epilog window; may be None.

        Returns:
            A fully-populated WorkloadAttributionRequest with phase=SLOW.
        """
        scope = WorkloadScope(
            nodes=(job_status.allocated_node_list or []),
            ranks=[],
            start_time=job_status.end_time,  # epilog window start = real job end
            end_time=epilog_end_time,  # epilog window end
        )
        # detected_at falls back to now(utc) when job.end_time is None (design §7.1)
        detected_at = job_status.end_time or datetime.now(tz=UTC)
        failure_context = FailureContext(
            detected_at=detected_at,
            detection_signal=anomaly_type,
        )
        workload_context = WorkloadContext(
            workflow_id=job_status.workflow_id,
            attempt_index=job_status.job_attempt_index,
            command=(job_status.command or ""),
            cluster=(job_status.cluster or ""),
            workload_name=(job_status.job_name or ""),
            state=(job_status.state or ""),
            node_count=job_status.node_count,
            total_gpu_count=job_status.total_gpu_count,
            job_end_time=job_status.end_time,  # real job end, distinct from scope.end_time
            anomaly_uuid=(anomaly_uuid or ""),
        )
        return WorkloadAttributionRequest(
            schema_version=ATTRIBUTION_SCHEMA_VERSION,
            workload_id=job_status.job_id,
            user_id=(job_status.user or ""),
            scope=scope,
            failure_context=failure_context,
            timeout_ms=self.settings.attribution_slow_http_timeout_seconds * 1000,
            workload_context=workload_context,
            continue_session=continue_session,
            phase=AttributionPhase.SLOW,
        )

    def attribute(
        self,
        request: WorkloadAttributionRequest,
        *,
        heimdall_job_id: str | None = None,
    ) -> WorkloadAttributionResponse:
        """Call attribution-adapter and return a WorkloadAttributionResponse.

        Never returns None and never raises to the caller.  On any failure
        (transport error, non-2xx HTTP status) an empty response with an
        informational AttributionError is returned (design §10, §D7).

        W3C trace context is injected via populate_headers_with_span_context so the
        attribution-adapter span is correctly parented (design §12.2).

        Args:
            request: A fully-populated WorkloadAttributionRequest to send.
            heimdall_job_id: Canonical Heimdall identifier used for audit events.

        Returns:
            A WorkloadAttributionResponse; empty (attributions=[]) on any failure.
        """
        if heimdall_job_id is None:
            logger.warning(
                "Skipping attribution audit events because heimdall_job_id is missing for "
                f"job_id={request.workload_id!r} attempt_index={request.workload_context.attempt_index!r}"
            )

        if not self.settings.attribution_enabled:
            return WorkloadAttributionResponse(
                schema_version=request.schema_version,
                service_identifier="policy-engine",
                service_version="",
                request_id=request.workload_id,
                attribution_session_id=(request.continue_session or ""),
                attributions=[],
            )
        if not self.settings.attribution_service_url:
            return self._empty_response(
                request=request,
                error_type=AttributionErrorType.SERVICE_ERROR,
                message="attribution enabled but attribution_service_url is not configured",
            )

        timeout = (
            self.settings.attribution_fast_http_timeout_seconds
            if request.phase == AttributionPhase.FAST
            else self.settings.attribution_slow_http_timeout_seconds
        )

        url = self.settings.attribution_service_url.rstrip("/") + "/v1/attribution"
        attributes: dict[str, AttributeValue] = {
            "job.id": request.workload_id,
            "job.attempt.source": request.workload_context.attempt_index,
            **recovery_span_attributes(),
        }
        with trace.get_tracer(__name__).start_as_current_span(
            "job.recovery.attribution.request", attributes=attributes
        ):
            headers: dict[str, str] = {"Content-Type": "application/json", **recovery_headers(None)}
            populate_headers_with_span_context(span=trace.get_current_span(), headers=headers)
            request_uuid = str(uuid.uuid4()) if heimdall_job_id is not None else None

            try:
                if heimdall_job_id is not None and request_uuid is not None:
                    self._emit_request_forwarded(heimdall_job_id, request, request_uuid)
                resp = requests.post(
                    url=url,
                    data=request.model_dump_json(),
                    headers=headers,
                    timeout=timeout,
                )
                resp.raise_for_status()
                logger.info(
                    f"Attribution call succeeded. workload_id={request.workload_id} phase={request.phase} "
                    f"status={resp.status_code}"
                )
                response = WorkloadAttributionResponse.model_validate_json(resp.text)
                if heimdall_job_id is not None and request_uuid is not None:
                    self._emit_response_received(heimdall_job_id, request, response, request_uuid)
                return response
            except requests.exceptions.Timeout as exc:
                logger.error(
                    f"Attribution request timed out. workload_id={request.workload_id} phase={request.phase}: {exc}"
                )
                return self._empty_response(
                    request=request,
                    error_type=AttributionErrorType.TIMEOUT,
                    message=f"attribution request timed out: {exc}",
                )
            except requests.exceptions.RequestException as exc:
                logger.error(
                    f"Attribution request failed. workload_id={request.workload_id} phase={request.phase}: {exc}"
                )
                return self._empty_response(
                    request=request,
                    error_type=AttributionErrorType.SERVICE_ERROR,
                    message=f"attribution request failed: {exc}",
                )

    def _emit_request_forwarded(
        self,
        heimdall_job_id: str,
        request: WorkloadAttributionRequest,
        request_uuid: str,
    ) -> None:
        self.audit_event_buffer.add(
            ADIAttributionRequestForwardedAuditEvent(
                heimdall_job_id=heimdall_job_id,
                event_level=AuditEventLevel.OPERATIONAL,
                payload=AttributionRequestForwardedPayload(
                    advice_id=heimdall_job_id,
                    job_id=request.workload_id,
                    attempt_index=request.workload_context.attempt_index,
                    request_payload=request,
                    request_uuid=request_uuid,
                    **operation_correlation(
                        trace.get_current_span(), recovery_headers(None).get("recovery-sequence-id")
                    ),
                ),
                event_produced_at=time.time_ns() // 1_000,
            )
        )

    def _emit_response_received(
        self,
        heimdall_job_id: str,
        request: WorkloadAttributionRequest,
        response: WorkloadAttributionResponse,
        request_uuid: str,
    ) -> None:
        self.audit_event_buffer.add(
            ADIAttributionResponseReceivedAuditEvent(
                heimdall_job_id=heimdall_job_id,
                event_level=AuditEventLevel.OPERATIONAL,
                payload=AttributionResponseReceivedPayload(
                    advice_id=heimdall_job_id,
                    job_id=request.workload_id,
                    attempt_index=request.workload_context.attempt_index,
                    request_uuid=request_uuid,
                    **operation_correlation(
                        trace.get_current_span(), recovery_headers(None).get("recovery-sequence-id")
                    ),
                    response_payload=response,
                ),
                event_produced_at=time.time_ns() // 1_000,
            )
        )

    @staticmethod
    def _empty_response(
        request: WorkloadAttributionRequest,
        error_type: AttributionErrorType,
        message: str,
    ) -> WorkloadAttributionResponse:
        """Build an empty WorkloadAttributionResponse with an informational error.

        Args:
            request: The original request (used for schema_version / request_id /
                continue_session / attribution_session_id mirroring).
            error_type: The AttributionErrorType to report.
            message: A human-readable error message.

        Returns:
            A WorkloadAttributionResponse with empty attributions and the given error.
        """
        return WorkloadAttributionResponse(
            schema_version=request.schema_version,
            service_identifier="policy-engine",
            service_version="",
            request_id=request.workload_id,
            attribution_session_id=(request.continue_session or ""),
            attributions=[],
            error=AttributionError(type=error_type, message=message),
        )
