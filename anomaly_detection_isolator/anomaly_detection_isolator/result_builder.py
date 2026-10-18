# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from datetime import UTC

from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.attribution_interface.enums import ErrorCategory, ResourceKind
from recovery_libs.schema.attribution_interface.response import WorkloadAttributionResponse
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus
from recovery_libs.storage.global_events import GlobalEvent
from recovery_libs.storage.job_execution_errors import JobExecutionError
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    FactStatus,
    NodeEvent,
)

from anomaly_detection_isolator import exclusion_policy
from anomaly_detection_isolator.node_id_utils import parse_uuid
from anomaly_detection_isolator.settings.settings import Settings
from anomaly_detection_isolator.utils.logger import logger

# error_category (NV) -> ErrorAttribution (Policy). Composing this with the NV
# AttributionClassEnum -> ErrorCategory map reproduces today's direct
# convert_fact_attribution_to_heimdall_attribution mapping (design §7.4).
ERROR_CATEGORY_TO_ATTRIBUTION: dict[ErrorCategory, ErrorAttribution] = {
    ErrorCategory.HARDWARE: ErrorAttribution.HARDWARE,
    ErrorCategory.SYSTEM_SOFTWARE: ErrorAttribution.SYSTEM,
    ErrorCategory.APPLICATION: ErrorAttribution.APPLICATION,
    ErrorCategory.AMBIGUOUS: ErrorAttribution.UNKNOWN,
    ErrorCategory.UNATTRIBUTED: ErrorAttribution.UNDEFINED,
    ErrorCategory.INSUFFICIENT_TELEMETRY: ErrorAttribution.UNDEFINED,
}

# evidence-reference tag -> ErrorAttributionSource. NV emits "source:" + Source.value
# for every FACT source; recognized values map below and unrecognized tags default
# to ErrorAttributionSource.NONE (preserving the legacy NONE sentinel, design §7.4/C4).
EVIDENCE_TAG_TO_SOURCE: dict[str, ErrorAttributionSource] = {
    "source:syslog": ErrorAttributionSource.SYSLOGS,
    "source:network_syslog": ErrorAttributionSource.SYSLOGS,
    "source:applog": ErrorAttributionSource.APPLICATION_LOGS,
    "source:ib_metrics": ErrorAttributionSource.NETWORK_METRICS,
    "source:eth_metrics": ErrorAttributionSource.NETWORK_METRICS,
    "source:nvlink_metrics": ErrorAttributionSource.NETWORK_METRICS,
    "source:node_metrics": ErrorAttributionSource.NODE_METRICS,
}


def _map_evidence_references(evidence_references: list[str]) -> list[ErrorAttributionSource]:
    """Map evidence-reference tags to deduped ErrorAttributionSource values in canonical order.

    Unknown tags map to ErrorAttributionSource.NONE. The result is deduped into a set and sorted by
    IntEnum value, both reproducing today's set-of-sources behavior AND fixing the previously
    nondeterministic serialization order (design §7.4, correction C4).
    """
    sources: set[ErrorAttributionSource] = set()
    for ref in evidence_references:
        sources.add(EVIDENCE_TAG_TO_SOURCE.get(ref, ErrorAttributionSource.NONE))
    return sorted(sources, key=lambda s: s.value)


def _map_error_category(category: ErrorCategory) -> ErrorAttribution:
    """Map an ErrorCategory to its ErrorAttribution, defaulting unknown values to UNDEFINED."""
    return ERROR_CATEGORY_TO_ATTRIBUTION.get(category, ErrorAttribution.UNDEFINED)


def build_attribution_result(
    response: WorkloadAttributionResponse,
    job_status: SlurmJobExecutionStatus,
    anomaly_type: str,
    anomaly_uuid: str,
    error_attribution_type: ErrorAttributionType,
    settings: Settings,
) -> AnomalyAttributionResult:
    """Reconstruct an AnomalyAttributionResult from a WorkloadAttributionResponse.

    ALWAYS returns an object (never None); node/global dicts are empty when the response carries no
    attributions. NodeEvent is built WITHOUT cluster/job_start_time/job_end_time/original_exclusion_type
    so they ride the dataclass defaults (cluster:"" / null timestamps / UNDEFINED), matching the
    byte-for-byte legacy output (design §7.2). Exclusion type is re-derived by exclusion_policy from
    {recommended_action, symptom} for Node attributions only; GlobalEvent has no exclusion_type.

    Args:
        response: The WorkloadAttributionResponse from the attribution-adapter.
        job_status: Status information about the job.
        anomaly_type: The type of anomaly (EventType value).
        anomaly_uuid: The UUID of the anomaly.
        error_attribution_type: The attribution type (FAST/SLOW).
        settings: Settings carrying the fact_*_errors lists used for exclusion derivation.

    Returns:
        AnomalyAttributionResult: The reconstructed attribution result.
    """
    logger.debug(f"Building AnomalyAttributionResult for job id: {job_status.job_id}")

    attribution_result = AnomalyAttributionResult(
        job_id=job_status.job_id,
        job_attempt_index=job_status.job_attempt_index,
        anomaly_type=EventType(anomaly_type),
        anomaly_uuid=anomaly_uuid,
        attribution_type=error_attribution_type,
        node_attributions={},
        global_attributions={},
    )

    job_anomaly_id = parse_uuid(anomaly_uuid)

    for attribution in response.attributions:
        observed_at = attribution.observed_at
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=UTC)

        error_attribution = _map_error_category(attribution.error_category)
        attribution_sources = _map_evidence_references(attribution.evidence_references)

        # Coerce None -> "" so the JobExecutionError hash and the doubly-encoded map key match
        # the legacy reconstruction (design §6 None->"" note).
        job_execution_error = JobExecutionError(
            code=0,
            message=attribution.evidence_excerpt or "",
            description=attribution.error_detail or "",
            signature=attribution.symptom or "",
        )

        if attribution.resource_set.kind == ResourceKind.NODE:
            exclusion_type = exclusion_policy.derive_exclusion(
                attribution.recommended_action, attribution.symptom or "", settings
            )
            node_event = NodeEvent(
                node=attribution.resource_set.value or "",
                timestamp=observed_at,
                job_id=job_status.job_id,
                job_anomaly_id=job_anomaly_id,
                job_attempt_index=job_status.job_attempt_index,
                error_attribution=error_attribution,
                error_attribution_type=error_attribution_type,
                error_attribution_origin=ErrorAttributionOrigin.FACT,
                error_attribution_sources=attribution_sources,
                exclusion_type=exclusion_type,
                fact_status=FactStatus.OK,
            )
            attribution_result.node_attributions.setdefault(job_execution_error, []).append(node_event)
        elif attribution.resource_set.kind == ResourceKind.ALL_JOB_RESOURCES:
            global_event = GlobalEvent(
                timestamp=observed_at,
                job_id=job_status.job_id,
                job_anomaly_id=job_anomaly_id,
                job_attempt_index=job_status.job_attempt_index,
                error_attribution=error_attribution,
                error_attribution_type=error_attribution_type,
                error_attribution_origin=ErrorAttributionOrigin.FACT,
                error_attribution_sources=attribution_sources,
                fact_status=FactStatus.OK,
            )
            attribution_result.global_attributions.setdefault(job_execution_error, []).append(global_event)

    return attribution_result
