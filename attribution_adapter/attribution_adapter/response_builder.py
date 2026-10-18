# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Project a FACT ``AttributionResponseV2`` onto the ``WorkloadAttributionResponse`` schema."""

from datetime import UTC, datetime

from nvidia_attribution_client.models.attribution_response_v2 import AttributionResponseV2
from nvidia_attribution_client.models.node_source import NodeSource
from nvidia_attribution_client.models.symptoms_value_inner import SymptomsValueInner
from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION
from recovery_libs.schema.attribution_interface.enums import ResourceKind
from recovery_libs.schema.attribution_interface.request import WorkloadAttributionRequest
from recovery_libs.schema.attribution_interface.response import (
    Attribution,
    ResourceSet,
    WorkloadAttributionResponse,
)

from attribution_adapter.identity import SERVICE_IDENTIFIER, _service_version
from attribution_adapter.taxonomy import outcome_to_category
from attribution_adapter.utils.logger import logger


def _observed_at(src_time: datetime | None, request: WorkloadAttributionRequest) -> datetime:
    """Resolve ``observed_at`` from a FACT source time, falling back to job_end_time or now-UTC.

    Args:
        src_time: The time field from a FACT source (may be ``None``).
        request: The originating workload attribution request (supplies the fallback time).

    Returns:
        A timezone-aware ``datetime`` for the attribution's ``observed_at``.
    """
    observed = src_time or request.workload_context.job_end_time or datetime.now(tz=UTC)
    return observed.replace(tzinfo=UTC) if observed.tzinfo is None else observed


def build_response(
    fact_attribution: AttributionResponseV2,
    request: WorkloadAttributionRequest,
    attribution_session_id: str,
) -> WorkloadAttributionResponse:
    """Convert a FACT ``AttributionResponseV2`` to a ``WorkloadAttributionResponse``.

    One ``Attribution`` per (symptom key, site). ``NodeSource`` -> NODE(node);
    ``ResourcelessSource`` -> ALL_JOB_RESOURCES. ``error_category`` is derived from the
    verdict-level ``attribution_outcome`` (display-only). ``recommended_action`` is always
    ``None`` (raw v2 has no recommendation; isolation is symptom-driven downstream).

    Args:
        fact_attribution: The FACT v2 attribution result.
        request: The originating workload attribution request.
        attribution_session_id: The opaque FACT session token to echo back.

    Returns:
        The projected ``WorkloadAttributionResponse``.
    """
    category = outcome_to_category(fact_attribution.verdict.attribution_outcome)
    detail = fact_attribution.verdict.detail or ""
    attributions: list[Attribution] = []

    for symptom_str, sites in (fact_attribution.symptoms or {}).items():
        for site in sites:
            # SymptomsValueInner is the oneOf wrapper; unwrap to get the typed instance.
            inst = site.actual_instance if isinstance(site, SymptomsValueInner) else site
            if inst is None:
                continue
            if isinstance(inst, NodeSource):
                resource_set = ResourceSet(kind=ResourceKind.NODE, value=inst.node)
            else:
                resource_set = ResourceSet(kind=ResourceKind.ALL_JOB_RESOURCES)
            src = inst.source
            attributions.append(
                Attribution(
                    resource_set=resource_set,
                    error_category=category,
                    error_detail=detail,
                    confidence=None,
                    symptom=symptom_str or "",
                    evidence_excerpt=(src.line if src and src.line else ""),
                    observed_at=_observed_at(src.time if src else None, request),
                    recommended_action=None,
                    evidence_references=sorted({f"source:{src.source.value}"}) if src and src.source else [],
                )
            )

    logger.info(
        f"Built {len(attributions)} attribution(s) from outcome "
        f"'{fact_attribution.verdict.attribution_outcome}' for workload {request.workload_id}"
    )
    return WorkloadAttributionResponse(
        schema_version=ATTRIBUTION_SCHEMA_VERSION,
        service_identifier=SERVICE_IDENTIFIER,
        service_version=_service_version(),
        request_id=request.workload_id,
        attribution_session_id=attribution_session_id,
        attributions=attributions,
        unattributed_targets=[],
        error=None,
    )
