# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Project a FACT ``AttributionResponseV2`` onto the ``WorkloadAttributionResponse`` schema."""

from datetime import UTC, datetime
from typing import Any

from nvidia_attribution_client.models.attribution_response_v2 import AttributionResponseV2
from nvidia_attribution_client.models.node_source import NodeSource
from nvidia_attribution_client.models.symptoms_value_inner import SymptomsValueInner
from recovery_libs.schema.anomaly_type import AnomalyType
from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION
from recovery_libs.schema.attribution_interface.enums import ErrorCategory, ResourceKind
from recovery_libs.schema.attribution_interface.request import WorkloadAttributionRequest
from recovery_libs.schema.attribution_interface.response import (
    Attribution,
    ResourceSet,
    WorkloadAttributionResponse,
)

from attribution_adapter.identity import SERVICE_IDENTIFIER, _service_version
from attribution_adapter.taxonomy import outcome_to_category
from attribution_adapter.utils.logger import logger

# FACT v2 reports straggler nodes in ``collectors``, not in ``symptoms``. Each of these digests maps
# a node name to ``{"pattern": str, "rank_scores": [{"rank": int, "score": float}], "sources": [...]}``.
STRAGGLER_COLLECTOR_KEYS = ("megatron_straggler", "nvrx_straggler")


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


def _parse_source_time(value: Any) -> datetime | None:
    """Parse the ``time`` of a FACT source in a collector digest.

    Args:
        value: An ISO 8601 string, a ``datetime``, or ``None``.

    Returns:
        A timezone-aware ``datetime``, or ``None`` when the value is missing or not a timestamp.
    """
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


def _latest_evidence(sources: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Pick the evidence for a straggler node: the most recent source that carries a log line.

    A source without a timestamp sorts before every timed source, so a timed line wins when both exist.

    Args:
        sources: The FACT sources of one straggler node report.

    Returns:
        The chosen source, or ``None`` when no source has a log line.
    """
    earliest = datetime.min.replace(tzinfo=UTC)
    with_line = [source for source in sources if source.get("line")]
    return max(with_line, key=lambda source: _parse_source_time(source.get("time")) or earliest, default=None)


def _flagged_ranks(report: Any) -> list[int]:
    """Read the flagged ranks from one straggler node report.

    Args:
        report: One value of a straggler collector digest. Its shape is not guaranteed.

    Returns:
        The distinct flagged ranks in ascending order, or ``[]`` when the report is malformed
        or flags no rank.
    """
    rank_scores = report.get("rank_scores") if isinstance(report, dict) else None
    if not isinstance(rank_scores, list):
        return []
    return sorted(
        {
            rank_score["rank"]
            for rank_score in rank_scores
            if isinstance(rank_score, dict) and isinstance(rank_score.get("rank"), int)
        }
    )


def _straggler_attributions(
    collectors: dict[str, Any] | None,
    category: ErrorCategory,
    request: WorkloadAttributionRequest,
) -> list[Attribution]:
    """Build one NODE ``Attribution`` for each node in FACT's straggler collector digests.

    The symptom is ``Straggler (<pattern>)``, so the downstream exclusion lists can match it.
    The evidence is the most recent log line that flagged a rank on the node. A digest entry
    that does not have the expected shape is skipped, because ``collectors`` is untyped.

    Args:
        collectors: The ``collectors`` field of the FACT v2 attribution result.
        category: The verdict-level error category, shared with the symptom attributions.
        request: The originating workload attribution request (supplies the fallback time).

    Returns:
        The straggler node attributions, ordered by collector and then by node name.
    """
    attributions: list[Attribution] = []
    for collector_key in STRAGGLER_COLLECTOR_KEYS:
        reports = (collectors or {}).get(collector_key)
        if not isinstance(reports, dict):
            continue
        for node, report in sorted(reports.items()):
            ranks = _flagged_ranks(report)
            if not node or not ranks:
                logger.warning(f"Skipping malformed {collector_key} report for node '{node}'")
                continue
            sources = [source for source in report.get("sources") or [] if isinstance(source, dict)]
            evidence = _latest_evidence(sources)
            attributions.append(
                Attribution(
                    resource_set=ResourceSet(kind=ResourceKind.NODE, value=node),
                    error_category=category,
                    error_detail=f"{collector_key} flagged rank(s) {', '.join(map(str, ranks))}",
                    confidence=None,
                    symptom=f"Straggler ({report.get('pattern') or collector_key})",
                    evidence_excerpt=evidence["line"] if evidence else "",
                    observed_at=_observed_at(_parse_source_time(evidence.get("time")) if evidence else None, request),
                    recommended_action=None,
                    evidence_references=sorted(
                        {f"source:{source['source']}" for source in sources if source.get("source")}
                    ),
                )
            )
    return attributions


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

    For a STRAGGLER anomaly, each node in FACT's straggler collector digests also becomes a
    NODE attribution. Other anomaly types ignore the digests, so a crash or hang attribution
    does not exclude a node only because it was slow earlier in the interest window.

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

    if request.failure_context.detection_signal == AnomalyType.STRAGGLER.value:
        attributions.extend(_straggler_attributions(fact_attribution.collectors, category, request))

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
