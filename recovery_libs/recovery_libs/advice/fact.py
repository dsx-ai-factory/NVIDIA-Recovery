# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.job_manager.models import Issue, RemediationAction, ResourceDetail
from recovery_libs.schema.queue_bad_nodes_isolation.anomaly_isolation import AnomalyIsolation
from recovery_libs.storage.node_events import ExclusionType

_EXCLUSION_TYPE_LABELS = {
    ExclusionType.TEMPORARY: "Exclude Node",
    ExclusionType.CONFIRMED: "Exclude Node (Confirmed)",
}


def build_fact_attribution(
    anomaly_attribution: AnomalyAttributionResult | AnomalyIsolation | None,
) -> dict | None:
    """Build a JSON-serializable dict summarising FACT attribution for the advice record."""
    if not isinstance(anomaly_attribution, AnomalyAttributionResult):
        return None

    node_attributions = []
    for error, events in anomaly_attribution.node_attributions.items():
        node_attributions.append(
            {
                "error": error.to_json(),
                "node_events": [event.to_json() for event in events],
            }
        )

    global_attributions = []
    for error, events in anomaly_attribution.global_attributions.items():  # type: ignore[assignment]
        global_attributions.append(
            {
                "error": error.to_json(),
                "global_events": [event.to_json() for event in events],
            }
        )

    return {
        "anomaly_type": anomaly_attribution.anomaly_type.value,
        "anomaly_uuid": anomaly_attribution.anomaly_uuid,
        "attribution_type": anomaly_attribution.attribution_type.value,
        "node_attributions": node_attributions,
        "global_attributions": global_attributions,
    }


def build_fact_issues(
    anomaly_attribution: AnomalyAttributionResult | AnomalyIsolation | None,
) -> list[Issue]:
    """Build Issue objects from FACT node attributions for inclusion in the advice record."""
    if not isinstance(anomaly_attribution, AnomalyAttributionResult):
        return []

    issues: list[Issue] = []
    for error, node_events in anomaly_attribution.node_attributions.items():
        if not node_events:
            continue

        remediation_actions: list[RemediationAction] = []
        for event in node_events:
            exclusion_label = _EXCLUSION_TYPE_LABELS.get(event.exclusion_type)
            if exclusion_label is None:
                continue
            remediation_actions.append(
                RemediationAction(
                    action_type=exclusion_label,
                    timestamp=event.timestamp,
                    reason=f"{error.signature}: {error.description}" if error.description else error.signature,
                    resource_details=[
                        ResourceDetail(
                            resource_type="Node",
                            resource_id=event.node,
                        )
                    ],
                )
            )

        earliest_ts = min(e.timestamp for e in node_events)
        issues.append(
            Issue(
                issue_type=anomaly_attribution.anomaly_type.value,
                timestamp=earliest_ts,
                detected_by="Heimdall FACT",
                relevant_log_lines=[error.message] if error.message else None,
                remediation_actions=remediation_actions or None,
            )
        )

    return issues
