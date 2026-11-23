# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Project observed recovery convergence onto existing attribution rows."""

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from recovery_libs.schema.anomaly_isolator.exclusion_application import ExclusionApplicationStatus
from recovery_libs.schema.job_manager.models import AdviceReconciliationDisposition, AdviceState
from recovery_libs.storage.dataclass import AdviceReconciliationStatusRecord, AdviceRecord
from recovery_libs.storage.node_events import ErrorAttributionType, ExclusionType, NodeEvent


def confirm_exclusions(session: Session, advice: AdviceRecord, status: AdviceReconciliationStatusRecord) -> int:
    """Confirm eligible FAST exclusions when the current restart generation converges.

    The caller owns the transaction and holds the Advice row lock. Select the latest
    FAST anomaly before filtering nodes or effective exclusion types, so a suppressed
    or unrelated newer result cannot accidentally confirm an older diagnosis. Rows
    without anomaly identity are restricted to the latest event timestamp instead.
    No scheduler commands or audit delivery participate in this projection.
    """
    if (
        status.observed_generation != advice.generation
        or status.disposition != AdviceReconciliationDisposition.CONVERGED.value
        or advice.advice_state != AdviceState.RESTARTED.value
        or not advice.node_exclusions
    ):
        return 0

    attempt_filter = (
        NodeEvent.job_id == advice.job_id,
        NodeEvent.job_attempt_index == advice.attempt_index,
        NodeEvent.error_attribution_type == ErrorAttributionType.FAST,
    )
    # Provider observation times may arrive out of order; persistence order identifies
    # the latest reported FAST anomaly that triggered downstream recovery.
    latest = session.scalars(select(NodeEvent).where(*attempt_filter).order_by(NodeEvent.id.desc()).limit(1)).first()
    if latest is None:
        return 0
    anomaly_filter = (
        NodeEvent.job_anomaly_id == latest.job_anomaly_id
        if latest.job_anomaly_id is not None
        else (NodeEvent.job_anomaly_id.is_(None) & (NodeEvent.timestamp == latest.timestamp))
    )
    result = session.execute(
        update(NodeEvent)
        .where(
            *attempt_filter,
            anomaly_filter,
            NodeEvent.node.in_(advice.node_exclusions),
            NodeEvent.exclusion_type.in_((ExclusionType.TEMPORARY, ExclusionType.CONFIRMED)),
        )
        .values(exclusion_applied=ExclusionApplicationStatus.APPLIED)
    )
    return result.rowcount
