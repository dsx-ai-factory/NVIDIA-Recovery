# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Conversions between Advice storage records and public HTTP models."""

import uuid
from datetime import UTC, datetime

from recovery_libs.schema.job_manager.models import (
    Advice,
    AdviceRecommendation,
    AdviceReconciliationDisposition,
    AdviceReconciliationReason,
    AdviceReconciliationStatus,
    AdviceState,
    NodeConstraints,
    RecoveryContext,
    RecoveryOperation,
)
from recovery_libs.storage.dataclass import AdviceReconciliationStatusRecord, AdviceRecord

from job_manager.logger import logger


def storage_to_http_advice(record: AdviceRecord) -> Advice:
    """Convert an Advice storage record to its public representation."""
    advice_state = None
    if record.advice_state:
        try:
            advice_state = AdviceState(record.advice_state)
        except ValueError:
            logger.warning(f"Unknown advice_state value: {record.advice_state}")

    node_constraints = None
    if record.node_exclusions or record.node_inclusions:
        node_constraints = NodeConstraints(
            exclusions=record.node_exclusions,
            inclusions=record.node_inclusions,
        )

    advice_recommendation = AdviceRecommendation(
        state=advice_state,
        node_constraints=node_constraints,
    )
    current_status = next(
        (
            storage_to_http_advice_reconciliation_status(item)
            for item in record.reconciliation_statuses
            if item.observed_generation == record.generation
        ),
        None,
    )
    recovery_context = None
    if (
        record.recovery_sequence_id is not None
        and record.recovery_traceparent is not None
        and record.recovery_triggered_at is not None
        and record.recovery_operation is not None
    ):
        try:
            recovery_context = RecoveryContext(
                sequence_id=str(record.recovery_sequence_id),
                traceparent=record.recovery_traceparent,
                tracestate=record.recovery_tracestate,
                triggered_at=record.recovery_triggered_at,
                operation=RecoveryOperation(record.recovery_operation),
            )
        except ValueError:
            logger.warning(f"Ignoring invalid stored recovery context on Advice {record.heimdall_id}")

    return Advice(
        id=str(record.heimdall_id),
        visible=record.visible,
        job_id=record.job_id,
        restart_count=record.attempt_index,
        scheduler_metadata=record.scheduler_metadata or {},
        advice=advice_recommendation,
        generation=record.generation,
        desired_state_changed_at=record.desired_state_changed_at,
        reconciliation_status=current_status,
        recovery_context=recovery_context,
        issues=record.issues,
        fact_attribution=record.fact_attribution,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def http_to_storage_advice(advice: Advice) -> AdviceRecord:
    """Convert a public Advice model to its storage representation."""
    advice_state = None
    if advice.advice and advice.advice.state:
        advice_state = advice.advice.state.value

    node_exclusions = None
    node_inclusions = None
    if advice.advice and advice.advice.node_constraints:
        node_exclusions = advice.advice.node_constraints.exclusions
        node_inclusions = advice.advice.node_constraints.inclusions

    now = datetime.now(UTC)

    return AdviceRecord(
        heimdall_id=uuid.UUID(advice.id),
        visible=advice.visible,
        job_id=advice.job_id,
        scheduler_metadata=advice.scheduler_metadata,
        attempt_index=advice.restart_count,
        advice_state=advice_state,
        node_exclusions=node_exclusions,
        node_inclusions=node_inclusions,
        issues=advice.issues,
        fact_attribution=advice.fact_attribution,
        generation=advice.generation,
        desired_state_changed_at=advice.desired_state_changed_at or now,
        recovery_sequence_id=(uuid.UUID(advice.recovery_context.sequence_id) if advice.recovery_context else None),
        recovery_traceparent=(advice.recovery_context.traceparent if advice.recovery_context else None),
        recovery_tracestate=(advice.recovery_context.tracestate if advice.recovery_context else None),
        recovery_triggered_at=(advice.recovery_context.triggered_at if advice.recovery_context else None),
        recovery_operation=(advice.recovery_context.operation.value if advice.recovery_context else None),
        created_at=advice.created_at or now,
        updated_at=advice.updated_at or now,
    )


def storage_to_http_advice_reconciliation_status(
    record: AdviceReconciliationStatusRecord,
) -> AdviceReconciliationStatus:
    """Convert one durable Advice reconciliation status to its HTTP model."""
    return AdviceReconciliationStatus(
        observed_generation=record.observed_generation,
        disposition=AdviceReconciliationDisposition(record.disposition),
        reason=AdviceReconciliationReason(record.reason),
        observed_at=record.observed_at,
        transitioned_at=record.transitioned_at,
        diagnostic_message=record.diagnostic_message,
    )


def http_to_storage_advice_reconciliation_status(
    advice_id: uuid.UUID,
    reconciliation_status: AdviceReconciliationStatus,
) -> AdviceReconciliationStatusRecord:
    """Convert an Advice reconciliation status write to its storage model."""
    return AdviceReconciliationStatusRecord(
        advice_id=advice_id,
        observed_generation=reconciliation_status.observed_generation,
        disposition=reconciliation_status.disposition.value,
        reason=reconciliation_status.reason.value,
        observed_at=reconciliation_status.observed_at,
        transitioned_at=reconciliation_status.transitioned_at,
        diagnostic_message=reconciliation_status.diagnostic_message,
    )
