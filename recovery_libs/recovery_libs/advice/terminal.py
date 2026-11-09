# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Callable

from recovery_libs.advice.fact import build_fact_attribution, build_fact_issues
from recovery_libs.advice.nodes import compute_exclusion_nodes
from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import (
    Advice,
    AdviceRecommendation,
    AdviceState,
    JobTerminalAdvice,
    NodeConstraints,
    SlurmJobExecutionStatus,
)

GetPreviousAttemptStatus = Callable[[str, int], SlurmJobExecutionStatus | None]


def map_advice_state(advice: JobTerminalAdvice | str) -> AdviceState:
    mapping: dict[str, AdviceState] = {
        JobTerminalAdvice.JOB_RELEASED: AdviceState.RESTARTED,
        JobTerminalAdvice.MANUAL_RELEASE: AdviceState.RESTARTED,
        JobTerminalAdvice.USER_CANCELED: AdviceState.TERMINATED,
        JobTerminalAdvice.MAX_REQUEUE_REACHED: AdviceState.TERMINATED,
        JobTerminalAdvice.MANUAL_KILL: AdviceState.TERMINATED,
        JobTerminalAdvice.APPLICATION_REQUESTED_CANCELLATION: AdviceState.TERMINATED,
    }
    if advice not in mapping:
        return AdviceState.TERMINATED
    return mapping[advice]


def build_terminal_advice(
    *,
    job_id: str,
    job_attempt_index: int,
    terminal_advice: JobTerminalAdvice,
    status: SlurmJobExecutionStatus | None = None,
    anomaly_attribution: AnomalyAttributionResult | None = None,
    existing_advice: Advice | None = None,
    exclusion_stickiness_enabled: bool = False,
    get_previous_attempt: GetPreviousAttemptStatus | None = None,
) -> Advice:
    """Build a terminal advice record for persistence in Job Manager.

    Constructs the advice payload directly, without HTTP I/O.
    """
    exclusion_nodes: list[str] = []
    if status is not None and anomaly_attribution is not None:
        exclusion_nodes = list(
            compute_exclusion_nodes(
                status,
                anomaly_attribution,
                exclusion_stickiness_enabled=exclusion_stickiness_enabled,
                get_previous_attempt=get_previous_attempt,
            )
        )

    advice_state = map_advice_state(terminal_advice)
    node_constraints = NodeConstraints(exclusions=exclusion_nodes) if exclusion_nodes else None
    effective_job_id = status.job_id if status is not None else job_id
    effective_attempt_index = status.job_attempt_index if status is not None else job_attempt_index

    scheduler_metadata = status.scheduler_metadata if status is not None else {}
    heimdall_job_id = str(generate_heimdall_job_id(effective_job_id, effective_attempt_index))
    existing_issues = existing_advice.issues if existing_advice is not None else None
    if existing_advice is not None:
        heimdall_job_id = existing_advice.id

    fact_attribution_data = build_fact_attribution(anomaly_attribution)
    fact_issues = build_fact_issues(anomaly_attribution)
    all_issues = (existing_issues or []) + fact_issues if fact_issues else existing_issues

    return Advice(
        id=heimdall_job_id,
        visible=True,
        job_id=effective_job_id,
        restart_count=effective_attempt_index,
        scheduler_metadata=scheduler_metadata or {},
        advice=AdviceRecommendation(state=advice_state, node_constraints=node_constraints),
        issues=all_issues,
        fact_attribution=fact_attribution_data,
    )
