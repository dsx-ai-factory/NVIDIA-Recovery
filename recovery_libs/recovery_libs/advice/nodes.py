# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Callable

from recovery_libs.schema.anomaly_isolator.attribution import AnomalyAttributionResult
from recovery_libs.schema.aps_spec.spec import AutoResumeModeEnum
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus
from recovery_libs.storage.node_events import ExclusionType

logger = logging.getLogger(__name__)

GetPreviousAttemptStatus = Callable[[str, int], SlurmJobExecutionStatus | None]


def compute_sticky_exclusion_nodes(
    job_status: SlurmJobExecutionStatus,
    *,
    get_previous_attempt: GetPreviousAttemptStatus | None = None,
) -> list[str]:
    """Return sticky exclusion nodes inherited from the current or previous attempt."""
    job_id = job_status.job_id
    job_attempt_index = job_status.job_attempt_index

    if (
        job_status.aps_spec
        and job_status.aps_spec.auto_resume_mode
        and job_status.aps_spec.auto_resume_mode == AutoResumeModeEnum.SINGLETON_DEPENDENCY
    ):
        logger.info(
            "Job %s attempt %s runs with singleton dependency mode, preserving the sticky "
            "exclusion node list of itself (%s).",
            job_id,
            job_attempt_index,
            job_status.sticky_exclusion_node_list or [],
        )
        return job_status.sticky_exclusion_node_list or []

    if job_attempt_index == 0:
        logger.warning(
            "Job %s attempt %s is the first attempt, preserving the sticky exclusion node "
            "list of itself (%s), this is likely because job manager is not yet updated with "
            "the latest job status due to delay somewhere.",
            job_id,
            job_attempt_index,
            job_status.sticky_exclusion_node_list or [],
        )
        return job_status.sticky_exclusion_node_list or []

    previous_attempt_index = job_attempt_index - 1
    if get_previous_attempt is None:
        logger.warning(
            "No previous-attempt lookup provided for job %s attempt %s; sticky exclusions unavailable.",
            job_id,
            job_attempt_index,
        )
        return []

    try:
        logger.info(
            "Fetching sticky exclusion nodes from job %s attempt %s for current attempt %s",
            job_id,
            previous_attempt_index,
            job_attempt_index,
        )
        previous_attempt = get_previous_attempt(job_id, previous_attempt_index)
        if previous_attempt is None:
            logger.info(
                "No previous attempt found for job %s attempt %s.",
                job_id,
                previous_attempt_index,
            )
            return []

        sticky_nodes = previous_attempt.sticky_exclusion_node_list or []
        if sticky_nodes:
            logger.info(
                "Inheriting %s sticky exclusion nodes from job %s attempt %s: %s",
                len(sticky_nodes),
                job_id,
                previous_attempt_index,
                sticky_nodes,
            )
        else:
            logger.info("No sticky exclusion nodes found in job %s, attempt %s.", job_id, previous_attempt_index)
        return sticky_nodes
    except Exception as error:
        logger.warning(
            "Failed to fetch previous attempt (attempt %s) for job %s. No sticky exclusion nodes to inherit.",
            previous_attempt_index,
            job_id,
            exc_info=True,
            extra={"error": str(error)},
        )
        return []


def compute_exclusion_nodes(
    status: SlurmJobExecutionStatus,
    anomaly_attribution: AnomalyAttributionResult,
    *,
    exclusion_stickiness_enabled: bool,
    get_previous_attempt: GetPreviousAttemptStatus | None = None,
) -> set[str]:
    """Compute exclusion nodes from job status and anomaly attribution."""
    exclusion_nodes: set[str] = set()

    if exclusion_stickiness_enabled:
        node_list = compute_sticky_exclusion_nodes(status, get_previous_attempt=get_previous_attempt)
        if node_list:
            logger.info(
                "Exclusion node stickiness is enabled, preserving exclusion node list %s for job %s attempt %s.",
                node_list,
                status.job_id,
                status.job_attempt_index,
            )
            exclusion_nodes.update(node_list)

    for node_events in anomaly_attribution.node_attributions.values():
        for node_event in node_events:
            if node_event.exclusion_type in (ExclusionType.TEMPORARY, ExclusionType.CONFIRMED):
                exclusion_nodes.add(node_event.node)

    return exclusion_nodes
