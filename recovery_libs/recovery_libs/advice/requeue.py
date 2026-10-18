# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Callable

from recovery_libs.schema.job_manager.constants import INFINITE_SYSTEM_REQUEUE_LIMIT
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus, SystemRequeueConfig
from recovery_libs.storage.dataclass import SlurmJobFinalStates

logger = logging.getLogger(__name__)

GetJobsBatch = Callable[..., list[SlurmJobExecutionStatus]]

_CHECKPOINT_EVAL_STATES = frozenset(
    {
        SlurmJobFinalStates.FAILED,
        SlurmJobFinalStates.REQUEUED,
        SlurmJobFinalStates.OUT_OF_MEMORY,
    }
)


def check_max_requeue_reached(
    status: SlurmJobExecutionStatus,
    *,
    system_requeue_config: SystemRequeueConfig | None,
    get_jobs: GetJobsBatch,
) -> tuple[bool, str]:
    """Determine whether a job has reached its requeue limit."""
    if status.state == SlurmJobFinalStates.COMPLETED:
        if status.aps_spec and status.aps_spec.requeue_on_completion is not None:
            if not status.aps_spec.requeue_on_completion:
                return (
                    True,
                    (
                        f"The job {status.job_id}, attempt {status.job_attempt_index} is in COMPLETED state and "
                        "the job's APS spec is configured to not requeue on completion."
                    ),
                )
        elif system_requeue_config and not system_requeue_config.requeue_on_completion:
            return (
                True,
                (
                    f"The job {status.job_id}, attempt {status.job_attempt_index} is in COMPLETED state and the "
                    "system is configured to not requeue a job on completion."
                ),
            )

    if system_requeue_config and (
        checkpoint_threshold := system_requeue_config.checkpoint_based_requeue_stop_threshold
    ):
        failed_without_checkpoint = 0
        checkpoint_found = False
        offset = 0
        batch_size = checkpoint_threshold

        while failed_without_checkpoint < checkpoint_threshold and not checkpoint_found:
            batch = get_jobs(
                job_id=status.job_id,
                sort_by="job_attempt_index:desc",
                limit=batch_size,
                offset=offset,
            )
            if not batch:
                break
            for job in batch:
                if job.checkpoints:
                    checkpoint_found = True
                    logger.debug(
                        "Checkpoint found in job %s, attempt %s; requeue early stop condition isn't met.",
                        job.job_id,
                        job.job_attempt_index,
                    )
                    break
                if job.state in _CHECKPOINT_EVAL_STATES:
                    failed_without_checkpoint += 1
                    if failed_without_checkpoint >= checkpoint_threshold:
                        break
            if len(batch) < batch_size:
                logger.debug("All attempts have been evaluated for job id %s.", status.job_id)
                break
            offset += batch_size

        if not checkpoint_found and failed_without_checkpoint >= checkpoint_threshold:
            message = (
                f"Job {status.job_id} has not reached a checkpoint in the last {checkpoint_threshold} consecutive "
                f"qualified failed attempts (states: {[state.value for state in _CHECKPOINT_EVAL_STATES]}), "
                "checkpoint-based early stop condition is met, so no more requeue will happen."
            )
            logger.info(message)
            return (True, message)

        if checkpoint_found:
            logger.info(
                "Job %s reached a checkpoint within the recent attempts; checkpoint-based early stop is not met.",
                status.job_id,
            )
        elif failed_without_checkpoint < checkpoint_threshold:
            logger.info(
                "Not enough qualified job attempts to evaluate checkpoint-based early stop for job %s; "
                "threshold=%s, qualified_attempts=%s, states=%s.",
                status.job_id,
                checkpoint_threshold,
                failed_without_checkpoint,
                [state.value for state in _CHECKPOINT_EVAL_STATES],
            )

    effective_requeue_limit: int | None = system_requeue_config.max_requeue_times if system_requeue_config else None

    if effective_requeue_limit is None or effective_requeue_limit == INFINITE_SYSTEM_REQUEUE_LIMIT:
        if status.aps_spec and status.aps_spec.max_requeue_times:
            effective_requeue_limit = status.aps_spec.max_requeue_times
        else:
            message = (
                f"System requeue limit is {effective_requeue_limit} and user requeue limit is not specified, so not "
                f"enforcing any requeue limit for job {status.job_id} (attempt {status.job_attempt_index})."
            )
            logger.info(message)
            return (False, message)
    elif status.aps_spec and status.aps_spec.max_requeue_times:
        effective_requeue_limit = min(effective_requeue_limit, status.aps_spec.max_requeue_times)

    if status.job_attempt_index >= effective_requeue_limit:
        message = (
            f"Job {status.job_id} (attempt {status.job_attempt_index}) has reached the requeue limit of "
            f"{effective_requeue_limit}."
        )
        logger.info(message)
        return (True, message)

    return (False, "The job has not reached the requeue limit, requeue will continue.")
