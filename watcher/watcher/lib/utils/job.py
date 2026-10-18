# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from uuid import UUID

from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus
from recovery_libs.schema.watcher.model import Event
from recovery_libs.utils.time import source_timestamp_in_attempt


@dataclass
class JobIdentity:
    """Known job attempt identity used by Watcher audit and deduplication.

    Attributes:
        job_id: Scheduler job ID shared by its attempts.
        attempt_index: Nonnegative attempt index within that job.
    """

    job_id: str
    attempt_index: int

    @property
    def heimdall_job_id(self) -> UUID:
        """Return the canonical UUID derived from the job ID and attempt index."""
        return generate_heimdall_job_id(self.job_id, self.attempt_index)


def get_job_attempt_for_event(
    event: Event,
    job_attempts: list[SlurmJobExecutionStatus],
) -> SlurmJobExecutionStatus | None:
    """Select the unique attempt whose execution window contains the source timestamp.

    Only attempts for the event's job are considered. Missing starts cannot
    establish a match. A missing end is bounded by the nearest known newer
    attempt start; only the latest registered attempt may remain open-ended.
    Missing or invalid timestamps and overlapping matches return None.
    """
    attempts = sorted(
        (attempt for attempt in job_attempts if attempt.job_id == str(event.job_id)),
        key=lambda attempt: attempt.job_attempt_index,
        reverse=True,
    )
    match: SlurmJobExecutionStatus | None = None
    newer_start = None
    for position, attempt in enumerate(attempts):
        end_time = attempt.end_time or newer_start
        # An older attempt cannot extend indefinitely into a registered retry
        # whose start has not yet arrived from the scheduler.
        can_match = position == 0 or end_time is not None
        if can_match and source_timestamp_in_attempt(event.timestamp, attempt.start_time, end_time):
            if match is not None:
                return None
            match = attempt
        newer_start = attempt.start_time or newer_start
    return match


def get_attempt_index(
    event: Event,
) -> int | None:
    """Return a nonnegative attempt label, or None if no label is supplied.

    Raises:
        ValueError: The supplied label is not an integer or is negative.
    """
    raw_attempt = (event.attributes.model_extra or {}).get("slurm_job_attempt") if event.attributes else None
    if raw_attempt is None:
        return None

    if not isinstance(raw_attempt, (str, int)) or isinstance(raw_attempt, bool):
        raise ValueError("Attempt label must be an integer")  # noqa: TRY004

    attempt_index = int(raw_attempt)
    if attempt_index < 0:
        raise ValueError("Attempt label must be nonnegative")

    return attempt_index


def try_get_job_identity_for_event(
    event: Event,
    *,
    match_time_with_job_attempt: SlurmJobExecutionStatus | None = None,
) -> JobIdentity | None:
    """Return the canonical job identity when the event's attempt can be resolved.

    Args:
        event: Event whose explicit attempt label takes precedence.
        match_time_with_job_attempt: Optional attempt used for timestamp matching when no label exists.
    """

    try:
        attempt_index = get_attempt_index(event)
        if attempt_index is not None:
            return JobIdentity(job_id=event.job_id, attempt_index=attempt_index)
    except (TypeError, ValueError):
        return None

    if match_time_with_job_attempt is None:
        return None

    # Job start/end time does not include event occurrence time.
    if not source_timestamp_in_attempt(
        event.timestamp, match_time_with_job_attempt.start_time, match_time_with_job_attempt.end_time
    ):
        return None

    return JobIdentity(
        job_id=event.job_id,
        attempt_index=match_time_with_job_attempt.job_attempt_index,
    )
