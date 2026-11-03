# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import time
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel
from recovery_libs.audit import ProducerEvent
from recovery_libs.tracing.recovery import RecoveryCorrelation


def current_unix_microseconds() -> int:
    """Return the current Unix timestamp in microseconds."""
    return time.time_ns() // 1_000


JobAuditSnapshot = dict[str, Any]
AdviceAuditSnapshot = dict[str, Any]


class ApplicationTerminationRequestedPayload(BaseModel):
    application_intention_id: int
    job_id: str
    attempt_index: int
    desired_state: str
    reason: str | None
    requested_at: datetime


class ApplicationTerminationRequestedEvent(
    ProducerEvent[ApplicationTerminationRequestedPayload],
    frozen=True,
):
    event_producer: Literal["job-manager"] = "job-manager"
    event_type: Literal["application_termination_requested"] = "application_termination_requested"


class JobAttemptFirstSeenPayload(BaseModel):
    job_id: str
    attempt_index: int
    job: JobAuditSnapshot


class JobAttemptFirstSeenEvent(ProducerEvent[JobAttemptFirstSeenPayload], frozen=True):
    event_producer: Literal["job-manager"] = "job-manager"
    event_type: Literal["job_attempt_observed"] = "job_attempt_observed"


class JobAttemptUpdatedPayload(BaseModel):
    job_id: str
    attempt_index: int
    old_job: JobAuditSnapshot
    new_job: JobAuditSnapshot


class JobAttemptUpdatedEvent(ProducerEvent[JobAttemptUpdatedPayload], frozen=True):
    event_producer: Literal["job-manager"] = "job-manager"
    event_type: Literal["job_attempt_updated"] = "job_attempt_updated"


class AdviceCreatedPayload(RecoveryCorrelation):
    advice_id: str
    job_id: str
    attempt_index: int
    advice: AdviceAuditSnapshot
    creation_reasons: list[str]
    correlation_uuid: str


class AdviceCreatedEvent(ProducerEvent[AdviceCreatedPayload], frozen=True):
    event_producer: Literal["job-manager"] = "job-manager"
    event_type: Literal["advice_created"] = "advice_created"


class AdviceUpdatedPayload(RecoveryCorrelation):
    advice_id: str
    job_id: str
    attempt_index: int
    old_advice: AdviceAuditSnapshot
    new_advice: AdviceAuditSnapshot
    change_reasons: list[str]
    correlation_uuid: str


class AdviceUpdatedEvent(ProducerEvent[AdviceUpdatedPayload], frozen=True):
    event_producer: Literal["job-manager"] = "job-manager"
    event_type: Literal["advice_updated"] = "advice_updated"


class AdviceUnpublishedPayload(RecoveryCorrelation):
    advice_id: str
    job_id: str
    """Scheduler job identifier retained from the unpublished Advice."""
    attempt_index: int
    """Scheduler attempt index retained from the unpublished Advice."""
    reason: str | None
    correlation_uuid: str


class AdviceUnpublishedEvent(ProducerEvent[AdviceUnpublishedPayload], frozen=True):
    event_producer: Literal["job-manager"] = "job-manager"
    event_type: Literal["advice_unpublished"] = "advice_unpublished"
