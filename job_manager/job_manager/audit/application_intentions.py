# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from recovery_libs.audit import AuditBuffer, AuditEventLevel, ProducerEvent
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.storage.dataclass import ApplicationIntentionRecord

from job_manager.audit.events import (
    ApplicationTerminationRequestedEvent,
    ApplicationTerminationRequestedPayload,
    current_unix_microseconds,
)
from job_manager.logger import logger


def audit_application_termination_requested(
    intention: ApplicationIntentionRecord,
    audit_buffer: AuditBuffer[ProducerEvent],
) -> None:
    """Record the first durable, job-level application termination request.

    The request can arrive before Job Manager observes an execution attempt.
    Its storage row records the latest known attempt when Job Manager first has
    enough information to emit this event.
    """
    try:
        if intention.audit_attempt_index is None:
            raise ValueError("application intention audit attempt has not been claimed")
        attempt_index = intention.audit_attempt_index
        audit_buffer.add(
            ApplicationTerminationRequestedEvent(
                heimdall_job_id=str(generate_heimdall_job_id(intention.job_id, attempt_index)),
                event_level=AuditEventLevel.RECOVERY,
                payload=ApplicationTerminationRequestedPayload(
                    application_intention_id=intention.id,
                    job_id=intention.job_id,
                    attempt_index=attempt_index,
                    desired_state=intention.desired_state,
                    reason=intention.reason,
                    requested_at=intention.created_at,
                ),
                event_produced_at=current_unix_microseconds(),
            )
        )
    except Exception:
        # Audit delivery is best effort and must not turn a durably accepted
        # application request into an API failure.
        logger.exception(
            "Failed to create application termination request audit event for job %s",
            intention.job_id,
            extra={"job_id": intention.job_id},
        )
