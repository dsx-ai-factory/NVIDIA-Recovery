# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Best-effort audit of checkpoint association, independent of trace sampling."""

import json
from uuid import NAMESPACE_URL, uuid5

from recovery_libs.audit import AuditBuffer, AuditEventLevel, ProducerEvent
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.watcher.model import Event
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord

from job_manager.audit.events import CheckpointAssociatedEvent, CheckpointAssociatedPayload, current_unix_microseconds
from job_manager.logger import logger


def audit_checkpoint_association(
    record: SlurmJobExecutionRecord,
    event: Event,
    audit_buffer: AuditBuffer[ProducerEvent],
) -> None:
    """Buffer a replay-stable association after a save commits or a load matches.

    Delivery or payload failures are diagnostic only: they must not retry the
    checkpoint write or change recovery behavior. Task and attempt are always
    derived from the resolved record, never the source's unresolved label.
    """
    try:
        if event.attributes is None:
            return
        kind = "save" if event.type == EventType.CHECKPOINTSAVE else "load"
        identity = generate_heimdall_job_id(record.job_id, record.job_attempt_index)
        source = ["checkpoint_associated", str(identity), event.attributes.uuid, kind]
        audit_buffer.add(
            CheckpointAssociatedEvent(
                heimdall_job_id=str(identity),
                event_level=AuditEventLevel.OPERATIONAL,
                payload=CheckpointAssociatedPayload(
                    job_id=record.job_id,
                    attempt_index=record.job_attempt_index,
                    source_event_uuid=event.attributes.uuid,
                    checkpoint_kind=kind,
                    event_occurred_at=int(event.timestamp) // 1000 if event.timestamp else None,
                ),
                event_produced_at=current_unix_microseconds(),
                idempotency_key=str(uuid5(NAMESPACE_URL, json.dumps(source))),
            )
        )
    except Exception:
        logger.exception("Unable to buffer checkpoint association audit")
