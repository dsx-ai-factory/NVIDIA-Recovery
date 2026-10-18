# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from uuid import NAMESPACE_URL, uuid5

from recovery_libs.audit import AuditBuffer, AuditEventLevel
from recovery_libs.schema.watcher.model import Event

from watcher.lib.utils.job import JobIdentity
from watcher.models.audit import WatcherAnomalyAuditEvent, WatcherAnomalyAuditPayload


def try_add_audit_event_from_anomaly(
    audit_event_buffer: AuditBuffer[WatcherAnomalyAuditEvent],
    anomaly_event: Event,
    workflow_step_name: str | None,
    detection_time_ns: int,
    audit_identity: JobIdentity | None,
) -> bool:
    """Buffer an anomaly audit using the independently resolved audit identity.

    Args:
        audit_event_buffer: Destination buffer for the constructed audit event.
        anomaly_event: Anomaly supplying the payload and source occurrence time.
        workflow_step_name: Workflow step included in the audit idempotency key.
        detection_time_ns: Detection time in Unix nanoseconds.
        audit_identity: Identity resolved before any inferred attempt label is added.

    Returns:
        True when the event is passed to the buffer, or False if its anomaly UUID
        or audit identity is missing.
    """

    if not anomaly_event.attributes or not anomaly_event.attributes.uuid:
        return False

    if audit_identity is None:
        return False

    heimdall_job_id = str(audit_identity.heimdall_job_id)
    attempt_index = audit_identity.attempt_index
    event_occurred_at = int(anomaly_event.timestamp) // 1_000 if anomaly_event.timestamp else detection_time_ns // 1_000

    # A source record can be returned by overlapping log-query windows or
    # replayed after Watcher restarts. Its anomaly UUID is stable, so use it
    # together with the job and workflow step to give Notifications the same
    # idempotency key on every delivery. Including the workflow step keeps two
    # legitimate actions triggered by one anomaly as separate audit events.
    idempotency_source = [
        "watcher-anomaly-audit",
        heimdall_job_id,
        anomaly_event.type.value,
        anomaly_event.attributes.uuid,
        workflow_step_name,
    ]
    idempotency_key = str(uuid5(NAMESPACE_URL, json.dumps(idempotency_source, sort_keys=True)))
    audit_event_buffer.add(
        WatcherAnomalyAuditEvent(
            heimdall_job_id=heimdall_job_id,
            event_level=AuditEventLevel.OPERATIONAL,
            payload=WatcherAnomalyAuditPayload(
                job_id=str(anomaly_event.job_id),
                attempt_index=attempt_index,
                anomaly_type=anomaly_event.type,
                anomaly_uuid=anomaly_event.attributes.uuid,
                workflow_step=workflow_step_name,
                event_occurred_at=event_occurred_at,
            ),
            event_produced_at=detection_time_ns // 1_000,
            idempotency_key=idempotency_key,
        )
    )

    return True
