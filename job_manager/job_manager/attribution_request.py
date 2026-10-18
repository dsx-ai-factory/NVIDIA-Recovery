# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Emit the AttributionRequest event to ADI.

The ``AttributionRequest`` event triggers ADI's bad-node analysis. Job Manager produces it
behind a flag, so exactly one producer is active at a time.

The emitter fires once per job attempt that carries an ADI-processable anomaly. It
does not require the attempt to be terminal: hang, straggler and watcher-detected
anomalies are attributed while the job is still running (and COMPLETING), regardless
of final state. Emission is de-duped via an atomic claim in storage; only the caller
that wins the claim publishes. The gate/flag/routing are passed in so this stays pure
and testable; the wiring reads them from config.
"""

from collections.abc import Callable
from datetime import UTC, datetime

import pika
from opentelemetry import trace
from opentelemetry.context import Context
from recovery_libs.event_bus.event_bus_utils import RabbitMQPublisher
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_updates_attribution_request.attribution_request import AttributionRequest
from recovery_libs.storage.dataclass import SlurmJobExecutionRecord
from recovery_libs.tracing.recovery import recovery_headers
from recovery_libs.tracing.traces import inject_trace_context, populate_headers_with_span_context

from job_manager.logger import logger
from job_manager.metrics import (
    ATTRIBUTION_REQUEST_EMITTED_COUNTER,
    ATTRIBUTION_REQUEST_PUBLISH_FAILED_COUNTER,
    ATTRIBUTION_REQUEST_SKIPPED_COUNTER,
)


def _has_adi_processable_anomaly(record: SlurmJobExecutionRecord, straggler_enabled: bool) -> bool:
    """Whether the record carries at least one anomaly ADI would actually process.

    ADI does not filter by anomaly type. STRAGGLER only counts when straggler
    emission is enabled.
    """
    if not record.anomalies:
        return False
    for anomaly in record.anomalies:
        if anomaly.anomaly_type == EventType.STRAGGLER.value and not straggler_enabled:
            continue
        return True
    return False


def emit_attribution_request_if_needed(
    record: SlurmJobExecutionRecord,
    *,
    storage,
    publisher: RabbitMQPublisher | None,
    routing_key: str,
    enabled: bool,
    straggler_enabled: bool,
    recovery_context_provider: Callable[[], Context | None] | None = None,
) -> bool:
    """Emit a AttributionRequest event to ADI for an anomalous attempt, exactly once.

    The attempt does not need to be terminal — ADI attribution runs on running,
    COMPLETING and final jobs alike (hang / straggler / watcher anomalies). Returns
    True if this call emitted the event; False otherwise (disabled, no processable
    anomaly, claim lost, or publish failed). The claim is taken before publishing so a
    retry cannot double-emit; a publish failure after a won claim is surfaced via a
    dedicated metric rather than retried, to keep at-most-once semantics.
    """
    if not enabled or publisher is None:
        return False
    if not _has_adi_processable_anomaly(record, straggler_enabled):
        ATTRIBUTION_REQUEST_SKIPPED_COUNTER.labels(reason="no_processable_anomaly").inc()
        logger.info(
            f"AttributionRequest skipped for job {record.job_id} attempt {record.job_attempt_index}: "
            "no ADI-processable anomaly."
        )
        return False
    if not storage.claim_attribution_request_emission(record.job_id, record.job_attempt_index):
        ATTRIBUTION_REQUEST_SKIPPED_COUNTER.labels(reason="already_claimed").inc()
        logger.info(
            f"AttributionRequest skipped for job {record.job_id} attempt {record.job_attempt_index}: emission already claimed."
        )
        return False

    # Attribution scope-end timestamp: the attempt's end time, or now() for a
    # still-running (hang/straggler) anomaly. ADI consumes it as the anomaly's
    # end_time / FACT scope.end_time.
    anomaly_time = (record.end_time or datetime.now(tz=UTC)).isoformat()
    event = AttributionRequest(
        job_id=record.job_id,
        job_attempt_index=record.job_attempt_index,
        anomaly_time=anomaly_time,
        allocated_nodes=record.allocated_node_list,
    )
    recovery_context = None
    if recovery_context_provider is not None:
        try:
            recovery_context = recovery_context_provider()
        except Exception as error:
            # Recovery context is observability data. Failure to obtain it must
            # not suppress an attribution request that already won its claim.
            logger.exception(
                f"Failed to obtain recovery context for AttributionRequest for job "
                f"{record.job_id} attempt {record.job_attempt_index}",
                extra={"job_id": record.job_id, "error": str(error)},
            )
    headers = (
        inject_trace_context({}, context=recovery_context)
        if recovery_context is not None
        else populate_headers_with_span_context(trace.get_current_span())
    )
    headers.update(recovery_headers(recovery_context))
    try:
        publisher.publish_message(
            message=event.model_dump_json(),
            routing_key=routing_key,
            properties=pika.BasicProperties(headers=headers) if headers else None,
        )
    except Exception as error:
        # The claim was already won, so a publish failure here would silently drop the
        # trigger. Surface it via a metric instead of retrying (retry could double-emit).
        ATTRIBUTION_REQUEST_PUBLISH_FAILED_COUNTER.inc()
        logger.exception(
            f"Failed to publish AttributionRequest for job {record.job_id} attempt {record.job_attempt_index}",
            extra={"job_id": record.job_id, "error": str(error)},
        )
        return False

    ATTRIBUTION_REQUEST_EMITTED_COUNTER.inc()
    logger.info(
        f"Emitted AttributionRequest to ADI for job {record.job_id} attempt {record.job_attempt_index} (state={record.state})."
    )
    return True
