# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

from job_manager.attribution_request import emit_attribution_request_if_needed
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import JobAnomaly, JobAnomalyHandledBy
from recovery_libs.schema.job_updates_attribution_request.attribution_request import AttributionRequest
from test_utils import create_example_slurm_job_record

ROUTING_KEY = "job.updates.attribution_request"


def _anomaly(anomaly_type: str = EventType.CRASH.value, handled_by=JobAnomalyHandledBy.HEIMDALL) -> JobAnomaly:
    return JobAnomaly(
        anomaly_type=anomaly_type,
        timestamp=datetime.now(UTC),
        uuid=str(uuid.uuid4()),
        handled_by=handled_by,
    )


def _terminal_record(anomalies=None):
    now = datetime.now(UTC)
    record = create_example_slurm_job_record(job_id="job-1", attempt_index=0, state="FAILED", end_time=now)
    record.anomalies = anomalies if anomalies is not None else [_anomaly()]
    record.allocated_node_list = ["node-1", "node-2"]
    return record


def _emit(record, *, storage=None, publisher="unset", enabled=True, straggler_enabled=False):
    storage = storage or MagicMock()
    publisher = MagicMock() if publisher == "unset" else publisher
    result = emit_attribution_request_if_needed(
        record,
        storage=storage,
        publisher=publisher,
        routing_key=ROUTING_KEY,
        enabled=enabled,
        straggler_enabled=straggler_enabled,
    )
    return result, storage, publisher


class TestEmitAttributionRequest(unittest.TestCase):
    def test_disabled_is_noop(self):
        result, storage, publisher = _emit(_terminal_record(), enabled=False)
        self.assertFalse(result)
        storage.claim_attribution_request_emission.assert_not_called()
        publisher.publish_message.assert_not_called()

    def test_no_publisher_is_noop(self):
        result, storage, _ = _emit(_terminal_record(), publisher=None)
        self.assertFalse(result)
        storage.claim_attribution_request_emission.assert_not_called()

    def test_non_terminal_running_still_emits(self):
        # Hang / straggler / watcher anomalies are attributed while the job is still
        # running (or COMPLETING); the emitter must NOT gate on terminal state.
        record = create_example_slurm_job_record(state="RUNNING")
        record.anomalies = [_anomaly()]
        storage = MagicMock()
        storage.claim_attribution_request_emission.return_value = True
        result, storage, publisher = _emit(record, storage=storage)
        self.assertTrue(result)
        storage.claim_attribution_request_emission.assert_called_once()
        publisher.publish_message.assert_called_once()

    def test_no_anomaly_is_noop(self):
        result, storage, _ = _emit(_terminal_record(anomalies=[]))
        self.assertFalse(result)
        storage.claim_attribution_request_emission.assert_not_called()

    def test_straggler_gated_off_is_noop(self):
        record = _terminal_record(anomalies=[_anomaly(anomaly_type=EventType.STRAGGLER.value)])
        result, storage, _ = _emit(record, straggler_enabled=False)
        self.assertFalse(result)
        storage.claim_attribution_request_emission.assert_not_called()

    def test_straggler_enabled_emits(self):
        record = _terminal_record(anomalies=[_anomaly(anomaly_type=EventType.STRAGGLER.value)])
        storage = MagicMock()
        storage.claim_attribution_request_emission.return_value = True
        result, _, publisher = _emit(record, storage=storage, straggler_enabled=True)
        self.assertTrue(result)
        publisher.publish_message.assert_called_once()

    def test_claim_lost_is_noop(self):
        storage = MagicMock()
        storage.claim_attribution_request_emission.return_value = False
        result, _, publisher = _emit(_terminal_record(), storage=storage)
        self.assertFalse(result)
        publisher.publish_message.assert_not_called()

    def test_emits_with_correct_payload(self):
        storage = MagicMock()
        storage.claim_attribution_request_emission.return_value = True
        result, _, publisher = _emit(_terminal_record(), storage=storage)

        self.assertTrue(result)
        publisher.publish_message.assert_called_once()
        call = publisher.publish_message.call_args
        self.assertEqual(call.kwargs["routing_key"], ROUTING_KEY)
        event = AttributionRequest.model_validate_json(call.kwargs["message"])
        self.assertEqual(event.job_id, "job-1")
        self.assertEqual(event.job_attempt_index, 0)
        self.assertEqual(event.allocated_nodes, ["node-1", "node-2"])
        self.assertTrue(event.anomaly_time)

    def test_publish_failure_returns_false(self):
        storage = MagicMock()
        storage.claim_attribution_request_emission.return_value = True
        publisher = MagicMock()
        publisher.publish_message.side_effect = RuntimeError("boom")
        result, _, _ = _emit(_terminal_record(), storage=storage, publisher=publisher)
        self.assertFalse(result)
