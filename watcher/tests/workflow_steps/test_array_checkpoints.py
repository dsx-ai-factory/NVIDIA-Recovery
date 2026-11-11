# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock

from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus
from recovery_libs.schema.watcher.model import Attributes, Event


def checkpoint(index):
    return Event(
        type=EventType.CHECKPOINTSAVE,
        body="saved checkpoint",
        timestamp="1793491200000000000",
        attributes=Attributes(
            uuid="source",
            job_id="4200",
            slurm_job_attempt=0,
            scheduler_metadata={"slurm_array_job_id": "4200", "slurm_array_task_id": str(index)},
        ),
    )


def test_siblings_keep_separate_delivery_dedup_and_audit(rules_engine):
    rules_engine.publisher.publish_message = MagicMock()
    rules_engine.audit_event_buffer = MagicMock()
    rules_engine.send_message_eventbus(
        events=[checkpoint(0), checkpoint(1), checkpoint(0)], routing_suffix="CHECKPOINTSAVE", unique=True, dedup=True
    )
    sent = [
        Event.model_validate_json(call.kwargs["message"])
        for call in rules_engine.publisher.publish_message.call_args_list
    ]
    assert [event.job_id for event in sent] == ["4200_0", "4200_1"]
    assert sent[0].attributes.uuid != sent[1].attributes.uuid
    audits = [call.args[0] for call in rules_engine.audit_event_buffer.add.call_args_list]
    assert [audit.payload.job_id for audit in audits] == ["4200_0", "4200_1"]
    assert [audit.heimdall_job_id for audit in audits] == [
        str(generate_heimdall_job_id(f"4200_{index}", 0)) for index in (0, 1)
    ]


def test_array_only_labels_bypass_dedup_and_audit_for_backend_redelivery(rules_engine, standalone_scheduler_registry):
    standalone_scheduler_registry.return_value = [
        SlurmJobExecutionStatus(
            job_id="4200_0",
            job_attempt_index=0,
            workflow_id="array",
            scheduler_metadata={"scheduler": "slurm", "slurm_array_job_id": "4200", "slurm_array_task_id": "0"},
        )
    ]
    rules_engine.publisher.publish_message = MagicMock()
    rules_engine.audit_event_buffer = MagicMock()
    event = checkpoint(0)
    del event.attributes.scheduler_metadata
    rules_engine.send_message_eventbus(events=[event, event], routing_suffix="CHECKPOINTSAVE", unique=True, dedup=True)
    assert rules_engine.publisher.publish_message.call_count == 2
    rules_engine.audit_event_buffer.add.assert_not_called()


def test_raw_provenance_resolves_before_audit_and_dedup(rules_engine, standalone_scheduler_registry):
    standalone_scheduler_registry.return_value = [
        SlurmJobExecutionStatus(
            job_id="4200_0",
            job_attempt_index=0,
            workflow_id="array",
            scheduler_metadata={
                "scheduler": "slurm",
                "slurm_raw_job_id": "4200",
                "slurm_array_job_id": "4200",
                "slurm_array_task_id": "0",
            },
        )
    ]
    rules_engine.publisher.publish_message = MagicMock()
    rules_engine.audit_event_buffer = MagicMock()
    event = checkpoint(0)
    event.attributes.scheduler_metadata = {"slurm_raw_job_id": "4200"}
    rules_engine.send_message_eventbus(events=[event, event], routing_suffix="CHECKPOINTSAVE", unique=True)
    assert rules_engine.publisher.publish_message.call_count == 1
    sent = Event.model_validate_json(rules_engine.publisher.publish_message.call_args.kwargs["message"])
    assert sent.job_id == "4200_0"
    assert rules_engine.audit_event_buffer.add.call_args.args[0].payload.job_id == "4200_0"


def test_explicit_scheduler_labels_translate_to_metadata_at_ingress(rules_engine):
    rules_engine.publisher.publish_message = MagicMock()
    event = checkpoint(0)
    del event.attributes.scheduler_metadata
    event.attributes.slurm_array_job_id = "4200"
    event.attributes.slurm_array_task_id = "0"
    rules_engine.send_message_eventbus(events=[event], routing_suffix="CHECKPOINTSAVE")
    sent = Event.model_validate_json(rules_engine.publisher.publish_message.call_args.kwargs["message"])
    assert sent.job_id == "4200_0"
    assert sent.attributes.scheduler_metadata["slurm_array_task_id"] == "0"


def test_overlapping_task_attempts_never_gain_an_inferred_attempt(rules_engine, mocker):
    from datetime import UTC, datetime, timedelta

    start = datetime(2026, 11, 1, tzinfo=UTC)
    mocker.patch(
        "watcher.workflow.get_job_attempts_sync",
        return_value=[
            SlurmJobExecutionStatus(
                job_id="4200_0",
                job_attempt_index=index,
                workflow_id="array",
                start_time=start - timedelta(hours=1),
                end_time=start + timedelta(hours=1),
            )
            for index in (1, 0)
        ],
    )
    rules_engine.publisher.publish_message = MagicMock()
    rules_engine.audit_event_buffer = MagicMock()
    event = checkpoint(0)
    del event.attributes.slurm_job_attempt
    rules_engine.send_message_eventbus(events=[event, event], routing_suffix="CHECKPOINTSAVE", unique=True)
    assert rules_engine.publisher.publish_message.call_count == 2
    rules_engine.audit_event_buffer.add.assert_not_called()
    sent = Event.model_validate_json(rules_engine.publisher.publish_message.call_args.kwargs["message"])
    assert (sent.attributes.model_extra or {}).get("slurm_job_attempt") is None
