# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from datetime import UTC, datetime

import pytest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord, LogsData, ResourceLogs, ScopeLogs
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus
from recovery_libs.schema.watcher.model import Event
from watcher.lib.utils.job import try_get_job_identity_for_event
from watcher.lib.utils.protobuf import deserialize_protobuf_logs
from watcher.lib.utils.workflow_parser import (
    _does_record_contain_filename,
    _does_record_contain_jobid,
    first_events_by_job_id_and_logfile_name,
    parse_log_record_to_workflow_item,
    parse_otlp_json_to_workflow_items,
    parse_records_from_response,
)


def test_first_events_by_job_id_no_items():
    workflow_items = []
    result = first_events_by_job_id_and_logfile_name(workflow_items)
    assert result == [], "Expected empty list when input is empty."


def test_first_events_preserve_unlabeled_shared_log_retries(crash_events_workflow_items_format):
    assert (
        first_events_by_job_id_and_logfile_name(crash_events_workflow_items_format)
        == crash_events_workflow_items_format
    )


def test_first_events_dedup_only_within_explicit_attempt(crash_events_workflow_items_format):
    for item in crash_events_workflow_items_format:
        item.record.attributes.slurm_job_attempt = 0
    result = first_events_by_job_id_and_logfile_name(crash_events_workflow_items_format)
    assert len(result) == 3
    crash_events_workflow_items_format[2].record.attributes.slurm_job_attempt = 1
    assert len(first_events_by_job_id_and_logfile_name(crash_events_workflow_items_format)) == 4


def test_parse_records_from_response_json_data():
    """Test parsing records with JSON data from Loki response."""
    # Sample Loki response with JSON data
    response = [
        {
            "values": [
                [
                    "1234567890",
                    json.dumps(
                        {"body": "test message", "attributes": {"job_id": "123", "filename": "/path/to/file.log"}}
                    ),
                ]
            ],
            "stream": json.dumps({"job": "test-job"}),
        }
    ]

    result = parse_records_from_response(response)

    assert len(result) == 1
    record = result[0]
    assert record["body"] == "test message"
    assert record["timestamp"] == "1234567890"
    assert record["attributes"]["job_id"] == "123"
    assert record["attributes"]["log.file.path"] == "/path/to/file.log"
    assert record["attributes"]["log.file.name"] == "file.log"
    assert "uuid" in record["attributes"]
    assert "filename" not in record["attributes"]


def test_parse_records_from_response_plain_text():
    """Test parsing records with plain text data from Loki response."""
    # Sample Loki response with plain text data
    response = [
        {
            "values": [["1234567890", "test message"]],
            "stream": json.dumps({"job_id": "123", "filename": "/path/to/file.log"}),
        }
    ]

    result = parse_records_from_response(response)

    assert len(result) == 1
    record = result[0]
    assert record["body"] == "test message"
    assert record["timestamp"] == "1234567890"
    assert record["attributes"]["job_id"] == "123"
    assert record["attributes"]["log.file.path"] == "/path/to/file.log"
    assert record["attributes"]["log.file.name"] == "file.log"
    assert "uuid" in record["attributes"]


def test_parse_records_from_response_missing_required_fields(caplog):
    """Test parsing records that are missing required fields."""
    # Sample Loki response missing job_id
    response = [
        {
            "values": [
                ["1234567890", json.dumps({"body": "test message", "attributes": {"filename": "/path/to/file.log"}})]
            ],
            "stream": "{}",
        }
    ]

    result = parse_records_from_response(response)
    assert len(result) == 0
    assert "Event does not contain one of the following attrs [job_id, filename]" in caplog.text

    # Sample Loki response missing filename
    response = [
        {
            "values": [["1234567890", json.dumps({"body": "test message", "attributes": {"job_id": "123"}})]],
            "stream": "{}",
        }
    ]

    result = parse_records_from_response(response)
    assert len(result) == 0
    assert "Event does not contain one of the following attrs [job_id, filename]" in caplog.text


def test_parse_records_from_response_multiple_records():
    """Test parsing multiple records from Loki response."""
    response = [
        {
            "values": [
                [
                    "1234567890",
                    json.dumps({"body": "message1", "attributes": {"job_id": "123", "filename": "/path/to/file1.log"}}),
                ],
                [
                    "1234567891",
                    json.dumps({"body": "message2", "attributes": {"job_id": "123", "filename": "/path/to/file2.log"}}),
                ],
            ],
            "stream": json.dumps({"job": "test-job"}),
        }
    ]

    result = parse_records_from_response(response)

    assert len(result) == 2
    assert result[0]["body"] == "message1"
    assert result[1]["body"] == "message2"
    assert result[0]["attributes"]["log.file.name"] == "file1.log"
    assert result[1]["attributes"]["log.file.name"] == "file2.log"


def test_does_record_contain_jobid():
    """Test checking if record contains job_id."""
    # Record with job_id
    record_with_jobid = {"attributes": {"job_id": "123"}}
    assert _does_record_contain_jobid(record_with_jobid) is True

    # Record without job_id
    record_without_jobid = {"attributes": {"other_field": "value"}}
    assert _does_record_contain_jobid(record_without_jobid) is False

    # Record without attributes
    record_without_attributes = {"body": "test"}
    assert _does_record_contain_jobid(record_without_attributes) is False


def test_does_record_contain_filename():
    """Test checking if record contains filename."""
    # Test with filename
    record_with_filename = {"attributes": {"filename": "/path/to/file.log"}}
    assert _does_record_contain_filename(record_with_filename) is True

    # Test with log.file.name
    record_with_log_filename = {"attributes": {"log.file.name": "file.log"}}
    assert _does_record_contain_filename(record_with_log_filename) is True

    # Test with log_file_name
    record_with_log_file_name = {"attributes": {"log_file_name": "file.log"}}
    assert _does_record_contain_filename(record_with_log_file_name) is True

    # Record without any filename attributes
    record_without_filename = {"attributes": {"other_field": "value"}}
    assert _does_record_contain_filename(record_without_filename) is False

    # Record without attributes
    record_without_attributes = {"body": "test"}
    assert _does_record_contain_filename(record_without_attributes) is False


def test_parse_otlp_json_to_workflow_items_basic():
    """Test basic parsing of OTLP JSON into WorkflowItem objects."""
    otlp_json = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "job_id", "value": {"stringValue": "123"}},
                        {"key": "service.name", "value": {"stringValue": "test-service"}},
                    ]
                },
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": "1234567890",
                                "body": {"stringValue": "test log message"},
                                "attributes": [
                                    {"key": "log.file.name", "value": {"stringValue": "test.log"}},
                                    {"key": "log.file.path", "value": {"stringValue": "/path/to/test.log"}},
                                ],
                            }
                        ]
                    }
                ],
            }
        ]
    }

    result = parse_otlp_json_to_workflow_items(otlp_json)

    assert len(result) == 1
    workflow_item = result[0]
    assert workflow_item.record.body == "test log message"
    assert workflow_item.record.timestamp == "1234567890"
    assert workflow_item.record.attributes.job_id == "123"
    assert workflow_item.record.attributes.log_file_name == "test.log"
    assert workflow_item.record.attributes.log_file_path == "/path/to/test.log"
    assert workflow_item.record.attributes.uuid is not None
    assert workflow_item.record.attributes.uuid != "test-uuid-123"  # Should be a new UUID


def test_parse_otlp_json_to_workflow_items_record_attrs_precedence():
    """Test that record attributes take precedence over resource attributes."""
    otlp_json = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "job_id", "value": {"stringValue": "123"}},
                        {"key": "service.name", "value": {"stringValue": "resource-service"}},
                    ]
                },
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": "1234567890",
                                "body": {"stringValue": "test log message"},
                                "attributes": [
                                    {"key": "service.name", "value": {"stringValue": "record-service"}},
                                    {"key": "log.file.name", "value": {"stringValue": "test.log"}},
                                ],
                            }
                        ]
                    }
                ],
            }
        ]
    }

    result = parse_otlp_json_to_workflow_items(otlp_json)

    assert len(result) == 1
    workflow_item = result[0]
    assert workflow_item.record.attributes.job_id == "123"  # from resource
    assert workflow_item.record.attributes.uuid is not None  # should be generated


def test_parse_otlp_json_to_workflow_items_missing_job_id(caplog):
    """Test that records without job_id are skipped and logged."""
    otlp_json = {
        "resourceLogs": [
            {
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "test-service"}}]},
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": "1234567890",
                                "body": {"stringValue": "test log message"},
                                "attributes": [{"key": "log.file.name", "value": {"stringValue": "test.log"}}],
                            }
                        ]
                    }
                ],
            }
        ]
    }

    with caplog.at_level(logging.WARNING):
        result = parse_otlp_json_to_workflow_items(otlp_json)

    assert len(result) == 0
    assert "Record skipped: missing required job_id field" in caplog.text


def test_parse_otlp_json_to_workflow_items_multiple_records():
    """Test parsing multiple log records from OTLP JSON."""
    otlp_json = {
        "resourceLogs": [
            {
                "resource": {"attributes": [{"key": "job_id", "value": {"stringValue": "123"}}]},
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": "1234567890",
                                "body": {"stringValue": "message 1"},
                                "attributes": [{"key": "log.file.name", "value": {"stringValue": "test1.log"}}],
                            },
                            {
                                "timeUnixNano": "1234567891",
                                "body": {"stringValue": "message 2"},
                                "attributes": [{"key": "log.file.name", "value": {"stringValue": "test2.log"}}],
                            },
                        ]
                    }
                ],
            }
        ]
    }

    result = parse_otlp_json_to_workflow_items(otlp_json)

    assert len(result) == 2
    assert result[0].record.body == "message 1"
    assert result[1].record.body == "message 2"
    assert result[0].record.attributes.log_file_name == "test1.log"
    assert result[1].record.attributes.log_file_name == "test2.log"
    assert result[0].record.attributes.uuid is not None
    assert result[1].record.attributes.uuid is not None
    assert result[0].record.attributes.uuid != result[1].record.attributes.uuid  # Different UUIDs for different records


def test_parse_otlp_json_to_workflow_items_empty():
    """Test parsing empty OTLP JSON."""
    otlp_json = {"resourceLogs": []}
    result = parse_otlp_json_to_workflow_items(otlp_json)
    assert len(result) == 0

    otlp_json = {}
    result = parse_otlp_json_to_workflow_items(otlp_json)
    assert len(result) == 0


def test_parse_otlp_json_to_workflow_items_missing_fields():
    """Test parsing OTLP JSON with missing fields."""
    otlp_json = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "job_id", "value": {"stringValue": "123"}},
                        {"key": "uuid", "value": {"stringValue": "test-uuid"}},
                    ]
                },
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": "1234567890",
                                # Missing body
                                "attributes": [{"key": "log.file.name", "value": {"stringValue": "test.log"}}],
                            }
                        ]
                    }
                ],
            }
        ]
    }

    result = parse_otlp_json_to_workflow_items(otlp_json)

    assert len(result) == 1
    workflow_item = result[0]
    assert workflow_item.record.body is None
    assert workflow_item.record.timestamp == "1234567890"
    assert workflow_item.record.attributes.job_id == "123"
    assert workflow_item.record.attributes.log_file_name == "test.log"
    assert workflow_item.record.attributes.uuid is not None  # should be generated


def test_parse_otlp_json_to_workflow_items_invalid_values():
    """Test parsing OTLP JSON with invalid or missing values."""
    otlp_json = {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "job_id", "value": {"stringValue": "123"}},  # Required field, must be valid
                        {"key": "uuid", "value": {"stringValue": "test-uuid"}},  # Required field, must be valid
                        {"key": "service.name", "value": {}},  # Optional field, can be invalid
                    ]
                },
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": "1234567890",
                                "body": {"stringValue": "test log message"},
                                "attributes": [
                                    {"key": "log.file.name"},  # Missing value
                                    {"key": "log.file.path", "value": {}},  # Missing stringValue
                                ],
                            }
                        ]
                    }
                ],
            }
        ]
    }

    result = parse_otlp_json_to_workflow_items(otlp_json)

    assert len(result) == 1
    workflow_item = result[0]
    assert workflow_item.record.body == "test log message"
    assert workflow_item.record.timestamp == "1234567890"
    assert workflow_item.record.attributes.job_id == "123"  # Required field should be present
    assert workflow_item.record.attributes.uuid is not None  # should be generated
    assert workflow_item.record.attributes.log_file_name is None
    assert workflow_item.record.attributes.log_file_path is None


def test_parse_log_record_to_workflow_item_basic():
    """Test basic parsing of LogRecord into WorkflowItem object."""
    # Create a LogRecord with basic attributes
    log_record = LogRecord(
        time_unix_nano=1234567890,
        body=AnyValue(string_value="test log message"),
        attributes=[
            KeyValue(key="job_id", value=AnyValue(string_value="123")),
            KeyValue(key="log.file.name", value=AnyValue(string_value="test.log")),
            KeyValue(key="log.file.path", value=AnyValue(string_value="/path/to/test.log")),
        ],
    )

    workflow_item = parse_log_record_to_workflow_item(log_record)

    assert workflow_item.record.body == "test log message"
    assert workflow_item.record.timestamp == "1234567890"
    assert workflow_item.record.attributes.job_id == "123"
    assert workflow_item.record.attributes.log_file_name == "test.log"
    assert workflow_item.record.attributes.log_file_path == "/path/to/test.log"
    assert workflow_item.record.attributes.uuid is not None
    assert workflow_item.step_name is None
    assert workflow_item.step_value is None


def test_parse_log_record_to_workflow_item_empty_body():
    """Test parsing LogRecord with empty body."""
    log_record = LogRecord(
        time_unix_nano=1234567890,
        body=AnyValue(string_value=""),
        attributes=[KeyValue(key="job_id", value=AnyValue(string_value="123"))],
    )

    workflow_item = parse_log_record_to_workflow_item(log_record)

    assert workflow_item.record.body == ""
    assert workflow_item.record.timestamp == "1234567890"
    assert workflow_item.record.attributes.job_id == "123"
    assert workflow_item.record.attributes.uuid is not None
    assert workflow_item.step_name is None
    assert workflow_item.step_value is None


def test_parse_log_record_to_workflow_item_multiple_attributes():
    """Test parsing LogRecord with multiple attributes."""
    log_record = LogRecord(
        time_unix_nano=1234567890,
        body=AnyValue(string_value="test log message"),
        attributes=[
            KeyValue(key="job_id", value=AnyValue(string_value="123")),
            KeyValue(key="service.name", value=AnyValue(string_value="test-service")),
            KeyValue(key="log.level", value=AnyValue(string_value="INFO")),
            KeyValue(key="custom.field", value=AnyValue(string_value="custom-value")),
        ],
    )

    workflow_item = parse_log_record_to_workflow_item(log_record)

    assert workflow_item.record.body == "test log message"
    assert workflow_item.record.timestamp == "1234567890"
    assert workflow_item.record.attributes.job_id == "123"
    assert workflow_item.record.attributes.uuid is not None
    assert workflow_item.step_name is None
    assert workflow_item.step_value is None


def test_parse_log_record_to_workflow_item_uuid_uniqueness():
    """Test that different log records get different UUIDs."""
    log_record1 = LogRecord(
        time_unix_nano=1234567890,
        body=AnyValue(string_value="test log message 1"),
        attributes=[KeyValue(key="job_id", value=AnyValue(string_value="123"))],
    )

    log_record2 = LogRecord(
        time_unix_nano=1234567891,
        body=AnyValue(string_value="test log message 2"),
        attributes=[KeyValue(key="job_id", value=AnyValue(string_value="123"))],
    )

    workflow_item1 = parse_log_record_to_workflow_item(log_record1)
    workflow_item2 = parse_log_record_to_workflow_item(log_record2)

    assert workflow_item1.record.attributes.uuid != workflow_item2.record.attributes.uuid


@pytest.mark.parametrize("source", ["1700000000000000000", None, "0"])
def test_otlp_json_and_protobuf_use_source_time_for_attempt_identity(source):
    """A fresh observed time cannot promote old or missing source time to a retry."""
    attrs = [
        {"key": "job_id", "value": {"stringValue": "123"}},
        {"key": "log.file.name", "value": {"stringValue": "shared.log"}},
    ]
    record = {
        "body": {"stringValue": "RuntimeError: old failure"},
        "attributes": attrs,
        "observedTimeUnixNano": "1700000060000000000",
    }
    if source is not None:
        record["timeUnixNano"] = source
    json_item = parse_otlp_json_to_workflow_items({"resourceLogs": [{"scopeLogs": [{"logRecords": [record]}]}]})[0]
    proto_item = parse_log_record_to_workflow_item(
        LogRecord(
            time_unix_nano=int(source or 0),
            observed_time_unix_nano=1700000060000000000,
            body=AnyValue(string_value="RuntimeError: old failure"),
            attributes=[
                KeyValue(key="job_id", value=AnyValue(string_value="123")),
                KeyValue(key="log.file.name", value=AnyValue(string_value="shared.log")),
            ],
        )
    )
    expected = source if source not in (None, "0") else None
    latest = SlurmJobExecutionStatus(
        job_id="123",
        job_attempt_index=1,
        workflow_id="test",
        start_time=datetime.fromtimestamp(1700000030, UTC),
    )
    for item in (json_item, proto_item):
        assert item.record.timestamp == expected
        event = Event(type=EventType.CRASH, **item.record.model_dump())
        assert try_get_job_identity_for_event(event, match_time_with_job_attempt=latest) is None


@pytest.mark.parametrize("attempt", [0, 5])
@pytest.mark.parametrize("on_resource", [False, True])
def test_otlp_integer_attempt_identity_survives_both_formats(attempt, on_resource):
    """An authoritative integer attempt must not disappear before acceptance."""
    json_attrs = [
        {"key": "job_id", "value": {"stringValue": "123"}},
        {"key": "log.file.name", "value": {"stringValue": "shared.log"}},
    ]
    json_identity = [{"key": "slurm_job_attempt", "value": {"intValue": str(attempt)}}]
    record = {
        "timeUnixNano": "1700000060000000000",
        "body": {"stringValue": "RuntimeError: failed"},
        "attributes": json_attrs + ([] if on_resource else json_identity),
    }
    json_item = parse_otlp_json_to_workflow_items(
        {
            "resourceLogs": [
                {
                    "resource": {"attributes": json_identity if on_resource else []},
                    "scopeLogs": [{"logRecords": [record]}],
                }
            ]
        }
    )[0]
    proto_attrs = [
        KeyValue(key="job_id", value=AnyValue(string_value="123")),
        KeyValue(key="log.file.name", value=AnyValue(string_value="shared.log")),
    ]
    proto_identity = [KeyValue(key="slurm_job_attempt", value=AnyValue(int_value=attempt))]
    log_record = LogRecord(
        time_unix_nano=1700000060000000000,
        body=AnyValue(string_value="RuntimeError: failed"),
        attributes=proto_attrs + ([] if on_resource else proto_identity),
    )
    data = LogsData(
        resource_logs=[
            ResourceLogs(
                resource=Resource(attributes=proto_identity if on_resource else []),
                scope_logs=[ScopeLogs(log_records=[log_record])],
            )
        ]
    )
    proto_item = parse_log_record_to_workflow_item(deserialize_protobuf_logs(data.SerializeToString())[0])
    latest = SlurmJobExecutionStatus(
        job_id="123",
        job_attempt_index=1,
        workflow_id="test",
        start_time=datetime.fromtimestamp(1700000000, UTC),
    )
    for item in (json_item, proto_item):
        event = Event(type=EventType.CRASH, **item.record.model_dump())
        assert event.attributes.model_extra["slurm_job_attempt"] == str(attempt)
        assert try_get_job_identity_for_event(event, match_time_with_job_attempt=latest).attempt_index == attempt


def test_otlp_record_attempt_overrides_resource_attempt():
    resource_identity = KeyValue(key="slurm_job_attempt", value=AnyValue(int_value=0))
    record_identity = KeyValue(key="slurm_job_attempt", value=AnyValue(int_value=5))
    record = LogRecord(attributes=[record_identity, KeyValue(key="job_id", value=AnyValue(string_value="123"))])
    data = LogsData(
        resource_logs=[
            ResourceLogs(
                resource=Resource(attributes=[resource_identity]), scope_logs=[ScopeLogs(log_records=[record])]
            )
        ]
    )
    item = parse_log_record_to_workflow_item(deserialize_protobuf_logs(data.SerializeToString())[0])
    assert item.record.attributes.model_extra["slurm_job_attempt"] == "5"
