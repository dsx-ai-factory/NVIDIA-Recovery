# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from opentelemetry.proto.common.v1.common_pb2 import AnyValue
from opentelemetry.proto.logs.v1.logs_pb2 import (
    LogRecord,
    LogsData,
    ResourceLogs,
    ScopeLogs,
)
from watcher.lib.utils.protobuf import deserialize_protobuf_logs, get_protobuf_str


def test_get_protobuf_str_with_string():
    """Test get_protobuf_str with a regular string input."""
    input_str = "test string"
    result = get_protobuf_str(input_str)
    assert result == input_str
    assert isinstance(result, str)


def test_get_protobuf_str_with_anyvalue():
    """Test get_protobuf_str with an AnyValue protobuf object."""
    any_value = AnyValue(string_value="test string")
    result = get_protobuf_str(any_value)
    assert result == "test string"
    assert isinstance(result, str)


def test_deserialize_protobuf_logs_empty():
    """Test deserialize_protobuf_logs with empty input."""
    logs_data = LogsData()
    serialized_data = logs_data.SerializeToString()
    result = deserialize_protobuf_logs(serialized_data)
    assert isinstance(result, list)
    assert len(result) == 0


def test_deserialize_protobuf_logs_single_record():
    """Test deserialize_protobuf_logs with a single log record."""
    # Create a log record
    log_record = LogRecord()
    log_record.body.string_value = "test log message"

    # Create the hierarchy
    scope_logs = ScopeLogs()
    scope_logs.log_records.append(log_record)

    resource_logs = ResourceLogs()
    resource_logs.scope_logs.append(scope_logs)

    logs_data = LogsData()
    logs_data.resource_logs.append(resource_logs)

    # Serialize and deserialize
    serialized_data = logs_data.SerializeToString()
    result = deserialize_protobuf_logs(serialized_data)

    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], LogRecord)
    assert result[0].body.string_value == "test log message"


def test_deserialize_protobuf_logs_multiple_records():
    """Test deserialize_protobuf_logs with multiple log records."""
    # Create multiple log records
    log_records = [LogRecord(body=AnyValue(string_value=f"test log message {i}")) for i in range(3)]

    # Create the hierarchy
    scope_logs = ScopeLogs()
    for record in log_records:
        scope_logs.log_records.append(record)

    resource_logs = ResourceLogs()
    resource_logs.scope_logs.append(scope_logs)

    logs_data = LogsData()
    logs_data.resource_logs.append(resource_logs)

    # Serialize and deserialize
    serialized_data = logs_data.SerializeToString()
    result = deserialize_protobuf_logs(serialized_data)

    assert isinstance(result, list)
    assert len(result) == 3
    for i, record in enumerate(result):
        assert isinstance(record, LogRecord)
        assert record.body.string_value == f"test log message {i}"


def test_deserialize_protobuf_logs_multiple_scopes():
    """Test deserialize_protobuf_logs with multiple scope logs."""
    # Create log records for different scopes
    scope1_records = [LogRecord(body=AnyValue(string_value=f"scope1 message {i}")) for i in range(2)]
    scope2_records = [LogRecord(body=AnyValue(string_value=f"scope2 message {i}")) for i in range(2)]

    # Create the hierarchy
    scope_logs1 = ScopeLogs()
    for record in scope1_records:
        scope_logs1.log_records.append(record)

    scope_logs2 = ScopeLogs()
    for record in scope2_records:
        scope_logs2.log_records.append(record)

    resource_logs = ResourceLogs()
    resource_logs.scope_logs.extend([scope_logs1, scope_logs2])

    logs_data = LogsData()
    logs_data.resource_logs.append(resource_logs)

    # Serialize and deserialize
    serialized_data = logs_data.SerializeToString()
    result = deserialize_protobuf_logs(serialized_data)

    assert isinstance(result, list)
    assert len(result) == 4
    assert result[0].body.string_value == "scope1 message 0"
    assert result[1].body.string_value == "scope1 message 1"
    assert result[2].body.string_value == "scope2 message 0"
    assert result[3].body.string_value == "scope2 message 1"
