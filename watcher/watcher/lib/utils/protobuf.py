# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from opentelemetry.proto.common.v1.common_pb2 import AnyValue
from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord, LogsData


def get_protobuf_str(value: str | AnyValue) -> str:
    """Extract text from a string or an OTLP string/integer attribute.

    Args:
        value (str | AnyValue): Input value that can be either a string or an AnyValue protobuf object.

    Returns:
        str: The string value extracted from the input.
    """
    if isinstance(value, AnyValue):
        return str(value.int_value) if value.HasField("int_value") else value.string_value

    return value


def deserialize_protobuf_logs(data: bytes) -> list[LogRecord]:
    """Deserialize OTLP protobuf message into a list of LogRecord objects.

    Args:
        data (bytes): Raw bytes containing the OTLP protobuf message.

    Returns:
        list[LogRecord]: A list of LogRecord objects extracted from the protobuf message.
    """
    logs_data = LogsData.FromString(data)
    records = []
    for resource_logs in logs_data.resource_logs:
        for scope_logs in resource_logs.scope_logs:
            for log_record in scope_logs.log_records:
                # Preserve resource identity as JSON does; record attributes win.
                record_keys = {attr.key for attr in log_record.attributes}
                log_record.attributes.extend(
                    attr for attr in resource_logs.resource.attributes if attr.key not in record_keys
                )
                records.append(log_record)

    return records
