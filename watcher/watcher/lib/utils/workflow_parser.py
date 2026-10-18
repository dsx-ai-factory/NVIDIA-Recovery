# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import os
from typing import Any

from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord as OTLPLogRecord
from recovery_libs.schema.watcher.model import Attributes

from watcher.lib.utils.data import generate_uuid7, is_json
from watcher.lib.utils.logging import logger
from watcher.lib.utils.protobuf import get_protobuf_str
from watcher.models.workflow import LogRecord as WorkflowLogRecord
from watcher.models.workflow import WorkflowItem


def parse_records_from_response(response: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Parse records from the Loki query response.

    Args:
        response (List[Dict[str, Any]]): The response object.

    Returns:
        list: A list of records parsed from the response,
                with each record being a dictionary
                containing the event data and timestamp.
    """
    records = []

    for entry in response:
        for event in entry["values"]:
            ts, data_text = event
            if is_json(data_text):
                # Loki API returns rich JSON object
                data = json.loads(data_text)
            else:
                # Loki API returns plain test
                data = {"body": data_text}
                data["attributes"] = json.loads(entry["stream"]) if is_json(entry["stream"]) else entry["stream"]

            data["timestamp"] = ts
            # quick hack to convert "filename" to "log.file.name" and "log.file.path"
            if "attributes" in data and "filename" in data["attributes"] and "log.file.name" not in data["attributes"]:
                data["attributes"]["log.file.path"] = data["attributes"]["filename"]
                data["attributes"]["log.file.name"] = os.path.basename(data["attributes"]["filename"])
                data["attributes"]["uuid"] = generate_uuid7()
                del data["attributes"]["filename"]

            if _does_record_contain_jobid(data) and _does_record_contain_filename(data):
                records.append(data)
            else:
                logger.warning(
                    f"Event does not contain one of the following attrs [job_id, filename], discarding it: {data}"
                )

    return records


def _does_record_contain_jobid(record: dict[str, Any]) -> bool:
    """
    Checks if record from the Loki query response contains [job_id].

    Args:
        record (Dict[str, Any]): Loki record object.

    Returns:
        bool: does record contain [job_id]
    """
    return "attributes" in record and "job_id" in record["attributes"]


def _does_record_contain_filename(record: dict[str, Any]) -> bool:
    """
    Checks if record from the Loki query response contains [filename].

    Args:
        record (Dict[str, Any]): Loki record object.

    Returns:
        bool: does record contain [filename]
    """
    filename_attrs = ("filename", "log.file.name", "log_file_name")
    return "attributes" in record and any(attr in record["attributes"] for attr in filename_attrs)


def parse_otlp_json_to_workflow_items(otlp_json: dict[str, Any]) -> list[WorkflowItem]:
    """
    Parse OTLP JSON data into a list of WorkflowItem objects.

    Args:
        otlp_json (Dict[str, Any]): The OTLP JSON data containing resource logs.

    Returns:
        List[WorkflowItem]: A list of WorkflowItem objects parsed from the OTLP data.

    Note:
        This function expects the OTLP JSON to follow the OpenTelemetry Protocol format
        with resourceLogs containing scopeLogs and logRecords.
        The job_id field is mandatory and must be present in either resource or record attributes.
        The uuid field will be generated using generate_uuid7() and will override any existing uuid.
        If log.file.name is empty, it will be set to log.file.path_resolved if available.
    """
    workflow_items = []

    for resource_log in otlp_json.get("resourceLogs", []):
        # Extract resource attributes
        resource_attrs = {}
        for attr in resource_log.get("resource", {}).get("attributes", []):
            key = attr.get("key")
            raw_value = attr.get("value", {})
            value = raw_value.get("stringValue", raw_value.get("intValue"))
            if key and (value is not None or key == "slurm_job_attempt"):
                resource_attrs[key] = str(value) if value is not None else ""

        for scope_log in resource_log.get("scopeLogs", []):
            for log_record in scope_log.get("logRecords", []):
                # Extract log record attributes
                record_attrs = {}
                for attr in log_record.get("attributes", []):
                    key = attr.get("key")
                    raw_value = attr.get("value", {})
                    value = raw_value.get("stringValue", raw_value.get("intValue"))
                    if key and (value is not None or key == "slurm_job_attempt"):
                        record_attrs[key] = str(value) if value is not None else ""

                # Merge resource attributes with record attributes - record attrs take precedence
                all_attrs = {**resource_attrs, **record_attrs}

                # Ensure job_id is present - skip records without job_id
                if "job_id" not in all_attrs:
                    logger.warning(
                        f"Record skipped: missing required job_id field. Resource attrs: {resource_attrs}, Record attrs: {record_attrs}"
                    )
                    continue

                # If log.file.name is empty and log.file.path_resolved is available, use it
                if not all_attrs.get("log.file.name") and all_attrs.get("log.file.path_resolved"):
                    all_attrs["log.file.name"] = all_attrs["log.file.path_resolved"]

                # Generate new uuid
                all_attrs["uuid"] = generate_uuid7()
                record = WorkflowLogRecord(
                    body=log_record.get("body", {}).get("stringValue"),
                    # Collection time cannot identify a replayed record's attempt.
                    timestamp=str(log_record["timeUnixNano"])
                    if log_record.get("timeUnixNano") not in (None, 0, "0")
                    else None,
                    attributes=Attributes(**all_attrs),
                )
                workflow_item = WorkflowItem(record=record)
                workflow_items.append(workflow_item)

    return workflow_items


def parse_log_record_to_workflow_item(log_record: OTLPLogRecord) -> WorkflowItem:
    """
    Convert a protobuf LogRecord into WorkflowItem objects.

    Args:
        log_record (LogRecord): The protobuf LogRecord to convert.

    Returns:
        WorkflowItem: WorkflowItem object created from the LogRecord.
    """
    # Extract attributes and bodyfrom the log record
    attributes = {get_protobuf_str(attr.key): get_protobuf_str(attr.value) for attr in log_record.attributes}
    body = get_protobuf_str(log_record.body)

    attributes["uuid"] = generate_uuid7()
    record = WorkflowLogRecord(
        body=body,
        timestamp=str(log_record.time_unix_nano) if log_record.time_unix_nano else None,
        attributes=attributes,
    )
    return WorkflowItem(record=record, step_name=None, step_value=None)


def first_events_by_job_id_and_logfile_name(workflow_items: list[WorkflowItem]) -> list[WorkflowItem]:
    """
    Given a list of WorkflowItem objects, return a new list that
    contains the first occurrence per explicit job attempt and log filename.
    Unlabeled records are retained until downstream attempt resolution.

    Args:
        workflow_items (List[WorkflowItem]): list of WorkflowItem objects.

    Returns:
        List[WorkflowItem]: list of WorkflowItem objects.
    """
    seen_combinations = set()
    results = []

    for item in workflow_items:
        job_id = item.record.attributes.job_id  # type: ignore[union-attr]
        log_file_name = item.record.attributes.log_file_name  # type: ignore[union-attr]

        attempt = (item.record.attributes.model_extra or {}).get("slurm_job_attempt")  # type: ignore[union-attr]
        # Without an attempt label, two records in this batch may belong to
        # different retries of a shared append-only file. Resolve downstream.
        if attempt is None:
            results.append(item)
            continue
        job_unique_identifier = (job_id, log_file_name, str(attempt))

        if job_unique_identifier not in seen_combinations:
            results.append(item)
            seen_combinations.add(job_unique_identifier)

    return results
