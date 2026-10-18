# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid

import pytest
from pydantic import ValidationError
from recovery_libs.schema.watcher.model import Attributes


def test_attributes_with_explicit_field():
    attrs = Attributes(
        job_id="12345",
        log_file_name="/tmp/file1.txt",
        uuid=str(uuid.uuid4()),
    )
    assert attrs.log_file_name == "/tmp/file1.txt"
    # Check that other fields remain None unless passed
    assert attrs.log_file_path is None
    assert attrs.detection_time is None


def test_attributes_with_dot_alias_key():
    data = {
        "job_id": "12345",
        "log.file.name": "/tmp/file2.txt",
        "uuid": str(uuid.uuid4()),
    }
    attrs = Attributes(**data)
    assert attrs.log_file_name == "/tmp/file2.txt"
    assert attrs.log_file_path is None


def test_attributes_using_from_json_loader():
    data = {
        "job_id": "12345",
        "log.file.name": "/tmp/file3.txt",
        "uuid": str(uuid.uuid4()),
    }
    attrs = Attributes.from_json(data)
    assert attrs.log_file_name == "/tmp/file3.txt"
    assert attrs.log_file_path is None


def test_attributes_missing_required_field_job_id():
    data = {
        "log.file.name": "/tmp/file4.txt",
        "uuid": str(uuid.uuid4()),
    }
    with pytest.raises(ValidationError):
        Attributes(**data)
