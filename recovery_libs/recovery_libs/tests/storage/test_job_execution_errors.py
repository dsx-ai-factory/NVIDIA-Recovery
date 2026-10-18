# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from recovery_libs.storage.job_execution_errors import JobExecutionError


def test_create_error():
    """Test creating an error record with the create() method"""
    error = JobExecutionError.create(
        code=123,
        message="Test error message",
        description="Test description",
        signature="test_signature",
    )

    assert error.code == 123
    assert error.message == "Test error message"
    assert error.description == "Test description"
    assert error.signature == "test_signature"


def test_error_to_json():
    """Test converting an error record to JSON"""
    error = JobExecutionError.create(
        code=456,
        message="JSON test message",
        description="JSON test description",
        signature="json_signature",
    )

    json_data = error.to_json()
    assert json_data["code"] == 456
    assert json_data["message"] == "JSON test message"
    assert json_data["description"] == "JSON test description"
    assert json_data["signature"] == "json_signature"


def test_error_from_json():
    """Test creating an error record from JSON data"""
    json_data = {
        "code": 789,
        "message": "From JSON message",
        "description": "From JSON description",
        "signature": "from_json_signature",
    }

    error = JobExecutionError.from_json(json_data)
    assert error.code == 789
    assert error.message == "From JSON message"
    assert error.description == "From JSON description"
    assert error.signature == "from_json_signature"


def test_error_json_serialization():
    """Test that an error can be serialized to JSON and back

    This test verifies that:
    1. An error can be converted to JSON using to_json()
    2. The JSON data can be converted back to an error using from_json()
    3. The resulting error matches the original in all fields
    """
    # Create original error with various fields set
    original_error = JobExecutionError.create(
        code=42,
        message="Original error message",
        description="Original error description",
        signature="original_signature",
    )

    # Convert to JSON and back
    json_data = original_error.to_json()
    reconstructed_error = JobExecutionError.from_json(json_data)

    # Verify all fields match
    assert reconstructed_error.code == original_error.code
    assert reconstructed_error.message == original_error.message
    assert reconstructed_error.description == original_error.description
    assert reconstructed_error.signature == original_error.signature


def test_error_default_values():
    """Test that error record uses correct default values when fields are omitted"""
    error = JobExecutionError.create()

    assert error.code == 0
    assert error.message == ""
    assert error.description == ""
    assert error.signature == ""
