# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
import uuid

import pytest
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.watcher.model import Attributes, Event


class TestEvent(unittest.TestCase):
    def test_event_creation(self):
        event_uuid = str(uuid.uuid4())
        attributes = Attributes(job_id="job123", detection_time="2023-06-08T10:00:00", uuid=event_uuid)
        event_load_chkpt = Event(type=EventType.CHECKPOINTLOAD, attributes=attributes)
        event_save_chkpt = Event(type=EventType.CHECKPOINTSAVE, attributes=attributes)

        self.assertEqual(event_load_chkpt.type, EventType.CHECKPOINTLOAD)
        self.assertIsInstance(event_load_chkpt.attributes, Attributes)
        self.assertEqual(event_load_chkpt.attributes.job_id, "job123")
        self.assertEqual(event_load_chkpt.attributes.detection_time, "2023-06-08T10:00:00")
        self.assertEqual(event_load_chkpt.attributes.uuid, event_uuid)

        self.assertEqual(event_save_chkpt.type, EventType.CHECKPOINTSAVE)
        self.assertIsInstance(event_save_chkpt.attributes, Attributes)
        self.assertEqual(event_save_chkpt.attributes.job_id, "job123")
        self.assertEqual(event_save_chkpt.attributes.detection_time, "2023-06-08T10:00:00")
        self.assertEqual(event_save_chkpt.attributes.uuid, event_uuid)

    def test_event_with_optional_fields(self):
        attributes = Attributes(
            job_id="job456",
            uuid=str(uuid.uuid4()),
            log_file_name="straggler.log",
            log_file_path="/var/log/straggler.log",
        )
        event = Event(
            type=EventType.CHECKPOINTSAVE, body="Error message", timestamp="2023-06-08T11:30:00", attributes=attributes
        )

        self.assertEqual(event.body, "Error message")
        self.assertEqual(event.type, EventType.CHECKPOINTSAVE)
        self.assertEqual(event.timestamp, "2023-06-08T11:30:00")
        self.assertIsInstance(event.attributes, Attributes)
        self.assertEqual(event.attributes.job_id, "job456")

    def test_event_without_attributes(self):
        event = Event(type=EventType.CHECKPOINTSAVE)
        self.assertEqual(event.type, EventType.CHECKPOINTSAVE)
        self.assertIsNone(event.attributes)

    def test_event_with_missing_required_fields(self):
        with self.assertRaises(ValueError):
            Event()

    def test_attributes_with_aliases(self):
        attributes = Attributes(
            job_id="job789",
            uuid=str(uuid.uuid4()),
            log_file_name="straggler.log",
            log_file_path="/var/log/straggler.log",
        )

        self.assertEqual(attributes.job_id, "job789")


EVENT_TYPE_EXPECTED_VALUES = {
    "CHECKPOINTLOAD": "CHECKPOINTLOAD",
    "CHECKPOINTSAVE": "CHECKPOINTSAVE",
    "CRASH": "CRASH",
    "CRASHAPPEXCEPTION": "CRASHAPPEXCEPTION",
    "CRASHNAN": "CRASHNAN",
    "CRASHOOM": "CRASHOOM",
    "CRASHSEGV": "CRASHSEGV",
    "END": "END",
    "FLTINJECTION": "FLTINJECTION",
    "FLTINJECTIONINFRA": "FLTINJECTIONINFRA",
    "HANG": "HANG",
    "HANGAPP": "HANGAPP",
    "HANGCOMM": "HANGCOMM",
    "HANGNOOUTPUT": "HANGNOOUTPUT",
    "HANGPUSHRECV": "HANGPUSHRECV",
    "LOWTHROUGHPUT": "LOWTHROUGHPUT",
    "LOWTHROUGHPUTEWNET": "LOWTHROUGHPUTEWNET",
    "LOWTHROUGHPUTGPU": "LOWTHROUGHPUTGPU",
    "LOWTHROUGHPUTNSNET": "LOWTHROUGHPUTNSNET",
    "LOWTHROUGHPUTSTORAGE": "LOWTHROUGHPUTSTORAGE",
    "NESTED_RESTARTER": "NESTED_RESTARTER",
    "TELEMETRY_RECORD": "TELEMETRY_RECORD",
    "STRAGGLER": "STRAGGLER",
    "UNKNOWN": "UNKNOWN",
}


@pytest.mark.parametrize("event_name, expected_value", EVENT_TYPE_EXPECTED_VALUES.items())
def test_eventtype_values(event_name, expected_value):
    # Check that the enum item has the correct value
    event = EventType[event_name]
    assert event.value == expected_value
    # Create enum from the value - should return the correct item
    assert EventType(expected_value) == event


def test_eventtype_enum_iteration():
    # When iterating over EventType, ensure we collect all expected events
    events = {event.name: event.value for event in EventType}
    assert events == EVENT_TYPE_EXPECTED_VALUES


def test_eventtype_string_casting():
    # Test enum item returns a string containing the event name
    item = EventType.CRASH
    assert str(item) == "CRASH"


if __name__ == "__main__":
    unittest.main()
