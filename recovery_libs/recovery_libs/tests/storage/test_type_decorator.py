# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import unittest
from datetime import UTC, datetime, timedelta, timezone

import pytest
from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec
from recovery_libs.schema.job_manager.models import JobAnomaly, JobCheckpointInfo
from recovery_libs.storage.type_decorators import (
    ApsSlurmJobSpecType,
    IssuesType,
    JobAnomaliesType,
    JobCheckpointsType,
    LimitedLengthStringType,
    TZDateTimeType,
)
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, insert, select


class TestApsSlurmJobSpecType(unittest.TestCase):
    def setUp(self) -> None:
        self.decorator = ApsSlurmJobSpecType()

    def test_aps_slurm_job_spec_type_none_value(self):
        self.assertEqual(self.decorator.process_bind_param(None, None), None)
        self.assertEqual(self.decorator.process_bind_param(None, None), None)

    def test_aps_slurm_job_spec_normal_value(self):
        aps_slurm_job_spec = ApsSlurmJobSpec(
            workflow_name="test_workflow",
        )
        self.assertEqual(
            self.decorator.process_bind_param(aps_slurm_job_spec, None),
            aps_slurm_job_spec.model_dump_json(),
        )
        self.assertEqual(
            self.decorator.process_result_value(aps_slurm_job_spec.model_dump_json(), None),
            aps_slurm_job_spec,
        )


class TestJobAnomaliesType(unittest.TestCase):
    def setUp(self) -> None:
        self.decorator = JobAnomaliesType()

    def test_job_anomalies_type_none_value(self):
        self.assertEqual(self.decorator.process_bind_param(None, None), None)
        self.assertEqual(self.decorator.process_result_value(None, None), None)

    def test_job_anomalies_normal_value(self):
        job_anomalies = [
            JobAnomaly(
                anomaly_type="HANGNOOUTPUT",
                timestamp="2021-08-01T00:00:00+00:00",
                uuid="12345678-1234-5678-1234-567812345678",
                original_watcher_anomaly_event="EVENT",
            ),
        ]
        self.assertEqual(
            self.decorator.process_bind_param(job_anomalies, None),
            json.dumps([anomaly.model_dump_json() for anomaly in job_anomalies]),
        )
        self.assertEqual(
            self.decorator.process_result_value(
                json.dumps([anomaly.model_dump_json() for anomaly in job_anomalies]),
                None,
            ),
            job_anomalies,
        )


class TestJobCheckpointsType(unittest.TestCase):
    def setUp(self) -> None:
        self.decorator = JobCheckpointsType()

    def test_job_checkpoints_type_none_value(self):
        self.assertEqual(self.decorator.process_bind_param(None, None), None)
        self.assertEqual(self.decorator.process_result_value(None, None), None)

    def test_job_checkpoints_normal_value(self):
        job_checkpoints = [
            JobCheckpointInfo(
                timestamp="2021-08-01T00:00:00+00:00",
                uuid="12345678-1234-5678-1234-567812345678",
            ),
        ]
        self.assertEqual(
            self.decorator.process_bind_param(job_checkpoints, None),
            json.dumps([checkpoint.model_dump_json() for checkpoint in job_checkpoints]),
        )
        self.assertEqual(
            self.decorator.process_result_value(
                json.dumps([checkpoint.model_dump_json() for checkpoint in job_checkpoints]),
                None,
            ),
            job_checkpoints,
        )


class TestTZDateTimeType(unittest.TestCase):
    def setUp(self) -> None:
        self.decorator = TZDateTimeType()

    def test_datetime_none_value(self):
        self.assertEqual(self.decorator.process_bind_param(None, None), None)
        self.assertEqual(self.decorator.process_result_value(None, None), None)

    def test_datetime_without_timezone_write_path(self):
        with self.assertRaises(TypeError):
            now = datetime.now()
            self.assertIsNone(now.tzinfo)

            self.decorator.process_bind_param(now, None)

    def test_datetime_without_timezone_read_path(self):
        now = datetime.now()
        self.assertIsNone(now.tzinfo)

        result = self.decorator.process_result_value(now, None)
        self.assertEqual(result.tzinfo, UTC)

    def test_datetime_timezone_conversion(self):
        now_pst = datetime.now(tz=timezone(timedelta(hours=-5), "US/Pacific"))
        result = self.decorator.process_bind_param(now_pst, None)

        self.assertEqual(result.tzinfo, UTC)
        self.assertEqual(result, now_pst.astimezone(UTC))

    def test_datetime_outside_mysql_timestamp_range_write_path(self):
        for value in (
            datetime(1969, 12, 31, 23, 59, 59, tzinfo=UTC),
            datetime(2106, 2, 7, 6, 28, 14, tzinfo=UTC),
        ):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "MySQL TIMESTAMP range"):
                self.decorator.process_bind_param(value, None)

    def test_non_datetime_database_value_has_clear_error(self):
        with self.assertRaisesRegex(TypeError, "database TIMESTAMP value must be a datetime, got str"):
            self.decorator.process_result_value("0000-00-00 00:00:00", None)


class TestLimitedLengthStringType(unittest.TestCase):
    def setUp(self) -> None:
        self.max_length = 10
        self.decorator = LimitedLengthStringType(self.max_length)

    def test_none_value(self):
        self.assertIsNone(self.decorator.process_bind_param(None, None))
        self.assertIsNone(self.decorator.process_result_value(None, None))

    def test_string_within_limit(self):
        value = "short"
        self.assertEqual(self.decorator.process_bind_param(value, None), value)
        self.assertEqual(self.decorator.process_result_value(value, None), value)

    def test_string_exact_limit(self):
        value = "abcdefghij"  # length == 10
        self.assertEqual(self.decorator.process_bind_param(value, None), value)
        self.assertEqual(self.decorator.process_result_value(value, None), value)

    def test_string_truncation(self):
        value = "this is a long string that exceeds the limit"
        result = self.decorator.process_bind_param(value, None)
        self.assertEqual(result, value[: self.max_length])

    def test_non_string_value(self):
        value = 12345
        with self.assertRaises(TypeError):
            self.decorator.process_bind_param(value, None)


@pytest.mark.regression
@pytest.mark.parametrize("column_type", [JobAnomaliesType, JobCheckpointsType, IssuesType])
def test_json_list_column_round_trips_empty_list(column_type):
    """An empty list used to be stored as a bare JSON array that failed to load with TypeError."""
    metadata = MetaData()
    table = Table("records", metadata, Column("id", Integer, primary_key=True), Column("payload", column_type))
    engine = create_engine("sqlite://")
    metadata.create_all(engine)

    with engine.begin() as connection:
        connection.execute(insert(table).values(id=1, payload=[]))
        assert connection.execute(select(table.c.payload)).scalar_one() == []
