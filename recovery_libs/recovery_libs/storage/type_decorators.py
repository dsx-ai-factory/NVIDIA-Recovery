# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from datetime import UTC, datetime
from enum import IntEnum
from typing import Any, cast

from sqlalchemy import JSON, Integer, String, TypeDecorator
from sqlalchemy.dialects.mysql import TIMESTAMP as MySQLTIMESTAMP
from sqlalchemy.types import TIMESTAMP, TypeEngine

from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec
from recovery_libs.schema.job_manager.models import Issue, JobAnomaly, JobCheckpointInfo
from recovery_libs.storage.timestamps import normalize_mysql_timestamp

# Type decorators are for converting Python objects to a format (specifically JSON) that can be stored in the database.
# This is useful for storing nested objects in the database, such as dataclasses or lists of dataclasses when we don't
# want to store them in separate tables using Ssqlalchemy's `relationship` feature.

logger = logging.getLogger(__name__)


class ApsSlurmJobSpecType(TypeDecorator):
    impl = JSON
    cache_ok = True

    def process_bind_param(self, value: ApsSlurmJobSpec | None, dialect) -> str | None:
        if value is None:
            return None
        return value.model_dump_json()

    def process_result_value(self, value: str | None, dialect) -> ApsSlurmJobSpec | None:
        if not value:
            return None
        return ApsSlurmJobSpec.model_validate_json(value)


class JobAnomaliesType(TypeDecorator):
    impl = JSON
    cache_ok = True

    def process_bind_param(self, value: list[JobAnomaly] | None, dialect) -> str | None:
        if value is None:
            return None
        return json.dumps([anomaly.model_dump_json() for anomaly in value])

    def process_result_value(self, value: str | None, dialect) -> list[JobAnomaly] | None:
        if value is None:
            return None
        return [JobAnomaly.model_validate_json(anomaly_json) for anomaly_json in json.loads(value)]


class JobCheckpointsType(TypeDecorator):
    impl = JSON
    cache_ok = True

    def process_bind_param(self, value: list[JobCheckpointInfo] | None, dialect) -> str | None:
        if value is None:
            return None
        return json.dumps([checkpoint.model_dump_json() for checkpoint in value])

    def process_result_value(self, value: str | None, dialect) -> list[JobCheckpointInfo] | None:
        if value is None:
            return None
        return [JobCheckpointInfo.model_validate_json(checkpoint_json) for checkpoint_json in json.loads(value)]


class IssuesType(TypeDecorator):
    """
    TypeDecorator to store a list of Issue objects in a JSON column.

    Handles serialization/deserialization of Issue Pydantic models for the advice table.
    """

    impl = JSON
    cache_ok = True

    def process_bind_param(self, value: list[Issue] | None, dialect) -> str | None:
        if value is None:
            return None
        return json.dumps([issue.model_dump_json() for issue in value])

    def process_result_value(self, value: str | None, dialect) -> list[Issue] | None:
        if value is None:
            return None
        return [Issue.model_validate_json(issue_json) for issue_json in json.loads(value)]


class TZDateTimeType(TypeDecorator):
    """
    This type decorator is used to guarantee that:
      - The datetime objects to be written into the database have the timezone information.
      - The datetime objects read from the database have the timezone information too.

    For MySQL specifically, TIMESTAMP column type converts all datetime into UTC timezone, however the datetime objects
    retrieved from the database are native datetime objects (i.e. with no timeozne), which cannot be used to do
    comparison and calculation with timezone-aware datetime objects.

    Reference implementation: https://docs.sqlalchemy.org/en/14/core/custom_types.html#store-timezone-aware-timestamps-as-timezone-naive-utc
    """

    impl: TypeEngine[Any] | type[TypeEngine[Any]] = TIMESTAMP
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect):
        if value is None:
            return None
        return normalize_mysql_timestamp(value)

    def process_result_value(self, value: datetime | None, dialect):
        if value is None:
            return None
        if not isinstance(value, datetime):
            raise TypeError(f"database TIMESTAMP value must be a datetime, got {type(value).__name__}")
        return value.replace(tzinfo=UTC)


class TZDateTimeMicrosecondsType(TZDateTimeType):
    """UTC timestamp storage that preserves cross-service event precision."""

    impl = MySQLTIMESTAMP(fsp=6)
    cache_ok = True


class IntEnumType(TypeDecorator):
    """
    TypeDecorator to store a single IntEnum value.
    """

    impl = Integer
    cache_ok = True

    def __init__(self, enumtype, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._enumtype = enumtype

    def process_bind_param(self, value: IntEnum | None, dialect) -> int | None:
        if value is None:
            return None
        return value.value

    def process_result_value(self, value: int | None, dialect) -> IntEnum | None:
        if value is None:
            return None
        return self._enumtype(value)


class IntEnumListType(TypeDecorator):
    """
    TypeDecorator to store a list of IntEnum values in a JSON column.
    """

    impl = JSON
    cache_ok = True

    def __init__(self, enum_class):
        super().__init__()
        self.enum_class = enum_class

    def process_bind_param(self, value: list[IntEnum] | None, dialect) -> list[int] | None:
        if value is None:
            return None
        return [role.value for role in value]

    def process_result_value(self, value: list[int] | None, dialect) -> list[IntEnum] | None:
        if value is None:
            return None
        return [self.enum_class(val) for val in value]


class LimitedLengthStringType(TypeDecorator):
    """
    TypeDecorator to store a string with a limited length with automatic truncation if it exceeds the limit.
    """

    impl = String
    cache_ok = True

    def process_bind_param(self, value: str | None, dialect) -> str | None:
        max_length = cast(String, self.impl_instance).length
        if value and max_length is not None and len(value) > max_length:
            logger.warning(
                f"Input string length ({len(value)}) exceeds maximum length of {max_length}. Raw input: {value}"
            )
            return value[:max_length]
        return value

    def process_result_value(self, value: str | None, dialect) -> str | None:
        return value
