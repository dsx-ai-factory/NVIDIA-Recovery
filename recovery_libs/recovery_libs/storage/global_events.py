# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Index, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from recovery_libs.storage.dataclass import DataClassBase
from recovery_libs.storage.node_events import (
    ErrorAttribution,
    ErrorAttributionOrigin,
    ErrorAttributionSource,
    ErrorAttributionType,
    FactStatus,
)
from recovery_libs.storage.type_decorators import (
    IntEnumListType,
    IntEnumType,
    TZDateTimeType,
)

if TYPE_CHECKING:
    from recovery_libs.storage.job_execution_errors import JobExecutionError


class GlobalEvent(DataClassBase):
    """
    Class to represent a global event (job-level attribution not tied to specific nodes) in the database.
    Each row in this table is associated with an error in the errors table.
    Unlike NodeEvent, GlobalEvent does not have node, exclusion_type, or original_exclusion_type fields.
    """

    __tablename__ = "global_events"

    # The unique identifier for the global event
    id: Mapped[int] = mapped_column(primary_key=True, init=False)

    # Identify an error in the errors table
    job_execution_error_id: Mapped[int] = mapped_column(ForeignKey("job_execution_errors.id"), init=False)

    # The event timestamp, all timestamp should be in UTC timezone.
    timestamp: Mapped[datetime] = mapped_column(TZDateTimeType, kw_only=True)

    # Identify a job in the job_execution_records table
    # These fields are sufficient for querying, while other job details remain in the job table
    job_id: Mapped[str] = mapped_column(String(64), default="", kw_only=True)
    job_attempt_index: Mapped[int] = mapped_column(default=0, kw_only=True)
    job_execution_error: Mapped["JobExecutionError"] = relationship(back_populates="global_events", init=False)
    job_anomaly_id: Mapped[uuid.UUID | None] = mapped_column(Uuid, default=None, kw_only=True)

    # The attribution of the global event
    error_attribution: Mapped[ErrorAttribution] = mapped_column(
        IntEnumType(ErrorAttribution), default=ErrorAttribution.UNDEFINED, kw_only=True
    )
    error_attribution_type: Mapped[ErrorAttributionType] = mapped_column(
        IntEnumType(ErrorAttributionType), default=ErrorAttributionType.UNDEFINED, kw_only=True
    )
    error_attribution_origin: Mapped[ErrorAttributionOrigin] = mapped_column(
        IntEnumType(ErrorAttributionOrigin), default=ErrorAttributionOrigin.UNDEFINED, kw_only=True
    )
    error_attribution_sources: Mapped[list[ErrorAttributionSource]] = mapped_column(
        IntEnumListType(ErrorAttributionSource), default_factory=list, kw_only=True
    )
    fact_status: Mapped[FactStatus] = mapped_column(IntEnumType(FactStatus), default=FactStatus.UNDEFINED, kw_only=True)

    # SQL table additional configurations
    __table_args__ = (
        Index("global_event_job_execution_error_id_index", "job_execution_error_id"),
        Index("global_event_job_id_attempt_index", "job_id", "job_attempt_index"),
        Index("global_event_job_anomaly_id_index", "job_anomaly_id"),
        Index("global_event_timestamp_index", "timestamp"),
    )

    @classmethod
    def create(
        cls,
        timestamp: datetime,
        job_id: str,
        job_attempt_index: int,
        job_anomaly_id: uuid.UUID | None = None,
        error_attribution: ErrorAttribution = ErrorAttribution.UNDEFINED,
        error_attribution_type: ErrorAttributionType = ErrorAttributionType.UNDEFINED,
        error_attribution_origin: ErrorAttributionOrigin = ErrorAttributionOrigin.UNDEFINED,
        error_attribution_sources: list[ErrorAttributionSource] | None = None,
        fact_status: FactStatus = FactStatus.UNDEFINED,
    ) -> "GlobalEvent":
        """
        Create a global event record.

        Args:
            timestamp: The timestamp of the event
            job_id: The job ID of the job that caused the error
            job_attempt_index: The job attempt index of the job that caused the error
            job_anomaly_id: The anomaly ID of the job that caused the error
            error_attribution: The attribution of the error
            error_attribution_type: The type of error attribution, refer to the enum for more details
            error_attribution_origin: The origin of the error attribution, refer to the enum for more details
            error_attribution_sources: The sources of the error attribution, refer to the enum for more details
            fact_status: The status of the fact, refer to the enum for more details

        Returns:
            A new GlobalEvent instance
        """
        return cls(
            timestamp=timestamp,
            job_id=job_id,
            job_attempt_index=job_attempt_index,
            job_anomaly_id=job_anomaly_id,
            error_attribution=error_attribution,
            error_attribution_type=error_attribution_type,
            error_attribution_origin=error_attribution_origin,
            error_attribution_sources=error_attribution_sources or [],
            fact_status=fact_status,
        )

    def to_json(self) -> dict:
        """
        Convert the global event record to a JSON-serializable dictionary.
        """
        return {
            "timestamp": self.timestamp.isoformat(),
            "job_id": self.job_id,
            "job_attempt_index": self.job_attempt_index,
            "job_anomaly_id": str(self.job_anomaly_id) if self.job_anomaly_id else None,
            "error_attribution": self.error_attribution.value,
            "error_attribution_type": self.error_attribution_type.value,
            "error_attribution_origin": self.error_attribution_origin.value,
            "error_attribution_sources": [source.value for source in self.error_attribution_sources],
            "fact_status": self.fact_status.value,
        }

    @classmethod
    def from_json(cls, data: dict) -> "GlobalEvent":
        """
        Create a global event record from a JSON-serializable dictionary.
        """
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            job_id=data["job_id"],
            job_attempt_index=data["job_attempt_index"],
            job_anomaly_id=uuid.UUID(data["job_anomaly_id"]) if data.get("job_anomaly_id") else None,
            error_attribution=ErrorAttribution(data["error_attribution"]),
            error_attribution_type=ErrorAttributionType(data["error_attribution_type"]),
            error_attribution_origin=ErrorAttributionOrigin(data["error_attribution_origin"]),
            error_attribution_sources=[ErrorAttributionSource(source) for source in data["error_attribution_sources"]],
            fact_status=FactStatus(data["fact_status"]),
        )
