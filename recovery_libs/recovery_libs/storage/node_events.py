# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import uuid
from datetime import datetime
from enum import IntEnum
from typing import TYPE_CHECKING

from sqlalchemy import ForeignKey, Index, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from recovery_libs.storage.dataclass import DataClassBase
from recovery_libs.storage.type_decorators import (
    IntEnumListType,
    IntEnumType,
    TZDateTimeType,
)

if TYPE_CHECKING:
    from recovery_libs.storage.job_execution_errors import JobExecutionError


class ErrorAttribution(IntEnum):
    """
    Enum for classifying the error as either hardware or software.
    For software errors, they can either be due to the infrastructure,
    in which case they are SYSTEM errors, or to the application itself.
    If the error could not be classified, then it is UNKNOWN.
    """

    UNDEFINED = 0
    UNKNOWN = 1
    HARDWARE = 2
    SYSTEM = 3
    APPLICATION = 4


class ErrorAttributionType(IntEnum):
    """
    Enum for the type of error attribution, which depends on the type of
    telemetry data that was used for attribution.
    """

    # The value has not been defined
    UNDEFINED = 0
    # No attribution was done
    NONE = 1
    # FAST telemetry was used for attribution, which is normally
    # available as soon as the job finishes
    FAST = 2
    # SLOW telemetry was used for attribution, which is normally
    # available a few minutes after the job finishes, e.g. the Slurm epilog
    SLOW = 3
    # REVISED telemetry was used for attribution, which is normally
    # available after the next job run has completed, and we know whether
    # the error was transient or not
    REVISED = 4


class ErrorAttributionOrigin(IntEnum):
    """
    Enum for the origin of the error attribution. This is the system that
    performed the attribution, and it is normally FACT, unless there is
    a communication error between ADI and FACT.
    """

    # The value has not been defined
    UNDEFINED = 0
    # No attribution was done
    NONE = 1
    # The origin of the error attribution is FACT
    FACT = 2
    # The origin of the error attribution is ADI
    ADI = 3
    # Capability Systems Resource Governance service
    CSRC = 4
    # NVIDIA Mission Control autonomous recovery engine
    HEIMDALL = 5
    # Attribution inferred from Lustre storage metrics or logs
    LUSTRE = 6
    # Attribution based on signals from the job scheduler
    SCHEDULER = 7


class ErrorAttributionSource(IntEnum):
    """
    Enum for telemetry data sources that were used for error attribution.
    """

    # The value has not been defined
    UNDEFINED = 0
    # No source was used
    NONE = 1
    # The source of the error attribution is application logs
    APPLICATION_LOGS = 2
    # The source of the error attribution is syslogs
    SYSLOGS = 3
    # The source of the error attribution is node metrics
    NODE_METRICS = 4
    # The source of the error attribution is scheduler metrics
    SCHEDULER_METRICS = 5
    # The source of the error attribution is network metrics
    NETWORK_METRICS = 6
    # The source of the error attribution is health checks
    HEALTH_CHECKS = 7
    # The source of the error attribution is epilog results
    EPILOG_RESULTS = 8


class ExclusionType(IntEnum):
    """
    Enum for the exclusion type, which depends on the type of
    decision taken by ADI as a result of the error attribution.
    """

    # The value has not been defined
    UNDEFINED = 0
    # No exclusion was taken
    NONE = 1
    # The exclusion is temporary until confirmed by the next job run
    TEMPORARY = 2
    # The exclusion is confirmed either by a hardware error or a
    # successful job run without this node
    CONFIRMED = 3
    # A temporary exclusion was revoked by a successful job run
    REVOKED = 4


class FactStatus(IntEnum):
    """
    Enum for the status of the FACT service.
    """

    # The value has not been defined
    UNDEFINED = 0
    # The FACT service is not available
    NONE = 1
    # The FACT service is available, and responded to the request
    OK = 2
    # The FACT service is available, but failed to respond to the request
    ERROR = 3


class NodeEvent(DataClassBase):
    """
    Class to represent a node event in the database. Each row in this table is associated with
    an error in the errors table.
    """

    __tablename__ = "node_events"

    # The unique identifier for the node event
    id: Mapped[int] = mapped_column(primary_key=True, init=False)

    # Identify an error in the errors table
    job_execution_error_id: Mapped[int] = mapped_column(ForeignKey("job_execution_errors.id"), init=False)

    # Identify a node, eventually this can be used to link to another table with node information, AIDOT-865
    node: Mapped[str] = mapped_column(String(255), default="", kw_only=True)

    # The event timestamp, all timestamp should be in UTC timezone.
    timestamp: Mapped[datetime] = mapped_column(TZDateTimeType, kw_only=True)

    # Identify a job in the job_execution_records table, because job ids may loop, we need to also
    # store start and end times to uniquely identify a job, but these are optional in the job execution
    # table, and so they are optional here too. All timestamps are in UTC timezone.
    job_id: Mapped[str] = mapped_column(String(64), default="", kw_only=True)
    job_attempt_index: Mapped[int] = mapped_column(default=0, kw_only=True)
    job_start_time: Mapped[datetime | None] = mapped_column(TZDateTimeType, default=None, kw_only=True)
    job_end_time: Mapped[datetime | None] = mapped_column(TZDateTimeType, default=None, kw_only=True)
    job_execution_error: Mapped["JobExecutionError"] = relationship(back_populates="node_events", init=False)
    job_anomaly_id: Mapped[uuid.UUID | None] = mapped_column(Uuid, default=None, kw_only=True)
    cluster: Mapped[str] = mapped_column(String(64), default="", kw_only=True)

    # The attribution of the node event
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
    exclusion_type: Mapped[ExclusionType] = mapped_column(
        IntEnumType(ExclusionType), default=ExclusionType.UNDEFINED, kw_only=True
    )
    original_exclusion_type: Mapped[ExclusionType] = mapped_column(
        IntEnumType(ExclusionType), default=ExclusionType.UNDEFINED, kw_only=True
    )
    fact_status: Mapped[FactStatus] = mapped_column(IntEnumType(FactStatus), default=FactStatus.UNDEFINED, kw_only=True)

    # SQL table additional configurations
    __table_args__ = (
        Index("job_execution_error_id_index", "job_execution_error_id"),
        Index("job_id_attempt_index", "job_id", "job_attempt_index"),
        Index("job_anomaly_id_index", "job_anomaly_id"),
        Index("node_index", "node"),
        Index("timestamp_index", "timestamp"),
    )

    @classmethod
    def create(
        cls,
        node: str,
        timestamp: datetime,
        job_id: str,
        job_attempt_index: int,
        job_start_time: datetime | None = None,
        job_end_time: datetime | None = None,
        job_anomaly_id: uuid.UUID | None = None,
        cluster: str = "",
        error_attribution: ErrorAttribution = ErrorAttribution.UNDEFINED,
        error_attribution_type: ErrorAttributionType = ErrorAttributionType.UNDEFINED,
        error_attribution_origin: ErrorAttributionOrigin = ErrorAttributionOrigin.UNDEFINED,
        error_attribution_sources: list[ErrorAttributionSource] | None = None,
        exclusion_type: ExclusionType = ExclusionType.UNDEFINED,
        original_exclusion_type: ExclusionType = ExclusionType.UNDEFINED,
        fact_status: FactStatus = FactStatus.UNDEFINED,
    ) -> "NodeEvent":
        """
        Create a node event record.

        Args:
            node: The name of the node
            timestamp: The timestamp of the event
            job_id: The job ID if job that caused the error on this node
            job_attempt_index: The job attempt index of the job that caused the error on this node
            job_start_time: The start time of the job
            job_end_time: The end time of the job
            job_anomaly_id: The anomaly ID of the job that caused the error on this node
            cluster: The cluster of the node
            error_attribution: The attribution of the error
            error_attribution_type: The type of error attribution, refer to the enum for more details
            error_attribution_origin: The origin of the error attribution, refer to the enum for more details
            error_attribution_sources: The sources of the error attribution, refer to the enum for more details
            exclusion_type: The type of exclusion, refer to the enum for more details
            original_exclusion_type: The original type of exclusion before sanitization, refer to the enum for more details
            fact_status: The status of the fact, refer to the enum for more details

        Returns:
            A new NodeEventsRecord instance
        """
        return cls(
            node=node,
            timestamp=timestamp,
            job_id=job_id,
            job_attempt_index=job_attempt_index,
            job_start_time=job_start_time,
            job_end_time=job_end_time,
            job_anomaly_id=job_anomaly_id,
            cluster=cluster,
            error_attribution=error_attribution,
            error_attribution_type=error_attribution_type,
            error_attribution_origin=error_attribution_origin,
            error_attribution_sources=error_attribution_sources or [],
            exclusion_type=exclusion_type,
            original_exclusion_type=original_exclusion_type,
            fact_status=fact_status,
        )

    def to_json(self) -> dict:
        """
        Convert the node event record to a JSON-serializable dictionary.
        """
        return {
            "node": self.node,
            "timestamp": self.timestamp.isoformat(),
            "job_id": self.job_id,
            "job_attempt_index": self.job_attempt_index,
            "job_start_time": self.job_start_time.isoformat() if self.job_start_time else None,
            "job_end_time": self.job_end_time.isoformat() if self.job_end_time else None,
            "cluster": self.cluster,
            "job_anomaly_id": str(self.job_anomaly_id) if self.job_anomaly_id else None,
            "error_attribution": self.error_attribution.value,
            "error_attribution_type": self.error_attribution_type.value,
            "error_attribution_origin": self.error_attribution_origin.value,
            "error_attribution_sources": [source.value for source in self.error_attribution_sources],
            "exclusion_type": self.exclusion_type.value,
            "original_exclusion_type": self.original_exclusion_type.value,
            "fact_status": self.fact_status.value,
        }

    @classmethod
    def from_json(cls, data: dict) -> "NodeEvent":
        """
        Create a node event record from a JSON-serializable dictionary.
        """
        return cls(
            node=data["node"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            job_id=data["job_id"],
            job_attempt_index=data["job_attempt_index"],
            job_start_time=datetime.fromisoformat(data["job_start_time"]) if data["job_start_time"] else None,
            job_end_time=datetime.fromisoformat(data["job_end_time"]) if data["job_end_time"] else None,
            job_anomaly_id=uuid.UUID(data["job_anomaly_id"]) if data["job_anomaly_id"] else None,
            cluster=data["cluster"],
            error_attribution=ErrorAttribution(data["error_attribution"]),
            error_attribution_type=ErrorAttributionType(data["error_attribution_type"]),
            error_attribution_origin=ErrorAttributionOrigin(data["error_attribution_origin"]),
            error_attribution_sources=[ErrorAttributionSource(source) for source in data["error_attribution_sources"]],
            exclusion_type=ExclusionType(data["exclusion_type"]),
            original_exclusion_type=ExclusionType(data.get("original_exclusion_type", ExclusionType.UNDEFINED.value)),
            fact_status=FactStatus(data["fact_status"]),
        )
