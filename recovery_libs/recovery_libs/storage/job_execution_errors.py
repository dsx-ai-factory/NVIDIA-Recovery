# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from sqlalchemy import Index
from sqlalchemy.orm import Mapped, mapped_column, relationship

from recovery_libs.storage.dataclass import DataClassBase
from recovery_libs.storage.global_events import GlobalEvent
from recovery_libs.storage.node_events import NodeEvent
from recovery_libs.storage.type_decorators import LimitedLengthStringType


class JobExecutionError(DataClassBase):
    """
    Class to represent an error that occurred during a job execution. Each row in this table is unique, this is because
    errors repeat themselves over time, and we want to save space by not storing identical error strings
    multiple times.
    """

    __tablename__ = "job_execution_errors"

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    code: Mapped[int] = mapped_column(default=0, kw_only=True)
    message: Mapped[str] = mapped_column(LimitedLengthStringType(1024), default="", kw_only=True)
    description: Mapped[str] = mapped_column(LimitedLengthStringType(1024), default="", kw_only=True)
    signature: Mapped[str] = mapped_column(LimitedLengthStringType(255), default="", kw_only=True)

    node_events: Mapped[list["NodeEvent"]] = relationship(
        "NodeEvent",
        order_by="NodeEvent.timestamp",
        init=False,
        default_factory=list,
        back_populates="job_execution_error",
    )

    global_events: Mapped[list["GlobalEvent"]] = relationship(
        "GlobalEvent",
        order_by="GlobalEvent.timestamp",
        init=False,
        default_factory=list,
        back_populates="job_execution_error",
    )

    # SQL table additional configurations
    __table_args__ = (
        Index("code_index", "code"),
        Index("signature_index", "signature"),
    )

    @classmethod
    def create(
        cls,
        code: int = 0,
        message: str = "",
        description: str = "",
        signature: str = "",
    ) -> "JobExecutionError":
        """
        Create an error record.
        """
        return cls(code=code, message=message, description=description, signature=signature)

    def to_json(self) -> dict:
        """
        Convert the error record to a JSON-serializable dictionary.
        """
        return {
            "code": self.code,
            "message": self.message,
            "description": self.description,
            "signature": self.signature,
        }

    @classmethod
    def from_json(cls, data: dict) -> "JobExecutionError":
        """
        Create an error record from a JSON-serializable dictionary.
        """
        return cls(
            code=data["code"],
            message=data["message"],
            description=data["description"],
            signature=data["signature"],
        )

    def __hash__(self) -> int:
        return hash((self.code, self.message, self.description, self.signature))
