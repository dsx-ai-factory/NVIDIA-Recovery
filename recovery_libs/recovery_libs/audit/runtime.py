# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Annotated

from pydantic import AnyHttpUrl, BaseModel, Field, model_validator

from recovery_libs.audit.buffered import AuditBuffer, AuditEventBuffer, Config
from recovery_libs.audit.destinations import LoggingSender, NotificationSender
from recovery_libs.audit.events import ProducerEvent

_MAX_NOTIFICATION_BATCH_SIZE = 1000


class AuditTrailSettings(BaseModel, frozen=True):
    """Shared configuration for best-effort audit-event delivery."""

    destination: AnyHttpUrl | None = None
    max_batch_size: Annotated[int, Field(gt=0, strict=True)] = 100
    max_buffer_size: Annotated[int, Field(gt=0, strict=True)] = 1000
    flush_interval_seconds: Annotated[float, Field(gt=0)] = 30

    @model_validator(mode="after")
    def _validate_notification_batch_size(self):
        if self.destination is not None and self.max_batch_size > _MAX_NOTIFICATION_BATCH_SIZE:
            raise ValueError(
                f"max_batch_size must not exceed {_MAX_NOTIFICATION_BATCH_SIZE} when destination is configured"
            )
        return self


def create_audit_buffer[EventT: ProducerEvent](
    settings: AuditTrailSettings,
    logger: logging.Logger,
) -> AuditBuffer[EventT]:
    """Create a delivery buffer using the configured endpoint or application logging."""

    destination = (
        NotificationSender(str(settings.destination)) if settings.destination is not None else LoggingSender(logger)
    )
    buffer = AuditEventBuffer[EventT](
        Config[EventT](
            max_batch_size=settings.max_batch_size,
            max_buffer_size=settings.max_buffer_size,
            flush_interval_seconds=settings.flush_interval_seconds,
            sender=destination.send,
        )
    )
    return buffer
