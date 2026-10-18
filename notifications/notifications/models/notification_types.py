# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from enum import StrEnum, auto


class NotificationType(StrEnum):
    optional = auto()
    """The least critical type of event.

    Optional notifications are usually ignored, but they may be useful when a user is trying to learn how the overall
    system works.
    """

    info = auto()
    """An abnormal event that does not require manual intervention.

    These events are typically handled by the scheduler, scheduler policies, or the autonomous recovery engine service
    itself.
    """

    performance_alert = auto()
    """An event that may impact training performance.

    When these events occur, the job can still continue running, though with reduced performance. Manual intervention
    is not required to keep the job running, but may be necessary to address the performance issue.
    """

    warning = auto()
    """An event that requires manual intervention for the job to progress."""
