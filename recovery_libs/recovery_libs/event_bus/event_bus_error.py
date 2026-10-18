# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


class EventBusError(Exception):
    """Base exception class for EventBus related errors."""

    def __init__(self, error_type, message):
        super().__init__(message)
        self.error_type = error_type
