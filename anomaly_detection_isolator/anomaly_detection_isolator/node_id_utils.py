# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import uuid as uuid_module


def parse_uuid(value: str) -> uuid_module.UUID | None:
    """Parse a UUID string, returning None if empty or malformed."""
    try:
        return uuid_module.UUID(value) if value else None
    except (ValueError, AttributeError):
        return None
