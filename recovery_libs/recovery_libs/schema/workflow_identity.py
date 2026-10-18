# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared public contract for canonical workflow identifiers."""

from typing import Annotated

from pydantic import AfterValidator, StringConstraints


def _validate_utf8(value: str) -> str:
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise ValueError("workflow_id must be valid UTF-8") from error
    return value


CanonicalWorkflowID = Annotated[
    str,
    StringConstraints(min_length=1, max_length=256, strict=True),
    AfterValidator(_validate_utf8),
]
