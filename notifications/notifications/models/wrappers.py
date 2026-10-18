# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Sequence

import pydantic

from .base import BaseModel


# Need to inherit from pydantic.BaseModel here to make sure the $ref gets generated correctly in OpenAPI spec.
class Items[ItemT: BaseModel](pydantic.BaseModel):
    items: Sequence[ItemT]
    total_items: int
