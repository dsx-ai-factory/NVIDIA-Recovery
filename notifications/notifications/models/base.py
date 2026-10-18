# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import ClassVar

import pydantic
import sqlalchemy as sa

# Prefix with underscore to prevent people from accidentally importing this one instead.
from sqlmodel import SQLModel as _SQLModel


class BaseModel(_SQLModel):
    model_config = pydantic.ConfigDict(use_attribute_docstrings=True)  # type: ignore[assignment]


class JsonModelField(sa.types.TypeDecorator):
    """Compatibility shim to treat a pydantic model as a sqlalchemy JSON field."""

    pydantic_model: ClassVar[type[pydantic.BaseModel]]  # OVERRIDE THIS IN SUBCLASSES

    impl = sa.JSON
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if isinstance(value, pydantic.BaseModel):
            value = value.model_dump()
        return value

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        return self.pydantic_model.model_validate(value)
