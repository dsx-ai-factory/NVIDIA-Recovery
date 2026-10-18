# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Locate test fixtures that the environment has already published where jobs can read them."""

from __future__ import annotations

from pathlib import PurePosixPath

from pydantic_settings import BaseSettings, SettingsConfigDict


class Fixtures(BaseSettings):
    """`E2E_FIXTURES_*` environment variables."""

    model_config = SettingsConfigDict(env_prefix="E2E_FIXTURES_", frozen=True)

    root: PurePosixPath = PurePosixPath("/home/e2e/fixtures")

    def path(self, name: str) -> str:
        """The path a job reads fixture `name` from, given relative to the published fixture tree."""

        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"fixture name must stay inside the fixture root: {name!r}")
        return str(self.root / relative)
