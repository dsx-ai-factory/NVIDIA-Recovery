# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""What scenario steps reach through `run.services`."""

from __future__ import annotations

from dataclasses import dataclass, fields

from recovery_libs.testing.kubernetes import StackControl

from e2e_harness.recovery_client import RecoveryClient


@dataclass(frozen=True)
class E2EServices(RecoveryClient):
    """Heimdall's read-only service APIs, plus control of the deployed stack for scenarios that change it."""

    stack: StackControl

    @classmethod
    def of(cls, recovery: RecoveryClient, stack: StackControl) -> E2EServices:
        return cls(**{field.name: getattr(recovery, field.name) for field in fields(recovery)}, stack=stack)
