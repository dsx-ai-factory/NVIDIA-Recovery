# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from enum import StrEnum


class Stage(StrEnum):
    STARTING = "starting"
    PROCESSING = "processing"
    COMPLETED = "completed"


class State(StrEnum):
    INITIALIZE = "initialize"
    HANDLING = "handling"
    FINALIZED = "finalized"
    ABORTED = "aborted"


@dataclass
class NestedRestarterStatus:
    state: State | None = None
    stage: Stage | None = None

    def __repr__(self):
        return f"NestedRestarterStatus(state={self.state}, stage={self.stage if self.stage else 'None'})"
