# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Native XID-79 fault injection through production Loki ingestion."""

from __future__ import annotations

from pathlib import Path

import pytest
from e2e_harness.config import E2EConfig
from e2e_harness.faults import fault_injection
from e2e_harness.recovery_client import RecoveryClient
from recovery_libs.testing.fixtures import Fixtures
from recovery_libs.testing.scenario import Scenario
from recovery_libs.testing.scenario_plugin import scenario

pytestmark = pytest.mark.exclusive


@pytest.mark.run_first
@scenario
def test_fault_injection_recovery(e2e_config: E2EConfig, fixtures: Fixtures) -> Scenario[RecoveryClient]:
    return fault_injection(
        e2e_config,
        fixtures,
        scenario_dir=Path(__file__).parent,
        crash_marker="segmentation fault here",
        exclusion_type="CONFIRMED",
        signature="XID-79-GPU_HAS_FALLEN_OFF_THE_BUS",
    )
