# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Qualify deployed reconciliation through public gRPC and Kubernetes interfaces."""

import os

import pytest
from e2e_harness.advice_operator import AdviceOperatorServices
from recovery_libs.testing.launch.jobs import JobSpec
from recovery_libs.testing.scenario import Run, Scenario, act, consistently, eventually
from recovery_libs.testing.scenario_plugin import scenario

RESTART = "ADVICE_STATE_RESTARTED"
TERMINATE = "ADVICE_STATE_TERMINATED"


def workload() -> JobSpec:
    """Keep a small owned JobSet alive while its Advice changes."""
    return JobSpec(
        name="advice-smoke",
        workload={
            "image": os.environ.get("KUBERNETES_SMOKE_IMAGE", "busybox:1.37.0"),
            "command": ["sh", "-c", "echo advice-owner-ready; sleep 300"],
        },
    )


@scenario
def test_advice_lifecycle() -> Scenario[AdviceOperatorServices]:
    """Create and update the exact owned Advice, then remove withdrawn source advice."""
    return Scenario(
        name="Advice source lifecycle",
        job=workload(),
        stages=(
            act("publish restart advice", lambda run: run.services.publish(run.job, RESTART)),
            eventually(
                "Advice created with exact owner", lambda run: run.services.matches(run.job, RESTART), within=45
            ),
            act("publish changed recommendation", lambda run: run.services.publish(run.job, TERMINATE)),
            eventually("Advice updated", lambda run: run.services.matches(run.job, TERMINATE), within=45),
            act("withdraw source advice", lambda run: run.services.withdraw()),
            eventually("withdrawn Advice removed", lambda run: run.services.absent(), within=45),
            consistently("withdrawn Advice stays absent", lambda run: run.services.absent(), for_seconds=3),
        ),
    )


def delete_owner_and_republish(run: Run[AdviceOperatorServices]) -> None:
    """Reset source observations after deletion while retaining the original owner UID."""
    run.launcher.cancel(job_id=run.job.job_id)
    run.services.publish(run.job, RESTART)


@pytest.mark.parametrize("owner", ["missing", "wrong-uid", "deleted"])
@scenario
def test_advice_owner_guard(owner: str) -> Scenario[AdviceOperatorServices]:
    """Remove existing Advice when its source references an absent or different owner."""
    invalidate = (
        act("delete owned workload and republish its advice", delete_owner_and_republish)
        if owner == "deleted"
        else act("invalidate source owner", lambda run: run.services.publish(run.job, RESTART, owner=owner))
    )
    return Scenario(
        name=f"Advice owner guard: {owner}",
        job=workload(),
        stages=(
            act("publish valid owned advice", lambda run: run.services.publish(run.job, RESTART)),
            eventually("valid Advice exists first", lambda run: run.services.matches(run.job, RESTART), within=45),
            act("watch every owner write before invalidation", lambda run: run.services.guard_owner(run.job)),
            invalidate,
            eventually("orphan Advice removed after source polls", lambda run: run.services.absent(), within=45),
            consistently("orphan Advice is not recreated", lambda run: run.services.absent(), for_seconds=3),
        ),
    )


@pytest.mark.parametrize("owner", ["missing", "wrong-uid"])
@scenario
def test_advice_initial_invalid_owner(owner: str) -> Scenario[AdviceOperatorServices]:
    """Reject even short-lived Advice creation for invalid owners before GC can erase it."""
    return Scenario(
        name=f"Advice initially invalid owner: {owner}",
        job=workload(),
        stages=(
            act("watch Advice before publishing", lambda run: run.services.guard_owner(run.job)),
            act("publish invalid owner", lambda run: run.services.publish(run.job, RESTART, owner=owner)),
            eventually("source polled without Advice creation", lambda run: run.services.absent(), within=45),
            consistently("no invalid owner writes", lambda run: run.services.absent(), for_seconds=3),
        ),
    )
