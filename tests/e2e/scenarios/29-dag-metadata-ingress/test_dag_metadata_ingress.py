# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""SAC preserves typed DAG policy through Job Manager's production job API."""

from __future__ import annotations

import uuid
from pathlib import Path

from e2e_harness import audit
from e2e_harness.checks import audit_trail, dag_job_spec, gate_on_name, held
from e2e_harness.config import E2EConfig
from e2e_harness.dag import dependency_by_id, dependency_by_name
from e2e_harness.recovery_client import RecoveryClient
from e2e_harness.workloads import SCHEDULER_BOUNDARY_RECOVERY, recovery_metadata, slurm_batch
from recovery_libs.testing.launch.jobs import JobSpec
from recovery_libs.testing.scenario import Scenario, eventually
from recovery_libs.testing.scenario_plugin import scenario

# Neither parent exists: the scenario checks only that Job Manager stores what the job declared.
MISSING_PARENT_ID = "900001"
MISSING_PARENT_NAME = "dag-parent-by-name"


@scenario
def test_dag_metadata_reaches_job_manager(e2e_config: E2EConfig) -> Scenario[RecoveryClient]:
    dependencies = [dependency_by_id(MISSING_PARENT_ID), dependency_by_name(MISSING_PARENT_NAME)]
    return Scenario(
        name="29-dag-metadata-ingress",
        job=JobSpec(
            name="dag-metadata-ingress",
            workload=slurm_batch(Path(__file__).with_name("job.sh"), hold=True),
            metadata=recovery_metadata(
                SCHEDULER_BOUNDARY_RECOVERY,
                {
                    "workflow_id": f"dag-metadata-e2e-{uuid.uuid4().hex}",
                    "dag_dependencies": dependencies,
                    "dag_action": "release",
                },
            ),
        ),
        stages=(
            eventually("held", held(), within=e2e_config.validation_timeout(240)),
            eventually(
                "stored",
                dag_job_spec(
                    ("id", lambda run: MISSING_PARENT_ID), gate_on_name(MISSING_PARENT_NAME), action="release"
                ),
                within=e2e_config.watcher_evidence_timeout_seconds,
            ),
            eventually(
                "audited",
                audit_trail(
                    lambda job: (
                        audit.job_observed_event(
                            job, job_subset={"aps_spec": {"dag_action": "release", "dag_dependencies": dependencies}}
                        ),
                    )
                ),
                within=90,
            ),
        ),
    )
