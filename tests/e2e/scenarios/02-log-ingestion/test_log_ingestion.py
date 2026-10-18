# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Qualify the selected store through the deployed Alloy file collection path."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest
from e2e_harness.actions import restart, staged_files, staging_directory, touch_file
from e2e_harness.checks import ingested, job_state, on_nodes
from e2e_harness.config import E2EConfig
from e2e_harness.services import E2EServices
from e2e_harness.workloads import HEALTHY_OBSERVE, JOB_LOG_DIRECTORY, recovery_metadata, slurm_batch
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import Scenario, Step, act, consistently, eventually
from recovery_libs.testing.scenario_plugin import scenario

pytestmark = pytest.mark.exclusive

# Cover the collector's discovery/position-sync cycle after the first match.
INGESTION_SETTLE_SECONDS = 15


@pytest.mark.run_first
@scenario
@pytest.mark.parametrize("partition,node_name", (("e2e-a", "e2e-a-0"), ("e2e-b", "e2e-b-0")))
def test_plain_and_json_file_ingestion(
    e2e_config: E2EConfig, tmp_path: Path, partition: str, node_name: str
) -> Scenario[E2EServices]:
    """Keep exact application counts and distinguish syslog from both source nodes."""

    markers = {domain: f"HEIMDALL_E2E_INGEST_{uuid4().hex}" for domain in ("application", "system")}
    messages = {
        domain: (
            marker + ' plain text with punctuation: [a=b] and quotes "kept"',
            json.dumps(
                {
                    "message": marker,
                    "cluster": "payload-only",
                    "appName": "payload-only",
                    "job_id": "payload-only",
                    "node_name": "payload-only",
                    "component": "payload-only",
                    "filename": "payload-only",
                }
            ),
        )
        for domain, marker in markers.items()
    }
    # Repeated source lines are meaningful and must survive without deduplication.
    messages["application"] += (markers["application"] + " repeated",) * 2
    paths = {}
    for domain, bodies in messages.items():
        path = tmp_path / f"{domain}.jsonl"
        path.write_text("\n".join(bodies) + "\n")
        paths[domain] = path
    directory = staging_directory()
    # The disposable nodes live in separate partitions. Schedule each source explicitly through sbatch instead of
    # injecting into a selected container.
    script = (
        f"#!/bin/bash\n#SBATCH --partition={partition}\n#SBATCH --nodelist={node_name}\n"
        + Path(__file__).with_name("job.sh").read_text()
    )
    within = e2e_config.validation_timeout(300)
    ingested_all = tuple(ingested(markers[domain], messages[domain], system=domain == "system") for domain in markers)
    return Scenario(
        name="02-log-ingestion",
        setup=(act("stage messages", staged_files(directory, *paths.values())),),
        job=JobSpec(
            name="log-ingestion-qualification",
            workload={"script": script, "working_directory": JOB_LOG_DIRECTORY},
            environment={
                "APPLICATION_MESSAGES": f"{directory}/{paths['application'].name}",
                "SYSTEM_MESSAGES": f"{directory}/{paths['system'].name}",
            },
            metadata=recovery_metadata(HEALTHY_OBSERVE),
        ),
        stages=(
            eventually(f"running on {node_name}", job_state(JobState.RUNNING), on_nodes(node_name), within=within),
            eventually("ingested", *ingested_all, within=90),
            # Cover the collector's discovery and position-sync cycle after the first match: a slower second reader
            # must not add duplicates.
            consistently("stays exact", *ingested_all, for_seconds=INGESTION_SETTLE_SECONDS),
        ),
    )


@pytest.mark.run_first
@scenario
def test_collector_positions_survive_replacement_and_file_changes(
    e2e_config: E2EConfig, tmp_path: Path
) -> Scenario[E2EServices]:
    """Replace the collector, append, truncate, and rotate only this job's output.

    The job owns all writes. The test releases each phase only after observing
    every earlier marker with its expected count, including after replacement.
    """

    marker = f"HEIMDALL_E2E_POSITIONS_{uuid4().hex}"
    phases = ("initial", "append", "truncate", "rotate")
    messages = {phase: (f"{marker} {phase}",) for phase in phases}
    paths = []
    for phase, bodies in messages.items():
        path = tmp_path / f"{phase}.txt"
        path.write_text("\n".join(bodies) + "\n")
        paths.append(path)
    directory = staging_directory()

    def exactly_ingested(phase: str) -> tuple[Step[E2EServices], ...]:
        """Every marker through `phase`, each exactly once, holding long enough that a second reader cannot pass."""

        written = phases[: phases.index(phase) + 1]
        expected = ingested(marker, tuple(line for done in written for line in messages[done]))
        return (
            eventually(f"{phase} ingested", expected, within=90),
            consistently(f"{phase} stays exact", expected, for_seconds=INGESTION_SETTLE_SECONDS),
        )

    return Scenario(
        name="02-log-ingestion",
        setup=(act("stage messages", staged_files(directory, *paths)),),
        job=JobSpec(
            name="collector-file-lifecycle",
            workload=slurm_batch(Path(__file__).with_name("lifecycle-job.sh")),
            environment={"MESSAGES_DIRECTORY": directory},
            metadata=recovery_metadata(HEALTHY_OBSERVE),
        ),
        stages=(
            eventually("running", job_state(JobState.RUNNING), within=e2e_config.validation_timeout(300)),
            *exactly_ingested("initial"),
            act("replace the collector", restart("alloy-application")),
            *(
                step
                for phase in phases[1:]
                for step in (
                    act(f"release {phase}", touch_file(f"{directory}/{phase}.ready")),
                    *exactly_ingested(phase),
                )
            ),
        ),
    )
