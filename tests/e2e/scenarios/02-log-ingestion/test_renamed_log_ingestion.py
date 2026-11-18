# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Collect arbitrary, colliding stdout basenames through named links and the normal log pipeline."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from e2e_harness.actions import shared_directory
from e2e_harness.checks import job_state
from e2e_harness.config import E2EConfig
from e2e_harness.dag import parse_job_status_response
from e2e_harness.ingestion import evaluate_ingestion_records
from e2e_harness.log_backend import decode_log_response
from e2e_harness.services import E2EServices
from e2e_harness.workloads import HEALTHY_OBSERVE, JOB_LOG_DIRECTORY, recovery_metadata
from recovery_libs.testing.kubernetes import StackControl
from recovery_libs.testing.launch.jobs import JobSpec, JobState
from recovery_libs.testing.scenario import (
    Action,
    Check,
    Evidence,
    Run,
    Scenario,
    act,
    consistently,
    eventually,
    on,
    unsafe,
)
from recovery_libs.testing.scenario_plugin import scenario

pytestmark = pytest.mark.exclusive


@unsafe
def _enable_renamed_links(stack: StackControl) -> Action[E2EServices]:
    """Temporarily enable the controller's normal rename option and restore its exact prior environment."""

    deployment = "slurm-advisor-controller"
    option = "SLURM_ADVISOR_LOG_LINKS_RENAME_ENABLED"

    def action(run: Run[E2EServices]) -> Run[E2EServices]:
        containers = stack.get("deployment", deployment)["spec"]["template"]["spec"]["containers"]
        container = next(item for item in containers if item["name"] == deployment)
        original_env = deepcopy(container.get("env"))

        def apply(environment: list[dict[str, object]] | None) -> None:
            current = stack.get("deployment", deployment)["spec"]["template"]["spec"]["containers"]
            controller = next(item for item in current if item["name"] == deployment)
            if environment is None:
                controller.pop("env", None)
            else:
                controller["env"] = deepcopy(environment)
            stack.patch("deployment", deployment, {"spec": {"template": {"spec": {"containers": current}}}})
            stack.restart(deployment, wait_for_old_pods=True)

        enabled = [item for item in original_env or [] if item["name"] != option]
        enabled.append({"name": option, "value": "true"})
        try:
            apply(enabled)
        except BaseException as error:
            try:
                apply(original_env)
            except Exception as rollback_error:  # noqa: BLE001
                error.add_note(f"restoring controller environment also failed: {rollback_error!r}")
            raise
        return run.with_cleanup("restore log-link naming", lambda: apply(original_env))

    return action


def _renamed_log_ingested(marker: str, source: str) -> Check[E2EServices]:
    """Require the actual collected filename to be published for this job and mapped to its source."""

    def check(run: Run[E2EServices]) -> Evidence:
        submitted = run.submitted_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        logs = run.services.logs
        # Do not filter by job_id: a collector that extracts the wrong identity must fail this check.
        response = logs.query(
            labels={"cluster": "slurm", "appName": "slurm-app-log"},
            contains=marker,
            start=submitted,
            end=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        records = decode_log_response(response.text, logs.backend)
        ingestion = evaluate_ingestion_records(
            records, messages=(marker,), submitted_at=submitted, job_id=run.job.job_id
        )
        if not ingestion.observed:
            return Evidence(False, ingestion.summary)
        status = parse_job_status_response(
            run.services.job_manager.jobs(job_id=run.job.job_id).text, job_id=run.job.job_id, attempt=0
        )
        metadata = status.get("scheduler_metadata", status.get("schedulerMetadata", {}))
        filename = records[0].labels["filename"]
        mapping = metadata.get("app_log_link_targets", {})
        matched = (
            filename != source
            and filename in (status.get("log_file_paths") or [])
            and filename in metadata.get("slurm_log_file_paths", [])
            and mapping.get(filename) == source
        )
        return Evidence(matched, f"collected {filename}; published source mapping is {mapping}")

    return check


@scenario
def test_same_arbitrary_basename_is_collected_for_each_job(
    e2e_config: E2EConfig, slurm_stack: StackControl
) -> Scenario[E2EServices]:
    """Two jobs own extensionless output files in different directories; both reach the store with their identity.

    The exclusive scenario enables renaming through the production Deployment environment for its duration.
    No log-store push or fabricated job metadata can bypass discovery, materialization, or collection.
    """

    directory = f"{JOB_LOG_DIRECTORY}/rename-{uuid4().hex}"
    roles = ("first", "second")
    sources = {role: f"{directory}-{role}/output" for role in roles}
    markers = {role: f"HEIMDALL_E2E_RENAMED_{uuid4().hex}" for role in roles}
    body = Path(__file__).with_name("renamed-job.sh").read_text()
    checks = tuple(on(role, _renamed_log_ingested(markers[role], sources[role])) for role in roles)
    return Scenario(
        name="02-log-ingestion",
        setup=(
            act("enable deterministic log names", _enable_renamed_links(slurm_stack)),
            *(act(f"prepare {role} output directory", shared_directory(f"{directory}-{role}")) for role in roles),
        ),
        jobs={
            role: JobSpec(
                name=f"renamed-{role}",
                workload={
                    "script": f"#!/bin/bash\n#SBATCH --output={sources[role]}\n{body}",
                    "working_directory": JOB_LOG_DIRECTORY,
                },
                environment={"MARKER": markers[role]},
                metadata=recovery_metadata(HEALTHY_OBSERVE),
            )
            for role in roles
        },
        stages=(
            eventually(
                "both jobs running",
                *(on(role, job_state(JobState.RUNNING)) for role in roles),
                within=e2e_config.validation_timeout(300),
            ),
            eventually("named logs ingested and published", *checks, within=90),
            consistently("each marker remains exactly once", *checks, for_seconds=15),
        ),
    )
