# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Scenario checks: read-only observations of jobs and of Heimdall's evidence through the recovery client."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime

from recovery_libs.testing.launch.jobs import Job, JobState
from recovery_libs.testing.scenario import Check, Evidence, Run

from e2e_harness import audit
from e2e_harness.audit import SubmittedJob
from e2e_harness.dag import advice_state as dag_advice_state
from e2e_harness.dag import dag_dependency_matches, parse_dag_dependencies_response
from e2e_harness.ingestion import evaluate_ingestion_records
from e2e_harness.log_backend import decode_log_response
from e2e_harness.recovery import (
    evaluate_advice_response,
    evaluate_attribution_response,
    evaluate_dag_job_spec_response,
    evaluate_detection_response,
    evaluate_healthy_status_response,
    evaluate_log_response,
    evaluate_no_hardware_attribution_response,
    evaluate_no_recovery_actions_response,
    evaluate_recovery_action_response,
    evaluate_scheduler_state_response,
    evaluate_scheduler_terminal_advice_response,
    evaluate_system_log_response,
    evaluate_terminated_advice_response,
)
from e2e_harness.recovery_client import ApiResponse, RecoveryApiError, RecoveryClient
from e2e_harness.recovery_trace import evaluate_recovery_trace, recovery_context_from_advice
from e2e_harness.services import E2EServices

AuditExpectations = Callable[[SubmittedJob], Sequence[audit.EventExpectation]]

# Text a job writes, fixed or derived from the job (for example, containing its id).
Marker = str | Callable[[Job], str]


def _submitted_at(run: Run[RecoveryClient]) -> str:
    return run.submitted_at.strftime("%Y-%m-%dT%H:%M:%SZ")


def _text(marker: Marker, run: Run[RecoveryClient]) -> str:
    return marker if isinstance(marker, str) else marker(run.job)


def _unless_missing(request: Callable[[], ApiResponse]) -> ApiResponse | None:
    """The response, or None while the service has not recorded the resource yet."""

    try:
        return request()
    except RecoveryApiError as error:
        if error.status_code == 404:
            return None
        raise


def job_state(state: JobState, *, restarts: int | None = None) -> Check[RecoveryClient]:
    """The job is in `state`, after exactly `restarts` restarts when given."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        job = run.job
        held = job.state is state and (restarts is None or job.restart_count == restarts)
        expected = f"{state}" + (f" after {restarts} restarts" if restarts is not None else "")
        return Evidence(
            held, f"job {job.job_id} is {job.state} after {job.restart_count} restarts; expected {expected}"
        )

    return check


def accounted(state: JobState, *, restarts: int) -> Check[RecoveryClient]:
    """Slurm accounting's latest attempt of the job is in `state`, after exactly `restarts` restarts.

    Accounting and the controller can each show an attempt the other does not show yet, such as a requeue-held
    successor, so limit checks read both.
    """

    def check(run: Run[RecoveryClient]) -> Evidence:
        job = run.launcher.history(job_id=run.job.job_id)
        held = job.state is state and job.restart_count == restarts
        return Evidence(
            held,
            f"accounting: job {job.job_id} is {job.state} after {job.restart_count} restarts; "
            f"expected {state} after {restarts} restarts",
        )

    return check


def healthy_status(*, attempt: int = 0) -> Check[RecoveryClient]:
    """Job Manager reports the attempt as healthy, with no anomalies."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = run.services.job_manager.job_status(job_id=run.job.job_id, attempt=attempt)
        evidence = evaluate_healthy_status_response(response.text, job_id=run.job.job_id, attempt=attempt)
        return Evidence(evidence.observed, evidence.summary)

    return check


def scheduler_state(status: str, *, slurm_state: str, attempt: int = 0) -> Check[RecoveryClient]:
    """Job Manager classifies the attempt as `status` from the raw Slurm state, with no anomalies."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.job_status(job_id=run.job.job_id, attempt=attempt))
        if response is None:
            return Evidence(False, f"job {run.job.job_id} attempt {attempt} not recorded yet")
        evidence = evaluate_scheduler_state_response(
            response.text,
            job_id=run.job.job_id,
            attempt=attempt,
            expected_status_state=status,
            expected_slurm_state=slurm_state,
            submitted_at=_submitted_at(run),
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def terminal_advice(*, attempt: int = 0) -> Check[RecoveryClient]:
    """Job Manager has Terminated Advice for the attempt from the Slurm adapter."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.advice(job_id=run.job.job_id, attempt=attempt))
        if response is None:
            return Evidence(False, f"no Advice yet for job {run.job.job_id} attempt {attempt}")
        evidence = evaluate_scheduler_terminal_advice_response(
            response.text, job_id=run.job.job_id, attempt=attempt, submitted_at=_submitted_at(run)
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def on_nodes(*nodes: str) -> Check[RecoveryClient]:
    """The job runs on exactly `nodes`."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        return Evidence(
            tuple(run.job.nodes) == nodes, f"job {run.job.job_id} runs on {list(run.job.nodes)}; expected {list(nodes)}"
        )

    return check


def component_log(deployment: str, *markers: Marker) -> Check[E2EServices]:
    """The deployment's logs since the job was submitted contain every marker."""

    def check(run: Run[E2EServices]) -> Evidence:
        text = run.services.stack.logs(deployment, since=_submitted_at(run))
        wanted = [_text(marker, run) for marker in markers]
        missing = [marker for marker in wanted if marker not in text]
        return Evidence(
            not missing, f"{deployment} logs lack {missing}" if missing else f"{deployment} logged {wanted}"
        )

    return check


def single_node() -> Check[RecoveryClient]:
    """The job runs on exactly one node."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        return Evidence(len(run.job.nodes) == 1, f"job {run.job.job_id} runs on {list(run.job.nodes)}")

    return check


def _attempt_node(run: Run[RecoveryClient], attempt: int) -> str | None:
    """The node Job Manager recorded for a single-node attempt."""

    status = _unless_missing(lambda: run.services.job_manager.job_status(job_id=run.job.job_id, attempt=attempt))
    nodes = status.json().get("allocated_node_list") if status else None
    return nodes[0] if nodes else None


def no_hardware_attribution(*, attempt: int = 0) -> Check[RecoveryClient]:
    """No hardware exclusion was attributed to the node the attempt ran on, as Job Manager recorded it."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        node = _attempt_node(run, attempt)
        if node is None:
            return Evidence(False, f"job {run.job.job_id} attempt {attempt} has no recorded node yet")
        response = run.services.job_manager.error_attributions(job_id=run.job.job_id, attempt=attempt)
        evidence = evaluate_no_hardware_attribution_response(
            response.text, job_id=run.job.job_id, attempt=attempt, node=node
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def no_attribution_exchange(*, attempt: int = 0) -> Check[RecoveryClient]:
    """The anomaly isolator neither forwarded an attribution request nor received a response for the attempt."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        exchanges = [
            event.event_type
            for event in _audit_events(run, attempt)
            if event.event_producer == audit.ADI_PRODUCER
            and event.event_type in ("attribution_request_forwarded", "attribution_response_received")
        ]
        return Evidence(not exchanges, f"attribution exchange events: {exchanges}" if exchanges else "no exchange")

    return check


def deployment_available(deployment: str) -> Check[E2EServices]:
    """Every replica the deployment asks for, at least one, is available."""

    def check(run: Run[E2EServices]) -> Evidence:
        resource = run.services.stack.get("deployment", deployment)
        desired = resource["spec"].get("replicas", 0)
        available = resource.get("status", {}).get("availableReplicas", 0)
        return Evidence(desired > 0 and available == desired, f"{deployment}: {available} of {desired} available")

    return check


def hardware_attribution(exclusion_type: str, *, signature: str = "", attempt: int = 0) -> Check[RecoveryClient]:
    """Attribution excluded the attempt's node as `exclusion_type`, with `signature` when given."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        node = _attempt_node(run, attempt)
        if node is None:
            return Evidence(False, f"job {run.job.job_id} attempt {attempt} has no recorded node yet")
        response = run.services.job_manager.error_attributions(job_id=run.job.job_id, attempt=attempt)
        evidence = evaluate_attribution_response(
            response.text,
            job_id=run.job.job_id,
            attempt=attempt,
            node=node,
            exclusion_type=exclusion_type,
            signature=signature,
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def recovered_away_from_initial_node() -> Check[RecoveryClient]:
    """The restarted job runs on another node, and Slurm excludes the node attempt 0 ran on."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        initial = _attempt_node(run, 0)
        excluded = run.job.metadata.get("excluded_nodes") or ""
        held = (
            initial is not None
            and run.job.restart_count >= 1
            and run.job.state is JobState.RUNNING
            and initial not in run.job.nodes
            and initial in excluded.split(",")
        )
        return Evidence(
            held,
            f"job {run.job.job_id} attempt {run.job.restart_count} runs on {list(run.job.nodes)}, "
            f"excludes {excluded or 'none'}; initial node {initial}",
        )

    return check


def system_log_line(marker: str, *, attempt: int = 0) -> Check[RecoveryClient]:
    """The log backend collected `marker` from the system journal of the node the attempt ran on."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        node = _attempt_node(run, attempt)
        if node is None:
            return Evidence(False, f"job {run.job.job_id} attempt {attempt} has no recorded node yet")
        logs = run.services.logs
        response = logs.query(
            labels={"cluster": "slurm", "component": "journal"},
            contains=marker,
            start=_submitted_at(run),
            end=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        evidence = evaluate_system_log_response(
            response.text, node_name=node, marker=marker, submitted_at=_submitted_at(run), backend=logs.backend
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def held() -> Check[RecoveryClient]:
    """Slurm is holding the job: it stays pending until released."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        record = run.job.metadata
        is_held = run.job.state is JobState.PENDING and record.get("hold") is True
        return Evidence(
            is_held,
            f"job {run.job.job_id} is {run.job.state} hold={record.get('hold')} reason={record.get('state_reason')}",
        )

    return check


def started_after(role: str, *predecessors: str) -> Check[RecoveryClient]:
    """The job launched as `role` started no earlier than every predecessor ended."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        job = run.jobs[role]
        ends = {name: run.jobs[name].ended_at for name in predecessors}
        if job.started_at is None or any(end is None for end in ends.values()):
            return Evidence(False, f"{role} started at {job.started_at}; predecessors ended at {ends}")
        early = [name for name, end in ends.items() if job.started_at < end]
        return Evidence(not early, f"{role} started at {job.started_at:%H:%M:%S}; before {early or 'none'} ended")

    return check


def restarted(*, times: int = 1) -> Check[RecoveryClient]:
    """Slurm restarted the job `times` times and the latest attempt has started.

    Slurm resets the submit time on requeue, so a start at or after it belongs to the latest attempt; a successor
    cancelled before it ran has no such start.
    """

    def check(run: Run[RecoveryClient]) -> Evidence:
        job = run.job
        started = job.started_at is not None and job.submitted_at is not None and job.started_at >= job.submitted_at
        held = job.restart_count == times and job.state is not JobState.PENDING and started
        return Evidence(
            held,
            f"job {job.job_id} is {job.state} after {job.restart_count} restarts, submitted {job.submitted_at}, "
            f"started {job.started_at}; expected {times} restarts and a start",
        )

    return check


def state_based_crash(*, attempt: int = 0) -> Check[RecoveryClient]:
    """Job Manager recorded a CRASH from the job's failed state, not from a Watcher log event."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.job_status(job_id=run.job.job_id, attempt=attempt))
        if response is None:
            return Evidence(False, f"job {run.job.job_id} attempt {attempt} not recorded yet")
        evidence = evaluate_detection_response(
            response.text,
            job_id=run.job.job_id,
            attempt=attempt,
            anomaly_type="CRASH",
            marker="Watcher marker must not be present",
            submitted_at=_submitted_at(run),
        )
        return Evidence(evidence.state_based_recovery_observed, evidence.summary)

    return check


def log_line(marker: Marker) -> Check[RecoveryClient]:
    """The log backend collected the job's line containing `marker`, labelled with the job and its file."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        text = _text(marker, run)
        logs = run.services.logs
        response = logs.query(
            labels={"cluster": "slurm", "appName": "slurm-app-log", "job_id": run.job.job_id},
            contains=text,
            start=_submitted_at(run),
            end=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        evidence = evaluate_log_response(
            response.text, job_id=run.job.job_id, marker=text, submitted_at=_submitted_at(run), backend=logs.backend
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def ingested(marker: str, messages: Sequence[str], *, system: bool = False) -> Check[RecoveryClient]:
    """The log backend holds exactly `messages` for `marker`, from the job's output or its node's syslog.

    Each message must appear as often as it was written, with the job's or node's collection labels.
    """

    def check(run: Run[RecoveryClient]) -> Evidence:
        logs = run.services.logs
        node = run.job.nodes[0] if system and run.job.nodes else None
        labels = (
            {"cluster": "slurm", "component": "journal"}
            if system
            else {"cluster": "slurm", "appName": "slurm-app-log", "job_id": run.job.job_id}
        )
        response = logs.query(
            labels=labels,
            contains=marker,
            start=_submitted_at(run),
            end=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        try:
            records = decode_log_response(response.text, logs.backend)
        except ValueError as error:
            return Evidence(False, str(error))
        evidence = evaluate_ingestion_records(
            records,
            messages=tuple(messages),
            submitted_at=_submitted_at(run),
            job_id=None if system else run.job.job_id,
            node_name=node,
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def watcher_anomaly(anomaly_type: str, *, marker: Marker, attempt: int = 0) -> Check[RecoveryClient]:
    """Job Manager recorded a Watcher `anomaly_type` whose source event contains `marker`."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.job_status(job_id=run.job.job_id, attempt=attempt))
        if response is None:
            return Evidence(False, f"job {run.job.job_id} attempt {attempt} not recorded yet")
        evidence = evaluate_detection_response(
            response.text,
            job_id=run.job.job_id,
            attempt=attempt,
            anomaly_type=anomaly_type,
            marker=_text(marker, run),
            submitted_at=_submitted_at(run),
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def advice(
    *,
    issue_type: str,
    marker: Marker,
    source: str,
    state: str | None = "Restarted",
    attempt: int = 0,
) -> Check[RecoveryClient]:
    """Job Manager has Advice for the attempt with the expected issue, source, and recommendation."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.advice(job_id=run.job.job_id, attempt=attempt))
        if response is None:
            return Evidence(False, f"no Advice yet for job {run.job.job_id} attempt {attempt}")
        evidence = evaluate_advice_response(
            response.text,
            job_id=run.job.job_id,
            attempt=attempt,
            issue_type=issue_type,
            marker=_text(marker, run),
            submitted_at=_submitted_at(run),
            expected_state=state,
            expected_source=source,
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def requeue_notified(*, anomaly_type: str, attempt: int = 0) -> Check[RecoveryClient]:
    """Notifications recorded a JobRequeued action for the attempt."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = run.services.notifications.notifications(job_id=run.job.job_id, created_after=_submitted_at(run))
        evidence = evaluate_recovery_action_response(
            response.text,
            job_id=run.job.job_id,
            attempt=attempt,
            anomaly_type=anomaly_type,
            submitted_at=_submitted_at(run),
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def recorded_attempt(
    *, attempt: int = 0, state: str | None = None, checkpointed: bool | None = None
) -> Check[RecoveryClient]:
    """Job Manager records the attempt, in `state` when set, and with a saved checkpoint exactly when
    `checkpointed` is set."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.job_status(job_id=run.job.job_id, attempt=attempt))
        if response is None:
            return Evidence(False, f"job {run.job.job_id} attempt {attempt} not recorded yet")
        status = response.json()
        has_checkpoint = bool(status.get("checkpoints"))
        held = state in (None, status.get("state")) and checkpointed in (None, has_checkpoint)
        return Evidence(
            held,
            f"attempt {attempt} is {status.get('state')} with checkpoint={has_checkpoint}; expected {state or 'any'}",
        )

    return check


def terminated_advice(*, attempt: int) -> Check[RecoveryClient]:
    """Job Manager has Terminated Advice for the attempt, such as when it reaches the requeue limit."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.advice(job_id=run.job.job_id, attempt=attempt))
        if response is None:
            return Evidence(False, f"no Advice yet for job {run.job.job_id} attempt {attempt}")
        evidence = evaluate_terminated_advice_response(
            response.text, job_id=run.job.job_id, attempt=attempt, submitted_at=_submitted_at(run)
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def advice_reason(text: str, *, attempt: int) -> Check[RecoveryClient]:
    """An Advice created or updated for the attempt gives a reason containing `text`."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        reasons = [
            reason
            for event in _audit_events(run, attempt)
            if event.event_type in ("advice_created", "advice_updated")
            for reason in event.payload.get("creation_reasons", event.payload.get("change_reasons", []))
        ]
        return Evidence(any(text in reason for reason in reasons), f"attempt {attempt} Advice reasons: {reasons}")

    return check


Gate = tuple[str, Callable[[Run[RecoveryClient]], str]]
"""A DAG dependency as Job Manager reports it: its target type and how to find its identifier in the run."""


def job_id_of(role: str) -> Callable[[Run[RecoveryClient]], str]:
    """The job ID of the job launched as `role`, read when a check runs."""

    return lambda run: run.jobs[role].job_id


def gate_on_job(role: str) -> Gate:
    """A dependency on the job launched as `role`, by job ID."""

    return ("id", job_id_of(role))


def gate_on_name(name: str) -> Gate:
    """A dependency on jobs named `name`."""

    return ("name", lambda run: name)


def gates(
    *expected: Gate,
    status: str,
    reason_code: str | None = None,
    reason_details: Mapping[str, object | Callable[[Run[RecoveryClient]], object]] | None = None,
) -> Check[RecoveryClient]:
    """Job Manager reports exactly the expected dependencies, each with `status` and the optional reason.

    A `reason_details` value may be a function of the run, such as `job_id_of("parent")`.
    """

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = run.services.job_manager.dag_dependencies(dependent_job_id=run.job.job_id)
        rows = parse_dag_dependencies_response(response.text)
        wanted = {(kind, identifier(run)) for kind, identifier in expected}
        details = (
            {key: value(run) if callable(value) else value for key, value in reason_details.items()}
            if reason_details
            else None
        )
        matching = {
            (str(row.get("dependency_job_identifier_type")), str(row.get("dependency_job_identifier")))
            for row in rows
            if dag_dependency_matches(row, action_status=status, reason_code=reason_code, reason_details=details)
        }
        observed = [
            (row.get("dependency_job_identifier"), row.get("action_status"), row.get("reason_code")) for row in rows
        ]
        return Evidence(
            len(rows) == len(wanted) and matching == wanted,
            f"job {run.job.job_id} gates {observed}; expected {sorted(wanted)} {status} {reason_code or ''}".strip(),
        )

    return check


def advice_state(state: str, *, attempt: int = 0) -> Check[RecoveryClient]:
    """Job Manager's Advice for the attempt recommends `state`, such as Released or Terminated, and is the Advice
    for that attempt (its `restartCount`)."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.advice(job_id=run.job.job_id, attempt=attempt))
        payload = response.json() if response else None
        observed = dag_advice_state(payload)
        restart_count = payload.get("restartCount") if payload else None
        return Evidence(
            observed == state and restart_count == attempt,
            f"job {run.job.job_id} attempt {attempt} Advice is {observed} for restartCount {restart_count}",
        )

    return check


def advice_identity(role: str, *, attempt: int = 0) -> Callable[[Run[RecoveryClient]], object]:
    """Read the ID, state, and issues of the Advice for the job launched as `role`; use with `remember`."""

    def read(run: Run[RecoveryClient]) -> object:
        job_id = run.jobs[role].job_id
        response = _unless_missing(lambda: run.services.job_manager.advice(job_id=job_id, attempt=attempt))
        payload = response.json() if response else None
        if payload is None:
            return (None, None, None)
        return (payload.get("id"), dag_advice_state(payload), payload.get("issues"))

    return read


def advice_unchanged(baseline: str, *, attempt: int = 0) -> Check[RecoveryClient]:
    """The Advice for the attempt has the ID, state, and issues remembered as `baseline`."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        observed = advice_identity(run.role, attempt=attempt)(run)
        expected = run.notes[baseline]
        return Evidence(
            observed == expected, f"job {run.job.job_id} attempt {attempt} Advice is {observed}; expected {expected}"
        )

    return check


def advice_converged(*, attempt: int = 0) -> Check[RecoveryClient]:
    """The controller reported to Job Manager that it observed the current Advice for the attempt as satisfied.

    Job Manager reports reconciliation status only for the Advice's current generation, so a changed
    recommendation reads as not converged until the controller observes it.
    """

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.advice(job_id=run.job.job_id, attempt=attempt))
        payload = response.json() if response else {}
        status = payload.get("reconciliationStatus") or {}
        return Evidence(
            status.get("disposition") == "CONVERGED",
            f"job {run.job.job_id} attempt {attempt} Advice generation {payload.get('generation')} reconciliation "
            f"is {status.get('disposition')} ({status.get('reason')})",
        )

    return check


def no_advice_except(*allowed: str, attempt: int = 0) -> Check[RecoveryClient]:
    """Job Manager has no Advice for the attempt, or only Advice in one of the `allowed` states."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.advice(job_id=run.job.job_id, attempt=attempt))
        observed = dag_advice_state(response.json()) if response else None
        return Evidence(
            observed is None or observed in allowed,
            f"job {run.job.job_id} attempt {attempt} Advice is {observed}; allowed {list(allowed) or 'none'}",
        )

    return check


LIFECYCLE_ADVICE_STATES = frozenset(("Released", "Restarted", "Terminated"))


def no_lifecycle_advice(*, attempt: int = 0) -> Check[RecoveryClient]:
    """Job Manager has not advised releasing, restarting, or terminating the attempt."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.advice(job_id=run.job.job_id, attempt=attempt))
        observed = dag_advice_state(response.json()) if response else None
        return Evidence(
            observed not in LIFECYCLE_ADVICE_STATES, f"job {run.job.job_id} attempt {attempt} Advice is {observed}"
        )

    return check


def dag_job_spec(*dependencies: Gate, action: str) -> Check[RecoveryClient]:
    """Job Manager stored the job's DAG dependencies and action exactly as submitted."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = run.services.job_manager.jobs(job_id=run.job.job_id)
        evidence = evaluate_dag_job_spec_response(
            response.text,
            job_id=run.job.job_id,
            attempt=0,
            expected_dependencies={(kind, identifier(run)) for kind, identifier in dependencies},
            expected_action=action,
            submitted_at=_submitted_at(run),
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


def aps_spec_lacks(key: str, *, attempt: int = 0) -> Check[RecoveryClient]:
    """Job Manager's record of the attempt has no `key` in its APS spec."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        status = _unless_missing(lambda: run.services.job_manager.job_status(job_id=run.job.job_id, attempt=attempt))
        if status is None:
            return Evidence(False, f"job {run.job.job_id} attempt {attempt} not recorded yet")
        spec = status.json().get("aps_spec") or {}
        return Evidence(key not in spec, f"job {run.job.job_id} APS spec keys: {sorted(spec)}")

    return check


def no_advice(*, attempt: int = 0) -> Check[RecoveryClient]:
    """Job Manager has no Advice for the attempt."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = _unless_missing(lambda: run.services.job_manager.advice(job_id=run.job.job_id, attempt=attempt))
        if response is None:
            return Evidence(True, f"no Advice for job {run.job.job_id} attempt {attempt}")
        return Evidence(False, f"unexpected Advice for job {run.job.job_id}: {response.text[:500]}")

    return check


def no_recovery_actions() -> Check[RecoveryClient]:
    """Notifications recorded no recovery action for the job."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        response = run.services.notifications.notifications(job_id=run.job.job_id, created_after=_submitted_at(run))
        evidence = evaluate_no_recovery_actions_response(response.text, job_id=run.job.job_id)
        return Evidence(evidence.observed, evidence.summary)

    return check


def audit_trail(expectations: AuditExpectations, *, attempt: int = 0) -> Check[RecoveryClient]:
    """The attempt's audit trail contains the expected events and no malformed ones."""

    def check(run: Run[RecoveryClient]) -> Evidence:
        evaluation = audit.evaluate_audit_trail(_audit_events(run, attempt), expectations(_audit_job(run)))
        return Evidence(evaluation.satisfied, evaluation.summary)

    return check


def gateway_audit_matches() -> Check[RecoveryClient]:
    """The public gateway returns every persisted event for attempt zero, the resumed attempt, and the whole job.

    Its eventType filter must also return the attempt's persisted anomaly.
    """

    def check(run: Run[RecoveryClient]) -> Evidence:
        zero = _audit_events(run, 0)
        one = _audit_events(run, 1)
        resumed = audit.evaluate_audit_trail(one, (audit.job_observed_event(_audit_job(run), attempt=1),))
        anomalies = tuple(event for event in zero if event.event_type == "anomaly_detected")
        if not resumed.satisfied or not anomalies:
            return Evidence(False, f"waiting for persisted recovery: {resumed.summary}; anomalies={len(anomalies)}")
        gateway_zero = _gateway_events(run, attempt=0)
        gateway_one = _gateway_events(run, attempt=1)
        comparisons = {
            "attempt 0": (zero, gateway_zero),
            "attempt 1": (one, gateway_one),
            "whole job": ((*gateway_zero, *gateway_one), _gateway_events(run, attempt=None)),
            "eventType=anomaly_detected": (anomalies, _gateway_events(run, attempt=0, event_type="anomaly_detected")),
        }
        omitted = [name for name, (expected, actual) in comparisons.items() if any(e not in actual for e in expected)]
        if omitted:
            return Evidence(False, f"gateway omitted persisted events for: {', '.join(omitted)}")
        return Evidence(True, f"gateway returned all persisted events for job {run.job.job_id}")

    return check


def recovery_trace(*, require_attribution: bool, operation: str = "restart", attempt: int = 0) -> Check[RecoveryClient]:
    """Tempo holds the attempt's backend-started recovery trace, and its release matches the audit trail.

    The canonical trace comes from the attempt's Advice, never from a sequence the test creates. Trace export and
    audit delivery are independent, so each poll rereads both.
    """

    def check(run: Run[RecoveryClient]) -> Evidence:
        tempo = run.services.tempo
        if tempo is None:
            return Evidence(False, "recovery trace evidence needs E2E_TEMPO_SERVICE or E2E_TEMPO_URL")
        job_id = run.job.job_id
        advice = _unless_missing(lambda: run.services.job_manager.advice(job_id=job_id, attempt=attempt))
        if advice is None:
            return Evidence(False, f"no Advice yet for job {job_id} attempt {attempt}")
        context = recovery_context_from_advice(advice.text, job_id=job_id, attempt=attempt, operation=operation)
        # Tempo answers 404 until the trace's spans have been ingested.
        trace = _unless_missing(lambda: tempo.trace(trace_id=context.trace_id))
        if trace is None:
            return Evidence(False, f"Tempo has no trace {context.trace_id} yet")
        evidence = evaluate_recovery_trace(
            trace.text,
            context=context,
            job_id=job_id,
            attempt=attempt,
            operation=operation,
            require_attribution=require_attribution,
            audit_events=_audit_events(run, attempt),
        )
        return Evidence(evidence.observed, evidence.summary)

    return check


OTLP_SERVICE_LOGS = (
    # Job Manager (Python exporter) names the job only in its message text; the comma ends the ID.
    ("job_manager", "Upserting job: job_id={job_id},", {}),
    # The controller (Go exporter) attaches the job as a structured field.
    ("slurm-advisor-controller", "", {"job_id": "{job_id}"}),
)
"""Per service: its OTLP `service.name`, a message substring, and structured fields that place a record on the job."""


def service_logs_exported() -> Check[RecoveryClient]:
    """Job Manager and the controller exported logs about this job to the log store through the shared OTLP endpoint.

    Each service reaches the store only through its own OTLP log exporter and the collector, so a record proves that
    path end to end, including the bearer token. Both languages' exporters are covered.
    """

    def check(run: Run[RecoveryClient]) -> Evidence:
        logs = run.services.logs
        # Loki indexes the OTLP resource attribute `service.name` as `service_name`; VictoriaLogs keeps its name.
        service_label = "service_name" if logs.backend == "loki" else "service.name"
        job_id = run.job.job_id
        missing: list[str] = []
        for service, contains, fields in OTLP_SERVICE_LOGS:
            response = logs.query(
                labels={service_label: service},
                contains=contains.format(job_id=job_id),
                fields={key: value.format(job_id=job_id) for key, value in fields.items()},
                start=_submitted_at(run),
                end=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            try:
                records = decode_log_response(response.text, logs.backend)
            except ValueError as error:
                return Evidence(False, str(error))
            if not records:
                missing.append(service)
        if missing:
            return Evidence(False, f"no OTLP logs for job {job_id} yet from: {', '.join(missing)}")
        return Evidence(True, f"Job Manager and controller logs for job {job_id} reached {logs.backend} through OTLP")

    return check


def _audit_job(run: Run[RecoveryClient]) -> SubmittedJob:
    return SubmittedJob(run.job.job_id, _submitted_at(run))


def _gateway_events(
    run: Run[RecoveryClient], *, attempt: int | None, event_type: str | None = None
) -> tuple[audit.ObservedAuditEvent, ...]:
    """Read every page from the public gateway; a response for another job, attempt, or type is a defect."""

    job_id = run.job.job_id
    events: list[audit.ObservedAuditEvent] = []
    seen: set[str] = set()
    cursor = ""
    for _ in range(audit.AUDIT_MAX_PAGES):
        page = run.services.gateway.audit_events(
            job_id=job_id, limit=audit.AUDIT_PAGE_LIMIT, attempt=attempt, event_type=event_type, cursor=cursor
        ).json()
        for item in page["items"]:
            event = audit.ObservedAuditEvent(
                heimdall_job_id=item["heimdallJobId"],
                event_producer=item["eventProducer"],
                event_type=item["eventType"],
                event_level=item["eventLevel"],
                payload=item["payload"],
                event_produced_at=int(item["eventProducedAt"]),
            )
            assert event.payload.get("job_id") == job_id, "gateway returned another scheduler job"
            if attempt is not None:
                assert event.heimdall_job_id == audit.heimdall_job_id(job_id, attempt), (
                    "gateway returned another attempt"
                )
            if event_type is not None:
                assert event.event_type == event_type, "gateway ignored the eventType filter"
            events.append(event)
        cursor = page["cursor"]["nextCursor"]
        if not cursor:
            return tuple(events)
        assert cursor not in seen, "gateway repeated an audit cursor"
        seen.add(cursor)
    raise AssertionError(f"gateway audit pagination exceeded {audit.AUDIT_MAX_PAGES} pages for job {job_id}")


def _audit_events(run: Run[RecoveryClient], attempt: int) -> tuple[audit.ObservedAuditEvent, ...]:
    query = {
        "heimdall_job_id": audit.heimdall_job_id(run.job.job_id, attempt),
        "min_event_produced_at": int(run.submitted_at.timestamp() * 1_000_000),
        "limit": audit.AUDIT_PAGE_LIMIT,
    }
    events: list[audit.ObservedAuditEvent] = []
    cursor = ""
    for _ in range(audit.AUDIT_MAX_PAGES):
        page = run.services.notifications.audit_events(**query, cursor=cursor).json()
        events.extend(
            audit.ObservedAuditEvent(
                heimdall_job_id=item["heimdall_job_id"],
                event_producer=item["event_producer"],
                event_type=item["event_type"],
                event_level=item["event_level"],
                payload=item["payload"],
                event_produced_at=int(item["event_produced_at"]),
            )
            for item in page["items"]
        )
        cursor = page["cursor"]["next_cursor"]
        if not cursor:
            return tuple(events)
    raise AssertionError(f"audit pagination exceeded {audit.AUDIT_MAX_PAGES} pages for job {run.job.job_id}")
