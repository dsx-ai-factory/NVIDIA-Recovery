# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Stateless observations from the production recovery stack."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from e2e_harness.log_backend import decode_log_response

EVIDENCE_TEXT_LIMIT = 2_048
TIMESTAMP_PRECISION_TOLERANCE = timedelta(seconds=1)
EVIDENCE_VALUE_LIMIT = 256
EVIDENCE_DISTINCT_LIMIT = 8
SYSTEM_LOG_FORBIDDEN_LABELS = frozenset(
    (
        "appName",
        "job_id",
        "slurm_job_id",
        "job_attempt",
        "job_attempt_index",
        "slurm_job_attempt",
        "workflow_id",
    )
)


def _bounded_text(value: str) -> str:
    text = value.strip()
    if not text:
        return "<none>"
    if len(text) <= EVIDENCE_TEXT_LIMIT:
        return text
    suffix = "... (truncated)"
    return text[: EVIDENCE_TEXT_LIMIT - len(suffix)] + suffix


def _joined_values(pairs: list[tuple[str, str]], index: int) -> str:
    distinct = sorted({pair[index] for pair in pairs if pair[index]})
    values = [value[:EVIDENCE_VALUE_LIMIT] for value in distinct[:EVIDENCE_DISTINCT_LIMIT]]
    if len(distinct) > EVIDENCE_DISTINCT_LIMIT:
        values.append(f"...(+{len(distinct) - EVIDENCE_DISTINCT_LIMIT})")
    return "|".join(values) or "<none>"


@dataclass(frozen=True, slots=True)
class WatcherEvidence:
    observed: bool
    state_based_recovery_observed: bool
    summary: str
    observed_at: str = ""


@dataclass(frozen=True, slots=True)
class AttributionEvidence:
    observed: bool
    summary: str


@dataclass(frozen=True, slots=True)
class CausalEvidence:
    observed: bool
    summary: str
    observed_at: str = ""


def _parse_utc_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _is_current_timestamp(value: object, submitted_at: str) -> bool:
    observed = _parse_utc_timestamp(value)
    submitted = _parse_utc_timestamp(submitted_at)
    return observed is not None and submitted is not None and observed >= submitted


def evaluate_detection_response(
    response_text: str,
    *,
    job_id: str,
    attempt: int,
    anomaly_type: str,
    marker: str,
    submitted_at: str,
) -> WatcherEvidence:
    """Evaluate durable Job Manager evidence for a Watcher-sourced anomaly."""

    try:
        payload = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        return WatcherEvidence(False, False, "Job Manager response is not valid JSON")
    if not isinstance(payload, dict):
        return WatcherEvidence(False, False, "Job Manager response is not an object")
    if payload.get("job_id") != job_id or type(payload.get("job_attempt_index")) is not int:
        return WatcherEvidence(False, False, "Job Manager response has the wrong job identity")
    if payload["job_attempt_index"] != attempt:
        return WatcherEvidence(False, False, "Job Manager response has the wrong attempt")

    anomalies = payload.get("anomalies")
    if not isinstance(anomalies, list):
        return WatcherEvidence(False, False, "Job Manager response has no anomaly list")

    state_based = False
    state_based_at = ""
    rejected: list[str] = []
    for anomaly in anomalies:
        if not isinstance(anomaly, dict) or anomaly.get("anomaly_type") != anomaly_type:
            continue
        if not _is_current_timestamp(anomaly.get("timestamp"), submitted_at):
            rejected.append("stale anomaly")
            continue
        raw_event = anomaly.get("original_watcher_anomaly_event")
        if raw_event is None:
            state_based = True
            state_based_at = str(anomaly["timestamp"])
            rejected.append("job-state-based anomaly")
            continue
        if not isinstance(raw_event, str):
            rejected.append("invalid Watcher event")
            continue
        try:
            event = json.loads(raw_event)
        except json.JSONDecodeError:
            rejected.append("malformed Watcher event")
            continue
        attributes = event.get("attributes") if isinstance(event, dict) else None
        if not isinstance(attributes, dict) or attributes.get("job_id") != job_id:
            rejected.append("Watcher event has wrong job")
            continue
        if event.get("type") != anomaly_type:
            rejected.append("Watcher event has wrong type")
            continue
        if attributes.get("job_manager_generated") is True:
            # Job Manager also persists serialized events for state-based anomalies.
            # They establish state detection, never collection by the Watcher.
            event_attempt = attributes.get("slurm_job_attempt")
            if type(event_attempt) is not int or event_attempt != attempt:
                rejected.append("job-state event has wrong attempt")
                continue
            state_based = True
            state_based_at = str(anomaly["timestamp"])
            rejected.append("job-state-based anomaly")
            continue
        body = event.get("body")
        if not isinstance(body, str) or marker not in body:
            rejected.append("Watcher event lacks marker")
            continue
        summary = f"Watcher {anomaly_type} persisted for job_id={job_id} attempt={attempt} marker={marker}"
        return WatcherEvidence(True, state_based, _bounded_text(summary), str(anomaly["timestamp"]))

    details = "|".join(sorted(set(rejected))) or "matching anomaly absent"
    summary = f"Watcher evidence missing for job_id={job_id} attempt={attempt}: {details}"
    return WatcherEvidence(False, state_based, _bounded_text(summary), state_based_at)


def evaluate_advice_response(
    response_text: str,
    *,
    job_id: str,
    attempt: int,
    issue_type: str,
    marker: str,
    submitted_at: str,
    expected_state: str | None = "Restarted",
    expected_source: str = "Heimdall Watcher",
) -> CausalEvidence:
    """Evaluate exact-attempt Advice source and recommendation evidence."""

    try:
        payload = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        return CausalEvidence(False, "Advice response is not valid JSON")
    if not isinstance(payload, dict):
        return CausalEvidence(False, "Advice response is not an object")
    if payload.get("jobId") != job_id or type(payload.get("restartCount")) is not int:
        return CausalEvidence(False, "Advice response has the wrong job identity")
    if payload["restartCount"] != attempt:
        return CausalEvidence(False, "Advice response has the wrong attempt")
    recommendation = payload.get("advice")
    if not isinstance(recommendation, dict):
        return CausalEvidence(False, "Advice response has no recommendation")
    if expected_state is not None and recommendation.get("state") != expected_state:
        return CausalEvidence(False, f"Advice does not recommend {expected_state}")
    issues = payload.get("issues")
    if not isinstance(issues, list):
        return CausalEvidence(False, "Advice response has no issue list")
    for issue in issues:
        if not isinstance(issue, dict) or issue.get("issueType") != issue_type:
            continue
        if issue.get("detectedBy") != expected_source:
            continue
        if not _is_current_timestamp(issue.get("timestamp"), submitted_at):
            continue
        lines = issue.get("relevantLogLines")
        if not isinstance(lines, list) or not any(isinstance(line, str) and marker in line for line in lines):
            continue
        state = expected_state or "source-only"
        summary = f"Advice {issue_type}/{state} observed for job_id={job_id} attempt={attempt} marker={marker}"
        observed_at = payload.get("desiredStateChangedAt") if expected_state is not None else issue.get("timestamp")
        if not _is_current_timestamp(observed_at, submitted_at):
            return CausalEvidence(False, "Advice update timestamp is missing or stale")
        return CausalEvidence(True, _bounded_text(summary), str(observed_at))
    return CausalEvidence(
        False,
        f"Advice lacks current {expected_source} {issue_type} issue containing marker={marker}",
    )


def evaluate_scheduler_terminal_advice_response(
    response_text: str,
    *,
    job_id: str,
    attempt: int,
    submitted_at: str,
) -> CausalEvidence:
    """Evaluate exact-attempt terminal Advice caused by a scheduler boundary."""

    try:
        payload = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        return CausalEvidence(False, "terminal Advice response is not valid JSON")
    if not isinstance(payload, dict):
        return CausalEvidence(False, "terminal Advice response is not an object")
    restart_count = payload.get("restartCount")
    if payload.get("jobId") != job_id or type(restart_count) is not int or restart_count != attempt:
        return CausalEvidence(False, "terminal Advice response has the wrong job attempt")
    recommendation = payload.get("advice")
    if not isinstance(recommendation, dict) or recommendation.get("state") != "Terminated":
        return CausalEvidence(False, "terminal Advice does not recommend Terminated")
    scheduler_metadata = payload.get("schedulerMetadata")
    if not isinstance(scheduler_metadata, dict) or scheduler_metadata.get("scheduler") != "slurm":
        return CausalEvidence(False, "terminal Advice is not owned by the Slurm adapter")
    observed_at = payload.get("updatedAt")
    if not _is_current_timestamp(observed_at, submitted_at):
        return CausalEvidence(False, "terminal Advice update timestamp is missing or stale")
    return CausalEvidence(
        True,
        f"Slurm terminal Advice observed for job_id={job_id} attempt={attempt}",
        str(observed_at),
    )


def evaluate_terminated_advice_response(
    response_text: str,
    *,
    job_id: str,
    attempt: int,
    submitted_at: str,
) -> CausalEvidence:
    """Evaluate exact-attempt Terminated Advice without requiring an Issue."""
    try:
        payload = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        return CausalEvidence(False, "terminal Advice response is not valid JSON")
    if not isinstance(payload, dict):
        return CausalEvidence(False, "terminal Advice response is not an object")
    if payload.get("jobId") != job_id or payload.get("restartCount") != attempt:
        return CausalEvidence(False, "terminal Advice response has the wrong job attempt")
    recommendation = payload.get("advice")
    if not isinstance(recommendation, dict) or recommendation.get("state") != "Terminated":
        return CausalEvidence(False, "Advice does not recommend Terminated")
    observed_at = payload.get("updatedAt")
    if not _is_current_timestamp(observed_at, submitted_at):
        return CausalEvidence(False, "Advice update timestamp is missing or stale")
    return CausalEvidence(
        True,
        f"Terminated Advice observed for job_id={job_id} attempt={attempt}",
        str(observed_at),
    )


def evaluate_recovery_action_response(
    response_text: str,
    *,
    job_id: str,
    attempt: int,
    anomaly_type: str,
    submitted_at: str,
) -> CausalEvidence:
    """Evaluate the persisted notification audit record for a requeue action."""

    try:
        payload = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        return CausalEvidence(False, "Notifications response is not valid JSON")
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return CausalEvidence(False, "Notifications response has no items list")
    for item in items:
        if not isinstance(item, dict) or item.get("job_id") != job_id:
            continue
        if not _is_current_timestamp(item.get("created_time"), submitted_at):
            continue
        template = item.get("template_data")
        if not isinstance(template, dict):
            continue
        if template.get("template_id") != "/templates/JobRequeued":
            continue
        if type(template.get("job_attempt_index")) is not int or template["job_attempt_index"] != attempt:
            continue
        if template.get("job_id") != job_id or template.get("event_type") != anomaly_type:
            continue
        summary = f"JobRequeued audit observed for job_id={job_id} attempt={attempt} reason={anomaly_type}"
        return CausalEvidence(True, summary, str(item["created_time"]))
    return CausalEvidence(False, f"current JobRequeued audit missing for job_id={job_id} attempt={attempt}")


def _is_current_log_timestamp(value: str, submitted_at: str) -> bool:
    observed = _parse_utc_timestamp(value)
    return (
        _is_current_timestamp(value, submitted_at)
        and observed is not None
        and observed <= datetime.now(UTC) + TIMESTAMP_PRECISION_TOLERANCE
    )


def evaluate_log_response(
    response_text: str,
    *,
    job_id: str,
    marker: str,
    submitted_at: str,
    backend: str = "loki",
) -> CausalEvidence:
    """Evaluate an application marker under the ADR 0006 collection contract."""

    try:
        records = decode_log_response(response_text, backend)
    except ValueError as error:
        return CausalEvidence(False, str(error))
    for record in records:
        labels = record.labels
        if (
            labels.get("cluster") != "slurm"
            or labels.get("appName") != "slurm-app-log"
            or labels.get("job_id") != job_id
            or not labels.get("filename")
            or marker not in record.message
            or not _is_current_log_timestamp(record.observed_at, submitted_at)
        ):
            continue
        return CausalEvidence(
            True,
            f"application {backend} marker observed for job_id={job_id} marker={marker} filename={labels['filename']}",
            record.observed_at,
        )
    return CausalEvidence(False, f"current application {backend} marker missing for job_id={job_id} marker={marker}")


def evaluate_system_log_response(
    response_text: str,
    *,
    node_name: str,
    marker: str,
    submitted_at: str,
    backend: str = "loki",
) -> CausalEvidence:
    """Evaluate every current system marker against the job-free node contract."""

    try:
        records = decode_log_response(response_text, backend)
    except ValueError as error:
        return CausalEvidence(False, str(error))
    observations = [
        record
        for record in records
        if marker in record.message and _is_current_log_timestamp(record.observed_at, submitted_at)
    ]
    if not observations:
        return CausalEvidence(
            False, f"current system {backend} marker missing for node_name={node_name} marker={marker}"
        )

    violations: list[str] = []
    for record in observations:
        labels = record.labels
        if labels.get("cluster") != "slurm":
            violations.append(f"cluster={labels.get('cluster')!r}")
        if labels.get("component") != "journal":
            violations.append(f"component={labels.get('component')!r}")
        if labels.get("node_name") != node_name:
            violations.append(f"node_name={labels.get('node_name')!r}")
        forbidden = sorted(SYSTEM_LOG_FORBIDDEN_LABELS.intersection(labels))
        if forbidden:
            violations.append("forbidden_labels=" + ",".join(forbidden))
    if violations:
        return CausalEvidence(False, _bounded_text(f"system {backend} contract violations: " + "; ".join(violations)))

    return CausalEvidence(
        True,
        f"system {backend} marker observed for node_name={node_name} marker={marker} without workload labels",
        min(record.observed_at for record in observations),
    )


def evaluate_healthy_status_response(
    response_text: str,
    *,
    job_id: str,
    attempt: int,
) -> CausalEvidence:
    try:
        payload = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        return CausalEvidence(False, "healthy status response is not valid JSON")
    if not isinstance(payload, dict):
        return CausalEvidence(False, "healthy status response is not an object")
    identity_matches = payload.get("job_id") == job_id and payload.get("job_attempt_index") == attempt
    anomalies = payload.get("anomalies")
    clean = anomalies is None or anomalies == []
    if identity_matches and payload.get("state") == "COMPLETED" and clean:
        return CausalEvidence(True, f"job_id={job_id} attempt={attempt} completed without anomalies")
    return CausalEvidence(
        False,
        f"healthy status mismatch: identity={identity_matches} state={payload.get('state')!r} anomalies={anomalies!r}",
    )


def evaluate_scheduler_state_response(
    response_text: str,
    *,
    job_id: str,
    attempt: int,
    expected_status_state: str,
    expected_slurm_state: str,
    submitted_at: str,
) -> CausalEvidence:
    """Evaluate exact-attempt coarse scheduler classification from Job Manager."""

    try:
        payload = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        return CausalEvidence(False, "scheduler status response is not valid JSON")
    if not isinstance(payload, dict):
        return CausalEvidence(False, "scheduler status response is not an object")
    attempt_index = payload.get("job_attempt_index")
    if payload.get("job_id") != job_id or type(attempt_index) is not int or attempt_index != attempt:
        return CausalEvidence(False, "scheduler status response has the wrong job attempt")
    if payload.get("state") != expected_status_state:
        return CausalEvidence(False, f"scheduler status state is {payload.get('state')!r}, not {expected_status_state}")
    metadata = payload.get("schedulerMetadata")
    if not isinstance(metadata, dict) or metadata.get("scheduler") != "slurm":
        return CausalEvidence(False, "scheduler status is not owned by the Slurm adapter")
    if metadata.get("slurm_state") != expected_slurm_state:
        return CausalEvidence(False, f"raw Slurm state is {metadata.get('slurm_state')!r}, not {expected_slurm_state}")
    anomalies = payload.get("anomalies")
    if anomalies not in (None, []):
        return CausalEvidence(False, f"scheduler boundary unexpectedly produced anomalies: {anomalies!r}")
    observed_at = payload.get("last_updated_at")
    if not _is_current_timestamp(observed_at, submitted_at):
        return CausalEvidence(False, "scheduler status timestamp is missing or stale")
    return CausalEvidence(
        True,
        f"job_id={job_id} attempt={attempt} classified from Slurm state {expected_slurm_state}",
        str(observed_at),
    )


def evaluate_dag_job_spec_response(
    response_text: str,
    *,
    job_id: str,
    attempt: int,
    expected_dependencies: set[tuple[str, str]],
    expected_action: str,
    submitted_at: str,
) -> CausalEvidence:
    """Evaluate exact DAG metadata stored through Job Manager's production job API."""

    try:
        payload = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        return CausalEvidence(False, "Job Manager jobs response is not valid JSON")
    statuses = payload.get("statuses") if isinstance(payload, dict) else None
    if not isinstance(statuses, list):
        return CausalEvidence(False, "Job Manager jobs response has no status list")

    matches = [
        status
        for status in statuses
        if isinstance(status, dict) and status.get("job_id") == job_id and status.get("job_attempt_index") == attempt
    ]
    if len(matches) != 1:
        return CausalEvidence(False, f"expected one exact DAG job status, observed {len(matches)}")

    status = matches[0]
    spec = status.get("aps_spec")
    if not isinstance(spec, dict):
        return CausalEvidence(False, "exact job status has no APS specification")
    dependencies = spec.get("dag_dependencies")
    if not isinstance(dependencies, list):
        return CausalEvidence(False, "APS specification has no DAG dependency list")

    observed_dependencies: set[tuple[str, str]] = set()
    for dependency in dependencies:
        if not isinstance(dependency, dict) or dependency.get("trigger") != "completed":
            return CausalEvidence(False, f"invalid DAG dependency step: {dependency!r}")
        target = dependency.get("target")
        if not isinstance(target, dict):
            return CausalEvidence(False, f"DAG dependency has no target: {dependency!r}")
        target_type = target.get("type")
        target_key = "id" if target_type == "id" else "name" if target_type == "name" else ""
        target_identifier = target.get(target_key) if target_key else None
        if not isinstance(target_identifier, str):
            return CausalEvidence(False, f"DAG dependency has an invalid target: {target!r}")
        observed_dependencies.add((target_type, target_identifier))

    observed_at = status.get("last_updated_at")
    if not _is_current_timestamp(observed_at, submitted_at):
        return CausalEvidence(False, "DAG job status timestamp is missing or stale")
    if observed_dependencies != expected_dependencies or spec.get("dag_action") != expected_action:
        return CausalEvidence(
            False,
            f"DAG policy mismatch: dependencies={sorted(observed_dependencies)} action={spec.get('dag_action')!r}",
        )
    return CausalEvidence(
        True,
        f"job_id={job_id} attempt={attempt} preserved DAG policy through Job Manager",
        str(observed_at),
    )


def evaluate_no_recovery_actions_response(response_text: str, *, job_id: str) -> CausalEvidence:
    try:
        payload = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        return CausalEvidence(False, "Notifications response is not valid JSON")
    items = payload.get("items") if isinstance(payload, dict) else None
    if items == []:
        return CausalEvidence(True, f"no recovery action audits for job_id={job_id}")
    return CausalEvidence(False, f"unexpected recovery action audits for job_id={job_id}: {items!r}")


def _is_matching_attribution_event(
    event: object,
    *,
    job_id: str,
    attempt: int,
    node: str,
) -> bool:
    if not isinstance(event, dict):
        return False
    if str(event.get("job_id", "")) != job_id or event.get("node") != node:
        return False

    attempt_index = event.get("job_attempt_index")
    # JSON booleans become Python bools, which isinstance(..., int) would accept.
    if type(attempt_index) is not int or attempt_index != attempt:
        return False

    origin = event.get("error_attribution_origin")
    status = event.get("fact_status")
    return isinstance(origin, str) and origin.upper() == "FACT" and isinstance(status, str) and status.upper() == "OK"


def evaluate_attribution_response(
    response_text: str,
    *,
    job_id: str,
    attempt: int,
    node: str,
    exclusion_type: str,
    signature: str = "",
) -> AttributionEvidence:
    expected_type = exclusion_type.upper()
    try:
        payload = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        return AttributionEvidence(False, "response is not valid JSON")
    events = payload.get("events") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return AttributionEvidence(False, "response events is not a list")
    candidates: list[tuple[str, str]] = []
    for event in events:
        if not _is_matching_attribution_event(event, job_id=job_id, attempt=attempt, node=node):
            continue
        error = event.get("job_execution_error")
        raw_signature = error.get("signature", "") if isinstance(error, dict) else ""
        event_signature = raw_signature if isinstance(raw_signature, str) else "<invalid>"
        raw_type = event.get("exclusion_type")
        event_type = raw_type.upper() if isinstance(raw_type, str) else "<invalid>"
        candidates.append((event_type, event_signature))
    matches = [pair for pair in candidates if pair[0] == expected_type and (not signature or pair[1] == signature)]
    conflicts = [pair for pair in candidates if pair not in matches]
    summary = (
        f"job_id={job_id} attempt={attempt} node={node} "
        f"expected_exclusion_type={expected_type} "
        f"matched_exclusion_types={_joined_values(matches, 0)} "
        f"expected_signature={signature or '<any>'} "
        f"matched_signatures={_joined_values(matches, 1)} "
        f"attribution_matches={len(matches)} conflicts={len(conflicts)} "
        f"conflicting_types={_joined_values(conflicts, 0)} "
        f"conflicting_signatures={_joined_values(conflicts, 1)}"
    )
    return AttributionEvidence(bool(matches) and not conflicts, _bounded_text(summary))


def evaluate_no_hardware_attribution_response(
    response_text: str,
    *,
    job_id: str,
    attempt: int,
    node: str,
) -> AttributionEvidence:
    """Reject any exact-attempt hardware exclusion for a negative control."""

    try:
        payload = json.loads(response_text)
    except (json.JSONDecodeError, TypeError):
        return AttributionEvidence(False, "negative attribution response is not valid JSON")
    events = payload.get("events") if isinstance(payload, dict) else None
    if not isinstance(events, list):
        return AttributionEvidence(False, "negative attribution response events is not a list")

    hardware_types: list[str] = []
    for event in events:
        if not _is_matching_attribution_event(event, job_id=job_id, attempt=attempt, node=node):
            continue
        raw_type = event.get("exclusion_type")
        event_type = raw_type.upper() if isinstance(raw_type, str) else "<invalid>"
        if event_type in {"TEMPORARY", "CONFIRMED"}:
            hardware_types.append(event_type)
        elif event_type not in {"NONE"}:
            return AttributionEvidence(False, f"invalid exact-attempt attribution type: {event_type}")
    if hardware_types:
        return AttributionEvidence(
            False,
            f"unexpected hardware attribution for job_id={job_id} attempt={attempt} node={node}: "
            + ",".join(sorted(set(hardware_types))),
        )
    return AttributionEvidence(
        True,
        f"no hardware attribution for job_id={job_id} attempt={attempt} node={node}",
    )
