# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from notifications.models.audit_events import (
    AuditEventBase,
    AuditEventHumanSummary,
    AuditEventHumanSummaryGranularityClass,
)
from notifications.utils.logger import logger

_SUMMARY_FIELD_LIMIT = 3
_ChangePath = tuple[str, ...]
_JOB_IGNORED_FIELDS = frozenset({"aps_spec"})
_ADVICE_IGNORED_FIELDS = frozenset({"visible", "generation", "issues", "desired_state_changed_at"})


def _changed_paths(old: object, new: object, prefix: _ChangePath = ()) -> list[_ChangePath]:
    """Return changed leaf paths, treating JSON arrays as single fields."""
    if isinstance(old, dict) or isinstance(new, dict):
        old_mapping = old if isinstance(old, dict) else {}
        new_mapping = new if isinstance(new, dict) else {}
        paths = [
            changed_path
            for field in dict.fromkeys((*old_mapping, *new_mapping))
            for changed_path in _changed_paths(
                old_mapping.get(field),
                new_mapping.get(field),
                (*prefix, field),
            )
        ]
        if paths or old == new:
            return paths

    return [prefix] if old != new else []


def _changed_field_names(paths: list[_ChangePath], ignored_top_level: frozenset[str]) -> list[str]:
    return [".".join(path) for path in paths if path and path[0] not in ignored_top_level]


def _append_changed_fields(description: str, fields: list[str]) -> str:
    if not fields:
        return description
    return f"{description}; other changes: {', '.join(fields[:_SUMMARY_FIELD_LIMIT])}"


def _format_reasons(reasons: object) -> str:
    """Format validated reason values for a published Advice summary."""
    if not isinstance(reasons, list):
        return ""
    rendered_reasons = [reason for reason in reasons if isinstance(reason, str)]
    return f" (Reason: {', '.join(rendered_reasons)})" if rendered_reasons else ""


def summarize_audit_event(event: AuditEventBase) -> AuditEventHumanSummary:
    """Build presentation metadata without letting malformed payloads break a query page."""
    try:
        return _summarize_audit_event(event)
    except (AttributeError, TypeError, ValueError) as error:
        # Payloads are stored without event-specific validation, including historical
        # events. Avoid logging exception messages, which may contain payload values.
        logger.warning(
            "Using generic audit summary: event_type=%s event_producer=%s exception_type=%s",
            event.event_type,
            event.event_producer,
            type(error).__name__,
        )
        return _generic_summary(event)


def _generic_summary(event: AuditEventBase) -> AuditEventHumanSummary:
    """Describe an event using only its type when detailed formatting is unavailable."""
    return AuditEventHumanSummary(
        title=event.event_type,
        description=event.event_type.replace("_", " "),
        granularity=AuditEventHumanSummaryGranularityClass.FINE,
    )


# TODO: Move events to OpenAPI/protobufs so they can be generated and used
# across services.
def _summarize_audit_event(event: AuditEventBase) -> AuditEventHumanSummary:
    """Build event-specific metadata from the producer's expected payload structure."""
    match event.event_type:
        case "anomaly_detected":
            anomaly_type = event.payload.get("anomaly_type", "")
            if anomaly_type in ("CHECKPOINTLOAD", "CHECKPOINTSAVE"):
                return AuditEventHumanSummary(
                    title="Job lifecycle observed",
                    description="Checkpoint loaded" if anomaly_type == "CHECKPOINTLOAD" else "Checkpoint saved",
                    granularity=AuditEventHumanSummaryGranularityClass.COARSE,
                )
            return AuditEventHumanSummary(
                title="Anomaly detected",
                description=f"Anomaly {anomaly_type} detected",
                granularity=AuditEventHumanSummaryGranularityClass.COARSE,
            )

        case "attribution_request_forwarded":
            request_payload = event.payload.get("request_payload", {})
            phase = request_payload.get("phase", None)
            formatted_phase = f"({phase})" if phase is not None else ""

            return AuditEventHumanSummary(
                title="Attribution request sent",
                description=f"Attribution request {formatted_phase} sent",
                granularity=AuditEventHumanSummaryGranularityClass.FINE,
            )

        case "attribution_response_received":
            return AuditEventHumanSummary(
                title="Attribution response received",
                description="Attribution response received",
                granularity=AuditEventHumanSummaryGranularityClass.FINE,
            )

        case "job_attempt_observed":
            job_id = event.payload.get("job_id")
            attempt_index = event.payload.get("attempt_index")
            job = event.payload.get("job", {})

            return AuditEventHumanSummary(
                title="Job observed",
                description=f"Job {job_id} ({attempt_index}) was first observed in {job.get('state')}",
                granularity=AuditEventHumanSummaryGranularityClass.COARSE,
            )

        case "job_attempt_updated":
            old_job = event.payload.get("old_job", {})
            new_job = event.payload.get("new_job", {})

            old_state_value = old_job.get("state")
            new_state_value = new_job.get("state")
            old_state = old_state_value if isinstance(old_state_value, str) else None
            new_state = new_state_value if isinstance(new_state_value, str) else None
            changed_fields = _changed_field_names(_changed_paths(old_job, new_job), _JOB_IGNORED_FIELDS)

            if old_state != new_state and old_state not in {None, ""} and new_state not in {None, ""}:
                other_changes = [field for field in changed_fields if field != "state"]
                description = _append_changed_fields(
                    f"Job changed state from {old_state} to {new_state}",
                    other_changes,
                )
                return AuditEventHumanSummary(
                    title="Job state updated",
                    description=description,
                    granularity=AuditEventHumanSummaryGranularityClass.COARSE,
                )

            return AuditEventHumanSummary(
                title="Job updated",
                description=(
                    f"Fields {', '.join(changed_fields[:_SUMMARY_FIELD_LIMIT])} updated"
                    if changed_fields
                    else "Job updated"
                ),
                granularity=AuditEventHumanSummaryGranularityClass.FINE,
            )

        case "advice_created":
            advice = event.payload.get("advice", {})
            recommendation = advice.get("advice", {})
            visible = advice.get("visible", False)
            advised_state = recommendation.get("state", "")
            creation_reasons = event.payload.get("creation_reasons", [])

            if visible:
                return AuditEventHumanSummary(
                    title="Advice published",
                    description=f"Advice published with state {advised_state}{_format_reasons(creation_reasons)}",
                    granularity=AuditEventHumanSummaryGranularityClass.COARSE,
                )

            return AuditEventHumanSummary(
                title="Advice created",
                description=f"Advice created with state {advised_state}, but not published",
                granularity=AuditEventHumanSummaryGranularityClass.FINE,
            )

        case "advice_updated":
            old_advice = event.payload.get("old_advice", {})
            new_advice = event.payload.get("new_advice", {})
            old_state = old_advice.get("advice", {}).get("state")
            new_state = new_advice.get("advice", {}).get("state")
            old_visible_value = old_advice.get("visible")
            new_visible_value = new_advice.get("visible")
            old_visible = old_visible_value if isinstance(old_visible_value, bool) else None
            new_visible = new_visible_value if isinstance(new_visible_value, bool) else None
            changed_fields = _changed_field_names(_changed_paths(old_advice, new_advice), _ADVICE_IGNORED_FIELDS)
            change_reasons = event.payload.get("change_reasons")

            if old_visible is False and new_visible is True:
                advised_state = new_state or ""
                other_changes = [field for field in changed_fields if field != "advice.state"]
                return AuditEventHumanSummary(
                    title="Advice published",
                    description=_append_changed_fields(
                        f"Advice published with state {advised_state}{_format_reasons(change_reasons)}",
                        other_changes,
                    ),
                    granularity=AuditEventHumanSummaryGranularityClass.COARSE,
                )

            if old_state != new_state and old_state is not None and new_state is not None:
                other_changes = [field for field in changed_fields if field != "advice.state"]
                return AuditEventHumanSummary(
                    title="Advice state updated",
                    description=_append_changed_fields(
                        f"Advice changed state from {old_state} to {new_state}",
                        other_changes,
                    ),
                    granularity=AuditEventHumanSummaryGranularityClass.FINE,
                )

            return AuditEventHumanSummary(
                title="Advice updated",
                description=(
                    f"Fields {', '.join(changed_fields[:_SUMMARY_FIELD_LIMIT])} updated"
                    if changed_fields
                    else "Advice updated"
                ),
                granularity=AuditEventHumanSummaryGranularityClass.FINE,
            )

        case "advice_unpublished":
            return AuditEventHumanSummary(
                title="Advice unpublished",
                description="Advice unpublished",
                granularity=AuditEventHumanSummaryGranularityClass.FINE,
            )

        case "advice_scheduler_action_completed":
            success: bool | None = event.payload.get("success", False)
            action: str | None = event.payload.get("action", "")

            if success:
                return AuditEventHumanSummary(
                    title="Scheduler action performed",
                    description=f"Action {action} performed successfully",
                    granularity=AuditEventHumanSummaryGranularityClass.COARSE,
                )
            else:
                error: str | None = event.payload.get("error", None)
                error_rendered = f" (Error: {error})" if error is not None else ""

                return AuditEventHumanSummary(
                    title="Action failed with scheduler",
                    description=f"Action {action} failed{error_rendered}",
                    granularity=AuditEventHumanSummaryGranularityClass.COARSE,
                )

        case "advice_application_applied":
            return AuditEventHumanSummary(
                title="Advice application completed",
                description="Advice application completed",
                granularity=AuditEventHumanSummaryGranularityClass.FINE,
            )

        case "advice_application_evaluated":
            actions = ", ".join(event.payload.get("planned_actions", []))
            return AuditEventHumanSummary(
                title="Advice evaluated",
                description=f"Actions would be applied based on Advice: {actions}",
                granularity=AuditEventHumanSummaryGranularityClass.COARSE,
            )

        case "advice_application_failed":
            error = event.payload.get("error", None)
            error_rendered = f" (Error: {error})" if error is not None else ""

            return AuditEventHumanSummary(
                title="Advice failed",
                description=f"Failed to apply Advice{error_rendered}",
                granularity=AuditEventHumanSummaryGranularityClass.COARSE,
            )

        case "advice_reconciliation_converged":
            return AuditEventHumanSummary(
                title="Advice converged",
                description="State matches Advice",
                granularity=AuditEventHumanSummaryGranularityClass.FINE,
            )

        case "advice_reconciliation_abandoned":
            abandonment_reason = event.payload.get("reason", None)
            reason_rendered = f" (Reason: {abandonment_reason})" if abandonment_reason is not None else ""
            return AuditEventHumanSummary(
                title="Advice no longer considered",
                description=f"Advice no longer considered{reason_rendered}",
                granularity=AuditEventHumanSummaryGranularityClass.COARSE,
            )

        case "job_auto_requeue_hold_completed":
            success = event.payload.get("success", None)

            if success:
                return AuditEventHumanSummary(
                    title="Job set to REQUEUE_HOLD",
                    description="Job set to REQUEUE_HOLD due to detected failure",
                    granularity=AuditEventHumanSummaryGranularityClass.COARSE,
                )
            else:
                error = event.payload.get("error", None)
                error_formatted = f" (Error: {error})" if error is not None else ""
                return AuditEventHumanSummary(
                    title="Job failed to be set to REQUEUE_HOLD",
                    description=f"Failed to set to REQUEUE_HOLD due to detected failure{error_formatted}",
                    granularity=AuditEventHumanSummaryGranularityClass.COARSE,
                )

        case "watchdog_release_completed":
            success = event.payload.get("success", None)

            if success:
                return AuditEventHumanSummary(
                    title="Watchdog released",
                    description="Job released by Watchdog after hold timeout",
                    granularity=AuditEventHumanSummaryGranularityClass.COARSE,
                )
            else:
                error = event.payload.get("error", None)
                error_formatted = f" (Error: {error})" if error is not None else ""
                return AuditEventHumanSummary(
                    title="Watchdog failed to release",
                    description=f"Job failed to be released by Watchdog{error_formatted}",
                    granularity=AuditEventHumanSummaryGranularityClass.COARSE,
                )

        case _:
            return _generic_summary(event)
