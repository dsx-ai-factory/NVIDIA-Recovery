# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Helpers that derive watcher runtime behavior from ApsSlurmJobSpec.

This module centralizes per-job feature flag and threshold lookups driven by
``ApsSlurmJobSpec.anomaly_detection_config`` so workflow code can remain
agnostic of spec field layout. New behavior toggles can be added by extending
the mapping tables defined here.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError
from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec
from recovery_libs.schema.event_type import EventType

from watcher.lib.utils.logging import logger
from watcher.models.workflow import WorkflowStepNooutputJobPhase

# Mapping of event_type -> aps_spec.anomaly_detection_config flag that gates
# anomaly publication. When the corresponding flag is explicitly set to False,
# the watcher will drop the event for that job.
_EVENT_TYPE_TO_DETECTION_FLAG: Mapping[str, str] = {
    EventType.CRASH: "log_based_crash_enabled",
    EventType.HANGNOOUTPUT: "log_based_timeout_enabled",
}

# Mapping of nooutput job phase -> aps_spec.anomaly_detection_config field that
# overrides the per-job no-output detection threshold (in seconds).
_PHASE_TO_NOOUTPUT_TIMEOUT_FIELD: Mapping[WorkflowStepNooutputJobPhase, str] = {
    WorkflowStepNooutputJobPhase.PRE_TRAINING: "log_based_timeout_threshold_pretraining",
    WorkflowStepNooutputJobPhase.TRAINING_LOOP: "log_based_timeout_threshold_trainingloop",
}


def is_event_detection_disabled(aps_spec: ApsSlurmJobSpec | None, event_type: str) -> bool:
    """
    Returns True iff event detection is explicitly disabled in the APS spec for ``event_type``.

    Returns False (default-on) when ``aps_spec`` is missing or the event type is not gated
    via ``_EVENT_TYPE_TO_DETECTION_FLAG``.
    """
    if not aps_spec:
        return False
    flag_field = _EVENT_TYPE_TO_DETECTION_FLAG.get(event_type)
    if not flag_field:
        return False
    return getattr(aps_spec.anomaly_detection_config, flag_field, True) is False


def get_nooutput_timeout(
    aps_spec: ApsSlurmJobSpec | None,
    job_phase: WorkflowStepNooutputJobPhase,
    default_timeout: int,
) -> int:
    """
    Returns the effective per-job no-output timeout (in seconds) for ``job_phase``.

    The aps_spec override takes precedence when ``aps_spec`` is provided and the phase has a
    positive integer value. Otherwise ``default_timeout`` is returned. Mirrors the precedence
    used for the log-based crash detection flag: an APS-managed job's spec is the source of
    truth; non-APS jobs fall back to the workflow-step default.
    """
    if not aps_spec:
        return default_timeout
    field = _PHASE_TO_NOOUTPUT_TIMEOUT_FIELD.get(job_phase)
    if not field:
        return default_timeout
    value = getattr(aps_spec.anomaly_detection_config, field, None)
    if isinstance(value, int) and value > 0:
        return value
    return default_timeout


def parse_aps_spec_from_job_info(jm_job_info: dict[str, Any] | None) -> ApsSlurmJobSpec | None:
    """
    Best-effort parser of ``aps_spec`` from a Job Manager ``job_info`` dict.

    Returns None when the dict is empty, when ``aps_spec`` is absent, or when validation fails.
    Validation errors are intentionally swallowed so a malformed spec does not abort detection
    workflows.
    """
    if not jm_job_info:
        return None
    aps_spec_dict = jm_job_info.get("aps_spec")
    if not aps_spec_dict:
        return None
    try:
        return ApsSlurmJobSpec.model_validate(aps_spec_dict)
    except (ValidationError, TypeError, ValueError) as err:
        logger.warning(
            "Unable to parse aps_spec from Job Manager job_info "
            f"(job_id={jm_job_info.get('job_id', 'unknown')}): {err}",
            exc_info=True,
        )
        return None
