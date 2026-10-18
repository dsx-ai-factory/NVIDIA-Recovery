# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Shared golden-snapshot fixtures for the attribution_adapter FACT v2 migration (design §11).

This module builds representative FACT ``AttributionResponseV2`` cases plus the
job-status / anomaly / settings inputs used to drive the pipeline through a
comparison against the frozen golden snapshot in ``golden_messages.json``.

The golden snapshot is regenerated as part of the v2 migration (Task 4).  The
pipeline driven by ``test_golden_message.py`` is:

    FACT ``AttributionResponseV2``
      -> ``attribution_adapter.response_builder.build_response``  (NV side)
      -> ``anomaly_detection_isolator.result_builder.build_attribution_result``  (Policy side)
      -> ``AnomalyProcessor._sanitize_attribution_result``  (Policy side)
      -> compare to ``golden_messages.json``

The capture config is deliberately FIXED so that ``_sanitize_attribution_result``
acts as a pass-through:
  * ``rack_aware_isolation_enabled=False``
  * ``max_isolated_nodes`` large (1000) and ``max_isolated_nodes_per_rack=None`` ->
    no per-cluster/per-rack down-grading of exclusion types,
  * all ``fact_isolated_errors / fact_ambiguous_errors / fact_non_isolated_errors``
    (+``_env``) = ``[]`` -> exclusion is driven only by the default-TEMPORARY rule
    (v2 has no DRAIN recommendation; ``recommended_action=None`` for all cases),
  * ``allocated_node_list`` includes every attributed node -> no allocation filtering.
"""

from datetime import UTC, datetime

from nvidia_attribution_client.models.attribution_response_v2 import AttributionResponseV2
from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION
from recovery_libs.schema.attribution_interface.enums import AttributionPhase
from recovery_libs.schema.attribution_interface.request import (
    FailureContext,
    WorkloadAttributionRequest,
    WorkloadContext,
    WorkloadScope,
)
from recovery_libs.schema.event_type import EventType
from recovery_libs.schema.job_manager.models import JobAnomaly, SlurmJobExecutionStatus

# Frozen timestamps so the captured snapshot and the new pipeline are deterministic.
FROZEN_OBS = datetime(2026, 1, 1, 0, 30, tzinfo=UTC)
FROZEN_JOB_START = datetime(2026, 1, 1, tzinfo=UTC)
FROZEN_JOB_END = datetime(2026, 1, 1, 1, tzinfo=UTC)

ANOMALY_TYPE = EventType.CRASHNAN.value
ANOMALY_UUID = "11111111-1111-1111-1111-111111111111"

# Nodes attributed by the fixtures; the capture config allocates all of them so
# sanitize never filters by allocation.
ATTRIBUTED_NODES = ["node1", "node2"]


class _CaptureSettings:
    """A minimal settings stand-in carrying every attribute the ADI pipeline reads.

    Using a plain object (not ``Settings()``) avoids the ~30 required BaseSettings
    fields while exactly pinning the capture config described in this module's docstring.
    """

    # Retained for callers that pass capture_settings() to FACTClient-aware code.
    fact_url = None
    fact_http_timeout_seconds = 30

    # _sanitize_attribution_result / isolation-limit reads — pass-through config.
    rack_aware_isolation_enabled = False
    max_isolated_nodes = 1000
    max_isolated_nodes_per_rack = None
    max_racks_to_isolate = 9
    rack_extraction_pattern = None

    def __init__(self) -> None:
        # Exclusion lists — all empty so only the default TEMPORARY rule fires.
        # (v2 has recommended_action=None always; no DRAIN short-circuit.)
        self.fact_isolated_errors: list[str] = []
        self.fact_ambiguous_errors: list[str] = []
        self.fact_non_isolated_errors: list[str] = []
        self.fact_isolated_errors_env: list[str] = []
        self.fact_ambiguous_errors_env: list[str] = []
        self.fact_non_isolated_errors_env: list[str] = []


def capture_settings() -> _CaptureSettings:
    """Return a fresh fixed capture-config settings object (see module docstring)."""
    return _CaptureSettings()


def capture_settings_with(
    isolated: list[str] | None = None,
    ambiguous: list[str] | None = None,
    non_isolated: list[str] | None = None,
) -> _CaptureSettings:
    """Return a capture-config settings object with populated fact_*_errors lists.

    Useful for tests that exercise symptom-driven exclusion discrimination (design §11).
    Any list left as ``None`` stays empty (the default from ``_CaptureSettings``).
    """
    s = _CaptureSettings()
    if isolated is not None:
        s.fact_isolated_errors = isolated
    if ambiguous is not None:
        s.fact_ambiguous_errors = ambiguous
    if non_isolated is not None:
        s.fact_non_isolated_errors = non_isolated
    return s


def make_job_status() -> SlurmJobExecutionStatus:
    """Build the SlurmJobExecutionStatus used for golden capture (allocates all attributed nodes)."""
    return SlurmJobExecutionStatus(
        job_id="12345",
        job_attempt_index=0,
        workflow_id="workflow-test",
        cluster="test-cluster",
        start_time=FROZEN_JOB_START,
        end_time=FROZEN_JOB_END,
        allocated_node_list=list(ATTRIBUTED_NODES),
    )


def make_anomaly() -> JobAnomaly:
    """Build the JobAnomaly used for golden capture."""
    return JobAnomaly(
        job_id="12345",
        timestamp=FROZEN_OBS,
        anomaly_type=ANOMALY_TYPE,
        uuid=ANOMALY_UUID,
    )


def make_request() -> WorkloadAttributionRequest:
    """Build the WorkloadAttributionRequest used to drive the golden pipeline and regeneration.

    Mirrors what ``AttributionClient.build_fast_request(make_job_status(), make_anomaly())``
    produces, constructed inline so that the nv tests have no hard dep on ADI's
    ``AttributionClient`` just to build a request.
    """
    return WorkloadAttributionRequest(
        schema_version=ATTRIBUTION_SCHEMA_VERSION,
        workload_id="12345",
        user_id="",
        scope=WorkloadScope(
            nodes=list(ATTRIBUTED_NODES),
            ranks=[],
            start_time=FROZEN_JOB_START,
            end_time=FROZEN_JOB_END,
        ),
        failure_context=FailureContext(
            detected_at=FROZEN_OBS,
            detection_signal=ANOMALY_TYPE,
        ),
        workload_context=WorkloadContext(
            workflow_id="workflow-test",
            attempt_index=0,
            command="",
            cluster="test-cluster",
            workload_name="",
            state="",
            node_count=None,
            total_gpu_count=None,
            job_end_time=FROZEN_JOB_END,
            anomaly_uuid=ANOMALY_UUID,
        ),
        continue_session=None,
        phase=AttributionPhase.FAST,
        timeout_ms=90000,
    )


def _v2_case(
    outcome: str,
    detail: str,
    symptoms: dict,
) -> AttributionResponseV2:
    """Build an ``AttributionResponseV2`` from outcome/detail/symptoms dict.

    ``symptoms`` is passed directly as the ``symptoms`` field value (each entry is a
    list of source-site dicts as expected by ``AttributionResponseV2.from_dict``).
    The root_symptom is derived from the first symptom key and its first site's source.
    """
    first_symptom = next(iter(symptoms))
    first_site = symptoms[first_symptom][0]
    root_source = first_site.get("source", {})
    resp = AttributionResponseV2.from_dict(
        {
            "model_type": "AttributionResponseV2",
            "verdict": {
                "attribution_outcome": outcome,
                "terminal_states": ["Pass" if outcome == "Pass" else "Fail"],
                "detail": detail,
                "root_symptom": {
                    "symptom": first_symptom,
                    "source": root_source,
                },
            },
            "symptoms": symptoms,
            "observation_context": [1],
        }
    )
    assert resp is not None
    return resp


def build_cases() -> dict[str, AttributionResponseV2]:
    """Return the representative FACT AttributionResponseV2 cases keyed by name.

    Cases mirror the v1 conceptual coverage (design §11):
      * ``node_single``              — single NodeSource HARDWARE attribution (Fail InfraHW).
      * ``global_single``            — single ResourcelessSource SOFTWARE attribution (Fail InfraSW).
      * ``node_multi_key``           — two NodeSource attributions on the same node, different symptoms.
      * ``node_no_sources``          — NodeSource with null source time -> observed_at falls back to
                                       job end_time (FROZEN_JOB_END).
      * ``node_health_check_source`` — NodeSource with health_check source -> NONE sentinel in ADI.
      * ``node_no_error_class``      — Pass / UNATTRIBUTED (no hardware fault found).
      * ``node_empty_class``         — Fail Undetermined / UNATTRIBUTED (ambiguous/incomplete).
      * ``node_and_global``          — both a NodeSource and a ResourcelessSource attribution.

    In v2 all ``recommended_action`` values are ``None`` (symptom-driven exclusion downstream),
    so the ADI exclusion policy defaults to TEMPORARY for every node site when the
    ``fact_*_errors`` lists are empty (as in the capture config).
    """
    # Canonical source dicts — reused across cases for determinism.
    _syslog_src = {
        "observation_id": 1,
        "source": "syslog",
        "pos": None,
        "line": '{"123": "123"}',
        "rule": "r",
        "time": "2026-01-01T00:30:00Z",
    }
    _applog_src = {
        "observation_id": 1,
        "source": "applog",
        "pos": None,
        "line": '{"123": "123"}',
        "rule": "r",
        "time": "2026-01-01T00:30:00Z",
    }
    _health_check_src = {
        "observation_id": 1,
        "source": "health_check",
        "pos": None,
        "line": '{"123": "123"}',
        "rule": "r",
        "time": "2026-01-01T00:30:00Z",
    }
    # Null time -> observed_at falls back to job_end_time (FROZEN_JOB_END).
    _no_time_src = {
        "observation_id": 1,
        "source": "syslog",
        "pos": None,
        "line": "",
        "rule": "r",
        "time": None,
    }

    return {
        "node_single": _v2_case(
            "Fail InfraHW",
            "Test error",
            {
                "Symptom 1": [
                    {
                        "type": "NodeSource",
                        "status": "bad",
                        "node": "node1",
                        "source": _syslog_src,
                    }
                ]
            },
        ),
        "global_single": _v2_case(
            "Fail InfraSW",
            "global error",
            {
                "global symptom": [
                    {
                        "type": "ResourcelessSource",
                        "status": "bad",
                        "source": _applog_src,
                    }
                ]
            },
        ),
        "node_multi_key": _v2_case(
            "Fail InfraHW",
            "error A",
            {
                "symptom A": [
                    {
                        "type": "NodeSource",
                        "status": "bad",
                        "node": "node1",
                        "source": _syslog_src,
                    }
                ],
                "symptom B": [
                    {
                        "type": "NodeSource",
                        "status": "bad",
                        "node": "node1",
                        "source": _applog_src,
                    }
                ],
            },
        ),
        "node_no_sources": _v2_case(
            "Fail InfraHW",
            "no-source error",
            {
                "no-source symptom": [
                    {
                        "type": "NodeSource",
                        "status": "bad",
                        "node": "node1",
                        "source": _no_time_src,
                    }
                ]
            },
        ),
        "node_health_check_source": _v2_case(
            "Fail InfraHW",
            "health-check error",
            {
                "health-check symptom": [
                    {
                        "type": "NodeSource",
                        "status": "bad",
                        "node": "node1",
                        "source": _health_check_src,
                    }
                ]
            },
        ),
        "node_no_error_class": _v2_case(
            "Pass",
            "no error found",
            {
                "ok": [
                    {
                        "type": "NodeSource",
                        "status": "bad",
                        "node": "node1",
                        "source": _syslog_src,
                    }
                ]
            },
        ),
        "node_empty_class": _v2_case(
            "Fail Undetermined",
            "empty class",
            {
                "empty": [
                    {
                        "type": "NodeSource",
                        "status": "bad",
                        "node": "node1",
                        "source": _applog_src,
                    }
                ]
            },
        ),
        "node_and_global": _v2_case(
            "Fail InfraHW",
            "node error",
            {
                "node symptom": [
                    {
                        "type": "NodeSource",
                        "status": "bad",
                        "node": "node1",
                        "source": _syslog_src,
                    }
                ],
                "global symptom": [
                    {
                        "type": "ResourcelessSource",
                        "status": "bad",
                        "source": _applog_src,
                    }
                ],
            },
        ),
    }


# Stable list of case names for callers that need to assert completeness.
CASE_NAMES = list(build_cases().keys())
