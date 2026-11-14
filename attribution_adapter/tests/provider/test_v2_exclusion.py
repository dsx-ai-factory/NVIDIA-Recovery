# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Symptom-driven exclusion discrimination tests for the FACT v2 pipeline (design §11).

Spec §11 requires that, in v2, isolation is driven by the symptom key matched against the
configured ``fact_isolated_errors`` / ``fact_non_isolated_errors`` lists — NOT by a
``recommended_action`` field (which is always ``None`` in v2).  These tests prove that
the same pipeline used by the golden tests discriminates exclusion type correctly when the
capture-settings error lists are populated.

Pipeline under test (identical to ``test_golden_message.py``):
    FACT ``AttributionResponseV2``
      -> ``attribution_adapter.response_builder.build_response``   (NV side)
      -> ``anomaly_detection_isolator.result_builder.build_attribution_result``  (Policy side)
      -> ``AnomalyProcessor._sanitize_attribution_result``            (Policy side)
"""

import golden_fixtures
import pytest
from anomaly_detection_isolator.anomaly_processor import AnomalyProcessor
from anomaly_detection_isolator.result_builder import build_attribution_result
from recovery_libs.schema.event_type import EventType
from recovery_libs.storage.node_events import ErrorAttributionType, ExclusionType

from attribution_adapter import response_builder

# Hardware symptom key used in the discrimination cases.
_SYMPTOM = "XID-79-GPU_HAS_FALLEN_OFF_THE_BUS"

# Source dict for a syslog NodeSource site.
_SYSLOG_SRC = {
    "observation_id": 1,
    "source": "syslog",
    "pos": None,
    "line": '{"xid": 79}',
    "rule": "r",
    "time": "2026-01-01T00:30:00Z",
}

# A single-node FACT v2 response carrying _SYMPTOM on "node1".
_FACT_RESPONSE = golden_fixtures._v2_case(
    "Fail InfraHW",
    "GPU has fallen off the bus",
    {
        _SYMPTOM: [
            {
                "type": "NodeSource",
                "status": "bad",
                "node": "node1",
                "source": _SYSLOG_SRC,
            }
        ]
    },
)


# A running job with no node-bound symptom. Its straggler digest flags a rank on "node2".
_STRAGGLER_SYMPTOM = "Straggler (Megatron Pattern)"
_STRAGGLER_FACT_RESPONSE = golden_fixtures._v2_case(
    "Pass",
    "Not Applicable",
    {
        "running without error": [
            {
                "type": "ResourcelessSource",
                "status": "bad",
                "source": {
                    "observation_id": 1,
                    "source": "slurm_info",
                    "pos": None,
                    "line": "",
                    "rule": "r",
                    "time": None,
                },
            }
        ]
    },
)
_STRAGGLER_FACT_RESPONSE.collectors = {
    "megatron_straggler": {
        "node2": {
            "pattern": "Megatron Pattern",
            "rank_scores": [{"rank": 5, "score": 213.46}],
            "sources": [
                {
                    "observation_id": 1,
                    "source": "applog",
                    "pos": 1,
                    "line": "^^^^ Bottom 2 Ranks with lowest  Etpt(TF): 213.46/5/node2, 287.92/1/node1,",
                    "rule": "r",
                    "time": "2026-01-01T00:30:00Z",
                }
            ],
        }
    }
}


def _run_pipeline(
    settings,
    fact_response=_FACT_RESPONSE,
    anomaly_type: str = golden_fixtures.ANOMALY_TYPE,
) -> dict[str, ExclusionType]:
    """Drive the full golden pipeline and return {node: exclusion_type} for every NodeEvent.

    Mirrors the pipeline in ``test_golden_message.py`` exactly, using the provided settings
    object (which controls exclusion-list matching) instead of the empty-list capture config.
    ``anomaly_type`` is the detection signal of the request and the anomaly type of the result.
    """
    request = golden_fixtures.make_request()
    request.failure_context.detection_signal = anomaly_type
    job_status = golden_fixtures.make_job_status()

    resp = response_builder.build_response(fact_response, request, attribution_session_id="sess")

    result = build_attribution_result(
        resp,
        job_status,
        anomaly_type,
        golden_fixtures.ANOMALY_UUID,
        ErrorAttributionType.FAST,
        settings,
    )

    # Apply _sanitize_attribution_result via the same bypass-__init__ pattern as the golden test.
    proc = AnomalyProcessor.__new__(AnomalyProcessor)
    proc.settings = settings
    result = proc._sanitize_attribution_result(result, job_status)

    node_exclusions: dict[str, ExclusionType] = {}
    for events in result.node_attributions.values():
        for event in events:
            node_exclusions[event.node] = event.exclusion_type
    return node_exclusions


def test_confirmed_when_symptom_in_isolated_list():
    """A NodeSource symptom that matches ``fact_isolated_errors`` resolves to CONFIRMED (int 3).

    Proves symptom-driven isolation without ``recommended_action`` (v2 always has None).
    """
    settings = golden_fixtures.capture_settings_with(isolated=[_SYMPTOM])
    exclusions = _run_pipeline(settings)

    assert "node1" in exclusions, "node1 must appear in node_attributions"
    assert exclusions["node1"] == ExclusionType.CONFIRMED, (
        f"Expected CONFIRMED (3) but got {exclusions['node1']!r}; "
        "symptom-driven isolation must work without recommended_action in v2"
    )


def test_none_when_symptom_in_non_isolated_list():
    """A NodeSource symptom that matches ``fact_non_isolated_errors`` resolves to NONE (int 1).

    Proves the symptom is the discriminator: identical input, different list placement -> different outcome.
    """
    settings = golden_fixtures.capture_settings_with(non_isolated=[_SYMPTOM])
    exclusions = _run_pipeline(settings)

    assert "node1" in exclusions, "node1 must appear in node_attributions"
    assert exclusions["node1"] == ExclusionType.NONE, (
        f"Expected NONE (1) but got {exclusions['node1']!r}; symptom in non_isolated list must suppress isolation"
    )


@pytest.mark.parametrize(
    "isolated, non_isolated, expected",
    [
        # Symptom in isolated list -> CONFIRMED regardless of non_isolated being empty.
        ([_SYMPTOM], [], ExclusionType.CONFIRMED),
        # Symptom in non_isolated list -> NONE (isolated list empty).
        ([], [_SYMPTOM], ExclusionType.NONE),
        # Neither list contains the symptom -> falls through to default TEMPORARY.
        ([], [], ExclusionType.TEMPORARY),
    ],
)
def test_exclusion_discrimination_parametrized(isolated, non_isolated, expected):
    """Parametrized coverage: isolated-list, non-isolated-list, and default-fallback paths."""
    settings = golden_fixtures.capture_settings_with(isolated=isolated, non_isolated=non_isolated)
    exclusions = _run_pipeline(settings)

    assert "node1" in exclusions, "node1 must appear in node_attributions"
    assert exclusions["node1"] == expected, (
        f"Expected {expected!r} but got {exclusions['node1']!r} (isolated={isolated}, non_isolated={non_isolated})"
    )


@pytest.mark.parametrize(
    "isolated, non_isolated, expected",
    [
        # No list names the straggler symptom -> default TEMPORARY, as for any unlisted symptom.
        ([], [], ExclusionType.TEMPORARY),
        # An operator turns straggler exclusion off, or makes it CONFIRMED, with the same lists.
        ([], ["Straggler ("], ExclusionType.NONE),
        ([_STRAGGLER_SYMPTOM], [], ExclusionType.CONFIRMED),
    ],
)
def test_straggler_node_exclusion_follows_the_symptom_lists(isolated, non_isolated, expected):
    """A straggler node from FACT's collector digest is excluded by the same symptom-driven policy."""
    settings = golden_fixtures.capture_settings_with(isolated=isolated, non_isolated=non_isolated)

    exclusions = _run_pipeline(settings, _STRAGGLER_FACT_RESPONSE, EventType.STRAGGLER.value)

    assert exclusions == {"node2": expected}


def test_straggler_digest_excludes_no_node_for_a_crash():
    """The straggler digest names a node only for a STRAGGLER anomaly."""
    exclusions = _run_pipeline(golden_fixtures.capture_settings(), _STRAGGLER_FACT_RESPONSE)

    assert exclusions == {}
