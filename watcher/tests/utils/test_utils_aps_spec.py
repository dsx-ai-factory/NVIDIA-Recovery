# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from recovery_libs.schema.aps_spec.spec import AnomalyDetectionConfig, ApsSlurmJobSpec
from recovery_libs.schema.event_type import EventType
from watcher.lib.utils.aps_spec import (
    get_nooutput_timeout,
    is_event_detection_disabled,
    parse_aps_spec_from_job_info,
)
from watcher.models.workflow import WorkflowStepNooutputJobPhase as Phase


def _spec(**flags):
    return ApsSlurmJobSpec(anomaly_detection_config=AnomalyDetectionConfig(**flags))


@pytest.mark.parametrize(
    "aps_spec,event_type,expected",
    [
        (None, EventType.CRASH, False),
        (ApsSlurmJobSpec(), EventType.CRASH, False),
        (_spec(log_based_crash_enabled=False), EventType.CRASH, True),
        (_spec(log_based_timeout_enabled=False), EventType.HANGNOOUTPUT, True),
        (_spec(log_based_timeout_enabled=True), EventType.HANGNOOUTPUT, False),
        (_spec(log_based_timeout_enabled=False), EventType.CRASH, False),
        (_spec(log_based_crash_enabled=False), "UNMAPPED_EVENT", False),
    ],
)
def test_is_event_detection_disabled(aps_spec, event_type, expected):
    assert is_event_detection_disabled(aps_spec, event_type) is expected


@pytest.mark.parametrize(
    "aps_spec,phase,default,expected",
    [
        (None, Phase.PRE_TRAINING, 100, 100),
        (ApsSlurmJobSpec(), Phase.PRE_TRAINING, 100, 960),
        (ApsSlurmJobSpec(), Phase.TRAINING_LOOP, 100, 840),
        (_spec(log_based_timeout_threshold_pretraining=1500), Phase.PRE_TRAINING, 100, 1500),
        (_spec(log_based_timeout_threshold_pretraining=1500), Phase.TRAINING_LOOP, 100, 840),
        (_spec(), "UNMAPPED_PHASE", 77, 77),
    ],
)
def test_get_nooutput_timeout(aps_spec, phase, default, expected):
    assert get_nooutput_timeout(aps_spec, phase, default) == expected


@pytest.mark.parametrize(
    "jm_info,expected",
    [
        (None, None),
        ({}, None),
        ({"aps_spec": None}, None),
        (
            {"aps_spec": {"anomaly_detection_config": {"log_based_timeout_enabled": False}}},
            _spec(log_based_timeout_enabled=False),
        ),
        ({"aps_spec": "not-a-dict"}, None),
        ({"aps_spec": {"anomaly_detection_config": "bad"}}, None),
    ],
)
def test_parse_aps_spec_from_job_info(jm_info, expected):
    assert parse_aps_spec_from_job_info(jm_info) == expected
