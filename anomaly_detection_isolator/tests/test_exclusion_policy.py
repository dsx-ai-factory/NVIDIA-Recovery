# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from types import SimpleNamespace

import pytest
from anomaly_detection_isolator.exclusion_policy import derive_exclusion
from recovery_libs.storage.node_events import ExclusionType


def make_settings(**overrides):
    base = {
        "fact_isolated_errors": ["XID 79", "uncorrectable"],
        "fact_isolated_errors_env": ["ISOLATED_ENV"],
        "fact_ambiguous_errors": ["timeout"],
        "fact_ambiguous_errors_env": ["AMBIGUOUS_ENV"],
        "fact_non_isolated_errors": ["user oom"],
        "fact_non_isolated_errors_env": ["NONISO_ENV"],
    }
    base.update(overrides)
    return SimpleNamespace(**base)


@pytest.mark.parametrize(
    "recommended_action, symptom, expected",
    [
        # "drain" is FACT's RecommendationEnum.DRAIN.value — the string the Policy Engine's DRAIN short-circuit matches
        ("drain", "anything at all", ExclusionType.CONFIRMED),
        ("DRAIN", "user oom", ExclusionType.CONFIRMED),  # guard normalizes case
        (None, "saw XID 79 on bus", ExclusionType.CONFIRMED),  # isolated list
        ("", "uncorrectable ecc", ExclusionType.CONFIRMED),  # isolated list, empty action
        (None, "got ISOLATED_ENV here", ExclusionType.CONFIRMED),  # isolated _env list
        (None, "nccl timeout detected", ExclusionType.TEMPORARY),  # ambiguous list
        (None, "AMBIGUOUS_ENV flagged", ExclusionType.TEMPORARY),  # ambiguous _env list
        (None, "user oom kill", ExclusionType.NONE),  # non-isolated list
        (None, "NONISO_ENV occurred", ExclusionType.NONE),  # non-isolated _env list
        (None, "totally unknown symptom", ExclusionType.TEMPORARY),  # default
        (None, "", ExclusionType.TEMPORARY),  # default, empty symptom
    ],
)
def test_derive_exclusion(recommended_action, symptom, expected):
    assert derive_exclusion(recommended_action, symptom, make_settings()) == expected


@pytest.mark.parametrize("missing", ["fact_isolated_errors", "fact_ambiguous_errors", "fact_non_isolated_errors"])
def test_derive_exclusion_handles_none_lists(missing):
    settings = make_settings(**{missing: None, f"{missing}_env": None})
    assert derive_exclusion(None, "totally unknown symptom", settings) == ExclusionType.TEMPORARY


def test_derive_exclusion_all_lists_none():
    """All lists None and no DRAIN -> default TEMPORARY (no AttributeError)."""
    settings = make_settings(
        fact_isolated_errors=None,
        fact_isolated_errors_env=None,
        fact_ambiguous_errors=None,
        fact_ambiguous_errors_env=None,
        fact_non_isolated_errors=None,
        fact_non_isolated_errors_env=None,
    )
    assert derive_exclusion(None, "anything", settings) == ExclusionType.TEMPORARY
    # DRAIN short-circuit still works with all-None lists.
    assert derive_exclusion("drain", "anything", settings) == ExclusionType.CONFIRMED
