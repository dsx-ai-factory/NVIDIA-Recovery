# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


import pytest
from attribution_adapter.taxonomy import outcome_to_category
from recovery_libs.schema.attribution_interface.enums import ErrorCategory


@pytest.mark.parametrize(
    "outcome,expected",
    [
        ("Fail InfraHW", ErrorCategory.HARDWARE),
        ("Fail InfraSW", ErrorCategory.SYSTEM_SOFTWARE),
        ("Fail User", ErrorCategory.APPLICATION),
        ("Fail Multi", ErrorCategory.AMBIGUOUS),
        ("Fail MultiCategory", ErrorCategory.AMBIGUOUS),
        ("Pass", ErrorCategory.UNATTRIBUTED),
        ("Fail Undetermined", ErrorCategory.UNATTRIBUTED),
        ("Cancelled by User", ErrorCategory.UNATTRIBUTED),
        ("Cancelled by cluster operators", ErrorCategory.UNATTRIBUTED),
        ("Expected Fail", ErrorCategory.UNATTRIBUTED),
        ("Artificial", ErrorCategory.UNATTRIBUTED),
        ("Incomplete Info", ErrorCategory.INSUFFICIENT_TELEMETRY),
        ("Some Future Outcome FACT Adds", ErrorCategory.UNATTRIBUTED),  # default
        ("", ErrorCategory.UNATTRIBUTED),
    ],
)
def test_outcome_to_category(outcome, expected):
    assert outcome_to_category(outcome) == expected
