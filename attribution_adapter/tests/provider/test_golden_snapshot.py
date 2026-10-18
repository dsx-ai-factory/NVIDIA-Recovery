# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Sanity test for the frozen v2 golden snapshot.

The full byte-identity comparison of the v2 pipeline against this snapshot is in
``test_golden_message.py``; here we only assert the committed ``golden_messages.json``
loads and carries every fixture case key so a regenerate that drops a case is caught early.
"""

import json
from pathlib import Path

import golden_fixtures

GOLDEN_PATH = Path(__file__).parent / "golden_messages.json"


def test_golden_snapshot_exists():
    """The committed golden snapshot loads and has an entry for every fixture case."""
    data = json.loads(GOLDEN_PATH.read_text())
    assert isinstance(data, dict)
    assert set(data.keys()) == set(golden_fixtures.CASE_NAMES)
    # Each entry is a serialized AnomalyAttributionResult with the expected top-level keys.
    for case in golden_fixtures.CASE_NAMES:
        entry = data[case]
        assert set(entry.keys()) >= {
            "job_id",
            "anomaly_type",
            "attribution_type",
            "node_attributions",
            "global_attributions",
        }
