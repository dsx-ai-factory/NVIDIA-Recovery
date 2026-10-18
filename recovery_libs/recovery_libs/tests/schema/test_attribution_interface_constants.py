# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION


def test_attribution_schema_version_value():
    """The single agreed schema-version string both services pin to (design §12.6)."""
    assert ATTRIBUTION_SCHEMA_VERSION == "0.1.0"
