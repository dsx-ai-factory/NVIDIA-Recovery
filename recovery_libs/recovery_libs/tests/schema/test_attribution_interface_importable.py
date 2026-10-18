# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib

import pytest


@pytest.mark.parametrize(
    "module_path",
    [
        "recovery_libs.schema.attribution_interface.constants",
        "recovery_libs.schema.attribution_interface.enums",
        "recovery_libs.schema.attribution_interface.request",
        "recovery_libs.schema.attribution_interface.response",
    ],
)
def test_module_importable(module_path):
    """Every attribution_interface module imports cleanly (workspace dep available to both services)."""
    assert importlib.import_module(module_path) is not None


def test_public_names_resolve():
    """All frozen-contract public names are importable from their modules."""
    from recovery_libs.schema.attribution_interface.constants import ATTRIBUTION_SCHEMA_VERSION
    from recovery_libs.schema.attribution_interface.enums import (
        AttributionErrorType,
        AttributionPhase,
        ErrorCategory,
        ResourceKind,
    )
    from recovery_libs.schema.attribution_interface.request import (
        FailureContext,
        WorkloadAttributionRequest,
        WorkloadContext,
        WorkloadScope,
    )
    from recovery_libs.schema.attribution_interface.response import (
        Attribution,
        AttributionError,
        ResourceSet,
        WorkloadAttributionResponse,
    )

    assert ATTRIBUTION_SCHEMA_VERSION
    assert all(
        name is not None
        for name in (
            ErrorCategory,
            ResourceKind,
            AttributionPhase,
            AttributionErrorType,
            WorkloadScope,
            FailureContext,
            WorkloadContext,
            WorkloadAttributionRequest,
            ResourceSet,
            Attribution,
            AttributionError,
            WorkloadAttributionResponse,
        )
    )
