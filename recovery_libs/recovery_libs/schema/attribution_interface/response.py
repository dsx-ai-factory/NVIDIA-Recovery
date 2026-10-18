# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Response models for the workload attribution HTTP interface (design §6).

error is informational only; per design §10 it does not change the Policy
Engine's publish decision. error_detail / symptom / evidence_excerpt are
non-nullable with a "" default to preserve byte-identity of the reconstructed
JobExecutionError (design §6 None->"" note).
"""

from datetime import datetime

from pydantic import BaseModel, Field

from recovery_libs.schema.attribution_interface.enums import (
    AttributionErrorType,
    ErrorCategory,
    ResourceKind,
)


class ResourceSet(BaseModel):
    """A resource an attribution targets: a single Node (value=hostname) or AllJobResources (value=None)."""

    kind: ResourceKind
    value: str | None = None


class Attribution(BaseModel):
    """A single attributed finding against a resource set (design §6)."""

    resource_set: ResourceSet
    error_category: ErrorCategory
    error_detail: str = ""
    confidence: float | None = None
    symptom: str = ""
    evidence_excerpt: str = ""
    observed_at: datetime
    recommended_action: str | None = None
    evidence_references: list[str] = Field(default_factory=list)


class AttributionError(BaseModel):
    """Informational error on a response; never changes the publish decision (design §10)."""

    type: AttributionErrorType
    message: str = ""


class WorkloadAttributionResponse(BaseModel):
    """The attribution result returned by attribution-adapter to the Policy Engine (design §6)."""

    schema_version: str
    service_identifier: str
    service_version: str
    request_id: str
    attribution_session_id: str = ""
    attributions: list[Attribution] = Field(default_factory=list)
    unattributed_targets: list[ResourceSet] = Field(default_factory=list)
    error: AttributionError | None = None
