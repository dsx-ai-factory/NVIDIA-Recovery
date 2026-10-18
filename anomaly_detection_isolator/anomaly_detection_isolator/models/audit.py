# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Literal

from pydantic import ConfigDict
from recovery_libs.audit import ProducerEvent
from recovery_libs.schema.attribution_interface.request import WorkloadAttributionRequest
from recovery_libs.schema.attribution_interface.response import WorkloadAttributionResponse
from recovery_libs.tracing.recovery import RecoveryCorrelation


class AttributionRequestForwardedPayload(RecoveryCorrelation):
    """Details recorded when ADI forwards an attribution request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    advice_id: str
    job_id: str
    attempt_index: int
    request_payload: WorkloadAttributionRequest
    request_uuid: str


class AttributionResponseReceivedPayload(RecoveryCorrelation):
    """Details recorded when ADI receives an attribution response."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    advice_id: str
    job_id: str
    attempt_index: int
    request_uuid: str
    response_payload: WorkloadAttributionResponse


class ADIAttributionRequestForwardedAuditEvent(ProducerEvent[AttributionRequestForwardedPayload], frozen=True):
    """Audit event emitted at ADI's outbound attribution boundary."""

    event_producer: Literal["anomaly-detection-isolator"] = "anomaly-detection-isolator"
    event_type: Literal["attribution_request_forwarded"] = "attribution_request_forwarded"


class ADIAttributionResponseReceivedAuditEvent(ProducerEvent[AttributionResponseReceivedPayload], frozen=True):
    """Audit event emitted after ADI parses an attribution response."""

    event_producer: Literal["anomaly-detection-isolator"] = "anomaly-detection-isolator"
    event_type: Literal["attribution_response_received"] = "attribution_response_received"
