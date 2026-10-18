# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

from anomaly_detection_isolator.anomaly_processor import AnomalyProcessor
from anomaly_detection_isolator.service import AnomalyDetectionIsolator
from anomaly_detection_isolator.settings.settings import Settings
from recovery_libs.audit import AuditEventBuffer


def test_service_passes_audit_buffer_to_anomaly_processor() -> None:
    settings = MagicMock(spec=Settings)
    audit_event_buffer = MagicMock(spec=AuditEventBuffer)

    with (
        patch("anomaly_detection_isolator.service.AnomalyProcessor") as processor,
        patch("anomaly_detection_isolator.service.JobManagerClient"),
    ):
        AnomalyDetectionIsolator(settings, audit_event_buffer=audit_event_buffer)

    processor.assert_called_once_with(
        settings=settings,
        audit_event_buffer=audit_event_buffer,
    )


def test_anomaly_processor_passes_audit_buffer_to_attribution_client() -> None:
    settings = MagicMock(spec=Settings)
    audit_event_buffer = MagicMock(spec=AuditEventBuffer)

    with patch("anomaly_detection_isolator.anomaly_processor.AttributionClient") as client:
        AnomalyProcessor(settings, audit_event_buffer=audit_event_buffer)

    client.assert_called_once_with(
        settings,
        audit_event_buffer=audit_event_buffer,
    )
