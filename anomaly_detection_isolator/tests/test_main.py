# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import pytest
from recovery_libs.audit import AuditBuffer

from anomaly_detection_isolator import main as main_module


def test_main_starts_passes_and_stops_audit_buffer() -> None:
    settings = MagicMock()
    audit_settings = settings.audit_trail
    audit_buffer = MagicMock(spec=AuditBuffer)
    service = MagicMock()

    with (
        patch("anomaly_detection_isolator.main.start_http_server"),
        patch("anomaly_detection_isolator.main.Settings", return_value=settings),
        patch("anomaly_detection_isolator.main.validate_settings", return_value=True),
        patch("anomaly_detection_isolator.main.set_up_trace_provider"),
        patch("anomaly_detection_isolator.main.create_audit_buffer", return_value=audit_buffer) as create_buffer,
        patch("anomaly_detection_isolator.main.AnomalyDetectionIsolator", return_value=service) as isolator,
    ):
        assert main_module.main() is True

    audit_buffer.start.assert_called_once_with()
    create_buffer.assert_called_once_with(
        audit_settings,
        logger=main_module.logger.getChild("audit"),
    )
    isolator.assert_called_once_with(settings=settings, audit_event_buffer=audit_buffer)
    service.setup_event_bus_and_start_consuming.assert_called_once_with()
    audit_buffer.stop.assert_called_once_with()


def test_main_rejects_invalid_settings_before_starting_consumer() -> None:
    """The console entrypoint must fail rather than return a successful exit code."""
    with (
        patch("anomaly_detection_isolator.main.start_http_server"),
        patch("anomaly_detection_isolator.main.Settings"),
        patch("anomaly_detection_isolator.main.validate_settings", return_value=False),
        patch("anomaly_detection_isolator.main.AnomalyDetectionIsolator") as isolator,
        pytest.raises(ValueError),
    ):
        main_module.main()
    isolator.assert_not_called()
