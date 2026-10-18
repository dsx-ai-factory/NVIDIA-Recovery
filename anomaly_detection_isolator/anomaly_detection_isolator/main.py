# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import sys

from prometheus_client import start_http_server
from pydantic import HttpUrl
from recovery_libs.audit import AuditBuffer, NoOpAuditBuffer, ProducerEvent, create_audit_buffer
from recovery_libs.tracing.traces import set_up_trace_provider

from anomaly_detection_isolator.service import AnomalyDetectionIsolator
from anomaly_detection_isolator.settings.settings import Settings
from anomaly_detection_isolator.utils.logger import logger


def validate_settings(settings: Settings) -> bool:
    """
    Validate the settings.
    """
    if not settings.rabbitmq_url:
        logger.error("RabbitMQ URL is not set.")
        return False

    if not settings.rabbitmq_vhost:
        logger.error("RabbitMQ VHost is not set.")
        return False

    if settings.slow_signals_delay_seconds <= 0:
        logger.error(f"slow_signals_delay_seconds value is not valid {settings.slow_signals_delay_seconds}.")
        return False

    if settings.epilog_query_duration_minutes <= 0:
        logger.error(
            f"epilog_query_duration_minutes must be greater than 0, got {settings.epilog_query_duration_minutes}."
        )
        return False
    # Check if all fact error lists are empty
    if all(
        not lst
        for lst in [
            settings.fact_non_isolated_errors,
            settings.fact_isolated_errors,
            settings.fact_ambiguous_errors,
            settings.fact_non_isolated_errors_env,
            settings.fact_isolated_errors_env,
            settings.fact_ambiguous_errors_env,
        ]
    ):
        logger.error("All fact error lists are empty, at least one list must be populated")
        return False

    # When attribution is enabled, the attribution service URL must be configured.
    if settings.attribution_enabled:
        try:
            HttpUrl(settings.attribution_service_url or "")
        except ValueError:
            logger.error("Attribution is enabled but attribution_service_url is not a valid HTTP(S) URL.")
            return False

    return True


def main() -> bool:
    """
    Entrypoint into the Anomaly Detection Isolation Service.
    """
    audit_buffer: AuditBuffer[ProducerEvent] = NoOpAuditBuffer[ProducerEvent]()
    try:
        start_http_server(port=8080)
        settings = Settings()

        if not validate_settings(settings):
            # The installed console script passes our return value to sys.exit;
            # returning False here would report a successful startup.
            raise ValueError("Invalid settings, exiting.")

        set_up_trace_provider(service_name="anomaly_detection_isolator")

        audit_buffer = create_audit_buffer(
            settings.audit_trail,
            logger=logger.getChild("audit"),
        )
        audit_buffer.start()

        service = AnomalyDetectionIsolator(
            settings=settings,
            audit_event_buffer=audit_buffer,
        )
        service.setup_event_bus_and_start_consuming()
        return True

    except KeyboardInterrupt:
        return False
    finally:
        audit_buffer.stop()


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
