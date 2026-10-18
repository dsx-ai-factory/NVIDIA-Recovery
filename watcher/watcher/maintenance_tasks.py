# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Maintenance task functions for the Watcher workflow."""

from recovery_libs.log_query import CertificateExpiryReporter

from watcher.lib.plugins.log_query import LogQueryBatcher
from watcher.lib.utils import metrics
from watcher.lib.utils.data import EventDeduplicator
from watcher.lib.utils.logging import logger


def update_deduplicator_capacity_metrics(deduplicator: EventDeduplicator | None) -> None:
    """
    Update Prometheus metrics with EventDeduplicator capacity information.

    Args:
        deduplicator: EventDeduplicator instance to monitor.
                      If None, metrics will not be updated.
    """
    if not deduplicator:
        return

    metrics._initialize_deduplicator_metrics()
    if metrics.EVENT_DEDUP_CACHE_UTILIZATION is not None:
        metrics.EVENT_DEDUP_CACHE_UTILIZATION.set(deduplicator.utilization)
    logger.debug(f"Updated deduplicator metrics: utilization={deduplicator.utilization:.2f}%")


def update_certificate_expiration_metrics(log_query_client: LogQueryBatcher | None) -> None:
    """
    Update Prometheus metrics with certificate expiration information.

    Reads certificate and CA certificate expiration dates when the batcher's
    backend reports them. Metrics are only created when certificate-based
    authentication is detected.

    Args:
        log_query_client: Selected log-query batcher. If None or if its backend
            does not report certificate expiry, metrics are not updated.
    """
    if log_query_client is None or not isinstance(log_query_client.backend, CertificateExpiryReporter):
        return

    days_until_cert = log_query_client.backend.days_until_cert_expires()
    days_until_ca_cert = log_query_client.backend.days_until_ca_cert_expires()

    # Only initialize and export metrics if cert-based auth is being used
    if days_until_cert is not None or days_until_ca_cert is not None:
        metrics._initialize_cert_metrics()

        if days_until_cert is not None and metrics.LOKI_CERT_EXPIRATION_DAYS is not None:
            metrics.LOKI_CERT_EXPIRATION_DAYS.set(days_until_cert)
            logger.debug(f"Updated LOKI_CERT_EXPIRATION_DAYS metric: {days_until_cert} days")

        if days_until_ca_cert is not None and metrics.LOKI_CA_CERT_EXPIRATION_DAYS is not None:
            metrics.LOKI_CA_CERT_EXPIRATION_DAYS.set(days_until_ca_cert)
            logger.debug(f"Updated LOKI_CA_CERT_EXPIRATION_DAYS metric: {days_until_ca_cert} days")
