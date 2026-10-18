# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for maintenance tasks."""

import unittest
from unittest.mock import MagicMock, patch

from recovery_libs.log_query import AsyncLokiBackend, AsyncVictoriaLogsBackend
from watcher.maintenance_tasks import (
    update_certificate_expiration_metrics,
    update_deduplicator_capacity_metrics,
)


def _batcher_with_loki_client(days_until_cert: int | None, days_until_ca_cert: int | None) -> MagicMock:
    backend = MagicMock(spec=AsyncLokiBackend)
    backend.days_until_cert_expires.return_value = days_until_cert
    backend.days_until_ca_cert_expires.return_value = days_until_ca_cert
    batcher = MagicMock()
    batcher.backend = backend
    return batcher


class TestUpdateCertificateExpirationMetrics(unittest.TestCase):
    """Test certificate expiration metrics update."""

    @patch("watcher.maintenance_tasks.metrics._initialize_cert_metrics")
    @patch("watcher.maintenance_tasks.metrics.LOKI_CERT_EXPIRATION_DAYS")
    @patch("watcher.maintenance_tasks.metrics.LOKI_CA_CERT_EXPIRATION_DAYS")
    def test_updates_both_metrics(self, mock_ca_metric, mock_cert_metric, mock_init):
        """Test that both metrics are updated when certificates are configured."""
        update_certificate_expiration_metrics(_batcher_with_loki_client(30, 90))

        mock_init.assert_called_once()
        mock_cert_metric.set.assert_called_once_with(30)
        mock_ca_metric.set.assert_called_once_with(90)

    @patch("watcher.maintenance_tasks.metrics._initialize_cert_metrics")
    @patch("watcher.maintenance_tasks.metrics.LOKI_CERT_EXPIRATION_DAYS")
    @patch("watcher.maintenance_tasks.metrics.LOKI_CA_CERT_EXPIRATION_DAYS")
    def test_handles_none_values(self, mock_ca_metric, mock_cert_metric, mock_init):
        """Test that metrics are not updated without certificates, a batcher, or a certificate-reporting backend."""
        update_certificate_expiration_metrics(_batcher_with_loki_client(None, None))
        update_certificate_expiration_metrics(None)
        victorialogs_batcher = MagicMock()
        victorialogs_batcher.backend = MagicMock(spec=AsyncVictoriaLogsBackend)
        update_certificate_expiration_metrics(victorialogs_batcher)

        mock_init.assert_not_called()
        mock_cert_metric.set.assert_not_called()
        mock_ca_metric.set.assert_not_called()

    @patch("watcher.maintenance_tasks.metrics._initialize_cert_metrics")
    @patch("watcher.maintenance_tasks.metrics.LOKI_CERT_EXPIRATION_DAYS")
    @patch("watcher.maintenance_tasks.metrics.LOKI_CA_CERT_EXPIRATION_DAYS")
    def test_updates_only_cert_metric(self, mock_ca_metric, mock_cert_metric, mock_init):
        """Test that only cert metric is updated when CA cert is not configured."""
        update_certificate_expiration_metrics(_batcher_with_loki_client(45, None))

        mock_init.assert_called_once()
        mock_cert_metric.set.assert_called_once_with(45)
        mock_ca_metric.set.assert_not_called()

    @patch("watcher.maintenance_tasks.metrics._initialize_cert_metrics")
    @patch("watcher.maintenance_tasks.metrics.LOKI_CERT_EXPIRATION_DAYS")
    @patch("watcher.maintenance_tasks.metrics.LOKI_CA_CERT_EXPIRATION_DAYS")
    def test_updates_only_ca_cert_metric(self, mock_ca_metric, mock_cert_metric, mock_init):
        """Test that only CA cert metric is updated when cert is not configured."""
        update_certificate_expiration_metrics(_batcher_with_loki_client(None, 60))

        mock_init.assert_called_once()
        mock_cert_metric.set.assert_not_called()
        mock_ca_metric.set.assert_called_once_with(60)


class TestUpdateDeduplicatorCapacityMetrics(unittest.TestCase):
    """Test deduplicator capacity metrics update."""

    @patch("watcher.maintenance_tasks.metrics._initialize_deduplicator_metrics")
    @patch("watcher.maintenance_tasks.metrics.EVENT_DEDUP_CACHE_UTILIZATION")
    def test_updates_deduplicator_metrics(self, mock_util, mock_init):
        mock_dedup = MagicMock()
        mock_dedup.utilization = 5.0

        update_deduplicator_capacity_metrics(mock_dedup)

        mock_init.assert_called_once()
        mock_util.set.assert_called_once_with(5.0)

    def test_handles_none_deduplicator(self):
        update_deduplicator_capacity_metrics(None)  # Should not raise


if __name__ == "__main__":
    unittest.main()
