# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
import unittest
from unittest.mock import MagicMock, patch

from recovery_libs.utils.constants import OTLP_GRPC_DEFAULT_PORT, OTLP_HTTP_DEFAULT_PORT
from recovery_libs.utils.lazy_otlp_handler import (
    LazyOTLPHandler,
    _check_endpoint_reachable,
)
from recovery_libs.utils.types import OTLPProtocol


class TestCheckEndpointReachable(unittest.TestCase):
    """Test suite for _check_endpoint_reachable function."""

    @patch("recovery_libs.utils.lazy_otlp_handler.socket.socket")
    def test_reachable_endpoint_with_url(self, mock_socket_cls):
        """Test reachable endpoint with full URL format."""
        mock_socket = MagicMock()
        mock_socket_cls.return_value = mock_socket

        result = _check_endpoint_reachable("http://localhost:4317", OTLPProtocol.GRPC)

        self.assertTrue(result)
        mock_socket.connect.assert_called_once_with(("localhost", 4317))
        mock_socket.close.assert_called_once()

    @patch("recovery_libs.utils.lazy_otlp_handler.socket.socket")
    def test_reachable_endpoint_bare_host_port(self, mock_socket_cls):
        """Test reachable endpoint with bare host:port format."""
        mock_socket = MagicMock()
        mock_socket_cls.return_value = mock_socket

        result = _check_endpoint_reachable("localhost:4317", OTLPProtocol.GRPC)

        self.assertTrue(result)
        mock_socket.connect.assert_called_once_with(("localhost", 4317))

    @patch("recovery_libs.utils.lazy_otlp_handler.socket.socket")
    def test_unreachable_endpoint(self, mock_socket_cls):
        """Test unreachable endpoint returns False."""
        mock_socket = MagicMock()
        mock_socket.connect.side_effect = ConnectionRefusedError("Connection refused")
        mock_socket_cls.return_value = mock_socket

        result = _check_endpoint_reachable("http://localhost:4317", OTLPProtocol.GRPC)

        self.assertFalse(result)

    @patch("recovery_libs.utils.lazy_otlp_handler.socket.socket")
    def test_timeout(self, mock_socket_cls):
        """Test timeout returns False."""

        mock_sock = MagicMock()
        mock_sock.connect.side_effect = TimeoutError("Connection timed out")
        mock_socket_cls.return_value = mock_sock

        result = _check_endpoint_reachable("http://localhost:4317", OTLPProtocol.GRPC, timeout=1.0)

        self.assertFalse(result)

    @patch("recovery_libs.utils.lazy_otlp_handler.socket.socket")
    def test_default_port_grpc(self, mock_socket_cls):
        """Test default port for gRPC is OTLP_GRPC_DEFAULT_PORT."""
        mock_socket = MagicMock()
        mock_socket_cls.return_value = mock_socket

        _check_endpoint_reachable("http://localhost", OTLPProtocol.GRPC)

        mock_socket.connect.assert_called_once_with(("localhost", OTLP_GRPC_DEFAULT_PORT))

    @patch("recovery_libs.utils.lazy_otlp_handler.socket.socket")
    def test_default_port_http(self, mock_socket_cls):
        """Test default port for HTTP is OTLP_HTTP_DEFAULT_PORT."""
        mock_socket = MagicMock()
        mock_socket_cls.return_value = mock_socket

        _check_endpoint_reachable("http://localhost", OTLPProtocol.HTTP)

        mock_socket.connect.assert_called_once_with(("localhost", OTLP_HTTP_DEFAULT_PORT))


class TestLazyOTLPHandler(unittest.TestCase):
    @patch("recovery_libs.utils.lazy_otlp_handler._check_endpoint_reachable", return_value=False)
    def test_handler_initialization(self, mock_check):
        """Test that handler initializes without blocking."""
        start = time.time()
        handler = LazyOTLPHandler(
            endpoint="http://localhost:4318", protocol=OTLPProtocol.HTTP, token="t", service_name="s"
        )
        self.assertLess(time.time() - start, 1.0)
        self.assertFalse(handler.is_connected)
        handler.close()

    @patch("recovery_libs.utils.lazy_otlp_handler.OTLPHandler")
    @patch("recovery_libs.utils.lazy_otlp_handler._check_endpoint_reachable", return_value=True)
    def test_successful_connection(self, mock_check, mock_cls):
        """Test successful connection on first attempt."""
        mock_cls.return_value = MagicMock()
        handler = LazyOTLPHandler(
            endpoint="http://localhost:4318",
            protocol=OTLPProtocol.HTTP,
            token="t",
            service_name="s",
            initial_retry_interval=0.1,
        )
        time.sleep(0.3)
        self.assertTrue(handler.is_connected)
        handler.close()

    @patch("recovery_libs.utils.lazy_otlp_handler._check_endpoint_reachable", return_value=False)
    def test_emit_before_connection(self, mock_check):
        """Test that emit silently drops logs before connection is established."""
        handler = LazyOTLPHandler(
            endpoint="http://localhost:4318",
            protocol=OTLPProtocol.HTTP,
            token="t",
            service_name="s",
            initial_retry_interval=60,
        )
        handler.emit(logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None))
        self.assertFalse(handler.is_connected)
        handler.close()

    @patch("recovery_libs.utils.lazy_otlp_handler._check_endpoint_reachable", return_value=False)
    def test_exponential_backoff(self, mock_check):
        """Test that retry interval increases exponentially."""
        handler = LazyOTLPHandler(
            endpoint="http://localhost:4318",
            protocol=OTLPProtocol.HTTP,
            token="t",
            service_name="s",
            initial_retry_interval=0.05,
            max_retry_interval=1.0,
        )
        self.assertEqual(handler._current_retry_interval, 0.05)
        time.sleep(0.2)
        self.assertGreaterEqual(handler._current_retry_interval, 0.1)
        time.sleep(0.3)
        self.assertGreaterEqual(handler._current_retry_interval, 0.2)
        handler.close()

    @patch("recovery_libs.utils.lazy_otlp_handler.OTLPHandler")
    @patch("recovery_libs.utils.lazy_otlp_handler._check_endpoint_reachable", return_value=True)
    def test_graceful_shutdown(self, mock_check, mock_cls):
        """Test graceful shutdown flushes and closes handler."""
        mock_handler = MagicMock()
        mock_cls.return_value = mock_handler
        handler = LazyOTLPHandler(
            endpoint="http://localhost:4318",
            protocol=OTLPProtocol.HTTP,
            token="t",
            service_name="s",
            initial_retry_interval=0.1,
        )
        time.sleep(0.3)
        handler.close()
        mock_handler.flush.assert_called()
        mock_handler.close.assert_called()

    @patch("recovery_libs.utils.lazy_otlp_handler.OTLPHandler")
    @patch("recovery_libs.utils.lazy_otlp_handler._check_endpoint_reachable")
    def test_connection_after_endpoint_becomes_available(self, mock_check, mock_cls):
        """Test that handler connects after endpoint becomes available."""
        mock_check.side_effect = [False, False, True]
        mock_cls.return_value = MagicMock()

        handler = LazyOTLPHandler(
            endpoint="http://localhost:4318",
            protocol=OTLPProtocol.HTTP,
            token="t",
            service_name="s",
            initial_retry_interval=0.05,
            max_retry_interval=0.5,
        )

        time.sleep(0.5)
        self.assertTrue(handler.is_connected)
        handler.close()
