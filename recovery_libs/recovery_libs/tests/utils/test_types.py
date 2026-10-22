# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Unit tests for the types module."""

import pytest
from recovery_libs.utils.types import OTLPProtocol


class TestOTLPProtocol:
    """Test suite for OTLPProtocol enum."""

    @pytest.mark.parametrize(
        "protocol,expected_value",
        [(OTLPProtocol.HTTP, "http"), (OTLPProtocol.GRPC, "grpc")],
    )
    def test_protocol_values(self, protocol: OTLPProtocol, expected_value: str):
        """Test protocol enum values are correct."""
        assert protocol.value == expected_value
        assert isinstance(protocol.value, str)

    @pytest.mark.parametrize(
        "protocol_string,expected_enum",
        [
            ("http", OTLPProtocol.HTTP),
            ("grpc", OTLPProtocol.GRPC),
            ("HTTP", OTLPProtocol.HTTP),
            ("GRPC", OTLPProtocol.GRPC),
            ("HtTp", OTLPProtocol.HTTP),
            ("http/protobuf", OTLPProtocol.HTTP),
        ],
        ids=["http-lower", "grpc-lower", "HTTP-upper", "GRPC-upper", "http-mixed", "http-protobuf-spec"],
    )
    def test_from_string_valid_protocols(self, protocol_string: str, expected_enum: OTLPProtocol):
        """Test from_string converts valid protocol strings case-insensitively."""
        assert OTLPProtocol.from_string(protocol_string) == expected_enum

    @pytest.mark.parametrize("invalid_protocol", ["tcp", "udp", "websocket", "", "http/json"])
    def test_from_string_invalid_protocols(self, invalid_protocol: str):
        """Test from_string raises ValueError for invalid protocols."""
        with pytest.raises(ValueError) as exc_info:
            OTLPProtocol.from_string(invalid_protocol)

        error_msg = str(exc_info.value)
        assert "Invalid OTLP protocol" in error_msg
        assert invalid_protocol in error_msg

    def test_enum_comparison_and_iteration(self):
        """Test enum comparison and iteration."""
        # Comparison
        assert OTLPProtocol.HTTP != OTLPProtocol.GRPC

        # Iteration
        protocols = list(OTLPProtocol)
        assert len(protocols) == 2
        assert OTLPProtocol.HTTP in protocols
