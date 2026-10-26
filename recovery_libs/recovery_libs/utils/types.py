# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Type definitions for the utils module.

This module contains common types and enums used throughout the
utils package.
"""

from enum import Enum


class OTLPProtocol(str, Enum):
    """OpenTelemetry Protocol types.

    Defines the supported protocols for OTLP communication.

    Attributes:
        HTTP: OTLP over HTTP with protobuf payloads. ``from_string`` accepts the
            spec value ``http/protobuf`` and the legacy alias ``http``.
        GRPC: gRPC protocol for OTLP communication.
    """

    HTTP = "http"
    GRPC = "grpc"

    @classmethod
    def from_string(cls, value: str) -> "OTLPProtocol":
        """Convert a string to OTLPProtocol enum.

        The OTLP specification names HTTP transport ``http/protobuf``. The Go
        controller reads the same shared environment, so both services must
        accept the spec value; ``http`` stays accepted for existing deployments.
        ``http/json`` is rejected because no JSON exporter is installed.

        Args:
            value: String representation of the protocol ("grpc", "http/protobuf", or "http").

        Returns:
            OTLPProtocol enum value.

        Raises:
            ValueError: If the value is not a valid protocol.

        Example:
            >>> protocol = OTLPProtocol.from_string("http")
            >>> protocol == OTLPProtocol.HTTP
            True
        """
        value_lower = value.strip().lower()
        if value_lower == "http/protobuf":
            return cls.HTTP
        try:
            return cls(value_lower)
        except ValueError:
            valid_values = "grpc, http/protobuf, http"
            raise ValueError(f"Invalid OTLP protocol: '{value}'. Must be one of: {valid_values}")
