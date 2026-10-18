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
        HTTP: HTTP protocol for OTLP communication.
        GRPC: gRPC protocol for OTLP communication.
    """

    HTTP = "http"
    GRPC = "grpc"

    @classmethod
    def from_string(cls, value: str) -> "OTLPProtocol":
        """Convert a string to OTLPProtocol enum.

        Args:
            value: String representation of the protocol ("http" or "grpc").

        Returns:
            OTLPProtocol enum value.

        Raises:
            ValueError: If the value is not a valid protocol.

        Example:
            >>> protocol = OTLPProtocol.from_string("http")
            >>> protocol == OTLPProtocol.HTTP
            True
        """
        value_lower = value.lower()
        try:
            return cls(value_lower)
        except ValueError:
            valid_values = ", ".join([p.value for p in cls])
            raise ValueError(f"Invalid OTLP protocol: '{value}'. Must be one of: {valid_values}")
