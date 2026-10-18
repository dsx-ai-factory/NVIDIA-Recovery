# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from enum import StrEnum

# OTLP default ports
OTLP_GRPC_DEFAULT_PORT = 4317
OTLP_HTTP_DEFAULT_PORT = 4318

# OTLP endpoint availability check retry configuration
OTLP_ENDPOINT_CHECK_MAX_RETRIES = 5
OTLP_ENDPOINT_CHECK_MIN_RETRY_INTERVAL = 1  # seconds
OTLP_ENDPOINT_CHECK_MAX_RETRY_INTERVAL = 5  # seconds
OTLP_ENDPOINT_CHECK_TIMEOUT = 5  # seconds


class EnvironmentTag(StrEnum):
    """
    Environment tags used across components for job to environment matching.
    """

    PROD = "prod"
    STAGING = "staging"
    DEV = "dev"
    TEST = "test"

    # UNSPECIFIED can be used when it's not necessary to specify the environment tag.
    # It's used as the default value in workflow controller, denoting that there's only one deployment and no
    # environment tag is needed.
    UNSPECIFIED = "unspecified"
