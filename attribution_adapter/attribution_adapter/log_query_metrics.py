# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Backend-neutral metrics for Attribution log queries."""

from prometheus_client import Counter, Histogram

LOG_QUERY_TIME = Histogram(
    "adi_downstream_log_query_time",
    "Time taken in seconds to query the selected log backend",
    ["backend", "query_id"],
)
LOG_QUERY_ERRORS = Counter(
    "adi_downstream_log_query_errors",
    "Errors querying the selected log backend",
    ["backend", "query_id", "error_kind"],
)
LOG_QUERY_REQUESTS = Counter(
    "adi_downstream_log_query_requests_total",
    "Completed requests to the selected log backend",
    ["backend", "query_id", "outcome"],
)
LOG_QUERY_RECORDS = Counter(
    "adi_downstream_log_query_records_total",
    "Normalized log records returned by the selected log backend",
    ["backend", "query_id"],
)
