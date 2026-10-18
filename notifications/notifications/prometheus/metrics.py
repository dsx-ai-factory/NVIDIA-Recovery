# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from prometheus_client import Counter, Histogram, Info

NAMESPACE = "aps"
SUBSYSTEM = "notifications"

# API metrics
CREATE_NOTIFICATION_API_COUNTER = Counter(
    name="create_notification_api_counter",
    documentation="The number of create notification API calls",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)

GET_NOTIFICATION_API_COUNTER = Counter(
    name="get_notification_api_counter",
    documentation="The number of get notification API calls",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)

UPDATE_NOTIFICATION_API_COUNTER = Counter(
    name="update_notification_api_counter",
    documentation="The number of update notification API calls",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)

# Notification processing metrics
NOTIFICATION_SEND_LATENCY = Histogram(
    name="notification_send_latency_nanoseconds",
    documentation="Time (in nanoseconds) taken to send notifications. (Does not include time spent in task queue.)",
    labelnames=["channel"],
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)

# Notification delivery metrics
NOTIFICATION_SENT_COUNTER = Counter(
    name="notification_sent_total",
    documentation="Number of notifications sent",
    labelnames=["channel", "status"],
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)

NOTIFICATION_DELIVERY_FAILURES = Counter(
    name="notification_delivery_failures_total",
    documentation="Number of notification delivery failures",
    labelnames=["channel", "reason"],
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)

# Database metrics
DATABASE_OPERATION_LATENCY = Histogram(
    name="database_operation_latency_nanoseconds",
    documentation="Time (in nanoseconds) taken for database operations",
    labelnames=["operation"],
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)

DATABASE_INITIALIZATION_ERRORS = Counter(
    name="database_initialization_errors_total",
    documentation="Number of database initialization errors",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)

# Config metrics
SERVICE_CONFIG_INFO = Info(
    name="service_config_info",
    documentation="Information about the notifications service configuration",
    namespace=NAMESPACE,
    subsystem=SUBSYSTEM,
)
