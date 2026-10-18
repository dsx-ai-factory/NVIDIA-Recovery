# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import os

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings
from recovery_libs.audit import AuditTrailSettings


class Settings(BaseSettings):
    # FACT exclusion-policy error lists (consumed by exclusion_policy.derive_exclusion).
    fact_non_isolated_errors: list[str] | None = None
    fact_isolated_errors: list[str] | None = None
    fact_ambiguous_errors: list[str] | None = None
    fact_non_isolated_errors_env: list[str] | None = None
    fact_isolated_errors_env: list[str] | None = None
    fact_ambiguous_errors_env: list[str] | None = None
    root_path: str
    exchange_name: str
    routing_key: str
    queue_name: str
    rabbitmq_user: str
    rabbitmq_password: SecretStr
    rabbitmq_url: str
    rabbitmq_port: int
    rabbitmq_vhost: str
    attribution_request_exchange: str
    attribution_request_queue: str
    isolator_completed_exchange: str
    slow_signals_exchange: str
    slow_signals_queue: str
    slow_signals_enabled: bool | None = True
    epilogue_started_wait_seconds: int
    epilog_query_duration_minutes: int
    slow_signals_delay_seconds: int
    num_worker_threads: int
    jobmanager_host: str
    jobmanager_port: int
    jobmanager_status_path: str
    max_isolated_nodes: int
    rack_aware_isolation_enabled: bool | None = False
    max_isolated_nodes_per_rack: int | None = None
    max_racks_to_isolate: int | None = 9
    rack_extraction_pattern: str | None = None

    # HTTP request timeouts (seconds) -- applied to outbound HTTP calls.
    jobmanager_http_timeout_seconds: int = 30

    # Attribution service (attribution-adapter) configuration.
    # The Policy Engine calls attribution-adapter over HTTP via AttributionClient.
    attribution_service_url: str | None = None
    attribution_enabled: bool = True
    attribution_fast_http_timeout_seconds: int = 90
    attribution_slow_http_timeout_seconds: int = 180

    audit_trail: AuditTrailSettings = Field(default_factory=AuditTrailSettings)

    class Config:
        env_file = os.environ.get("CONFIG_FILE_PATH")
