# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from pydantic import Field, SecretStr
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class JobManagerSettingsBase(BaseSettings):
    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Let environment override init provided values
        return (env_settings, dotenv_settings, init_settings)


class RabbitMQConnectionSettings(JobManagerSettingsBase):
    model_config = SettingsConfigDict(env_prefix="rabbitmq_", extra="ignore")

    host: str
    username: str
    password: SecretStr
    vhost: str


class MySQLConnectionSettings(JobManagerSettingsBase):
    model_config = SettingsConfigDict(env_prefix="MYSQL_")

    host: str
    port: int
    username: str
    password: SecretStr
    database_name: str
    ssl_enabled: bool = False
    ssl_ca_path: str | None = "/etc/mysql-ca/ca.pem"
    write_pool_enabled: bool = Field(default=True, description="Use a dedicated connection pool for job upserts.")
    write_pool_size: int = Field(default=5, description="Pool size for the dedicated upsert connection pool.")
    write_max_overflow: int = Field(
        default=10, description="Max overflow connections for the dedicated upsert connection pool."
    )


class KubernetesLeaderElectionSettings(JobManagerSettingsBase):
    model_config = SettingsConfigDict(env_prefix="k8s_leader_")

    # The name of the lease resource used for leader election
    lease_name: str = "job-manager-leader-lease"
    # Lock time to live in seconds
    lease_duration_seconds: int = 15
    # How often to renew the lease in seconds
    renew_deadline_seconds: int = 10
    # How long a candidate will wait for a response before restarting an election
    retry_period_seconds: int = 5
    # Whether to use in-cluster configuration
    in_cluster: bool = True
    # Path to kubeconfig file if not using in-cluster config
    kubeconfig_path: str | None = None
