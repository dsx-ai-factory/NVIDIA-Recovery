# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from pydantic import Field, SecretStr
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class WatcherSettingsBase(BaseSettings):
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


class RabbitMQConnectionSettings(WatcherSettingsBase):
    model_config = SettingsConfigDict(env_prefix="rabbitmq_", extra="ignore")

    host: str
    username: str
    password: str = Field(repr=False)
    vhost: str


class LokiConnectionSettings(WatcherSettingsBase):
    model_config = SettingsConfigDict(env_prefix="loki_", extra="ignore")

    api_token: SecretStr | None = None

    cert_file: str | None = None
    key_file: str | None = None
    ca_cert: str | None = None
    x_scope_org_id: str | None = None

    url: str


class VictoriaLogsConnectionSettings(WatcherSettingsBase):
    """Environment-overridable VictoriaLogs query connection settings."""

    model_config = SettingsConfigDict(env_prefix="victorialogs_", extra="ignore")

    username: str | None = None
    password: SecretStr | None = Field(default=None, repr=False)
    api_token: SecretStr | None = Field(default=None, repr=False)
    cert_file: str | None = None
    key_file: str | None = None
    ca_cert: str | None = None
    account_id: int | None = Field(default=None, ge=0, le=4_294_967_295)
    project_id: int | None = Field(default=None, ge=0, le=4_294_967_295)
    require_tenant: bool = False
    url: str
