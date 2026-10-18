# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Provider-specific log-query connection configuration."""

from __future__ import annotations

import math
from dataclasses import dataclass
from urllib.parse import urlparse

_MAX_TENANT_ID = 2**32 - 1


def _validate_common_tls(
    *,
    url: str,
    cert_file: str | None,
    key_file: str | None,
    ca_cert: str | None,
    verify_tls: bool,
    connect_timeout_seconds: float | None,
) -> None:
    """Validate connection settings shared by both provider configurations."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("url must be an absolute http or https URL")
    if (cert_file is None) != (key_file is None):
        raise ValueError("cert_file and key_file must be configured together")
    if not verify_tls and ca_cert is not None:
        raise ValueError("ca_cert cannot be used when verify_tls is false")
    if connect_timeout_seconds is not None and (
        isinstance(connect_timeout_seconds, bool)
        or not isinstance(connect_timeout_seconds, (int, float))
        or not math.isfinite(connect_timeout_seconds)
        or connect_timeout_seconds <= 0
    ):
        raise ValueError("connect_timeout_seconds must be positive and finite when provided")


@dataclass(frozen=True, slots=True)
class LokiBackendConfig:
    """Connection, authentication, tenancy, and TLS settings for Loki.

    Attributes:
        url: Loki base URL.
        auth_token: Existing raw Authorization value, or Basic-auth password.
        tenants: Existing Basic-auth username when ``auth_token`` is set.
        org_id: Native Loki ``X-Scope-OrgID`` tenant header.
        cert_file: Optional mutual-TLS client certificate path.
        key_file: Optional mutual-TLS client private-key path.
        ca_cert: Optional additional CA certificate path.
        verify_tls: Whether server certificates must be verified.
        connect_timeout_seconds: Optional transport connect-phase timeout.
    """

    url: str
    auth_token: str | None = None
    tenants: str | None = None
    org_id: str | None = None
    cert_file: str | None = None
    key_file: str | None = None
    ca_cert: str | None = None
    verify_tls: bool = True
    connect_timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        """Reject partial authentication or TLS configuration."""
        _validate_common_tls(
            url=self.url,
            cert_file=self.cert_file,
            key_file=self.key_file,
            ca_cert=self.ca_cert,
            verify_tls=self.verify_tls,
            connect_timeout_seconds=self.connect_timeout_seconds,
        )
        if self.tenants and not self.auth_token:
            raise ValueError("Loki tenants-based basic authentication requires auth_token")


@dataclass(frozen=True, slots=True)
class VictoriaLogsBackendConfig:
    """Connection, proxy authentication, tenant, and TLS settings for VictoriaLogs.

    Attributes:
        url: VictoriaLogs base URL.
        account_id: Native unsigned 32-bit ``AccountID`` tenant identifier.
        project_id: Native unsigned 32-bit ``ProjectID`` tenant identifier.
        require_tenant: Reject implicit single-tenant configuration when true.
        username: Optional proxy Basic-auth username.
        password: Optional proxy Basic-auth password.
        bearer_token: Optional proxy bearer credential, excluding its scheme.
        cert_file: Optional mutual-TLS client certificate path.
        key_file: Optional mutual-TLS client private-key path.
        ca_cert: Optional additional CA certificate path.
        verify_tls: Whether server certificates must be verified.
        connect_timeout_seconds: Optional transport connect-phase timeout.
    """

    url: str
    account_id: int | None = None
    project_id: int | None = None
    require_tenant: bool = False
    username: str | None = None
    password: str | None = None
    bearer_token: str | None = None
    cert_file: str | None = None
    key_file: str | None = None
    ca_cert: str | None = None
    verify_tls: bool = True
    connect_timeout_seconds: float | None = None

    def __post_init__(self) -> None:
        """Reject ambiguous proxy authentication and tenant configuration."""
        _validate_common_tls(
            url=self.url,
            cert_file=self.cert_file,
            key_file=self.key_file,
            ca_cert=self.ca_cert,
            verify_tls=self.verify_tls,
            connect_timeout_seconds=self.connect_timeout_seconds,
        )
        if (self.account_id is None) != (self.project_id is None):
            raise ValueError("VictoriaLogs account_id and project_id must be configured together")
        if self.account_id is not None:
            valid_account = (
                isinstance(self.account_id, int)
                and not isinstance(self.account_id, bool)
                and 0 <= self.account_id <= _MAX_TENANT_ID
            )
            valid_project = (
                isinstance(self.project_id, int)
                and not isinstance(self.project_id, bool)
                and 0 <= self.project_id <= _MAX_TENANT_ID
            )
            if not valid_account or not valid_project:
                raise ValueError("VictoriaLogs tenant identifiers must be unsigned 32-bit integers")
        if self.require_tenant and self.account_id is None:
            raise ValueError("VictoriaLogs tenant identifiers are required for this deployment")
        if (self.username is None) != (self.password is None):
            raise ValueError("VictoriaLogs username and password must be configured together")
        if self.bearer_token and self.username:
            raise ValueError("VictoriaLogs basic and bearer authentication are mutually exclusive")
