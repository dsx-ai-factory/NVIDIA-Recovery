# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""The suite's settings, read once from the environment."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass

from e2e_harness.log_backend import validate_log_backend, validate_log_backend_url


@dataclass(frozen=True, slots=True)
class E2EConfig:
    """One immutable snapshot of the suite's environment-derived inputs."""

    kubeconfig: str | None
    helm_release: str
    profile: str
    slurm_namespace: str
    validate_timeout_override_seconds: float | None
    job_min_runtime_seconds: float
    recovery_poll_interval_seconds: float
    attribution_mode: str
    loki_url: str
    log_backend: str
    victorialogs_url: str
    jitter_seconds_override: float | None
    recovery_attempt_hold_seconds: float
    watcher_evidence_timeout_seconds: float
    attribution_timeout_seconds: float
    stack_namespace: str
    stack_timeout_seconds: int

    @property
    def log_backend_url(self) -> str:
        """Return the endpoint configured for the selected collection backend."""

        return self.loki_url if self.log_backend == "loki" else self.victorialogs_url

    def workload_name(self, component: str) -> str:
        """Resolve the shipped chart's default name for this suite's Helm release."""

        release = self.helm_release
        if component == "attribution-service":
            return f"{release[:43].removesuffix('-')}-attribution-service"
        name = release if component in release else f"{release}-{component}"
        return name[:63].removesuffix("-")

    def jitter_seconds(self, default: float) -> float:
        """Return the caller override or a scenario-owned default."""

        if self.jitter_seconds_override is None:
            return default
        return self.jitter_seconds_override

    def validation_timeout(self, default: float) -> float:
        """Return the caller override or a scenario-owned deadline."""

        if self.validate_timeout_override_seconds is None:
            return default
        return self.validate_timeout_override_seconds


def _number(environment: Mapping[str, str], name: str, default: str) -> float:
    raw_value = environment.get(name, default)
    try:
        value = float(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be numeric, got {raw_value!r}") from error
    if not math.isfinite(value):
        raise ValueError(f"{name} must be finite, got {raw_value!r}")
    if value < 0:
        raise ValueError(f"{name} must not be negative, got {raw_value!r}")
    return value


def _positive_number(environment: Mapping[str, str], name: str, default: str) -> float:
    value = _number(environment, name, default)
    if value == 0:
        raise ValueError(f"{name} must be positive, got {environment.get(name, default)!r}")
    return value


def e2e_config_from_environment(environment: Mapping[str, str]) -> E2EConfig:
    """Validate a process-environment snapshot and return explicit config."""

    profile = environment.get("E2E_PROFILE", "advisor-act")
    log_backend = validate_log_backend(environment.get("E2E_LOG_BACKEND", "loki"))
    attribution_mode = environment.get("E2E_ATTRIBUTION_MODE", "available")
    if attribution_mode not in ("unavailable", "available"):
        raise ValueError(f"invalid E2E_ATTRIBUTION_MODE={attribution_mode}; expected unavailable or available")
    selected_url = environment.get("LOKI_URL" if log_backend == "loki" else "VICTORIALOGS_URL", "")
    validate_log_backend_url(log_backend, selected_url)
    return E2EConfig(
        kubeconfig=environment.get("KUBECONFIG"),
        helm_release=environment.get("HELM_RELEASE", "nvidia-recovery"),
        profile=profile,
        slurm_namespace=environment.get("SLURM_NS", "slurm"),
        validate_timeout_override_seconds=(
            _number(environment, "VALIDATE_TIMEOUT", "0") if "VALIDATE_TIMEOUT" in environment else None
        ),
        job_min_runtime_seconds=_number(environment, "JOB_MIN_RUNTIME", "20"),
        recovery_poll_interval_seconds=_positive_number(environment, "SLURM_RECOVERY_POLL_INTERVAL", "2"),
        attribution_mode=attribution_mode,
        loki_url=environment.get("LOKI_URL", ""),
        log_backend=log_backend,
        victorialogs_url=environment.get("VICTORIALOGS_URL", ""),
        jitter_seconds_override=(_number(environment, "JITTER_SEC", "0") if "JITTER_SEC" in environment else None),
        recovery_attempt_hold_seconds=_number(environment, "RECOVERY_ATTEMPT_HOLD_SEC", "120"),
        watcher_evidence_timeout_seconds=_number(environment, "WATCHER_EVIDENCE_TIMEOUT", "90"),
        attribution_timeout_seconds=_number(environment, "ATTRIBUTION_TIMEOUT", "180"),
        stack_namespace=environment.get("E2E_STACK_NAMESPACE", ""),
        stack_timeout_seconds=int(_number(environment, "E2E_STACK_TIMEOUT", "180")),
    )
