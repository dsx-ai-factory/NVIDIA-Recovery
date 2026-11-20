# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Recognize attempts published by direct Kubernetes workload discovery."""

from collections.abc import Mapping


def is_direct_kubernetes_observation(metadata: Mapping[str, object] | None) -> bool:
    """Return whether scheduler metadata identifies a discovered Kubernetes workload object.

    Discovery stamps the Kubernetes object UID on every observation. Other
    producers may report ``scheduler: kubernetes`` without that concrete
    identity and keep their existing behavior.
    """
    if not metadata or metadata.get("scheduler") != "kubernetes":
        return False
    workload_uid = metadata.get("workload_uid")
    return isinstance(workload_uid, str) and bool(workload_uid)


KUBERNETES_LOOKUP_KEYS = ("namespace", "job_name", "job_kind", "job_api_version")
"""Scheduler metadata keys that locate a discovered workload object.

Restart inventory and Advice job references resolve the workload from these
keys, so they must stay usable and must not change for one workload UID.
"""


def missing_kubernetes_lookup_keys(metadata: Mapping[str, object]) -> list[str]:
    """Return lookup keys that are absent, null, or empty in ``metadata``."""
    return [key for key in KUBERNETES_LOOKUP_KEYS if metadata.get(key) in (None, "")]


def validate_kubernetes_lookup(metadata: Mapping[str, object]) -> None:
    """Reject present lookup values that cannot address a Kubernetes object.

    Only absence is allowed; a present value must be usable as given. Values
    are not trimmed or case-folded because that would silently retarget the
    lookup. The checks are structural: kinds and names are resolved by the
    consumers that serve them, so this does not enforce DNS-1123 or a kind
    allow-list.

    Raises:
        ValueError: If a present lookup value is not a usable string.
    """
    for key in KUBERNETES_LOOKUP_KEYS:
        if key not in metadata:
            continue
        value = metadata[key]
        # No Kubernetes name, namespace, kind, or API version contains whitespace.
        if not isinstance(value, str) or not value or any(char.isspace() for char in value):
            raise ValueError(f"Kubernetes {key} must be a nonempty string without whitespace")
        if key == "job_api_version":
            segments = value.split("/")
            if len(segments) > 2 or not all(segments):
                raise ValueError("Kubernetes job_api_version must be 'version' or 'group/version'")
        elif "/" in value:
            raise ValueError(f"Kubernetes {key} must not contain '/'")
