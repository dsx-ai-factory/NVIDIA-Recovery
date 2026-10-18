# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Validate the deployment-local identity shared by audit producers."""

import uuid
from typing import Any

from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id


def scheduler_job_id_from_payload(heimdall_job_id: str, payload: dict[str, Any]) -> str | None:
    """Project only a complete payload identity that agrees with its envelope UUID.

    Do not coerce or repair old payloads: ambiguous events retain their exact-ID
    query path without accidentally joining another scheduler job's history.
    """
    if not isinstance(payload, dict):
        return None
    job_id = payload.get("job_id")
    attempt_index = payload.get("attempt_index")
    if not isinstance(job_id, str) or not job_id.strip() or len(job_id) > 255:
        return None
    if type(attempt_index) is not int or attempt_index < 0:
        return None
    try:
        if uuid.UUID(heimdall_job_id) == generate_heimdall_job_id(job_id, attempt_index):
            return job_id
    except ValueError:
        pass
    return None
