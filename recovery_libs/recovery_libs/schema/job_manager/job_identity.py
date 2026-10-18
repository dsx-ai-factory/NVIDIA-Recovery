# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid


def generate_heimdall_job_id(job_id: str, job_attempt_index: int) -> uuid.UUID:
    """Return the stable Heimdall UUID for one scheduler job attempt."""
    return uuid.uuid5(uuid.NAMESPACE_URL, f"{job_id}-{job_attempt_index}")
