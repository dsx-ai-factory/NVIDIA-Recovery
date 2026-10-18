# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Conversions between Job storage records and public HTTP models."""

from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus
from recovery_libs.storage.dataclass import JobConfigValidationFailureTypes, SlurmJobExecutionRecord


def storage_to_http_job_status(record: SlurmJobExecutionRecord) -> SlurmJobExecutionStatus:
    """Convert a Job storage record to its public HTTP representation."""
    internal_record_dict = record.to_dict()
    internal_record_dict["config_validation_failure_types"] = (
        [failure.name for failure in record.config_validation_failure_types]
        if record.config_validation_failure_types
        else None
    )
    if record.heimdall_job_id:
        internal_record_dict["filename"] = str(record.heimdall_job_id)
    return SlurmJobExecutionStatus(**internal_record_dict)


def http_to_storage_job_status(http_job: SlurmJobExecutionStatus) -> SlurmJobExecutionRecord:
    """Convert a public HTTP Job representation to its storage record."""
    job_dict = http_job.model_dump(exclude_none=True)
    job_dict.setdefault("last_updated_at", None)

    if job_dict.get("config_validation_failure_types"):
        job_dict["config_validation_failure_types"] = [
            JobConfigValidationFailureTypes[failure_type]
            for failure_type in job_dict["config_validation_failure_types"]
        ]

    # Preserve the typed specification expected by the storage type decorators.
    if job_dict.get("aps_spec") and isinstance(job_dict["aps_spec"], dict):
        job_dict["aps_spec"] = ApsSlurmJobSpec.model_validate(job_dict["aps_spec"])

    job_dict.pop("id", None)
    # `filename` is an external compatibility field and has no dedicated DB column.
    job_dict.pop("filename", None)

    return SlurmJobExecutionRecord(**job_dict)
