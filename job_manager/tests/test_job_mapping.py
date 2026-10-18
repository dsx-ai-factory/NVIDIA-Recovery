# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from job_manager.job_mapping import http_to_storage_job_status
from recovery_libs.schema.aps_spec.spec import ApsSlurmJobSpec, AutoResumeModeEnum
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus


def test_http_to_storage_job_status_keeps_aps_spec_model():
    job = SlurmJobExecutionStatus(
        job_id="job-aps-spec-model",
        job_attempt_index=0,
        workflow_id="workflow-test",
        state="RUNNING",
        aps_spec={"auto_resume_mode": AutoResumeModeEnum.REQUEUE.value},
    )

    record = http_to_storage_job_status(job)

    assert isinstance(record.aps_spec, ApsSlurmJobSpec)
    assert record.aps_spec.auto_resume_mode == AutoResumeModeEnum.REQUEUE
