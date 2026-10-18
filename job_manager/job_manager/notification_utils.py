# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


from recovery_libs.notifications_client import JobRequeued, ReachedMaxRequeueTime, TemplateData
from recovery_libs.schema.job_manager.models import JobTerminalAdvice
from recovery_libs.storage.dataclass import JobConfigValidationFailureTypes, SlurmJobExecutionRecord

invalid_job_config_messages = {
    JobConfigValidationFailureTypes.LOG_FILE_PATH_NOT_FOUND: (
        "No valid job log file path found, the log file name must start with '<job_name>_<job_id>_date' "
        "to be considered valid. Preferably with timestamp information included as the unique identifier"
    ),
}

_MAX_REQUEUE_MESSAGE = "Reached the maximum number of requeue attempts."


def build_recovery_notification(
    job_record: SlurmJobExecutionRecord, terminal_advice: JobTerminalAdvice
) -> TemplateData | None:
    """Build the dashboard notification payload for a recovery terminal advice.

    Persists the records that keep the dashboard's notifications panel working
    (persisted for the dashboard only; no Slack/email):
      * JOB_RELEASED        -> JobRequeued (the attempt is being requeued)
      * MAX_REQUEUE_REACHED -> ReachedMaxRequeueTime

    Other terminal advice (e.g. USER_CANCELED) carries no user-facing notification. Returns
    None when no notification applies or a required field (job_name / user) is missing.
    """
    if job_record.job_name is None or job_record.user is None:
        return None
    if terminal_advice == JobTerminalAdvice.JOB_RELEASED:
        event_type = job_record.anomalies[-1].anomaly_type if job_record.anomalies else "ANOMALY"
        return TemplateData(
            JobRequeued(
                job_id=job_record.job_id,
                job_attempt_index=job_record.job_attempt_index,
                job_name=job_record.job_name,
                event_type=event_type,
                user=job_record.user,
            )
        )
    if terminal_advice == JobTerminalAdvice.MAX_REQUEUE_REACHED:
        return TemplateData(
            ReachedMaxRequeueTime(
                job_id=job_record.job_id,
                job_attempt_index=job_record.job_attempt_index,
                job_name=job_record.job_name,
                message=_MAX_REQUEUE_MESSAGE,
            )
        )
    return None
