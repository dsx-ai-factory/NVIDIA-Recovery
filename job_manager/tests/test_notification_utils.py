# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import unittest
from datetime import UTC, datetime

from job_manager.notification_utils import build_recovery_notification
from recovery_libs.notifications_client import JobRequeued, ReachedMaxRequeueTime
from recovery_libs.schema.job_manager.models import JobAnomaly, JobTerminalAdvice
from test_utils import create_example_slurm_job_record


def _record(job_name="job-a", user="alice", anomalies=None):
    record = create_example_slurm_job_record(job_id="job-1", attempt_index=2)
    record.job_name = job_name
    record.user = user
    record.anomalies = anomalies
    return record


class TestBuildRecoveryNotification(unittest.TestCase):
    def test_job_released_builds_job_requeued(self):
        anomaly = JobAnomaly(anomaly_type="CRASH", timestamp=datetime.now(UTC))
        template_data = build_recovery_notification(_record(anomalies=[anomaly]), JobTerminalAdvice.JOB_RELEASED)
        self.assertIsInstance(template_data.actual_instance, JobRequeued)
        self.assertEqual(template_data.actual_instance.job_id, "job-1")
        self.assertEqual(template_data.actual_instance.job_attempt_index, 2)
        self.assertEqual(template_data.actual_instance.event_type, "CRASH")
        self.assertEqual(template_data.actual_instance.user, "alice")

    def test_job_released_without_anomaly_defaults_event_type(self):
        template_data = build_recovery_notification(_record(anomalies=None), JobTerminalAdvice.JOB_RELEASED)
        self.assertIsInstance(template_data.actual_instance, JobRequeued)
        self.assertEqual(template_data.actual_instance.event_type, "ANOMALY")

    def test_max_requeue_builds_reached_max(self):
        template_data = build_recovery_notification(_record(), JobTerminalAdvice.MAX_REQUEUE_REACHED)
        self.assertIsInstance(template_data.actual_instance, ReachedMaxRequeueTime)
        self.assertEqual(template_data.actual_instance.job_name, "job-a")

    def test_user_canceled_has_no_notification(self):
        self.assertIsNone(build_recovery_notification(_record(), JobTerminalAdvice.USER_CANCELED))

    def test_missing_user_is_none(self):
        self.assertIsNone(build_recovery_notification(_record(user=None), JobTerminalAdvice.JOB_RELEASED))

    def test_missing_job_name_is_none(self):
        self.assertIsNone(build_recovery_notification(_record(job_name=None), JobTerminalAdvice.MAX_REQUEUE_REACHED))


if __name__ == "__main__":
    unittest.main()
