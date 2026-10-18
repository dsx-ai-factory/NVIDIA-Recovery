# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import time

import requests
from prometheus_client import Counter, Histogram
from recovery_libs.schema.job_manager.models import SlurmJobExecutionStatus

from anomaly_detection_isolator.settings.settings import Settings
from anomaly_detection_isolator.utils.logger import logger

JOB_MANAGER_ERROR_LABEL = "jobmanager_error"
JOB_MANAGER_FETCH_TIME = Histogram(
    "adi_downstream_jobmanager_call_time", "Time taken (in seconds) to call job manager to fetch anomalies"
)
JOB_MANAGER_FETCH_ERRORS = Counter(
    "adi_downstream_jobmanager_call_errors", "Error calling job manager to fetch anomalies", [JOB_MANAGER_ERROR_LABEL]
)


class JobManagerClient:
    def __init__(self, settings: Settings):
        """Initialize JobManagerClient with settings.

        Args:
            settings (Settings): Application settings containing Job Manager configuration
        """
        self.settings = settings
        self.timeout = settings.jobmanager_http_timeout_seconds
        if self.settings.jobmanager_port:
            self.base_url = f"{settings.jobmanager_host}:{settings.jobmanager_port}"
        else:
            self.base_url = f"{settings.jobmanager_host}"

    def get_job_status(
        self, delivery_tag: str, job_id: str, job_attempt_index: int | None = None
    ) -> SlurmJobExecutionStatus | None:
        """Get job status from Job Manager service.

        Args:
            delivery_tag (str): Message delivery tag (to differenetiate between ADI threads)
            job_id (str): ID of the job
            job_attempt_index (Optional[int]): Index of job attempt

        Returns:
            SlurmJobExecutionStatus: Parsed job status information

        Raises:
            None: If the API call fails
        """
        url = f"{self.base_url}{self.settings.jobmanager_status_path}{job_id}"

        params = {}
        if job_attempt_index is not None:
            params["job_attempt_index"] = job_attempt_index

        try:
            start_time = time.time()
            response = requests.get(url, params=params, timeout=self.timeout)
            end_time = time.time()
            JOB_MANAGER_FETCH_TIME.observe(float(end_time - start_time))

            response.raise_for_status()

            response_data = response.json()
            return SlurmJobExecutionStatus(**response_data)
        except requests.exceptions.RequestException as e:
            JOB_MANAGER_FETCH_ERRORS.labels({JOB_MANAGER_ERROR_LABEL: str(e)}).inc()
            logger.error(f"[Worker][{delivery_tag}] Error getting the anomaly details from Job Manager: {e}")
            return None
