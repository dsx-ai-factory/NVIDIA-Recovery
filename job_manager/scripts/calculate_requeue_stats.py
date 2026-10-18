#!/usr/bin/env python3

# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import sys
import traceback
from datetime import timedelta

import requests
from recovery_libs.schema.job_manager.models import (
    JobExecutionStatuses,
    SlurmJobExecutionStatus,
)
from recovery_libs.storage.dataclass import SlurmJobFinalStates

"""
This script calculates recovery and requeue statistics for attempts matching a
job name.

To run it, please make sure pypi package "requests" was installed (either through pip or uv).
"""

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True,
)
logger = logging.getLogger(__name__)

SLURM_JOB_FINAL_STATE_SET = {s.value for s in SlurmJobFinalStates}

# Check if the script is being executed with the expected number of arguments
if len(sys.argv) != 3:
    logger.error("Usage: python3 calculate_requeue_stats.py <job-manager-url> <job-name>")
    sys.exit(1)

job_manager_url = sys.argv[1]
job_name = sys.argv[2].strip()

job_attempts = []
job_attempts_by_id: dict[str, list[SlurmJobExecutionStatus]] = {}

longest_requeue_sequence_length = 0
longest_requeue_sequence_job_id = None
longest_requeue_job_run_duration = timedelta(0)
longest_requeue_job_run_duration_job_id = None
longest_requeue_e2e_duration = timedelta(0)
longest_requeue_e2e_duration_job_id = None

total_duration = timedelta(0)

try:
    offset = 0
    while response := requests.get(
        f"{job_manager_url}/jobs?job_name={job_name}&sort_by=job_id:asc,job_attempt_index:asc&limit=30&offset={offset}"
    ):
        job_statuses = JobExecutionStatuses.model_validate(response.json())

        job_attempts.extend(job_statuses.statuses)
        offset += len(job_statuses.statuses)

        logger.info(
            f"Fetched {offset} job records out of total {job_statuses.item_count} records for job name {job_name} so far."
        )

        if offset >= job_statuses.item_count:
            logger.info("All matching records are found.")
            break

    for job_attempt in job_attempts:
        job_attempts_by_id.setdefault(job_attempt.job_id, [])
        job_attempts_by_id[job_attempt.job_id].append(job_attempt)

    for job_id, attempts in job_attempts_by_id.items():
        logger.info(f"Job ID {job_id} has {len(attempts)} attempts.")
        if len(attempts) > longest_requeue_sequence_length:
            logger.info(f"New longest requeue sequence found with length {len(attempts)} for job ID {job_id}.")
            longest_requeue_sequence_length = len(attempts)
            longest_requeue_sequence_job_id = job_id

        total_job_run_duration = timedelta(0)
        job_id_sequence_submit_time = None
        job_id_sequence_end_time = None
        for attempt in attempts:
            if attempt.state not in SLURM_JOB_FINAL_STATE_SET:
                logger.warning(
                    f"Job ID {job_id} attempt index {attempt.job_attempt_index} is not in final state {attempt.state}, "
                    "skipping duration calculation."
                )
                continue

            if not attempt.start_time or not attempt.end_time or not attempt.submit_time:
                logger.warning(
                    f"Job ID {job_id} attempt index {attempt.job_attempt_index} is missing submit, start or end time, "
                    f"submit_time: {attempt.submit_time} start_time: {attempt.start_time}, end_time: {attempt.end_time}, "
                    " skipping duration calculation."
                )
                continue

            attempt_duration = attempt.end_time - attempt.start_time
            total_job_run_duration += attempt_duration
            total_duration += attempt_duration

            if not job_id_sequence_submit_time or attempt.submit_time < job_id_sequence_submit_time:
                job_id_sequence_submit_time = attempt.submit_time

            if not job_id_sequence_end_time or attempt.end_time > job_id_sequence_end_time:
                job_id_sequence_end_time = attempt.end_time

        if job_id_sequence_end_time and job_id_sequence_submit_time:
            job_e2e_duration = job_id_sequence_end_time - job_id_sequence_submit_time
            logger.info(f"Total e2e duration for job ID {job_id} is {job_e2e_duration}.")

        if job_e2e_duration > longest_requeue_e2e_duration:
            logger.info(f"New longest requeue job e2e duration found: {job_e2e_duration} for job ID {job_id}.")
            longest_requeue_e2e_duration = job_e2e_duration
            longest_requeue_e2e_duration_job_id = job_id

        logger.info(f"Total requeue duration for job ID {job_id} is {total_job_run_duration}.")
        if total_job_run_duration > longest_requeue_job_run_duration:
            logger.info(f"New longest requeue job run duration found: {total_job_run_duration} for job ID {job_id}.")
            longest_requeue_job_run_duration = total_job_run_duration
            longest_requeue_job_run_duration_job_id = job_id

        print("--------------------------------------------------\n")

    logger.info(f"Requeue stats calculation completed for job name {job_name}: ")
    logger.info(
        f"Longest requeue sequence length is {longest_requeue_sequence_length} for job ID {longest_requeue_sequence_job_id}."
    )
    logger.info(
        f"Longest requeue job run duration is {longest_requeue_job_run_duration} for job ID {longest_requeue_job_run_duration_job_id}."
    )
    logger.info(
        f"Longest requeue job end-to-end duration is {longest_requeue_e2e_duration} for job ID {longest_requeue_e2e_duration_job_id}."
    )
    logger.info(f"Total requeue job run duration across all job attempts is {total_duration}.")

except Exception as e:
    logger.exception(
        f"Unable to calculate the requeue stats for job name {job_name}",
        extra={"job_name": job_name, "error": str(e)},
    )
    traceback.print_exc()
    sys.exit(1)
