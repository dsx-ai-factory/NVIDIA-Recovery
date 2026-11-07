# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import time

import httpx
from recovery_libs.schema.job_manager.models import JobExecutionStatuses, SlurmJobExecutionStatus

from watcher.lib.utils.constants import JOB_MANAGER_PATH_GET_STATUS
from watcher.lib.utils.http import get_async
from watcher.lib.utils.logging import logger
from watcher.lib.utils.metrics import JOB_MANAGER_API_ERRORS, JOB_MANAGER_API_LATENCY, JOB_MANAGER_API_REQUESTS
from watcher.lib.utils.multithreading import timed


@timed
async def get_job_info(
    endpoint: str,
    job_id: str,
    job_attempt_index: int | None = None,
) -> dict[str, str] | None:
    """
    Retrieves training job info from Job Manager service.

    Args:
        endpoint (str): Job Manager endpoint
        job_id (str): The unique identifier of the job to check.
        job_attempt_index (Optional[int]): The job attempt index ID.

    Returns:
        Dict[str, str]: The job info response from Job Manager service.
    """
    JOB_MANAGER_API_REQUESTS.labels(job_id=job_id).inc()
    url = f"{endpoint}/{JOB_MANAGER_PATH_GET_STATUS}/{job_id}"
    params = {"job_attempt_index": job_attempt_index} if job_attempt_index is not None else None
    start = time.perf_counter()
    try:
        job_status_json = await get_async(url=url, params=params, service="job_manager")
        if job_status_json is None:
            return None
        # get_async may return raw bytes/str (when content was already JSON) or a parsed dict/list.
        if isinstance(job_status_json, (str, bytes, bytearray)):
            job_status = json.loads(job_status_json)
        else:
            job_status = job_status_json
        return job_status
    except (ValueError, json.decoder.JSONDecodeError) as err:
        # Intentionally NOT catching TypeError here: TypeError is never a legitimate
        # outcome of `await get_async(...)` or `json.loads(<str|bytes>)`, so swallowing
        # it would only hide real bugs (e.g. an async/sync mismatch like the httpx
        # AsyncClient sync-event-hook crash that masqueraded as "Unable to decode JSON
        # response from Job Manager: object NoneType can't be used in 'await' expression").
        logger.error(f"Unable to decode JSON response from Job Manager: {err}")
        JOB_MANAGER_API_ERRORS.labels(job_id=job_id).inc()
        return None
    finally:
        JOB_MANAGER_API_LATENCY.labels(job_id=job_id).observe(time.perf_counter() - start)


def get_job_info_sync(
    endpoint: str,
    job_id: str,
) -> SlurmJobExecutionStatus | None:
    """
    Retrieves training job info from Job Manager service (synchronous).

    Args:
        endpoint (str): Job Manager endpoint
        job_id (str): The unique identifier of the job to check.

    Returns:
        SlurmJobExecutionStatus: The validated job status, or None on failure.
    """
    url = f"{endpoint}/{JOB_MANAGER_PATH_GET_STATUS}/{job_id}"
    try:
        with httpx.Client() as client:
            response = client.get(url)
            response.raise_for_status()
            return SlurmJobExecutionStatus.model_validate(response.json())
    except httpx.HTTPStatusError as err:
        logger.error(f"HTTP error from Job Manager (sync): {err}")
    except (json.JSONDecodeError, ValueError) as err:
        logger.error(f"Unable to decode JSON response from Job Manager (sync): {err}")
    except Exception as err:
        logger.exception("Unable to retrieve job info from Job Manager (sync)", extra={"error": str(err)})

    return None


def get_job_attempts_sync(
    endpoint: str,
    job_id: str | None,
    *,
    scheduler_metadata: dict[str, str] | None = None,
    first_match_only: bool = False,
) -> list[SlurmJobExecutionStatus] | None:
    """Fetch all attempts for a job, ordered by descending attempt index.

    Args:
        endpoint: Job Manager endpoint.
        job_id: The unique identifier of the job to query; omit only with metadata filters.
        scheduler_metadata: Optional exact scheduler metadata string matches.
        first_match_only: Return at most one candidate for existence checks.

    Returns:
        The deduplicated attempt history, an empty list if none exist, or None
        if fetching or validating any page fails. Pagination tolerates new
        attempts being registered and does not provide a consistent snapshot.
    """
    url = f"{endpoint}/jobs"
    if job_id is None and not scheduler_metadata:
        return None
    attempts: list[SlurmJobExecutionStatus] = []
    seen_attempts: set[tuple[str, int]] = set()
    offset = 0
    try:
        with httpx.Client() as client:
            for _ in range(100):
                response = client.get(
                    url,
                    params={
                        **({"job_id": job_id} if job_id is not None else {}),
                        **({"scheduler_metadata": json.dumps(scheduler_metadata)} if scheduler_metadata else {}),
                        "sort_by": "id:asc" if scheduler_metadata else "job_attempt_index:desc",
                        "limit": 1 if first_match_only else 50,
                        "offset": offset,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                for item in payload.get("statuses", []):
                    if "schedulerMetadata" in item:
                        item["scheduler_metadata"] = item["schedulerMetadata"]
                page = JobExecutionStatuses.model_validate(payload)
                if not page.statuses:
                    return attempts

                # New retries can shift offsets and repeat attempts across pages.
                # Advance by the raw page size so deduplication does not stall progress.
                offset += len(page.statuses)

                for attempt in page.statuses:
                    # An older backend may ignore the filter. Never treat its
                    # unfiltered response as a complete scheduler identity mapping.
                    if scheduler_metadata and any(
                        attempt.scheduler_metadata.get(key) != value for key, value in scheduler_metadata.items()
                    ):
                        logger.warning("Job Manager returned a record outside the scheduler metadata filter")
                        return None
                    identity = (attempt.job_id, attempt.job_attempt_index)
                    if identity in seen_attempts:
                        continue

                    seen_attempts.add(identity)
                    attempts.append(attempt)
                if first_match_only:
                    return attempts
            logger.warning("Job identity lookup exceeded its bounded page budget for %s", job_id)
    except (httpx.HTTPError, ValueError) as err:
        logger.error(f"Unable to retrieve job attempt history for {job_id}: {err}")

    return None


def get_jobs_by_scheduler_metadata_sync(
    endpoint: str,
    metadata: dict[str, str],
    *,
    first_match_only: bool = False,
) -> list[SlurmJobExecutionStatus] | None:
    """Read filtered identity candidates, failing closed on incomplete pagination."""
    return get_job_attempts_sync(
        endpoint,
        None,
        scheduler_metadata=metadata,
        first_match_only=first_match_only,
    )
