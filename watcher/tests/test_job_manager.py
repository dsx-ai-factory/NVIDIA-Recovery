# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from unittest.mock import AsyncMock

import httpx
import pytest
from watcher.lib.plugins.job_manager import get_job_attempts_sync, get_job_info
from watcher.lib.utils.constants import JOB_MANAGER_PATH_GET_STATUS
from watcher.lib.utils.metrics import JOB_MANAGER_API_ERRORS, JOB_MANAGER_API_LATENCY, JOB_MANAGER_API_REQUESTS

ENDPOINT = "http://job-manager"
JOB_ID = "608070"


def _counter(metric, job_id=JOB_ID):
    return metric.labels(job_id=job_id)._value.get()


@pytest.mark.asyncio
async def test_get_job_info_success(mocker, jm_job_status_running):
    expected_url = f"{ENDPOINT}/{JOB_MANAGER_PATH_GET_STATUS}/{JOB_ID}"
    job_status = json.loads(jm_job_status_running)
    mock_get_async = AsyncMock(return_value=jm_job_status_running)
    mocker.patch("watcher.lib.plugins.job_manager.get_async", mock_get_async)

    req_before, err_before = _counter(JOB_MANAGER_API_REQUESTS), _counter(JOB_MANAGER_API_ERRORS)
    lat_sum_before = JOB_MANAGER_API_LATENCY.labels(job_id=JOB_ID)._sum.get()
    result = await get_job_info(endpoint=ENDPOINT, job_id=JOB_ID, job_attempt_index=0)
    assert result == job_status
    mock_get_async.assert_awaited_once_with(url=expected_url, params={"job_attempt_index": 0}, service="job_manager")
    assert _counter(JOB_MANAGER_API_REQUESTS) == req_before + 1
    assert _counter(JOB_MANAGER_API_ERRORS) == err_before
    assert JOB_MANAGER_API_LATENCY.labels(job_id=JOB_ID)._sum.get() > lat_sum_before


@pytest.mark.asyncio
async def test_get_job_info_json_decode_error(mocker, caplog):
    mocker.patch("watcher.lib.plugins.job_manager.get_async", AsyncMock(return_value="{miles davis..."))
    err_before = _counter(JOB_MANAGER_API_ERRORS)
    with caplog.at_level(logging.ERROR):
        result = await get_job_info(endpoint=ENDPOINT, job_id=JOB_ID)
    assert result is None
    assert any("Unable to decode JSON response from Job Manager" in r.message for r in caplog.records)
    assert _counter(JOB_MANAGER_API_ERRORS) == err_before + 1


@pytest.mark.asyncio
async def test_get_job_info_propagates_type_error(mocker):
    """Regression: TypeError must propagate, not be mis-logged as JSON decode error."""
    mocker.patch("watcher.lib.plugins.job_manager.get_async", AsyncMock(side_effect=TypeError))
    with pytest.raises(TypeError):
        await get_job_info(ENDPOINT, JOB_ID)


@pytest.mark.parametrize("growing", [False, True])
def test_history_pagination_preserves_old_attempts_during_new_retry(mocker, growing):
    """Offset shifts may repeat a record, but must not truncate older history."""
    pages = {
        0: {"item_count": 3, "statuses": [_history_status(2), _history_status(1)]},
        2: {
            "item_count": 4 if growing else 3,
            "statuses": [_history_status(1), _history_status(0)] if growing else [_history_status(0)],
        },
        4 if growing else 3: {"item_count": 4 if growing else 3, "statuses": []},
    }
    offsets = []

    def respond(request):
        assert request.url.path == "/jobs"
        assert request.url.params["job_id"] == JOB_ID
        assert request.url.params["sort_by"] == "job_attempt_index:desc"
        offset = int(request.url.params["offset"])
        offsets.append(offset)
        return httpx.Response(200, json=pages[offset])

    client = httpx.Client(transport=httpx.MockTransport(respond))
    mocker.patch("watcher.lib.plugins.job_manager.httpx.Client", return_value=client)
    attempts = get_job_attempts_sync(ENDPOINT, JOB_ID)

    assert [attempt.job_attempt_index for attempt in attempts] == [2, 1, 0]
    assert offsets == ([0, 2, 4] if growing else [0, 2, 3])


def _history_status(index):
    return {"job_id": JOB_ID, "job_attempt_index": index, "workflow_id": "test"}


@pytest.mark.parametrize("failure", ["http", "timeout", "invalid_json", "invalid_status"])
def test_history_page_failure_does_not_return_partial_history(mocker, failure):
    def respond(request):
        if request.url.params["offset"] == "0":
            return httpx.Response(200, json={"item_count": 2, "statuses": [_history_status(1)]})
        if failure == "timeout":
            raise httpx.ReadTimeout("Timed out", request=request)
        if failure == "http":
            return httpx.Response(503)
        if failure == "invalid_json":
            return httpx.Response(200, text="not json")
        return httpx.Response(200, json={"item_count": 2, "statuses": [{"job_id": JOB_ID}]})

    client = httpx.Client(transport=httpx.MockTransport(respond))
    mocker.patch("watcher.lib.plugins.job_manager.httpx.Client", return_value=client)
    assert get_job_attempts_sync(ENDPOINT, JOB_ID) is None


def test_history_empty_job(mocker):
    client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"item_count": 0, "statuses": []}))
    )
    mocker.patch("watcher.lib.plugins.job_manager.httpx.Client", return_value=client)
    assert get_job_attempts_sync(ENDPOINT, JOB_ID) == []


def test_metadata_lookup_preserves_siblings_and_wire_metadata(mocker):
    """Filtered pagination must not collapse task siblings sharing an attempt index."""
    metadata = {"scheduler": "slurm", "slurm_raw_job_id": "4200"}
    offsets = []

    def respond(request):
        assert "job_id" not in request.url.params
        assert json.loads(request.url.params["scheduler_metadata"]) == metadata
        offset = int(request.url.params["offset"])
        offsets.append(offset)
        if offset == 2:
            return httpx.Response(200, json={"item_count": 2, "statuses": []})
        return httpx.Response(
            200,
            json={
                "item_count": 2,
                "statuses": [
                    {
                        "job_id": f"4200_{index}",
                        "job_attempt_index": 0,
                        "workflow_id": "array",
                        "schedulerMetadata": {
                            **metadata,
                            "slurm_array_job_id": "4200",
                            "slurm_array_task_id": str(index),
                        },
                    }
                    for index in (0, 1)
                ],
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(respond))
    mocker.patch("watcher.lib.plugins.job_manager.httpx.Client", return_value=client)
    records = get_job_attempts_sync(ENDPOINT, None, scheduler_metadata=metadata)
    assert [record.job_id for record in records] == ["4200_0", "4200_1"]
    assert [record.scheduler_metadata["slurm_array_task_id"] for record in records] == ["0", "1"]
    assert offsets == [0, 2]


def test_metadata_lookup_fails_closed_when_server_ignores_filter(mocker):
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"item_count": 1, "statuses": [_history_status(0)]},
            )
        )
    )
    mocker.patch("watcher.lib.plugins.job_manager.httpx.Client", return_value=client)
    assert get_job_attempts_sync(ENDPOINT, None, scheduler_metadata={"slurm_raw_job_id": "4200"}) is None
