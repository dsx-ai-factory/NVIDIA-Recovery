# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid

import pytest
from httpx import AsyncClient
from notifications.routers.audit_events import (
    AuditEventIngestionResult,
    AuditEventPersistenceOverloadedError,
    insert_audit_events,
)
from recovery_libs.schema.job_manager.job_identity import generate_heimdall_job_id

_SUCCESSFUL_ACTION_SUMMARY = {
    "title": "Scheduler action performed",
    "description": "Action release performed successfully",
    "granularity": 1,
}


def _producer_event(
    *,
    heimdall_job_id: str | None = None,
    event_producer: str = "slurm-advisor-controller",
    event_type: str = "advice_scheduler_action_completed",
    event_level: str = "recovery",
    event_produced_at: int = 1_700_000_000_123_456,
    idempotency_key: str | None = None,
) -> dict:
    return {
        "heimdall_job_id": heimdall_job_id or str(uuid.uuid4()),
        "event_producer": event_producer,
        "event_type": event_type,
        "event_level": event_level,
        "payload": {"action": "release", "success": True},
        "idempotency_key": idempotency_key or str(uuid.uuid4()),
        "event_produced_at": event_produced_at,
    }


@pytest.mark.anyio
async def test_audit_api_persists_events_and_summarizes_query_results(client: AsyncClient):
    event = _producer_event()

    response = await client.post("/v1/audit/events", json={"items": [event]})
    response.raise_for_status()

    response = await client.get("/v1/audit/events", params={"heimdall_job_id": event["heimdall_job_id"]})
    response.raise_for_status()
    expected_event = {
        **{key: value for key, value in event.items() if key != "idempotency_key"},
        "summary": _SUCCESSFUL_ACTION_SUMMARY,
    }
    assert response.json() == {
        "cursor": {"prev_cursor": "", "next_cursor": ""},
        "items": [expected_event],
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("event_type", "payload", "expected_description"),
    [
        ("attribution_request_forwarded", {"request_payload": None}, "attribution request forwarded"),
        ("job_attempt_observed", {"job": None}, "job attempt observed"),
        ("job_attempt_updated", {"old_job": None}, "job attempt updated"),
        ("job_attempt_updated", {"new_job": []}, "job attempt updated"),
        ("advice_created", {"advice": None}, "advice created"),
        ("advice_created", {"advice": {"advice": None}}, "advice created"),
        ("advice_updated", {"old_advice": None}, "advice updated"),
        ("advice_updated", {"new_advice": None}, "advice updated"),
        ("advice_updated", {"old_advice": {"advice": None}}, "advice updated"),
        ("advice_updated", {"new_advice": {"advice": None}}, "advice updated"),
        ("advice_application_evaluated", {"planned_actions": None}, "advice application evaluated"),
        ("advice_application_evaluated", {"planned_actions": ["release", 42]}, "advice application evaluated"),
    ],
)
async def test_malformed_payload_does_not_break_audit_query_page(
    client: AsyncClient,
    event_type: str,
    payload: dict,
    expected_description: str,
):
    """Accepted malformed events remain readable alongside normally summarized events."""
    job_id = str(uuid.uuid4())
    malformed_event = {
        **_producer_event(heimdall_job_id=job_id, event_type=event_type, event_produced_at=100),
        "payload": payload,
    }
    valid_event = _producer_event(heimdall_job_id=job_id, event_produced_at=200)

    response = await client.post("/v1/audit/events", json={"items": [malformed_event, valid_event]})
    assert response.status_code == 202
    assert response.json() == {}

    response = await client.get("/v1/audit/events", params={"heimdall_job_id": job_id})
    response.raise_for_status()
    items = response.json()["items"]
    assert len(items) == 2
    items_by_type = {item["event_type"]: item for item in items}
    assert items_by_type[event_type]["payload"] == payload
    assert items_by_type[event_type]["summary"] == {
        "title": event_type,
        "description": expected_description,
        "granularity": 11,
    }
    assert items_by_type[valid_event["event_type"]]["payload"] == valid_event["payload"]
    assert items_by_type[valid_event["event_type"]]["summary"] == _SUCCESSFUL_ACTION_SUMMARY


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("sort", "expected_timestamps"),
    [(None, [200, 100]), ("event_produced_at:asc", [100, 200]), ("event_produced_at:desc", [200, 100])],
)
async def test_can_ingest_and_query_audit_events(client: AsyncClient, sort: str | None, expected_timestamps: list[int]):
    """Queries honor the requested time order and default to newest events first."""
    job_id = str(uuid.uuid4())
    events = [
        _producer_event(heimdall_job_id=job_id, event_produced_at=100),
        _producer_event(heimdall_job_id=job_id, event_produced_at=200),
    ]

    response = await client.post("/v1/audit/events", json={"items": events})

    assert response.status_code == 202
    assert response.json() == {}

    query = {"heimdall_job_id": job_id}
    if sort is not None:
        query["sort_by"] = sort
    response = await client.get("/v1/audit/events", params=query)
    response.raise_for_status()
    expected_items = [
        {
            **{key: value for key, value in event.items() if key != "idempotency_key"},
            "summary": _SUCCESSFUL_ACTION_SUMMARY,
        }
        for timestamp in expected_timestamps
        for event in events
        if event["event_produced_at"] == timestamp
    ]
    assert response.json() == {
        "cursor": {"prev_cursor": "", "next_cursor": ""},
        "items": expected_items,
    }


@pytest.mark.anyio
async def test_can_query_audit_events_by_scheduler_job_attempt(client: AsyncClient):
    job_id = "12345"
    attempt_index = 2
    matching_event = _producer_event(
        heimdall_job_id=str(generate_heimdall_job_id(job_id, attempt_index)),
        event_produced_at=100,
    )
    response = await client.post(
        "/v1/audit/events",
        json={
            "items": [
                matching_event,
                _producer_event(
                    heimdall_job_id=str(generate_heimdall_job_id(job_id, attempt_index + 1)),
                    event_produced_at=200,
                ),
            ]
        },
    )
    response.raise_for_status()

    response = await client.get(
        "/v1/audit/events",
        params={"job_id": job_id, "attempt_index": attempt_index},
    )

    response.raise_for_status()
    assert [event["event_produced_at"] for event in response.json()["items"]] == [100]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "params",
    [
        {"attempt_index": 2},
        {"heimdall_job_id": "internal-id", "job_id": "12345"},
        {"heimdall_job_id": "internal-id", "attempt_index": 0},
        {"job_id": "", "attempt_index": 2},
        {"job_id": "   ", "attempt_index": 2},
        {"job_id": "12345", "attempt_index": -1},
        {"heimdall_job_id": "internal-id", "job_id": "12345", "attempt_index": 2},
    ],
)
async def test_rejects_ambiguous_or_partial_job_identity(client: AsyncClient, params: dict[str, object]):
    response = await client.get("/v1/audit/events", params=params)

    assert response.status_code == 422


@pytest.mark.anyio
async def test_ingestion_is_idempotent(client: AsyncClient):
    event = _producer_event()

    for _ in range(2):
        response = await client.post("/v1/audit/events", json={"items": [event]})
        assert response.status_code == 202
        assert response.json() == {}

    response = await client.get("/v1/audit/events", params={"heimdall_job_id": event["heimdall_job_id"]})
    response.raise_for_status()
    assert len(response.json()["items"]) == 1


@pytest.mark.anyio
async def test_query_filters_are_inclusive(client: AsyncClient):
    job_id = str(uuid.uuid4())
    matching_event = _producer_event(
        heimdall_job_id=job_id,
        event_producer="slurm-advisor-controller",
        event_level="recovery",
        event_produced_at=200,
    )
    events = [
        _producer_event(heimdall_job_id=job_id, event_level="operational", event_produced_at=200),
        matching_event,
        _producer_event(heimdall_job_id=job_id, event_level="recovery", event_produced_at=201),
        _producer_event(heimdall_job_id=job_id, event_producer="job-manager", event_produced_at=200),
        _producer_event(event_level="recovery", event_produced_at=200),
    ]
    response = await client.post("/v1/audit/events", json={"items": events})
    response.raise_for_status()

    response = await client.get(
        "/v1/audit/events",
        params={
            "event_level": "recovery",
            "event_producer": "slurm-advisor-controller",
            "min_event_produced_at": 200,
            "max_event_produced_at": 200,
            "heimdall_job_id": job_id,
        },
    )

    response.raise_for_status()
    expected_event = {
        **{key: value for key, value in matching_event.items() if key != "idempotency_key"},
        "summary": _SUCCESSFUL_ACTION_SUMMARY,
    }
    assert response.json() == {
        "cursor": {"prev_cursor": "", "next_cursor": ""},
        "items": [expected_event],
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("sort", "expected_indexes"),
    [("event_produced_at:asc", [0, 1, 2, 3, 4]), ("event_produced_at:desc", [4, 3, 2, 1, 0])],
)
async def test_query_uses_limit_and_returned_cursors_to_navigate_pages(
    client: AsyncClient, sort: str, expected_indexes: list[int]
):
    """Both time orders traverse tied timestamps without skipping or repeating events."""
    job_id = str(uuid.uuid4())
    events = [
        _producer_event(heimdall_job_id=job_id, event_produced_at=timestamp, event_type=f"event_{index}")
        for index, timestamp in enumerate([0, 1, 1, 1, 2])
    ]
    response = await client.post("/v1/audit/events", json={"items": events})
    response.raise_for_status()

    query = {"heimdall_job_id": job_id, "limit": 2, "sort_by": sort}
    expected_types = [f"event_{index}" for index in expected_indexes]
    first_page = await client.get("/v1/audit/events", params=query)
    first_page.raise_for_status()
    first_page_body = first_page.json()
    assert [item["event_type"] for item in first_page_body["items"]] == expected_types[:2]
    assert first_page_body["cursor"]["prev_cursor"] == ""
    assert first_page_body["cursor"]["next_cursor"]

    second_page = await client.get(
        "/v1/audit/events",
        params={**query, "cursor": first_page_body["cursor"]["next_cursor"]},
    )
    second_page.raise_for_status()
    second_page_body = second_page.json()
    assert [item["event_type"] for item in second_page_body["items"]] == expected_types[2:4]
    assert second_page_body["cursor"]["prev_cursor"]
    assert second_page_body["cursor"]["next_cursor"]

    third_page = await client.get(
        "/v1/audit/events",
        params={**query, "cursor": second_page_body["cursor"]["next_cursor"]},
    )
    third_page.raise_for_status()
    third_page_body = third_page.json()
    assert [item["event_type"] for item in third_page_body["items"]] == expected_types[4:]
    assert third_page_body["cursor"]["prev_cursor"] == first_page_body["cursor"]["next_cursor"]
    assert third_page_body["cursor"]["next_cursor"] == ""

    previous_page = await client.get(
        "/v1/audit/events",
        params={**query, "cursor": third_page_body["cursor"]["prev_cursor"]},
    )
    previous_page.raise_for_status()
    assert [item["event_type"] for item in previous_page.json()["items"]] == expected_types[2:4]

    first_page_again = await client.get(
        "/v1/audit/events",
        params={**query, "cursor": second_page_body["cursor"]["prev_cursor"]},
    )
    first_page_again.raise_for_status()
    assert [item["event_type"] for item in first_page_again.json()["items"]] == expected_types[:2]
    assert first_page_again.json()["cursor"]["prev_cursor"] == ""

    larger_page = await client.get(
        "/v1/audit/events",
        params={**query, "limit": 100, "cursor": first_page_body["cursor"]["next_cursor"]},
    )
    larger_page.raise_for_status()
    assert [item["event_type"] for item in larger_page.json()["items"]] == expected_types[2:]
    assert larger_page.json()["cursor"]["prev_cursor"]


@pytest.mark.anyio
async def test_query_limit_defaults_to_100(client: AsyncClient):
    events = [_producer_event(event_produced_at=index) for index in range(101)]
    response = await client.post("/v1/audit/events", json={"items": events})
    response.raise_for_status()

    page = await client.get("/v1/audit/events")

    page.raise_for_status()
    assert len(page.json()["items"]) == 100
    assert page.json()["cursor"]["next_cursor"]


@pytest.mark.anyio
async def test_rejects_invalid_query_range_and_cursor(client: AsyncClient):
    response = await client.get(
        "/v1/audit/events",
        params={"min_event_produced_at": 20, "max_event_produced_at": 10},
    )
    assert response.status_code == 422

    response = await client.get("/v1/audit/events", params={"cursor": "not-a-cursor"})
    assert response.status_code == 422

    for invalid_limit in (0, 101):
        response = await client.get("/v1/audit/events", params={"limit": invalid_limit})
        assert response.status_code == 422


@pytest.mark.anyio
@pytest.mark.parametrize("event_level", [10, "unsupported"])
async def test_rejects_invalid_event_level(client: AsyncClient, event_level: str | int):
    response = await client.post("/v1/audit/events", json={"items": [_producer_event(event_level=event_level)]})

    assert response.status_code == 422

    response = await client.get("/v1/audit/events", params={"event_level": event_level})

    assert response.status_code == 422


@pytest.mark.anyio
async def test_rejects_batches_over_the_endpoint_limit(client: AsyncClient, mocker):
    mocker.patch("notifications.routers.audit_events.MAX_AUDIT_EVENTS_PER_BATCH", 1)

    response = await client.post("/v1/audit/events", json={"items": [_producer_event(), _producer_event()]})

    assert response.status_code == 413


@pytest.mark.anyio
async def test_reports_store_overload_without_accepting_events(client: AsyncClient, mocker):
    event = _producer_event()
    mocker.patch(
        "notifications.routers.audit_events.insert_audit_events",
        side_effect=AuditEventPersistenceOverloadedError,
    )

    response = await client.post("/v1/audit/events", json={"items": [event]})

    assert response.status_code == 429
    assert response.json() == {"detail": "The audit event store is overloaded; retry the entire batch."}

    query_response = await client.get("/v1/audit/events", params={"heimdall_job_id": event["heimdall_job_id"]})
    query_response.raise_for_status()
    assert query_response.json() == {
        "cursor": {"prev_cursor": "", "next_cursor": ""},
        "items": [],
    }


@pytest.mark.anyio
async def test_reports_partial_batch_acceptance(client: AsyncClient, mocker):
    job_id = str(uuid.uuid4())
    events = [
        _producer_event(heimdall_job_id=job_id, event_produced_at=100),
        _producer_event(heimdall_job_id=job_id, event_produced_at=200),
        _producer_event(heimdall_job_id=job_id, event_produced_at=300),
    ]

    def partially_accept(session, event_batch):
        insert_audit_events(session, event_batch[:2])
        return AuditEventIngestionResult(rejected_events=1, error_message="one event could not be stored")

    mocker.patch("notifications.routers.audit_events.insert_audit_events", side_effect=partially_accept)

    response = await client.post("/v1/audit/events", json={"items": events})

    assert response.status_code == 202
    assert response.json() == {
        "partial_success": {
            "rejected_events": 1,
            "error_message": "one event could not be stored",
        }
    }
    accepted_events = len(events) - response.json()["partial_success"]["rejected_events"]
    assert accepted_events == 2

    query_response = await client.get("/v1/audit/events", params={"heimdall_job_id": job_id})
    query_response.raise_for_status()
    assert [event["event_produced_at"] for event in query_response.json()["items"]] == [200, 100]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("rejected_events", "error_message"),
    [(1, "complete rejection"), (2, "more events rejected than submitted")],
)
async def test_rejects_invalid_partial_results(
    client: AsyncClient,
    mocker,
    rejected_events: int,
    error_message: str,
):
    mocker.patch(
        "notifications.routers.audit_events.insert_audit_events",
        return_value=AuditEventIngestionResult(rejected_events=rejected_events, error_message=error_message),
    )

    with pytest.raises(RuntimeError, match="audit event store"):
        await client.post("/v1/audit/events", json={"items": [_producer_event()]})


def test_rejects_negative_ingestion_result_count():
    with pytest.raises(ValueError, match="rejected_events must not be negative"):
        AuditEventIngestionResult(rejected_events=-1)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "sort", ["", "recorded_at:asc", "event_produced_at", "-event_produced_at", "event_produced_at:up"]
)
async def test_rejects_unsupported_sort(client: AsyncClient, sort: str):
    """A string query contract still rejects unsupported fields and directions."""
    response = await client.get("/v1/audit/events", params={"sort_by": sort})
    assert response.status_code == 422


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("producer", "event_type", "details"),
    [
        ("watcher", "anomaly_detected", {"anomaly_type": "CRASH", "anomaly_uuid": "anomaly-1"}),
        ("anomaly-detection-isolator", "attribution_request_forwarded", {"request_payload": {"phase": "fast"}}),
        ("anomaly-detection-isolator", "attribution_response_received", {"response_payload": {}}),
        ("job-manager", "job_attempt_observed", {"job": {"state": "RUNNING"}}),
        ("job-manager", "job_attempt_updated", {"old_job": {"state": "PENDING"}, "new_job": {"state": "RUNNING"}}),
        ("job-manager", "advice_created", {"advice": {"advice": {"state": "Restarted"}}, "creation_reasons": []}),
        ("job-manager", "advice_updated", {"old_advice": {}, "new_advice": {}, "change_reasons": []}),
        ("job-manager", "advice_unpublished", {"reason": "superseded"}),
        ("slurm-advisor-controller", "advice_scheduler_action_completed", {"action": "slurm.release", "success": True}),
        ("slurm-advisor-controller", "advice_application_applied", {"advice_id": "advice-1"}),
        ("slurm-advisor-controller", "advice_application_evaluated", {"outcome": "observed", "planned_actions": []}),
        ("slurm-advisor-controller", "advice_application_failed", {"error": "unavailable"}),
        ("slurm-advisor-controller", "advice_reconciliation_converged", {"generation": 1, "reason": "running"}),
        ("slurm-advisor-controller", "advice_reconciliation_abandoned", {"generation": 1, "reason": "attempt changed"}),
        ("slurm-advisor-controller", "job_auto_requeue_hold_completed", {"success": True}),
        ("slurm-watchdog", "watchdog_release_completed", {"success": True}),
    ],
)
async def test_catalog_events_are_queryable_by_scheduler_job_and_attempt_zero(
    client: AsyncClient, producer: str, event_type: str, details: dict
):
    """All catalog producers use the same top-level identity, regardless of event details."""
    event = _producer_event(
        heimdall_job_id=str(generate_heimdall_job_id("catalog-job", 0)),
        event_producer=producer,
        event_type=event_type,
    )
    event["payload"] = {"job_id": "catalog-job", "attempt_index": 0, **details}
    response = await client.post("/v1/audit/events", json={"items": [event]})
    assert response.status_code == 202

    for selector in ({"job_id": "catalog-job"}, {"job_id": "catalog-job", "attempt_index": 0}):
        response = await client.get("/v1/audit/events", params={**selector, "event_type": event_type})
        response.raise_for_status()
        assert len(response.json()["items"]) == 1
        assert response.json()["items"][0]["payload"] == event["payload"]
        assert "scheduler_job_id" not in response.json()["items"][0]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"job_id": "12345"},
        {"attempt_index": 0},
        {"job_id": None, "attempt_index": 0},
        {"job_id": "", "attempt_index": 0},
        {"job_id": "   ", "attempt_index": 0},
        {"job_id": 12345, "attempt_index": 0},
        {"job_id": "12345", "attempt_index": None},
        {"job_id": "12345", "attempt_index": True},
        {"job_id": "12345", "attempt_index": "0"},
        {"job_id": "12345", "attempt_index": 0.0},
        {"job_id": "12345", "attempt_index": -1},
        {"job_id": "12345", "attempt_index": 1},
        {"job_id": "other-job", "attempt_index": 0},
        {"job_id": "x" * 256, "attempt_index": 0},
    ],
)
async def test_unmappable_identity_preserves_payload_and_exact_attempt_access(client: AsyncClient, payload: dict):
    event = _producer_event(heimdall_job_id=str(generate_heimdall_job_id("12345", 0)))
    event["payload"] = payload
    response = await client.post("/v1/audit/events", json={"items": [event]})
    assert response.status_code == 202
    response = await client.get("/v1/audit/events", params={"job_id": "12345"})
    response.raise_for_status()
    assert response.json()["items"] == []
    for selector in ({"heimdall_job_id": event["heimdall_job_id"]}, {"job_id": "12345", "attempt_index": 0}):
        response = await client.get("/v1/audit/events", params=selector)
        response.raise_for_status()
        assert [item["payload"] for item in response.json()["items"]] == [payload]


@pytest.mark.anyio
@pytest.mark.parametrize("sort", ["event_produced_at:asc", "event_produced_at:desc"])
async def test_all_attempts_paginate_in_both_directions_with_filters(client: AsyncClient, sort: str):
    """More than a page of attempts can be merged and traversed without JM enumeration."""
    events = []
    for attempt in range(137):
        event = _producer_event(
            heimdall_job_id=str(generate_heimdall_job_id("many-attempts", attempt)),
            event_type="selected",
            event_produced_at=100 + attempt // 3,
        )
        event["payload"] = {"job_id": "many-attempts", "attempt_index": attempt}
        events.append(event)
    distractors = [
        {**events[0], "idempotency_key": str(uuid.uuid4()), "event_type": value}
        for value in ("other", "Selected", "selected ", "selected_extra")
    ]
    distractors.extend(
        [
            {**events[0], "idempotency_key": str(uuid.uuid4()), "event_level": "operational"},
            {**events[0], "idempotency_key": str(uuid.uuid4()), "event_producer": "watcher"},
            {**events[0], "idempotency_key": str(uuid.uuid4()), "event_produced_at": 99},
            {**events[0], "idempotency_key": str(uuid.uuid4()), "event_produced_at": 146},
        ]
    )
    response = await client.post("/v1/audit/events", json={"items": events + distractors})
    response.raise_for_status()
    query = {
        "job_id": "many-attempts",
        "event_type": "selected",
        "event_level": "recovery",
        "event_producer": "slurm-advisor-controller",
        "min_event_produced_at": 100,
        "max_event_produced_at": 145,
        "sort_by": sort,
        "limit": 17,
    }
    pages = []
    cursor = ""
    while True:
        response = await client.get("/v1/audit/events", params={**query, "cursor": cursor})
        response.raise_for_status()
        pages.append(response.json())
        cursor = pages[-1]["cursor"]["next_cursor"]
        if not cursor:
            break
        assert len(pages) < 10
    expected = list(range(137))
    if sort.endswith(":desc"):
        expected.reverse()
    assert [item["payload"]["attempt_index"] for page in pages for item in page["items"]] == expected
    assert pages[0]["cursor"]["prev_cursor"] == ""
    for index in range(len(pages) - 1, 0, -1):
        response = await client.get(
            "/v1/audit/events", params={**query, "cursor": pages[index]["cursor"]["prev_cursor"]}
        )
        response.raise_for_status()
        assert response.json() == pages[index - 1]
