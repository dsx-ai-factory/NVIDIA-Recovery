# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# coding: utf-8

import datetime
import uuid
import zoneinfo
from unittest import mock

import freezegun
import pytest
from conftest import random_string
from httpx import AsyncClient


@pytest.fixture(autouse=True)
def ensure_slack_is_disabled(config):
    config.SLACK_ENABLED = "false"


@pytest.mark.anyio
async def test_can_create_a_notification(client: AsyncClient):
    payload = {
        "recipient": "recipient-" + random_string(),
        "title": "title-" + random_string(),
        "message": "message-" + random_string(),
    }
    # Create a message
    res = await client.post("/notification", json=payload)
    assert res.status_code == 201
    created_notification = res.json()
    assert created_notification == {
        "created_time": mock.ANY,
        "id": mock.ANY,
        "job_id": None,
        "type": "info",
        "via_callback": "DISABLED",
        "template_data": None,
        **payload,
    }
    assert created_notification["created_time"] is not None

    notification_id = created_notification["id"]

    # Can retrieve this message by id
    res = await client.get("/notifications", params={"id": notification_id})
    res.raise_for_status()
    assert res.json()["items"] == [created_notification]


@pytest.mark.anyio
async def test_update_status_of_non_existent_notification_returns_an_error(client):
    bogus_id = uuid.uuid4()
    res = await client.put(f"/notification/{bogus_id}/status", json={"status": "SENT"})
    assert res.status_code == 404
    assert res.json() == {"detail": "Not Found"}


@pytest.mark.anyio
async def test_can_get_notifications_by_recipient(client, post_notification, generate_unique_recipient):
    recipient = await generate_unique_recipient()

    # Create some notifications with this recipient.
    created_notifications = [
        await post_notification(recipient=recipient),
        await post_notification(recipient=recipient),
        await post_notification(recipient=recipient),
    ]

    # All these notifications should show up under this recipient
    res = await client.get("/notifications", params={"recipient": recipient})
    res.raise_for_status()
    assert res.json()["items"] == list(reversed(created_notifications))


@pytest.mark.anyio
async def test_limit__can_limit_number_of_notifications_fetched(client, post_notification, generate_unique_recipient):
    recipient = await generate_unique_recipient()

    # Create 10 notifications
    created_notifications = [await post_notification(recipient=recipient, title=str(count)) for count in range(10)]
    ordered_notifications = list(reversed(created_notifications))

    # limit=1 just gets 1
    res = await client.get("/notifications", params={"limit": 1, "recipient": recipient})
    res.raise_for_status()
    assert res.json()["items"] == [ordered_notifications[0]]
    # limit=5 gets 5
    res = await client.get("/notifications", params={"limit": 5, "recipient": recipient})
    res.raise_for_status()
    assert res.json()["items"] == ordered_notifications[:5]


@pytest.mark.anyio
async def test_offset__can_start_notifications_at_arbitrary_point(client, post_notification, generate_unique_recipient):
    recipient = await generate_unique_recipient()

    # Create 10 notifications
    created_notifications = [await post_notification(recipient=recipient, title=str(count)) for count in range(10)]
    ordered_notifications = list(reversed(created_notifications))

    # offset=0 gets the whole thing
    res = await client.get("/notifications", params={"offset": 0, "recipient": recipient})
    res.raise_for_status()
    assert res.json()["items"] == ordered_notifications
    # offset=1 starts at index 1
    res = await client.get("/notifications", params={"offset": 1, "recipient": recipient})
    res.raise_for_status()
    assert res.json()["items"] == ordered_notifications[1:]
    # offset=9 starts at index 9
    res = await client.get("/notifications", params={"offset": 9, "recipient": recipient})
    res.raise_for_status()
    assert res.json()["items"] == ordered_notifications[9:]
    # An offset out of bounds returns an empty list
    res = await client.get("/notifications", params={"offset": 900, "recipient": recipient})
    res.raise_for_status()
    assert res.json()["items"] == []


@pytest.mark.anyio
async def test_can_filter_based_on_created_time(client, post_notification, generate_unique_recipient):
    recipient = await generate_unique_recipient()

    created_notifications = [await post_notification(recipient=recipient, title=str(count)) for count in range(10)]

    created_after = created_notifications[5]["created_time"]
    created_before = created_notifications[9]["created_time"]

    res = await client.get("/notifications", params={"created_after": created_after, "recipient": recipient})
    res.raise_for_status()
    assert res.json()["items"] == list(reversed(created_notifications[5:]))

    res = await client.get("/notifications", params={"created_before": created_before, "recipient": recipient})
    res.raise_for_status()
    assert res.json()["items"] == list(reversed(created_notifications[:9]))

    res = await client.get(
        "/notifications",
        params={"created_before": created_before, "created_after": created_after, "recipient": recipient},
    )
    res.raise_for_status()

    assert res.json()["items"] == list(reversed(created_notifications[5:9]))


@pytest.mark.anyio
async def test_can_filter_by_job_id(client, post_notification):
    job_id = "expected-job-" + random_string()
    # Should include these notifications
    expected_notifications = [
        await post_notification(
            job_id=job_id,
        ),
        await post_notification(
            job_id=job_id,
        ),
    ]
    # Should not include this one (since it has a different job id.)
    await post_notification(job_id="unrelated-job-id")
    res = await client.get(f"/notifications?job_id={job_id}")
    res.raise_for_status()
    assert res.json()["items"] == list(reversed(expected_notifications))


@pytest.mark.anyio
@pytest.mark.parametrize(
    ["query", "expected_message"],
    [
        pytest.param(
            {"created_before": "bogus"},
            "Input should be a valid datetime or date, input is too short",
            id="created_before is bogus",
        ),
        pytest.param(
            {"created_before": "2025-01-21T00:00:00"},
            "Value error, 'created_before' must be in UTC, got tz=''",
            id="created_before is missing timezone info",
        ),
        pytest.param(
            {"created_before": "2025-01-21T00:00:00+01:00"},
            "Value error, 'created_before' must be in UTC, got tz='+01:00'",
            id="created_before has non-UTC timezone info",
        ),
        pytest.param(
            {"created_after": "bogus"},
            "Input should be a valid datetime or date, input is too short",
            id="created_after is bogus",
        ),
        pytest.param(
            {"created_after": "2025-01-21T00:00:00"},
            "Value error, 'created_after' must be in UTC, got tz=''",
            id="created_after is missing timezone info",
        ),
        pytest.param(
            {"created_after": "2025-01-21T00:00:00+01:00"},
            "Value error, 'created_after' must be in UTC, got tz='+01:00'",
            id="created_after has non-UTC timezone info",
        ),
        pytest.param(
            {"created_before": "2025-01-01T00:00:00Z", "created_after": "2025-01-20T00:00:00Z"},
            "Value error, 'created_before' must come after 'created_after'.",
            id="created_before before created_after",
        ),
        pytest.param(
            {"created_before": "2025-01-01T00:00:00Z", "created_after": "2025-01-20T00:00:00"},
            "Value error, 'created_after' must be in UTC, got tz=''",
            id="check validity of timestamps before order",
        ),
    ],
)
async def test_invalid_created_before_and_created_after(client, query, expected_message):
    res = await client.get("/notifications", params=query)
    assert res.status_code == 422
    assert res.json()["detail"][0]["msg"] == expected_message


@pytest.mark.anyio
async def test_total_items__counts_the_total_number_of_items(client, post_notification, generate_unique_recipient):
    recipient = await generate_unique_recipient()

    created_notifications = [await post_notification(recipient=recipient, title=str(count)) for count in range(10)]

    # with no limit
    res = await client.get("/notifications", params={"recipient": recipient})
    res.raise_for_status()
    # total_items and items should match
    assert res.json()["total_items"] == 10
    assert len(res.json()["items"]) == 10

    # with limit=5
    res = await client.get("/notifications", params={"limit": 5, "recipient": recipient})
    res.raise_for_status()
    # total_items should reflect total
    assert res.json()["total_items"] == 10
    # items should reflect limit
    assert len(res.json()["items"]) == 5

    # with limit=2 and filtered results
    created_before = created_notifications[8]["created_time"]
    res = await client.get(
        "/notifications", params={"limit": 2, "created_before": created_before, "recipient": recipient}
    )
    res.raise_for_status()
    assert res.json()["total_items"] == 8
    assert len(res.json()["items"]) == 2

    # with limit=2, offset=2 and filtered results
    res = await client.get(
        "/notifications", params={"limit": 2, "offset": 2, "created_before": created_before, "recipient": recipient}
    )
    res.raise_for_status()
    assert res.json()["total_items"] == 8
    assert len(res.json()["items"]) == 2


@pytest.mark.anyio
async def test_created_time__is_not_always_the_same(post_notification):
    # Silly test name, but this test is making sure we're not using the same timestamp for everything, and that it
    # increments based on the current time.
    created_notifications = []
    with freezegun.freeze_time("2025-01-13 17:00:00"):
        created_notifications.append(await post_notification())
    with freezegun.freeze_time("2025-01-13 17:00:01"):
        created_notifications.append(await post_notification())
    with freezegun.freeze_time("2025-01-13 17:00:02"):
        created_notifications.append(await post_notification())

    created_times = [notification["created_time"] for notification in created_notifications]
    # They should not all be the same
    assert len(set(created_times)) == 3


@pytest.mark.anyio
async def test_long_message(client, post_notification, generate_unique_recipient):
    recipient = await generate_unique_recipient()

    message = "0123456789" * 26

    notification = await post_notification(recipient=recipient, message=message, title="some-title")

    assert notification["message"] == message

    res = await client.get("/notifications", params={"recipient": recipient})
    res.raise_for_status()
    assert res.json()["items"][0]["message"] == message


@pytest.mark.anyio
@pytest.mark.parametrize(
    ["given_time", "expected_format"],
    [
        ("2025-01-02T17:01:02.123456Z", "2025-01-02T17:01:02.123456Z"),
        ("2025-01-02T17:01:02Z", "2025-01-02T17:01:02Z"),
        ("2025-01-02T17:01:02.000000Z", "2025-01-02T17:01:02Z"),
        (datetime.datetime(2025, 1, 2, 17, 1, 2, tzinfo=datetime.UTC), "2025-01-02T17:01:02Z"),
        (
            datetime.datetime(2025, 1, 2, 17, 1, 2, tzinfo=zoneinfo.ZoneInfo("America/Los_Angeles")),
            "2025-01-03T01:01:02Z",
        ),
    ],
)
async def test_created_time__handles_different_time_zones_as_utc(post_notification, given_time, expected_format):
    with freezegun.freeze_time(given_time):
        notification = await post_notification()

    assert notification["created_time"] == expected_format


@pytest.mark.anyio
@pytest.mark.parametrize(
    "template_id,details,action,context",
    [
        ("JobStopped", {"event_type": "CRASH", "event_body": "lost worker"}, "stop", "lost worker"),
        ("JobRequeued", {"event_type": "CRASH", "event_body": "lost worker"}, "requeue", "lost worker"),
        ("BadNodesIsolated", {"bad_nodes": {"gpu-1": ["XID"]}, "non_isolated_nodes": {}}, "exclude", "gpu-1"),
        ("ReleasedJob", {}, "release", "some-job"),
    ],
)
async def test_recovery_notification_reports_advice_without_scheduler_execution(
    client, post_notification, template_id, details, action, context
):
    """Legacy recovery payloads report the decision and its target, not an applied action."""
    payload = {
        "template_id": f"/templates/{template_id}",
        "job_id": "456",
        "job_attempt_index": 0,
        "job_name": "some-job",
        "user": "alice",
        **details,
    }
    notification = await post_notification(recipient="alice", template_data=payload)

    assert notification["title"].startswith("Advice to ")
    assert "issued" in notification["title"]
    assert notification["message"].startswith(f"Advice to {action} ")
    assert "issued" in notification["message"]
    assert "456(0)" in notification["message"]
    assert context in notification["message"]
    assert not any(word in notification["message"].lower() for word in ("stopped", "requeued", "excluded", "released"))
    assert notification["template_data"]["template_id"] == payload["template_id"]
    response = await client.get("/notifications", params={"id": notification["id"]})
    response.raise_for_status()
    assert response.json()["items"] == [notification]


@pytest.mark.anyio
async def test_template_wording_changes_preserve_stored_messages(client, post_notification, monkeypatch):
    """Reading history must not rerender old notifications using the current template."""
    from notifications.templates.base import JobRequeued

    with monkeypatch.context() as patch:
        patch.setattr(JobRequeued, "TITLE_TEMPLATE", "Job {{job_id}} requeued")
        patch.setattr(JobRequeued, "TEMPLATE", "Job {{>JobRepr}} `REQUEUED`.")
        historical = await post_notification(
            recipient="alice",
            template_data={
                "template_id": "/templates/JobRequeued",
                "job_id": "456",
                "job_attempt_index": 0,
                "job_name": "some-job",
                "event_type": "CRASH",
                "user": "alice",
            },
        )

    response = await client.get("/notifications", params={"id": historical["id"]})
    response.raise_for_status()
    assert response.json()["items"] == [historical]
    assert historical["message"] == "Job 456(0) `REQUEUED`."
