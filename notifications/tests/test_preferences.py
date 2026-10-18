# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import httpx
import pytest
import respx
from conftest import random_string


@pytest.fixture(autouse=True)
def enable_callback(config):
    config.CALLBACK_ENABLED = "true"
    config.CALLBACK_URL = "https://some-url.test/alert"
    # Absorb any callback delivery that fires so tests never hit the network.
    # assert_all_called=False because most tests here assert creation-time gating only and
    # intentionally never trigger delivery.
    with respx.mock(assert_all_called=False) as mock:
        mock.route().mock(return_value=httpx.Response(200))
        yield


def _sample_template_data(recipient):
    return {
        "template_id": "/templates/JobStopped",
        "job_id": "456",
        "job_attempt_index": 7,
        "job_name": "some-job",
        "event_type": "CRASH",
        "user": recipient,
    }


@pytest.mark.anyio
@pytest.mark.parametrize("notification_type", ["info", "performance_alert", "warning"])
async def test_sends_notifications_by_default(post_notification, notification_type):
    recipient = "some-recipient-" + random_string()
    response = await post_notification(
        recipient=recipient,
        template_data=_sample_template_data(recipient),
        type=notification_type,
    )
    assert response["via_callback"] == "PENDING"


@pytest.mark.anyio
async def test_does_not_send_optional_notifications_by_default(post_notification):
    recipient = "some-recipient-" + random_string()
    response = await post_notification(
        recipient=recipient,
        template_data=_sample_template_data(recipient),
        type="optional",
    )
    assert response["via_callback"] == "DISABLED"


@pytest.mark.anyio
async def test_empty_preferences_still_uses_default(client, post_notification):
    recipient = "some-recipient-" + random_string()
    res = await client.put(f"/user-preferences/{recipient}", json={})
    res.raise_for_status()
    assert res.json() == {
        "notification_types": {
            "optional": False,
            "info": True,
            "performance_alert": True,
            "warning": True,
        },
    }
    response = await post_notification(
        recipient=recipient,
        template_data=_sample_template_data(recipient),
    )
    assert response["via_callback"] == "PENDING"


@pytest.mark.anyio
async def test_disabling_a_type_disables_messages_of_that_type(client, post_notification):
    recipient = "some-recipient-" + random_string()
    preferences = {
        "notification_types": {
            "optional": True,
            "info": True,
            "performance_alert": True,
            "warning": False,  # Disable warnings
        },
    }
    res = await client.put(f"/user-preferences/{recipient}", json=preferences)
    res.raise_for_status()
    assert res.json() == preferences

    response = await post_notification(
        recipient=recipient,
        template_data=_sample_template_data(recipient),
        type="warning",
    )
    assert response["via_callback"] == "DISABLED"


@pytest.mark.anyio
async def test_creating_a_new_resource_returns_201(client):
    recipient = "some-recipient-" + random_string()
    res = await client.put(f"/user-preferences/{recipient}", json={})
    res.raise_for_status()
    assert res.status_code == 201


@pytest.mark.anyio
async def test_updating_an_existing_resource_returns_200(client):
    recipient = "some-recipient-" + random_string()
    res = await client.put(f"/user-preferences/{recipient}", json={})
    res.raise_for_status()
    res = await client.put(f"/user-preferences/{recipient}", json={})
    res.raise_for_status()
    assert res.status_code == 200


@pytest.mark.anyio
async def test_can_get_user_preferences(client):
    recipient = "some-recipient-" + random_string()
    preferences = {
        "notification_types": {
            "optional": True,
            "info": False,
            "performance_alert": True,
            "warning": False,
        },
    }
    res = await client.put(f"/user-preferences/{recipient}", json=preferences)
    res.raise_for_status()
    res = await client.get(f"/user-preferences/{recipient}")
    res.raise_for_status()
    assert res.json() == preferences


@pytest.mark.anyio
async def test_raises_404_when_getting_non_existent_preferences(client):
    recipient = "bogus-recipient-" + random_string()
    res = await client.get(f"/user-preferences/{recipient}")
    assert res.status_code == 404


@pytest.mark.anyio
async def test_putting_an_empty_object_supplies_defaults(client):
    recipient = "some-recipient-" + random_string()
    res = await client.put(f"/user-preferences/{recipient}", json={})
    res.raise_for_status()
    res = await client.get(f"/user-preferences/{recipient}")
    res.raise_for_status()
    assert res.json() == {
        "notification_types": {
            "optional": False,
            "info": True,
            "performance_alert": True,
            "warning": True,
        },
    }
