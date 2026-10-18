# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import json

import httpx
import pytest

from notifications import sending


@pytest.mark.anyio
async def test_callback__can_forward_notifications_to_another_url(client, config, post_notification, respx_mock):
    config.CALLBACK_ENABLED = "true"
    config.CALLBACK_URL = "https://some-url.test/alert"

    mock_route = respx_mock.post("https://some-url.test/alert").mock(return_value=httpx.Response(201))

    recipient = "some-recipient"
    template_data = {
        "template_id": "/templates/JobStopped",
        "job_id": "456",
        "job_attempt_index": 7,
        "job_name": "some-job",
        "event_type": "CRASH",
        "user": recipient,
    }
    # Create a message
    created_notification = await post_notification(
        recipient=recipient,
        template_data=template_data,
    )
    await sending.broker.wait_all()

    # Should have called the endpoint with the un-altered payload
    mock_route.calls.assert_called_once()
    last_request = mock_route.calls.last.request
    assert json.loads(last_request.content) == created_notification

    # `via_callback` should be set to `SENT`
    res = await client.get(f"/notifications?id={created_notification['id']}")
    res.raise_for_status()
    [notification_item] = res.json()["items"]
    assert notification_item["via_callback"] == "SENT"


@pytest.mark.anyio
async def test_callback__can_modify_payload_with_jq(client, config, post_notification, respx_mock):
    config.CALLBACK_ENABLED = "true"
    config.CALLBACK_URL = "https://some-url.test/alert"
    config.CALLBACK_PAYLOAD_JQ = "{message: .message, user: .recipient}"

    mock_route = respx_mock.post("https://some-url.test/alert").mock(return_value=httpx.Response(201))

    recipient = "some-recipient"
    template_data = {
        "template_id": "/templates/JobStopped",
        "job_id": "456",
        "job_attempt_index": 7,
        "job_name": "some-job",
        "event_type": "CRASH",
        "user": recipient,
    }
    # Create a message
    created_notification = await post_notification(
        recipient=recipient,
        template_data=template_data,
    )
    await sending.broker.wait_all()

    # Should have called the endpoint with the modified payload
    mock_route.calls.assert_called_once()
    last_request = mock_route.calls.last.request
    assert json.loads(last_request.content) == {"message": created_notification["message"], "user": "some-recipient"}


@pytest.mark.anyio
async def test_callback__registers_failed_requests(client, config, post_notification, respx_mock):
    config.CALLBACK_ENABLED = "true"
    config.CALLBACK_URL = "https://some-url.test/alert"

    mock_route = respx_mock.post("https://some-url.test/alert").mock(return_value=httpx.Response(404))

    recipient = "some-recipient"
    template_data = {
        "template_id": "/templates/JobStopped",
        "job_id": "456",
        "job_attempt_index": 7,
        "job_name": "some-job",
        "event_type": "CRASH",
        "user": recipient,
    }
    # Create a message
    created_notification = await post_notification(
        recipient=recipient,
        template_data=template_data,
    )
    await sending.broker.wait_all()

    mock_route.calls.assert_called_once()

    # `via_callback` should be set to `FAILTOSEND`
    res = await client.get(f"/notifications?id={created_notification['id']}")
    res.raise_for_status()
    [notification_item] = res.json()["items"]
    assert notification_item["via_callback"] == "FAILTOSEND"


@pytest.mark.anyio
async def test_callback__can_modify_method(client, config, post_notification, respx_mock):
    config.CALLBACK_ENABLED = "true"
    config.CALLBACK_METHOD = "PUT"
    config.CALLBACK_URL = "https://some-url.test/alert"

    # Should use the specified method
    mock_route = respx_mock.put("https://some-url.test/alert").mock(return_value=httpx.Response(201))

    recipient = "some-recipient"
    template_data = {
        "template_id": "/templates/JobStopped",
        "job_id": "456",
        "job_attempt_index": 7,
        "job_name": "some-job",
        "event_type": "CRASH",
        "user": recipient,
    }
    # Create a message
    await post_notification(
        recipient=recipient,
        template_data=template_data,
    )
    await sending.broker.wait_all()

    mock_route.calls.assert_called_once()


@pytest.mark.anyio
async def test_callback__missing_url_fails_cleanly(client, config, post_notification):
    # Enabling callbacks without a URL should fail with a clear error, not an AttributeError.
    config.CALLBACK_ENABLED = "true"
    config.CALLBACK_URL = ""

    created_notification = await post_notification(
        recipient="some-recipient",
        template_data={
            "template_id": "/templates/JobStopped",
            "job_id": "456",
            "job_attempt_index": 7,
            "job_name": "some-job",
            "event_type": "CRASH",
            "user": "some-recipient",
        },
    )
    await sending.broker.wait_all()

    res = await client.get(f"/notifications?id={created_notification['id']}")
    res.raise_for_status()
    [notification_item] = res.json()["items"]
    assert notification_item["via_callback"] == "FAILTOSEND"
