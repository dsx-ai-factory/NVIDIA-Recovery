# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest


@pytest.mark.anyio
async def test_templates_have_a_default_type(client, config, post_notification):
    recipient = "some-recipient"

    created_notification = await post_notification(
        recipient=recipient,
        template_data={
            "template_id": "/templates/JobStopped",
            "job_id": "456",
            "job_attempt_index": 7,
            "job_name": "some-job",
            "event_type": "CRASH",
            "user": recipient,
        },
    )

    assert created_notification["type"] == "info"
    res = await client.get(f"/notifications?id={created_notification['id']}")
    res.raise_for_status()
    [notification_item] = res.json()["items"]
    assert notification_item["type"] == "info"


@pytest.mark.anyio
async def test_templates_can_override_default_type(client, config, post_notification):
    recipient = "some-recipient"

    created_notification = await post_notification(
        recipient=recipient,
        type="performance_alert",
        template_data={
            "template_id": "/templates/JobStopped",
            "job_id": "456",
            "job_attempt_index": 7,
            "job_name": "some-job",
            "event_type": "CRASH",
            "user": recipient,
        },
    )

    assert created_notification["type"] == "performance_alert"
    res = await client.get(f"/notifications?id={created_notification['id']}")
    res.raise_for_status()
    [notification_item] = res.json()["items"]
    assert notification_item["type"] == "performance_alert"
