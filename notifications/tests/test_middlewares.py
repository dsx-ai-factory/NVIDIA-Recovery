# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

import pytest


@pytest.mark.anyio
async def test_logging__simple_request(client, caplog):
    caplog.clear()
    res = await client.get("/notifications?recipient=some-user&limit=1")
    res.raise_for_status()
    assert caplog.record_tuples == [
        ("notifications", logging.DEBUG, "[200] GET /notifications?recipient=some-user&limit=1")
    ]


@pytest.mark.anyio
async def test_logging__internal_errors_get_logged_as_errors(mocker, client, caplog):
    client._transport.raise_app_exceptions = False

    caplog.clear()
    mocker.patch("notifications.routers.notifications.select", side_effect=Exception)
    await client.get("/notifications?recipient=some-user&limit=1")
    assert caplog.record_tuples == [
        ("notifications", logging.ERROR, "[5xx] GET /notifications?recipient=some-user&limit=1")
    ]


@pytest.mark.anyio
async def test_logging__validation_errors_get_logged_as_info(mocker, client, caplog):
    caplog.clear()
    mocker.patch("notifications.routers.notifications.select", side_effect=Exception)
    await client.post("/notification", json={})
    assert caplog.record_tuples == [
        (
            "notifications",
            logging.INFO,
            "[422] POST /notification",
        )
    ]
