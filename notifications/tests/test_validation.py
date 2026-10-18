# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest


@pytest.mark.anyio
@pytest.mark.parametrize(
    ["payload", "expected_response"],
    [
        pytest.param(
            {},
            {
                "detail": [
                    {
                        "input": {},
                        "loc": [
                            "body",
                            "recipient",
                        ],
                        "msg": "Field required",
                        "type": "missing",
                    },
                ],
            },
            id="payload must not be empty",
        ),
        pytest.param(
            {"recipient": "unimportant"},
            {
                "detail": [
                    {
                        "ctx": {
                            "error": {},
                        },
                        "input": {
                            "recipient": "unimportant",
                        },
                        "loc": [
                            "body",
                        ],
                        "msg": "Value error, Must supply either 'template_data' or 'title' and 'message'.",
                        "type": "value_error",
                    },
                ],
            },
            id="must have either message/title or template_data",
        ),
        pytest.param(
            {
                "message": "unimportant",
                "recipient": "unimportant",
                "template_data": {
                    "template_id": "/templates/NoMoreReleasingJobs",
                    "job_name": "some-job",
                    "user": "some-user",
                },
            },
            {
                "detail": [
                    {
                        "ctx": {
                            "error": {},
                        },
                        "input": {
                            "message": "unimportant",
                            "recipient": "unimportant",
                            "template_data": {
                                "job_name": "some-job",
                                "template_id": "/templates/NoMoreReleasingJobs",
                                "user": "some-user",
                            },
                        },
                        "loc": [
                            "body",
                        ],
                        "msg": "Value error, 'title' and 'message' cannot be used with 'template_data'.",
                        "type": "value_error",
                    },
                ],
            },
            id="'template_data' cannot be used with 'message'",
        ),
        pytest.param(
            {
                "title": "unimportant",
                "recipient": "unimportant",
                "template_data": {
                    "template_id": "/templates/NoMoreReleasingJobs",
                    "job_name": "some-job",
                    "user": "some-user",
                },
            },
            {
                "detail": [
                    {
                        "ctx": {
                            "error": {},
                        },
                        "input": {
                            "title": "unimportant",
                            "recipient": "unimportant",
                            "template_data": {
                                "job_name": "some-job",
                                "template_id": "/templates/NoMoreReleasingJobs",
                                "user": "some-user",
                            },
                        },
                        "loc": [
                            "body",
                        ],
                        "msg": "Value error, 'title' and 'message' cannot be used with 'template_data'.",
                        "type": "value_error",
                    },
                ],
            },
            id="'template_data' cannot be used with 'title'",
        ),
    ],
)
async def test_create_notification__invalid_payloads(client, payload, expected_response):
    # Create a message
    res = await client.post("/notification", json=payload)
    assert res.status_code == 422
    assert res.json() == expected_response
