# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from sqlmodel import create_engine


@pytest.mark.anyio
async def test_readyz_ok_when_database_reachable(client):
    res = await client.get("/readyz")
    assert res.status_code == 200


@pytest.mark.anyio
async def test_readyz_unavailable_when_database_unreachable(client, mocker, tmp_path):
    unreachable = create_engine(f"sqlite:///{tmp_path}/missing-dir/notifications.db")
    mocker.patch("notifications.database._engine", unreachable)

    res = await client.get("/readyz")
    assert res.status_code == 503


@pytest.mark.anyio
async def test_healthz_ok_when_database_unreachable(client, mocker, tmp_path):
    """Liveness must not depend on the database, or a database outage would restart every Pod."""
    unreachable = create_engine(f"sqlite:///{tmp_path}/missing-dir/notifications.db")
    mocker.patch("notifications.database._engine", unreachable)

    res = await client.get("/healthz")
    assert res.status_code == 200
