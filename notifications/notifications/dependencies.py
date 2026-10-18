# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Annotated

import httpx
from fastapi import Depends
from sqlmodel import Session
from taskiq import TaskiqDepends

from .database import get_engine


def get_session():
    with Session(get_engine()) as session:
        yield session


async def get_http_client():
    async with httpx.AsyncClient() as client:
        yield client


SessionDep = Annotated[Session, Depends(get_session), TaskiqDepends(get_session)]
HttpClientDep = Annotated[
    httpx.AsyncClient,
    Depends(get_http_client),
    TaskiqDepends(get_http_client),
]
