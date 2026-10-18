# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from notifications.dependencies import SessionDep
from notifications.utils.logger import logger

router = APIRouter(include_in_schema=False)


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Report that the process is serving HTTP requests."""
    return {"status": "ok"}


# Sync so a slow database check runs in the threadpool instead of blocking the event loop.
@router.get("/readyz")
def readyz(session: SessionDep) -> dict[str, str]:
    """Report whether the service can serve requests; 503 when the database is unreachable."""
    try:
        session.connection().execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        logger.warning(f"Readiness database query failed: {exc}")
        raise HTTPException(status_code=503, detail="Database unavailable") from exc
    return {"status": "ok"}
