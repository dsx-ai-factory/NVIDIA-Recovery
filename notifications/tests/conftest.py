# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import random
import string

import pytest
from httpx import ASGITransport, AsyncClient
from notifications.main import app, lifespan
from notifications.utils.config import Config
from sqlalchemy.pool import StaticPool
from sqlmodel import SQLModel, create_engine


def random_string(length: int = 4):
    """Create a random string to use in test data."""
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(length))


def pytest_addoption(parser, pluginmanager):
    parser.addoption(
        "--use-real-db", action="store_true", help="Use a real database connection instead of in-memory sqlite"
    )
    parser.addoption("--update-openapi", action="store_true", help="Update the generated OpenAPI spec file.")


@pytest.fixture()
def anyio_backend():
    """Tell anyio to only run tests via 'asyncio'."""
    return "asyncio"


@pytest.fixture(autouse=True)
def config():
    """Expose the service's config so it can be changed during tests.

    Reloads the config before each test.
    """
    Config.reload()
    yield Config


@pytest.fixture(autouse=True)
def enable_logger_propagation(caplog):
    """Enable logger propagation for tests so caplog can capture logs."""
    import logging

    from notifications.utils.logger import logger

    # Enable propagation so caplog can capture logs
    original_propagate = logger.propagate
    logger.propagate = True

    # Set logger level to DEBUG to capture all log levels
    original_level = logger.level
    logger.setLevel(logging.DEBUG)

    # Set all handlers to DEBUG level as well
    original_handler_levels = []
    for handler in logger.handlers:
        original_handler_levels.append(handler.level)
        handler.setLevel(logging.DEBUG)

    # Also set caplog to capture DEBUG level
    caplog.set_level(logging.DEBUG, logger=logger.name)

    yield

    # Restore original settings
    logger.propagate = original_propagate
    logger.setLevel(original_level)
    for handler, level in zip(logger.handlers, original_handler_levels):
        handler.setLevel(level)


@pytest.fixture
async def client(request, mocker):
    """An AsyncClient of the notifications app."""
    mock_engine = None
    # Replace the db engine with in-memory sqlite unless the user has specified `--use-real-db`.
    if not request.config.getoption("--use-real-db"):
        mock_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
        mocker.patch("notifications.database._engine", mock_engine)
        # SQLite is an isolated unit-test fixture. Production startup always uses Alembic.
        SQLModel.metadata.create_all(mock_engine)
        # The alembic config assumes mysql. Don't run alembic commands against sqlite.
        mocker.patch("notifications.database.run_migrations")

    async with lifespan(app), AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as test_client:
        yield test_client

    if mock_engine:
        mock_engine.dispose()


@pytest.fixture
def post_notification(client):
    """Create a new notification via the API.

    Returns the parsed JSON payload as a dict.
    """

    async def _post_notification(*, recipient=..., title=..., message=..., template_data=..., job_id=..., type=...):
        payload = {
            "template_data": template_data,
            "recipient": recipient,
            "title": title,
            "message": message,
            "job_id": job_id,
            "type": type,
        }
        if recipient is ...:
            payload["recipient"] = "recipient-" + random_string()
        if type is ...:
            del payload["type"]

        if template_data is ...:
            # No template_data specified.
            # Create random default values for title/message (if necessary.)
            del payload["template_data"]
            if title is ...:
                payload["title"] = "title-" + random_string()
            if message is ...:
                payload["message"] = "message-" + random_string()
            if job_id is ...:
                del payload["job_id"]
        else:
            # Using template_data.
            # Delete the title/message if not specified.
            if title is ...:
                del payload["title"]
            if message is ...:
                del payload["message"]
            if job_id is ...:
                payload["job_id"] = template_data.get("job_id")

        res = await client.request("POST", "/notification", json=payload)
        res.raise_for_status()
        return res.json()

    return _post_notification


@pytest.fixture()
def generate_unique_recipient(client):
    """Generate a recipient name and check that it's not already in the system."""

    async def _generate_unique_recipient():
        recipient = "recipient-" + random_string()
        # Sanity check that this recipient isn't already in the system.
        res = await client.get("/notifications", params={"recipient": recipient})
        res.raise_for_status()
        assert res.json()["items"] == []
        return recipient

    return _generate_unique_recipient
