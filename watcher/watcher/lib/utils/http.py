# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from json import JSONDecodeError
from typing import Any
from urllib.parse import ParseResult, urlparse

import httpx
from httpx_retries import Retry, RetryTransport

from watcher.lib.utils.data import is_json
from watcher.lib.utils.logging import logger
from watcher.lib.utils.metrics import HTTP_REQUESTS_TOTAL
from watcher.lib.utils.types import HeaderTypes, QueryParamTypes

_MAX_PATH_SEGMENTS = 2

retry = Retry(
    total=3,
    backoff_factor=1.5,
    backoff_jitter=0.15,
    max_backoff_wait=30,
    status_forcelist=[429, 500, 502, 503, 504],
)
transport = RetryTransport(retry=retry)


def _normalize_path(parsed: ParseResult) -> str:
    """Keep only the first N path segments to avoid high-cardinality dynamic IDs."""
    segments = [s for s in parsed.path.split("/") if s]
    return "/" + "/".join(segments[:_MAX_PATH_SEGMENTS]) if segments else "/"


def _make_response_hook(service: str):
    """Create an event hook that tags every HTTP attempt with service + normalized path.

    Must be ``async`` because :class:`httpx.AsyncClient` unconditionally awaits
    hooks; a sync hook returns ``None``, and ``await None`` raises
    ``TypeError: object NoneType can't be used in 'await' expression``.
    """

    async def _on_response(response: httpx.Response) -> None:
        try:
            parsed = urlparse(str(response.request.url))
            HTTP_REQUESTS_TOTAL.labels(
                method=response.request.method,
                host=parsed.hostname or "unknown",
                path=_normalize_path(parsed),
                service=service,
                status_code=response.status_code,
            ).inc()
        except Exception:
            logger.debug("Failed to record HTTP transport metric", exc_info=True)

    return _on_response


async def get_async(
    url: str,
    headers: HeaderTypes | None = None,
    params: QueryParamTypes | None = None,
    service: str = "",
) -> Any | None:
    """
    Makes an asynchronous GET request.

    Args:
        service: Optional caller-supplied tag for the ``service`` metric label.

    Returns:
        JSON response (decoded dict/list, or raw bytes when already valid JSON) if
        successful, or `None` if an error occurs.
    """
    hook = _make_response_hook(service)
    async with httpx.AsyncClient(transport=transport, event_hooks={"response": [hook]}) as client:
        try:
            response = await client.get(url=url, headers=headers, params=params)
            response.raise_for_status()
            if is_json(response.content):
                return response.content
            else:
                return response.json()

        except httpx.HTTPStatusError as error:
            logger.error(f"Error occurred during async request: {error}")
            return None
        except (httpx.ConnectError, httpx.TimeoutException) as err:
            logger.error(f"Connection/timeout error for {url}: {err}")
            return None
        except (JSONDecodeError, ValueError) as err:
            logger.error(f"Unable to decode JSON payload (status_code={response.status_code}, url={url}): {err}")
            return None
