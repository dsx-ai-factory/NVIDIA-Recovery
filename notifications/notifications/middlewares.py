# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from fastapi import Request

from notifications.utils.logger import logger


async def log_http_requests(request: Request, call_next):
    """Log HTTP requests made to the app."""
    path_and_query = request.url.replace(hostname="", scheme="")
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("[5xx] %s %s", request.method, path_and_query)
        raise

    if response.status_code >= 400:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG
    logger.log(log_level, "[%s] %s %s", response.status_code, request.method, path_and_query)
    return response
