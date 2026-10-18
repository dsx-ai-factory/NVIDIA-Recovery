# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
from collections.abc import Callable
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def exponential_backoff_retry(
    max_retries: int,
    base_delay: int,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    retry_func: Callable[[Any], tuple[bool, str]] = lambda _: (False, ""),
):
    """
    Exponential retry decorator, it retries the wrapped function `max_retries`
    times with initial delay `base_delay` seconds. Exponential backoff is calculated
    in the power of two.

    Arguments:
        max_retries: number of retries to attempt.
        base_delay: initial retry delay in seconds.
        exceptions: exception classes that should trigger a retry.
        retry_func: function that is called with the return value of the wrapped function and should return true if a retry should be triggered, along with a
            string description of the error or reason for the retry so it can be logged, else false.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries <= max_retries:
                try:
                    result_func = func(*args, **kwargs)
                    failed, e = retry_func(result_func)
                    if not failed or retries == max_retries:
                        return result_func

                    logger.error(
                        f"Attempt {retries + 1} failed with error, "
                        f"{max_retries - retries} attempts left. Error details: {e}."
                    )
                except exceptions as e:
                    logger.error(
                        f"Attempt {retries + 1} failed with exception, "
                        f"{max_retries - retries} attempts left. Exception details: {e}."
                    )
                    if retries == max_retries:
                        raise

                retries += 1
                delay = base_delay * (2**retries)
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)

        return wrapper

    return decorator
