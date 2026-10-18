# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import threading
import time
from collections.abc import Callable
from functools import wraps
from typing import Any

from watcher.lib.utils.logging import logger

ENABLE_TIMED_DECORATOR = False


class threadsafe_iter:
    """
    Takes an iterator/generator and makes it thread-safe by
    serializing call to the `next` method of given iterator/generator.
    """

    def __init__(self, it):
        self.it = it
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return self.it.__next__()


def threadsafe_generator(f):
    """
    A decorator that takes a generator function and makes it thread-safe.
    """

    def g(*a, **kw):
        return threadsafe_iter(f(*a, **kw))

    return g


def enable_coroutine(f):
    """
    Decorator to enable a regular function to be executed as a coroutine.

    Args:
        f (function): The function to be decorated and executed as a coroutine.

    Returns:
        function: The decorated function that can be called to execute
                  the original function as a coroutine.
    """

    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


def loopThr(loop):
    try:
        loop.run_forever()
    finally:
        loop.stop()


def get_duration_str(start: float) -> str:
    """Get human readable duration string from start time"""
    duration = time.time() - start
    if duration > 1:
        duration_str = f"{duration:,.3f}s"
    elif duration > 1e-3:
        duration_str = f"{round(duration * 1e3)}ms"
    elif duration > 1e-6:
        duration_str = f"{round(duration * 1e6)}us"
    else:
        duration_str = f"{duration * 1e9}ns"
    return duration_str


def timed(fn: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to log the duration of a function.

    Args:
        fn (Callable[..., Any]): The function to decorate.

    Returns:
        (Callable[..., Any]): The decorated function.
    """

    def wrapped_fn(*args: Any, **kwargs: Any) -> Any:
        """
        Wrapper function to log the duration of a synchronous function.

        Args:
            args (Any): Positional arguments to pass to the decorated function.
            kwargs (Any): Keyword arguments to pass to the decorated function.

        Returns:
            (Any): Result of the decorated function.
        """
        if ENABLE_TIMED_DECORATOR:
            start = time.time()
            try:
                ret = fn(*args, **kwargs)
                return ret
            finally:
                duration_str = get_duration_str(start)
                logger.info(f"[timed] Finished {fn.__name__} in: {duration_str}")
        else:
            return fn(*args, **kwargs)

    async def wrapped_fn_async(*args: Any, **kwargs: Any) -> Any:
        """
        Wrapper function to log the duration of an asynchronous function.

        Args:
            args (Any): Positional arguments to pass to the decorated function.
            kwargs (Any): Keyword arguments to pass to the decorated function.

        Returns:
            (Any): Result of the decorated function.
        """
        if ENABLE_TIMED_DECORATOR:
            start = time.time()
            try:
                ret = await fn(*args, **kwargs)
                return ret
            finally:
                duration_str = get_duration_str(start)
                logger.info(f"[timed] Finished {fn.__name__} in: {duration_str}")
        else:
            return await fn(*args, **kwargs)

    if asyncio.iscoroutinefunction(fn):
        return wrapped_fn_async
    else:
        return wrapped_fn
