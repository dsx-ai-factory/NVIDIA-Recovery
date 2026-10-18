# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Share one deployed stack between concurrent tests, letting a test that changes the stack run alone."""

from __future__ import annotations

import fcntl
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def stack_access(lock_path: Path, *, exclusive: bool) -> Iterator[None]:
    """Hold the stack shared or exclusively until the context exits.

    Any number of shared holders run together; an exclusive holder waits for them all and keeps new ones out. The
    lock is a file lock, so it spans every process that uses the same `lock_path`, such as pytest-xdist workers.
    """

    with lock_path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
        yield
