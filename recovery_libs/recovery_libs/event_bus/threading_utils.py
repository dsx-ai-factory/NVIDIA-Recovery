# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import threading


class ThreadSafeSet:
    def __init__(self):
        self._set = set()
        self._lock = threading.Lock()

    def get_all_items(self):
        with self._lock:
            return set(self._set)

    def update_all(self, new_items):
        with self._lock:
            self._set.clear()
            self._set.update(new_items)

    def __repr__(self):
        with self._lock:
            return f"ThreadSafeSet({self._set})"
