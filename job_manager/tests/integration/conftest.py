# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Disposable MySQL fixtures for Job Manager's persistence integration tests."""

from recovery_libs.testing.storage.mysql import mysql_container, storage

__all__ = ["mysql_container", "storage"]
