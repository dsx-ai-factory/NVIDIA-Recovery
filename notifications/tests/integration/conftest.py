# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Makes recovery_libs.testing.storage.mysql's fixtures (disposable_database,
mysql_connection_url) available to every test file in this directory without each one
importing them directly -- a per-file import shadows same-named test-function parameters (a
normal pytest fixture-injection pattern ruff can't distinguish from a real redefinition).
"""

from recovery_libs.testing.storage.mysql import disposable_database, mysql_connection_url

_ = disposable_database, mysql_connection_url
