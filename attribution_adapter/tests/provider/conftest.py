# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0


"""Provider qualification fixtures; private dependencies must be installed explicitly.

Adds the tests/ directory to ``sys.path`` so that ``import golden_fixtures``
resolves in the relocated golden/roundtrip tests (which used bare-module
imports when they lived in ADI's tests/).
"""

import os
import sys

# Allow bare ``import golden_fixtures`` (and similar local test helpers) to
# resolve when the tests/ package is not on the Python path by default.
sys.path.insert(0, os.path.dirname(__file__))
