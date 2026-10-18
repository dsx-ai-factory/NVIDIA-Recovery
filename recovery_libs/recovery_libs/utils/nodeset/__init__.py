# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Copyright (c) 2024, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Node set analysis utilities for slow rack identification.

This package provides tools for:
- Parsing node names and grouping by rack (topology)
- Parsing set algebra expressions (parser)
- Evaluating set operations on job node data (operations)
"""

from recovery_libs.utils.nodeset.operations import (
    EvaluationError,
    diff,
    evaluate,
    evaluate_expression,
    intersect,
    union,
)
from recovery_libs.utils.nodeset.parser import (
    ASTNode,
    BinaryOpNode,
    JobIdNode,
    Operator,
    ParseError,
    extract_job_ids,
    extract_job_ids_from_ast,
    parse_expression,
)
from recovery_libs.utils.nodeset.topology import (
    NodeTopology,
    can_group_by_rack,
    get_rack,
    group_by_rack,
    parse_node,
)

__all__ = [
    "ASTNode",
    "BinaryOpNode",
    # operations
    "EvaluationError",
    "JobIdNode",
    # topology
    "NodeTopology",
    # parser
    "Operator",
    "ParseError",
    "can_group_by_rack",
    "diff",
    "evaluate",
    "evaluate_expression",
    "extract_job_ids",
    "extract_job_ids_from_ast",
    "get_rack",
    "group_by_rack",
    "intersect",
    "parse_expression",
    "parse_node",
    "union",
]
