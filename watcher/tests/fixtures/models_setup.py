# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest_asyncio


@pytest_asyncio.fixture
def workflow_step_regex_parser_advanced_config_dict(rules_engine):
    workflow_config_rule = None

    for rule in rules_engine.config.rules:
        if rule.name == "local":
            workflow_config_rule = rule

    for step in workflow_config_rule.workflow:
        if step.name == "nested_restarter":
            for child in step.children:
                if child.name == "nested_restarter_regex_parse":
                    return child.to_json()

    return None
