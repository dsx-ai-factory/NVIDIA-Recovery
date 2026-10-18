# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

import doctest
import functools
import operator
from typing import get_type_hints

from notifications.templates import base as base_templates


def test_doctest(request):
    """Run doctest on this module's docstrings."""

    def assert_equal(left, right):
        assert left == right

    globs = {"assert_equal": assert_equal}
    failure_count, test_count = doctest.testmod(
        base_templates, optionflags=doctest.ELLIPSIS, extraglobs=globs, verbose=False
    )
    # Sanity check that this test is actually doing something.
    assert test_count != 0
    # None of the tests should have failed.
    assert failure_count == 0, "See captured stdout call for doctest output"


def test_any_template_type_hint():
    expected_members = tuple(base_templates.NotificationTemplate.__subclasses__())
    # Print copy/pastable code to stdout to make this easier to update.
    # It will only show up on failures.
    print("Union[")
    for member in expected_members:
        print(f"    {member.__name__},")
    print("]")
    print()
    annotations = get_type_hints(base_templates.TemplateData)
    assert annotations["root"] == functools.reduce(operator.or_, expected_members)


def test_overriding(mocker) -> None:

    class _OverriddenJobName(base_templates.JobName):
        TEMPLATE = "~'{{job_name}}'~"

    class _OverriddenJobRepr(base_templates.JobRepr):
        TEMPLATE = "'{{job_id}}'--({{job_attempt_index}})"

    class _DummyMessage(base_templates.NotificationTemplate):
        TITLE_TEMPLATE = "title {{>JobRepr}} | {{>JobName}}"
        TEMPLATE = "{{>JobRepr}} | {{>JobName}}"

        job_id: str
        job_attempt_index: int
        job_name: str

    overrides = {
        "JobName": _OverriddenJobName,
        "JobRepr": _OverriddenJobRepr,
    }

    message = _DummyMessage(
        job_id="345",
        job_attempt_index=7,
        job_name="some-job",
    )
    assert message.render_message(override_partials=overrides) == "'345'--(7) | ~'some-job'~"
    assert message.render_message() == "345(7) | some-job"

    assert message.render_title(override_partials=overrides) == "title '345'--(7) | ~'some-job'~"
    assert message.render_title() == "title 345(7) | some-job"
