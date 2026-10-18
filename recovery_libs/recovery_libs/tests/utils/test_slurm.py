# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pytest
from recovery_libs.utils.slurm import comment_is_APS, parse_nodelist, set_filter_if_any


def test_plain():
    assert list(parse_nodelist("example.hostname.com")) == ["example.hostname.com"]
    assert list(parse_nodelist("example.com")) == ["example.com"]
    assert list(parse_nodelist("example")) == ["example"]


def test_empty():
    assert parse_nodelist("") == []


def test_range():
    assert list(parse_nodelist("example[2-4].hostname.com")) == [
        "example2.hostname.com",
        "example3.hostname.com",
        "example4.hostname.com",
    ]


def test_pad_range():
    assert list(parse_nodelist("example[02-4].hostname.com")) == [
        "example02.hostname.com",
        "example03.hostname.com",
        "example04.hostname.com",
    ]
    assert list(parse_nodelist("example[2-04].hostname.com")) == [
        "example02.hostname.com",
        "example03.hostname.com",
        "example04.hostname.com",
    ]
    assert list(parse_nodelist("example[02-04].hostname.com")) == [
        "example02.hostname.com",
        "example03.hostname.com",
        "example04.hostname.com",
    ]


def test_commas():
    assert list(parse_nodelist("example[2,10,20].hostname.com")) == [
        "example2.hostname.com",
        "example10.hostname.com",
        "example20.hostname.com",
    ]


def test_nopad_commas():
    assert list(parse_nodelist("example[002,10,20].hostname.com")) == [
        "example002.hostname.com",
        "example10.hostname.com",
        "example20.hostname.com",
    ]


def test_pad_commas():
    assert list(parse_nodelist("example[2-4,10,20-21].hostname.com")) == [
        "example2.hostname.com",
        "example3.hostname.com",
        "example4.hostname.com",
        "example10.hostname.com",
        "example20.hostname.com",
        "example21.hostname.com",
    ]


def test_multi():
    assert frozenset(parse_nodelist("example[2-4].hostname[30-31]")) == frozenset(
        [
            "example2.hostname30",
            "example3.hostname30",
            "example4.hostname30",
            "example2.hostname31",
            "example3.hostname31",
            "example4.hostname31",
        ]
    )


@pytest.mark.parametrize(
    "job_partition,partition_filter,expected",
    [
        ["batch", None, False],
        ["batch", [], False],
        ["batch", ["pool0"], False],
        ["batch", ["pool0", "pool1"], False],
        ["batch", ["batch"], True],
        ["batch", ["batch", "pool0"], True],
    ],
)
def test_set_filter_if_any(job_partition: str, partition_filter: list[str] | None, expected: bool):
    assert set_filter_if_any(job_partition, partition_filter) == expected


@pytest.mark.parametrize(
    "input,output",
    [
        [
            '{"APS": {"auto_resume_mode": "singleton_dependency", "environment": "dev"}}',
            {"APS": {"auto_resume_mode": "singleton_dependency", "environment": "dev"}},
        ],
        [
            '{"HEIMDALL": {"auto_resume_mode": "singleton_dependency", "environment": "dev"}}',
            {"HEIMDALL": {"auto_resume_mode": "singleton_dependency", "environment": "dev"}},
        ],
        ['{"APS": {}}', {"APS": {}}],
        ["{}", None],
        ["", None],
        [" ", None],
        ["      ", None],
        ["abc", None],
        [None, None],
    ],
)
def test_comment_is_APS(input: str | None, output: dict[str, dict | str]):
    with patch("recovery_libs.utils.slurm.logger") as mock_logger:
        ret = comment_is_APS(input)
        assert ret == output
        assert not mock_logger.error.called
