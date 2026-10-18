# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from watcher.lib.utils.data import deduplicate_records


def _rec(ts, body, job_id="1"):
    return {"timestamp": ts, "body": body, "attributes": {"job_id": job_id}}


class TestDeduplicateRecords:
    def test_no_duplicates(self):
        records = [_rec("ts1", "a"), _rec("ts2", "b")]
        assert deduplicate_records(records) == records

    def test_removes_exact_duplicates(self):
        records = [_rec("ts1", "a"), _rec("ts1", "a", job_id="2"), _rec("ts1", "a", job_id="3")]
        result = deduplicate_records(records)
        assert len(result) == 1
        assert result[0]["attributes"]["job_id"] == "1"

    def test_same_body_different_timestamp_kept(self):
        records = [_rec("ts1", "a"), _rec("ts2", "a")]
        assert len(deduplicate_records(records)) == 2

    def test_same_timestamp_different_body_kept(self):
        records = [_rec("ts1", "a"), _rec("ts1", "b")]
        assert len(deduplicate_records(records)) == 2

    def test_empty_list(self):
        assert deduplicate_records([]) == []

    def test_preserves_order(self):
        records = [_rec("ts1", "a"), _rec("ts2", "b"), _rec("ts1", "a")]
        result = deduplicate_records(records)
        assert [r["timestamp"] for r in result] == ["ts1", "ts2"]

    def test_missing_keys_treated_as_empty(self):
        records = [{"attributes": {"job_id": "1"}}, {"attributes": {"job_id": "2"}}]
        assert len(deduplicate_records(records)) == 1
