# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for nodeset.operations module."""

import pytest
from recovery_libs.utils.nodeset.operations import (
    EvaluationError,
    diff,
    evaluate,
    evaluate_expression,
    intersect,
    union,
)
from recovery_libs.utils.nodeset.parser import parse_expression


class TestUnion:
    """Tests for union function."""

    def test_basic_union(self):
        """Test basic union of two sets."""
        result = union({"a", "b"}, {"b", "c"})
        assert result == {"a", "b", "c"}

    def test_disjoint_sets(self):
        """Test union of disjoint sets."""
        result = union({"a", "b"}, {"c", "d"})
        assert result == {"a", "b", "c", "d"}

    def test_identical_sets(self):
        """Test union of identical sets."""
        result = union({"a", "b"}, {"a", "b"})
        assert result == {"a", "b"}

    def test_empty_sets(self):
        """Test union with empty sets."""
        assert union(set(), {"a", "b"}) == {"a", "b"}
        assert union({"a", "b"}, set()) == {"a", "b"}
        assert union(set(), set()) == set()


class TestIntersect:
    """Tests for intersect function."""

    def test_basic_intersect(self):
        """Test basic intersection of two sets."""
        result = intersect({"a", "b", "c"}, {"b", "c", "d"})
        assert result == {"b", "c"}

    def test_disjoint_sets(self):
        """Test intersection of disjoint sets."""
        result = intersect({"a", "b"}, {"c", "d"})
        assert result == set()

    def test_identical_sets(self):
        """Test intersection of identical sets."""
        result = intersect({"a", "b"}, {"a", "b"})
        assert result == {"a", "b"}

    def test_subset(self):
        """Test intersection when one is subset of other."""
        result = intersect({"a", "b", "c"}, {"b"})
        assert result == {"b"}

    def test_empty_sets(self):
        """Test intersection with empty sets."""
        assert intersect(set(), {"a", "b"}) == set()
        assert intersect({"a", "b"}, set()) == set()


class TestDiff:
    """Tests for diff function."""

    def test_basic_diff(self):
        """Test basic difference of two sets."""
        result = diff({"a", "b", "c"}, {"b", "c", "d"})
        assert result == {"a"}

    def test_disjoint_sets(self):
        """Test difference of disjoint sets."""
        result = diff({"a", "b"}, {"c", "d"})
        assert result == {"a", "b"}

    def test_identical_sets(self):
        """Test difference of identical sets."""
        result = diff({"a", "b"}, {"a", "b"})
        assert result == set()

    def test_subset_removal(self):
        """Test difference where second is subset of first."""
        result = diff({"a", "b", "c"}, {"b"})
        assert result == {"a", "c"}

    def test_superset_removal(self):
        """Test difference where second is superset of first."""
        result = diff({"a", "b"}, {"a", "b", "c"})
        assert result == set()

    def test_empty_sets(self):
        """Test difference with empty sets."""
        assert diff(set(), {"a", "b"}) == set()
        assert diff({"a", "b"}, set()) == {"a", "b"}


class TestEvaluate:
    """Tests for evaluate function."""

    @pytest.fixture
    def job_nodes(self):
        """Sample job nodes data."""
        return {
            "12345/0": {"node1", "node2", "node3"},
            "12346/0": {"node2", "node3", "node4"},
            "12347/0": {"node3", "node4", "node5"},
            "12348/0": {"node4", "node5", "node6"},
        }

    def test_single_job(self, job_nodes):
        """Test evaluating a single job ID."""
        ast = parse_expression("12345/0")
        result = evaluate(ast, job_nodes)
        assert result == {"node1", "node2", "node3"}

    def test_single_job_returns_copy(self, job_nodes):
        """Test that single job evaluation returns a copy, not reference."""
        ast = parse_expression("12345/0")
        original_nodes = job_nodes["12345/0"].copy()

        result = evaluate(ast, job_nodes)
        result.add("rogue_node")  # Mutate the result

        # Original data should be unchanged
        assert job_nodes["12345/0"] == original_nodes
        assert "rogue_node" not in job_nodes["12345/0"]

    def test_union(self, job_nodes):
        """Test evaluating union expression."""
        ast = parse_expression("12345/0 U 12346/0")
        result = evaluate(ast, job_nodes)
        assert result == {"node1", "node2", "node3", "node4"}

    def test_intersect(self, job_nodes):
        """Test evaluating intersection expression."""
        ast = parse_expression("12345/0 & 12346/0")
        result = evaluate(ast, job_nodes)
        assert result == {"node2", "node3"}

    def test_diff(self, job_nodes):
        """Test evaluating difference expression."""
        ast = parse_expression("12345/0 - 12346/0")
        result = evaluate(ast, job_nodes)
        assert result == {"node1"}

    def test_complex_expression(self, job_nodes):
        """Test evaluating complex expression: (A U B) - (C U D)."""
        ast = parse_expression("(12345/0 U 12346/0) - (12347/0 U 12348/0)")
        result = evaluate(ast, job_nodes)
        # (node1, node2, node3, node4) - (node3, node4, node5, node6)
        assert result == {"node1", "node2"}

    def test_chain_of_unions(self, job_nodes):
        """Test evaluating chain: A U B U C."""
        ast = parse_expression("12345/0 U 12346/0 U 12347/0")
        result = evaluate(ast, job_nodes)
        assert result == {"node1", "node2", "node3", "node4", "node5"}

    def test_chain_of_intersections(self, job_nodes):
        """Test evaluating chain: A & B & C."""
        ast = parse_expression("12345/0 & 12346/0 & 12347/0")
        result = evaluate(ast, job_nodes)
        assert result == {"node3"}

    def test_job_not_found(self, job_nodes):
        """Test that missing job raises EvaluationError."""
        ast = parse_expression("99999/0")
        with pytest.raises(EvaluationError) as exc_info:
            evaluate(ast, job_nodes)
        assert "99999/0" in str(exc_info.value)
        assert exc_info.value.job_id == "99999/0"

    def test_job_not_found_in_complex_expression(self, job_nodes):
        """Test that missing job in complex expression raises error."""
        ast = parse_expression("12345/0 U 99999/0")
        with pytest.raises(EvaluationError) as exc_info:
            evaluate(ast, job_nodes)
        assert exc_info.value.job_id == "99999/0"


class TestEvaluateExpression:
    """Tests for evaluate_expression convenience function."""

    def test_basic_usage(self):
        """Test basic usage of evaluate_expression."""
        job_nodes = {
            "12345/0": {"a", "b", "c"},
            "12346/0": {"c", "d", "e"},
        }
        result = evaluate_expression("12345/0 U 12346/0", job_nodes)
        assert result == {"a", "b", "c", "d", "e"}

    def test_complex_expression(self):
        """Test complex expression."""
        job_nodes = {
            "12345/0": {"a", "b", "c"},
            "12346/0": {"b", "c", "d"},
            "12347/0": {"c", "d", "e"},
        }
        result = evaluate_expression("(12345/0 U 12346/0) - 12347/0", job_nodes)
        # (a, b, c, d) - (c, d, e) = (a, b)
        assert result == {"a", "b"}

    def test_parse_error_propagates(self):
        """Test that parse errors propagate."""
        from recovery_libs.utils.nodeset.parser import ParseError

        with pytest.raises(ParseError):
            evaluate_expression("invalid expression +", {})


class TestRealWorldScenario:
    """Tests simulating real-world usage with node names."""

    def test_slow_rack_identification(self):
        """Test the primary use case: finding nodes unique to slow jobs."""
        # Simulate slow jobs that include suspect rack nvl72080
        # Note: slow jobs share some nodes with fast jobs (nvl72013, nvl72015)
        slow_job_1 = {
            "nvl72013-T01",
            "nvl72013-T02",
            "nvl72080-T01",
            "nvl72080-T02",
        }
        slow_job_2 = {
            "nvl72015-T01",
            "nvl72015-T02",
            "nvl72080-T03",
            "nvl72080-T04",
        }

        # Simulate fast jobs that do NOT include suspect rack
        # Fast jobs use the SAME nodes from good racks (nvl72013, nvl72015)
        fast_job_1 = {
            "nvl72013-T01",  # Same as slow_job_1
            "nvl72013-T02",  # Same as slow_job_1
            "nvl72018-T01",
            "nvl72018-T02",
        }
        fast_job_2 = {
            "nvl72015-T01",  # Same as slow_job_2
            "nvl72015-T02",  # Same as slow_job_2
            "nvl72018-T03",
            "nvl72018-T04",
        }

        job_nodes = {
            "12345/0": slow_job_1,
            "12346/0": slow_job_2,
            "12347/0": fast_job_1,
            "12348/0": fast_job_2,
        }

        # Find nodes in slow jobs but not in fast jobs
        result = evaluate_expression("(12345/0 U 12346/0) - (12347/0 U 12348/0)", job_nodes)

        # Result should only contain nodes from the suspect rack nvl72080
        # because those are the only nodes unique to slow jobs
        expected = {
            "nvl72080-T01",
            "nvl72080-T02",
            "nvl72080-T03",
            "nvl72080-T04",
        }
        assert result == expected
