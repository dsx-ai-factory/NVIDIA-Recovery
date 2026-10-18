# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for nodeset.parser module."""

import pytest
from recovery_libs.utils.nodeset.parser import (
    BinaryOpNode,
    JobIdNode,
    Operator,
    ParseError,
    extract_job_ids,
    extract_job_ids_from_ast,
    parse_expression,
)


class TestParseExpression:
    """Tests for parse_expression function."""

    def test_single_job_id(self):
        """Test parsing a single job ID."""
        ast = parse_expression("12345/0")
        assert isinstance(ast, JobIdNode)
        assert ast.job_id == "12345/0"

    def test_union_two_jobs(self):
        """Test parsing union of two jobs."""
        ast = parse_expression("12345/0 U 12346/0")
        assert isinstance(ast, BinaryOpNode)
        assert ast.operator == Operator.UNION
        assert isinstance(ast.left, JobIdNode)
        assert ast.left.job_id == "12345/0"
        assert isinstance(ast.right, JobIdNode)
        assert ast.right.job_id == "12346/0"

    def test_intersection_two_jobs(self):
        """Test parsing intersection of two jobs."""
        ast = parse_expression("12345/0 & 12346/0")
        assert isinstance(ast, BinaryOpNode)
        assert ast.operator == Operator.INTERSECT

    def test_difference_two_jobs(self):
        """Test parsing difference of two jobs."""
        ast = parse_expression("12345/0 - 12346/0")
        assert isinstance(ast, BinaryOpNode)
        assert ast.operator == Operator.DIFF

    def test_parenthesized_expression(self):
        """Test parsing parenthesized expression."""
        ast = parse_expression("(12345/0 U 12346/0)")
        assert isinstance(ast, BinaryOpNode)
        assert ast.operator == Operator.UNION

    def test_complex_expression(self):
        """Test parsing complex expression: (A U B) - C"""
        ast = parse_expression("(12345/0 U 12346/0) - 12347/0")
        assert isinstance(ast, BinaryOpNode)
        assert ast.operator == Operator.DIFF
        assert isinstance(ast.left, BinaryOpNode)
        assert ast.left.operator == Operator.UNION
        assert isinstance(ast.right, JobIdNode)
        assert ast.right.job_id == "12347/0"

    def test_nested_parentheses(self):
        """Test parsing nested parentheses: (A U B) - (C U D)"""
        ast = parse_expression("(12345/0 U 12346/0) - (12347/0 U 12348/0)")
        assert isinstance(ast, BinaryOpNode)
        assert ast.operator == Operator.DIFF
        assert isinstance(ast.left, BinaryOpNode)
        assert isinstance(ast.right, BinaryOpNode)

    def test_chain_of_unions(self):
        """Test parsing chain: A U B U C (left-to-right associativity)."""
        ast = parse_expression("12345/0 U 12346/0 U 12347/0")
        # Should parse as ((A U B) U C)
        assert isinstance(ast, BinaryOpNode)
        assert ast.operator == Operator.UNION
        assert isinstance(ast.left, BinaryOpNode)
        assert ast.left.operator == Operator.UNION
        assert isinstance(ast.right, JobIdNode)
        assert ast.right.job_id == "12347/0"

    def test_chain_of_intersections(self):
        """Test parsing chain: A & B & C"""
        ast = parse_expression("12345/0 & 12346/0 & 12347/0")
        assert isinstance(ast, BinaryOpNode)
        assert ast.operator == Operator.INTERSECT

    def test_whitespace_handling(self):
        """Test that whitespace is handled correctly."""
        ast1 = parse_expression("12345/0 U 12346/0")
        ast2 = parse_expression("  12345/0   U   12346/0  ")
        assert isinstance(ast1, BinaryOpNode)
        assert isinstance(ast2, BinaryOpNode)

    def test_various_job_id_formats(self):
        """Test various job ID formats."""
        # Single digit attempt
        ast = parse_expression("12345/0")
        assert isinstance(ast, JobIdNode)
        assert ast.job_id == "12345/0"

        # Multi-digit attempt
        ast = parse_expression("12345/10")
        assert isinstance(ast, JobIdNode)
        assert ast.job_id == "12345/10"

        # Large job ID
        ast = parse_expression("9999999/0")
        assert isinstance(ast, JobIdNode)
        assert ast.job_id == "9999999/0"


class TestParseExpressionErrors:
    """Tests for parse error handling."""

    def test_empty_expression(self):
        """Test that empty expression raises error."""
        with pytest.raises(ParseError) as exc_info:
            parse_expression("")
        assert "Empty expression" in str(exc_info.value)

    def test_whitespace_only(self):
        """Test that whitespace-only expression raises error."""
        with pytest.raises(ParseError):
            parse_expression("   ")

    def test_invalid_character(self):
        """Test that invalid character raises error."""
        with pytest.raises(ParseError) as exc_info:
            parse_expression("12345/0 + 12346/0")
        assert "Unexpected character" in str(exc_info.value)

    def test_missing_operand(self):
        """Test that missing operand raises error."""
        with pytest.raises(ParseError):
            parse_expression("12345/0 U")

    def test_missing_operator(self):
        """Test that missing operator raises error."""
        with pytest.raises(ParseError):
            parse_expression("12345/0 12346/0")

    def test_unmatched_left_paren(self):
        """Test that unmatched left paren raises error."""
        with pytest.raises(ParseError):
            parse_expression("(12345/0 U 12346/0")

    def test_unmatched_right_paren(self):
        """Test that unmatched right paren raises error."""
        with pytest.raises(ParseError):
            parse_expression("12345/0 U 12346/0)")

    def test_invalid_job_id_format(self):
        """Test that invalid job ID format raises error."""
        with pytest.raises(ParseError):
            parse_expression("12345")  # Missing /attempt

    def test_operator_at_start(self):
        """Test that operator at start raises error."""
        with pytest.raises(ParseError):
            parse_expression("U 12345/0")

    def test_max_jobs_exceeded(self):
        """Test that exceeding MAX_JOBS_IN_EXPRESSION raises error."""
        from recovery_libs.utils.nodeset.parser import MAX_JOBS_IN_EXPRESSION

        # Build expression with MAX_JOBS_IN_EXPRESSION + 1 jobs
        job_ids = [f"{i}/0" for i in range(MAX_JOBS_IN_EXPRESSION + 1)]
        expression = " U ".join(job_ids)

        with pytest.raises(ParseError) as exc_info:
            parse_expression(expression)
        assert f"maximum allowed is {MAX_JOBS_IN_EXPRESSION}" in str(exc_info.value)


class TestExtractJobIds:
    """Tests for extract_job_ids function."""

    def test_single_job(self):
        """Test extracting single job ID."""
        assert extract_job_ids("12345/0") == ["12345/0"]

    def test_multiple_jobs(self):
        """Test extracting multiple job IDs."""
        result = extract_job_ids("(12345/0 U 12346/0) - 12347/0")
        assert result == ["12345/0", "12346/0", "12347/0"]

    def test_duplicate_jobs(self):
        """Test that duplicates are removed."""
        result = extract_job_ids("12345/0 U 12345/0")
        assert result == ["12345/0"]

    def test_order_preserved(self):
        """Test that order is preserved."""
        result = extract_job_ids("12347/0 U 12345/0 U 12346/0")
        assert result == ["12347/0", "12345/0", "12346/0"]

    def test_complex_expression(self):
        """Test complex expression."""
        result = extract_job_ids("(12345/0 U 12346/0) - (12347/0 U 12348/0)")
        assert result == ["12345/0", "12346/0", "12347/0", "12348/0"]


class TestExtractJobIdsFromAst:
    """Tests for extract_job_ids_from_ast function."""

    def test_single_job(self):
        """Test extracting from single job AST."""
        ast = parse_expression("12345/0")
        assert extract_job_ids_from_ast(ast) == ["12345/0"]

    def test_binary_expression(self):
        """Test extracting from binary expression AST."""
        ast = parse_expression("12345/0 U 12346/0")
        assert extract_job_ids_from_ast(ast) == ["12345/0", "12346/0"]

    def test_complex_expression(self):
        """Test extracting from complex expression AST."""
        ast = parse_expression("(12345/0 U 12346/0) - (12347/0 U 12348/0)")
        result = extract_job_ids_from_ast(ast)
        assert result == ["12345/0", "12346/0", "12347/0", "12348/0"]

    def test_duplicate_jobs_in_ast(self):
        """Test that duplicates are removed from AST extraction."""
        ast = parse_expression("12345/0 U 12345/0")
        assert extract_job_ids_from_ast(ast) == ["12345/0"]
