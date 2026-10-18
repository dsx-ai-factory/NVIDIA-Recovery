# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Set operations for node analysis.

Evaluates AST expressions against job node data to perform
union, intersection, and difference operations.
"""

from recovery_libs.utils.logger import get_logger
from recovery_libs.utils.nodeset.parser import ASTNode, BinaryOpNode, JobIdNode, Operator

logger = get_logger(__name__)

# Maximum recursion depth for AST evaluation (safety limit)
MAX_EVALUATION_DEPTH = 100


class EvaluationError(Exception):
    """Exception raised during expression evaluation."""

    def __init__(self, message: str, job_id: str = ""):
        self.message = message
        self.job_id = job_id
        super().__init__(message)


def union(set_a: set[str], set_b: set[str]) -> set[str]:
    """
    Return the union of two sets.

    Args:
        set_a: First set of nodes
        set_b: Second set of nodes

    Returns:
        Set containing all nodes from both sets.

    Examples:
        >>> union({'a', 'b'}, {'b', 'c'})
        {'a', 'b', 'c'}
    """
    return set_a | set_b


def intersect(set_a: set[str], set_b: set[str]) -> set[str]:
    """
    Return the intersection of two sets.

    Args:
        set_a: First set of nodes
        set_b: Second set of nodes

    Returns:
        Set containing only nodes present in both sets.

    Examples:
        >>> intersect({'a', 'b'}, {'b', 'c'})
        {'b'}
    """
    return set_a & set_b


def diff(set_a: set[str], set_b: set[str]) -> set[str]:
    """
    Return the difference of two sets.

    Args:
        set_a: First set of nodes
        set_b: Second set of nodes

    Returns:
        Set containing nodes in set_a but not in set_b.

    Examples:
        >>> diff({'a', 'b', 'c'}, {'b', 'c', 'd'})
        {'a'}
    """
    return set_a - set_b


def evaluate(ast: ASTNode, job_nodes: dict[str, set[str]], _depth: int = 0) -> set[str]:
    """
    Evaluate an AST expression against job node data.

    Args:
        ast: Root node of the parsed expression AST
        job_nodes: Mapping of job IDs to their node sets
        _depth: Internal parameter for tracking recursion depth (do not set manually)

    Returns:
        Resulting set of nodes after evaluating the expression.

    Raises:
        EvaluationError: If a job ID is not found or recursion depth exceeds limit.

    Examples:
        >>> from recovery_libs.utils.nodeset.parser import parse_expression
        >>> ast = parse_expression("12345/0 U 12346/0")
        >>> job_nodes = {
        ...     "12345/0": {"node1", "node2"},
        ...     "12346/0": {"node2", "node3"}
        ... }
        >>> evaluate(ast, job_nodes)
        {'node1', 'node2', 'node3'}
    """
    # Safety check: prevent stack overflow from deeply nested expressions
    if _depth > MAX_EVALUATION_DEPTH:
        raise EvaluationError(f"Expression too deeply nested (depth {_depth} exceeds limit {MAX_EVALUATION_DEPTH})")

    if isinstance(ast, JobIdNode):
        job_id = ast.job_id
        if job_id not in job_nodes:
            raise EvaluationError(
                f"Job '{job_id}' not found",
                job_id=job_id,
            )
        logger.debug(f"Resolved job '{job_id}' with {len(job_nodes[job_id])} nodes")
        # Return a copy to prevent caller from mutating original data
        return job_nodes[job_id].copy()

    if isinstance(ast, BinaryOpNode):
        left_result = evaluate(ast.left, job_nodes, _depth + 1)
        right_result = evaluate(ast.right, job_nodes, _depth + 1)

        if ast.operator == Operator.UNION:
            result = union(left_result, right_result)
            logger.debug(f"Union: {len(left_result)} + {len(right_result)} -> {len(result)} nodes")
            return result
        elif ast.operator == Operator.INTERSECT:
            result = intersect(left_result, right_result)
            logger.debug(f"Intersect: {len(left_result)} & {len(right_result)} -> {len(result)} nodes")
            return result
        elif ast.operator == Operator.DIFF:
            result = diff(left_result, right_result)
            logger.debug(f"Diff: {len(left_result)} - {len(right_result)} -> {len(result)} nodes")
            return result
        else:
            raise EvaluationError(f"Unknown operator: {ast.operator}")

    raise EvaluationError(f"Unknown AST node type: {type(ast)}")


def evaluate_expression(
    expression: str,
    job_nodes: dict[str, set[str]],
) -> set[str]:
    """
    Parse and evaluate an expression in one step.

    Convenience function that combines parsing and evaluation.

    Args:
        expression: Set expression string (e.g., "(12345/0 U 12346/0) - 12347/0")
        job_nodes: Mapping of job IDs to their node sets

    Returns:
        Resulting set of nodes.

    Raises:
        ParseError: If the expression is invalid.
        EvaluationError: If a job ID is not found in job_nodes.

    Examples:
        >>> job_nodes = {
        ...     "12345/0": {"a", "b", "c"},
        ...     "12346/0": {"b", "c", "d"},
        ...     "12347/0": {"c", "d", "e"}
        ... }
        >>> result = evaluate_expression("(12345/0 U 12346/0) - 12347/0", job_nodes)
        >>> result == {"a", "b"}
        True
    """
    from recovery_libs.utils.nodeset.parser import parse_expression

    logger.info(f"Evaluating expression with {len(job_nodes)} job(s)")

    ast = parse_expression(expression)
    result = evaluate(ast, job_nodes)

    logger.info(f"Expression evaluation complete: {len(result)} node(s) in result")
    return result
