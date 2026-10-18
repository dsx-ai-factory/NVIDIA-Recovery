# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Expression parser for set algebra expressions.

Parses expressions like:
  - (12345/0 U 12346/0) - (12347/0 U 12348/0)
  - A & B & C
  - A U B U C

Supported operators:
  - U : Union (nodes in either set)
  - & : Intersection (nodes in both sets)
  - - : Difference (nodes in first but not second)
  - () : Grouping
"""

import re
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar, Union

from recovery_libs.utils.logger import get_logger

logger = get_logger(__name__)

# Maximum number of jobs allowed in a single expression (safety limit)
MAX_JOBS_IN_EXPRESSION = 100


class Operator(Enum):
    """Set operators."""

    UNION = "U"
    INTERSECT = "&"
    DIFF = "-"


class TokenType(Enum):
    """Token types produced by the lexer."""

    JOB_ID = "JOB_ID"  # Job identifier: {job_id}/{attempt_index} (e.g., 12345/0)
    OPERATOR = "OPERATOR"  # Set operator: U (union), & (intersect), - (diff)
    LPAREN = "LPAREN"  # Left parenthesis: (
    RPAREN = "RPAREN"  # Right parenthesis: )
    EOF = "EOF"  # End of input


@dataclass
class Token:
    """Lexer token."""

    type: TokenType
    value: str
    position: int


@dataclass
class JobIdNode:
    """AST node for a job ID."""

    job_id: str


@dataclass
class BinaryOpNode:
    """AST node for a binary operation."""

    operator: Operator
    left: Union["BinaryOpNode", JobIdNode]
    right: Union["BinaryOpNode", JobIdNode]


# Type alias for AST nodes
ASTNode = BinaryOpNode | JobIdNode


class ParseError(Exception):
    """Exception raised for expression parsing errors."""

    def __init__(self, message: str, position: int = -1, token: str = ""):
        self.message = message
        self.position = position
        self.token = token
        super().__init__(message)

    def __str__(self) -> str:
        """Format error with position and token context if available."""
        if self.position >= 0 and self.token:
            return f"{self.message} (at position {self.position}, token='{self.token}')"
        if self.position >= 0:
            return f"{self.message} (at position {self.position})"
        return self.message


class Lexer:
    """Tokenizer for set expressions."""

    # Job ID pattern: digits/digits (e.g., 12345/0)
    JOB_ID_PATTERN = re.compile(r"\d+/\d+")

    # Valid operators (derived from Operator enum)
    OPERATORS: ClassVar[set[str]] = {op.value for op in Operator}

    def __init__(self, expression: str):
        self.expression = expression
        self.pos = 0
        self.length = len(expression)

    def _skip_whitespace(self) -> None:
        """Skip whitespace characters."""
        while self.pos < self.length and self.expression[self.pos].isspace():
            self.pos += 1

    def next_token(self) -> Token:
        """Get the next token from the expression."""
        self._skip_whitespace()

        if self.pos >= self.length:
            return Token(TokenType.EOF, "", self.pos)

        char = self.expression[self.pos]
        start_pos = self.pos

        # Single character tokens
        if char == "(":
            self.pos += 1
            return Token(TokenType.LPAREN, "(", start_pos)
        if char == ")":
            self.pos += 1
            return Token(TokenType.RPAREN, ")", start_pos)

        # Operators
        if char in self.OPERATORS:
            self.pos += 1
            return Token(TokenType.OPERATOR, char, start_pos)

        # Job ID (digits/digits)
        if char.isdigit():
            match = self.JOB_ID_PATTERN.match(self.expression, self.pos)
            if match:
                job_id = match.group()
                self.pos = match.end()
                return Token(TokenType.JOB_ID, job_id, start_pos)
            else:
                raise ParseError(
                    f"Invalid job ID format at position {start_pos}",
                    position=start_pos,
                    token=char,
                )

        raise ParseError(
            f"Unexpected character '{char}' at position {start_pos}",
            position=start_pos,
            token=char,
        )


class Parser:
    """
    Recursive descent parser for set expressions.

    Converts expression strings into an Abstract Syntax Tree (AST) for evaluation.

    Syntax rules:
        - Job IDs use format: {job_id}/{attempt_index}
        - Operators: U (union), & (intersect), - (difference)
        - Parentheses group sub-expressions
        - All operators have equal precedence, evaluated left-to-right

    Examples:
        "12345/0"
            -> Single job

        "12345/0 U 12346/0"
            -> Union: nodes in either job

        "12345/0 & 12346/0"
            -> Intersect: nodes in both jobs

        "12345/0 - 12346/0"
            -> Difference: nodes in first job but not second

        "(12345/0 U 12346/0) - 12347/0"
            -> Union first two, then subtract third

        "A U B U C"
            -> Evaluated as ((A U B) U C), left-to-right
    """

    def __init__(self, expression: str):
        self.lexer = Lexer(expression)
        self.current_token = self.lexer.next_token()

    def _consume(self, expected_type: TokenType) -> Token:
        """Consume a token of the expected type."""
        if self.current_token.type != expected_type:
            raise ParseError(
                f"Expected {expected_type.value}, got {self.current_token.type.value}",
                position=self.current_token.position,
                token=self.current_token.value,
            )
        token = self.current_token
        self.current_token = self.lexer.next_token()
        return token

    def parse(self) -> ASTNode:
        """Parse the expression and return the AST root."""
        if self.current_token.type == TokenType.EOF:
            raise ParseError("Empty expression", position=0)

        ast = self._parse_expr()

        if self.current_token.type != TokenType.EOF:
            raise ParseError(
                f"Unexpected token '{self.current_token.value}' after expression",
                position=self.current_token.position,
                token=self.current_token.value,
            )

        return ast

    def _parse_expr(self) -> ASTNode:
        """Parse an expression (handles left-to-right associativity)."""
        left = self._parse_term()

        while self.current_token.type == TokenType.OPERATOR:
            op_token = self.current_token
            self._consume(TokenType.OPERATOR)

            operator = {
                "U": Operator.UNION,
                "&": Operator.INTERSECT,
                "-": Operator.DIFF,
            }[op_token.value]

            right = self._parse_term()
            left = BinaryOpNode(operator=operator, left=left, right=right)

        return left

    def _parse_term(self) -> ASTNode:
        """Parse a term (job ID or parenthesized expression)."""
        if self.current_token.type == TokenType.LPAREN:
            self._consume(TokenType.LPAREN)
            expr = self._parse_expr()
            self._consume(TokenType.RPAREN)
            return expr

        if self.current_token.type == TokenType.JOB_ID:
            job_id = self.current_token.value
            self._consume(TokenType.JOB_ID)
            return JobIdNode(job_id=job_id)

        raise ParseError(
            f"Expected job ID or '(', got '{self.current_token.value}'",
            position=self.current_token.position,
            token=self.current_token.value,
        )


def parse_expression(expression: str) -> ASTNode:
    """
    Parse a set expression and return the AST.

    Args:
        expression: Set expression string (e.g., "(12345/0 U 12346/0) - 12347/0")

    Returns:
        Root node of the AST.

    Raises:
        ParseError: If the expression is invalid or exceeds MAX_JOBS_IN_EXPRESSION.

    Examples:
        >>> ast = parse_expression("12345/0 U 12346/0")
        >>> isinstance(ast, BinaryOpNode)
        True

        >>> ast = parse_expression("12345/0")
        >>> isinstance(ast, JobIdNode)
        True
    """
    logger.debug(f"Parsing expression: {expression[:100]}...")

    parser = Parser(expression.strip())
    ast = parser.parse()

    # Validate job count limit
    job_ids = extract_job_ids_from_ast(ast)
    if len(job_ids) > MAX_JOBS_IN_EXPRESSION:
        raise ParseError(
            f"Expression contains {len(job_ids)} jobs, maximum allowed is {MAX_JOBS_IN_EXPRESSION}",
            position=0,
        )

    logger.debug(f"Successfully parsed expression with {len(job_ids)} job(s)")
    return ast


def extract_job_ids(expression: str) -> list[str]:
    """
    Extract all job IDs from an expression without full parsing.

    This is useful for batch-fetching job data before evaluation.

    Args:
        expression: Set expression string

    Returns:
        List of unique job IDs in the order they appear.

    Examples:
        >>> extract_job_ids("(12345/0 U 12346/0) - 12347/0")
        ['12345/0', '12346/0', '12347/0']

        >>> extract_job_ids("12345/0 & 12345/0")
        ['12345/0']
    """
    pattern = re.compile(r"\d+/\d+")
    matches = pattern.findall(expression)
    # Preserve order while removing duplicates
    seen = set()
    result = []
    for job_id in matches:
        if job_id not in seen:
            seen.add(job_id)
            result.append(job_id)
    return result


def extract_job_ids_from_ast(ast: ASTNode) -> list[str]:
    """
    Extract all job IDs from a parsed AST.

    Args:
        ast: Root node of the AST

    Returns:
        List of unique job IDs.
    """
    job_ids: list[str] = []
    seen: set[str] = set()

    def _traverse(node: ASTNode) -> None:
        if isinstance(node, JobIdNode):
            if node.job_id not in seen:
                seen.add(node.job_id)
                job_ids.append(node.job_id)
        elif isinstance(node, BinaryOpNode):
            _traverse(node.left)
            _traverse(node.right)

    _traverse(ast)
    return job_ids
