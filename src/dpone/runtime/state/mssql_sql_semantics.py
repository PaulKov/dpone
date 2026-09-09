"""Small fail-closed SQL expression canonicalizer for state-table DDL.

Only the predicate/default grammar used by dpone's externally provisioned
SQL Server state tables is accepted.  Unsupported syntax is rejected instead
of being normalized with unsafe string replacement.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

SqlSemantic = tuple[Any, ...]

_NUMBER = re.compile(r"(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?")
_BARE_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_COMPARISON_OPERATORS = {"=", "<>", "!=", "<", ">", "<=", ">="}
_REVERSED_OPERATOR = {"=": "=", "<>": "<>", "!=": "<>", "<": ">", ">": "<", "<=": ">=", ">=": "<="}


@dataclass(frozen=True, slots=True)
class _Token:
    kind: str
    value: str


def canonical_sql_expression(value: str) -> SqlSemantic:
    """Parse and canonicalize the supported SQL Server expression subset."""

    parser = _Parser(_tokenize(value))
    expression = parser.parse()
    return _normalize(expression)


def _tokenize(value: str) -> tuple[_Token, ...]:
    tokens: list[_Token] = []
    offset = 0
    while offset < len(value):
        character = value[offset]
        if character.isspace():
            offset += 1
            continue
        if character == "[":
            identifier, offset = _bracket_identifier(value, offset)
            tokens.append(_Token("identifier", identifier.casefold()))
            continue
        number = _NUMBER.match(value, offset)
        if number:
            tokens.append(_Token("number", number.group(0)))
            offset = number.end()
            continue
        identifier_match = _BARE_IDENTIFIER.match(value, offset)
        if identifier_match:
            tokens.append(_Token("identifier", identifier_match.group(0).casefold()))
            offset = identifier_match.end()
            continue
        two_char = value[offset : offset + 2]
        if two_char in {"<>", "!=", "<=", ">="}:
            tokens.append(_Token("operator", two_char))
            offset += 2
            continue
        if character in "=<>(),":
            tokens.append(_Token("operator" if character in "=<>" else character, character))
            offset += 1
            continue
        raise ValueError(f"unsupported SQL expression token at offset {offset}")
    if not tokens:
        raise ValueError("SQL expression is empty")
    return tuple(tokens)


def _bracket_identifier(value: str, offset: int) -> tuple[str, int]:
    output: list[str] = []
    offset += 1
    while offset < len(value):
        character = value[offset]
        if character == "]":
            if offset + 1 < len(value) and value[offset + 1] == "]":
                output.append("]")
                offset += 2
                continue
            if not output:
                raise ValueError("empty bracket identifier")
            return "".join(output), offset + 1
        output.append(character)
        offset += 1
    raise ValueError("unterminated bracket identifier")


class _Parser:
    def __init__(self, tokens: tuple[_Token, ...]) -> None:
        self._tokens = tokens
        self._offset = 0

    def parse(self) -> SqlSemantic:
        expression = self._parse_or()
        trailing = self._peek()
        if trailing is not None:
            raise ValueError(f"unexpected SQL expression token: {trailing.value}")
        return expression

    def _parse_or(self) -> SqlSemantic:
        expressions = [self._parse_and()]
        while self._accept_keyword("or"):
            expressions.append(self._parse_and())
        return expressions[0] if len(expressions) == 1 else ("or", *expressions)

    def _parse_and(self) -> SqlSemantic:
        expressions = [self._parse_predicate()]
        while self._accept_keyword("and"):
            expressions.append(self._parse_predicate())
        return expressions[0] if len(expressions) == 1 else ("and", *expressions)

    def _parse_predicate(self) -> SqlSemantic:
        left = self._parse_primary()
        if self._accept_keyword("is"):
            negated = self._accept_keyword("not")
            self._expect_keyword("null")
            return ("is_not_null" if negated else "is_null", left)
        if self._accept_keyword("between"):
            lower = self._parse_primary()
            self._expect_keyword("and")
            upper = self._parse_primary()
            return ("between", left, lower, upper)
        token = self._peek()
        if token is not None and token.kind == "operator" and token.value in _COMPARISON_OPERATORS:
            operator = self._take().value
            return ("compare", operator, left, self._parse_primary())
        return left

    def _parse_primary(self) -> SqlSemantic:
        if self._accept_kind("("):
            expression = self._parse_or()
            self._expect_kind(")")
            return expression
        token = self._take()
        if token.kind == "number":
            return ("number", _canonical_number(token.value))
        if token.kind != "identifier":
            raise ValueError(f"expected SQL expression atom, got {token.value}")
        if token.value == "null":
            return ("null",)
        if self._accept_kind("("):
            self._expect_kind(")")
            return ("function", token.value)
        return ("identifier", token.value)

    def _peek(self) -> _Token | None:
        return self._tokens[self._offset] if self._offset < len(self._tokens) else None

    def _take(self) -> _Token:
        token = self._peek()
        if token is None:
            raise ValueError("unexpected end of SQL expression")
        self._offset += 1
        return token

    def _accept_kind(self, kind: str) -> bool:
        token = self._peek()
        if token is None or token.kind != kind:
            return False
        self._offset += 1
        return True

    def _expect_kind(self, kind: str) -> None:
        if not self._accept_kind(kind):
            raise ValueError(f"expected {kind}")

    def _accept_keyword(self, keyword: str) -> bool:
        token = self._peek()
        if token is None or token.kind != "identifier" or token.value != keyword:
            return False
        self._offset += 1
        return True

    def _expect_keyword(self, keyword: str) -> None:
        if not self._accept_keyword(keyword):
            raise ValueError(f"expected {keyword}")


def _canonical_number(value: str) -> str:
    try:
        decimal = Decimal(value)
    except InvalidOperation as exc:  # pragma: no cover - lexer already restricts numbers
        raise ValueError(f"invalid numeric literal: {value}") from exc
    if not decimal.is_finite():
        raise ValueError(f"non-finite numeric literal: {value}")
    normalized = format(decimal.normalize(), "f")
    return "0" if Decimal(normalized) == 0 else normalized


def _normalize(expression: SqlSemantic) -> SqlSemantic:
    kind = str(expression[0])
    if kind in {"identifier", "number", "function", "null"}:
        return expression
    if kind in {"is_null", "is_not_null"}:
        return (kind, _normalize(expression[1]))
    if kind == "between":
        value = _normalize(expression[1])
        lower = _normalize(expression[2])
        upper = _normalize(expression[3])
        return _normalize(("and", ("compare", ">=", value, lower), ("compare", "<=", value, upper)))
    if kind == "compare":
        operator = "<>" if expression[1] == "!=" else str(expression[1])
        left = _normalize(expression[2])
        right = _normalize(expression[3])
        if _semantic_sort_key(left) > _semantic_sort_key(right):
            left, right = right, left
            operator = _REVERSED_OPERATOR[operator]
        return ("compare", operator, left, right)
    if kind in {"and", "or"}:
        children: list[SqlSemantic] = []
        for child in expression[1:]:
            normalized = _normalize(child)
            if normalized[0] == kind:
                children.extend(normalized[1:])
            else:
                children.append(normalized)
        return (kind, *sorted(children, key=_semantic_sort_key))
    raise ValueError(f"unsupported SQL expression node: {kind}")


def _semantic_sort_key(expression: SqlSemantic) -> str:
    return repr(expression)


__all__ = ["SqlSemantic", "canonical_sql_expression"]
