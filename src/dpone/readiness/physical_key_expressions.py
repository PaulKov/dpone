"""Split physical key lists without changing their SQL expression spelling."""

from __future__ import annotations

from sqlglot.dialects.clickhouse import ClickHouse
from sqlglot.errors import TokenError
from sqlglot.tokens import TokenType

_OPEN = {
    TokenType.L_PAREN: TokenType.R_PAREN,
    TokenType.L_BRACKET: TokenType.R_BRACKET,
    TokenType.L_BRACE: TokenType.R_BRACE,
}
_CLOSE = frozenset(_OPEN.values())


def split_physical_key_expressions(value: str) -> list[str]:
    """Split only top-level commas; reject ambiguous or incomplete key lists.

    SQLGlot supplies quote, escape and comment-aware token boundaries. Original
    source slices are retained so this operation never rewrites SQL or converts
    literals. Both desired-state parsing and catalog introspection use it.
    """
    text = value.strip()
    if not text or text.lower() == "tuple()":
        return []
    try:
        tokens = ClickHouse.Tokenizer().tokenize(text)
    except TokenError as error:
        raise ValueError("Invalid physical key expression list: tokenization failed") from error
    stack: list[TokenType] = []
    items: list[str] = []
    start = 0
    has_expression = False
    for token in tokens:
        kind = token.token_type
        if kind in _OPEN:
            stack.append(_OPEN[kind])
        elif kind in _CLOSE:
            if not stack or stack.pop() != kind:
                raise ValueError("Invalid physical key expression list: unbalanced delimiters")
        elif kind == TokenType.COMMA and not stack:
            if not has_expression:
                raise ValueError("Invalid physical key expression list: empty expression")
            items.append(text[start : token.start].strip())
            start = token.end + 1
            has_expression = False
            continue
        has_expression = True
    if stack or not has_expression:
        raise ValueError("Invalid physical key expression list: incomplete expression")
    items.append(text[start:].strip())
    return items
