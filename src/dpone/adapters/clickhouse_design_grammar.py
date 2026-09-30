"""Complete closed CREATE parser and fixed candidate renderer, not arbitrary SQL.

Identifiers and string literals are lexed separately. Every token is consumed;
unknown clauses are rejected, never deleted to manufacture a supported design.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.clickhouse_authority import OperationBinding
from dpone.contracts.clickhouse_observation import CandidateColumn, CandidateDesign


def quote_column(name: str) -> str:
    """Quote exact identifiers, including embedded backticks and backslashes."""
    return "`" + name.replace("\\", "\\\\").replace("`", "\\`") + "`"


def _key(columns: tuple[str, ...]) -> str:
    return "tuple()" if not columns else "(" + ", ".join(map(quote_column, columns)) + ")"


def render_candidate_create(binding: OperationBinding, design: CandidateDesign) -> str:
    """CREATE cannot adopt an existing table; no caller expressions/settings."""
    columns = ", ".join(f"{quote_column(c.name)} {c.type_name}" for c in design.columns)
    return (
        f"CREATE TABLE `{binding.subject.database}`.`{binding.candidate}` ({columns}) ENGINE = MergeTree "
        f"PARTITION BY {_key(design.partition_key)} PRIMARY KEY {_key(design.primary_key)} "
        f"ORDER BY {_key(design.sorting_key)}"
    )


@dataclass(frozen=True)
class _Token:
    kind: str
    value: str


def _tokens(sql: str) -> list[_Token]:
    result: list[_Token] = []
    index = 0
    while index < len(sql):
        char = sql[index]
        if char.isspace():
            index += 1
        elif char in "(),.=":
            result.append(_Token(char, char))
            index += 1
        elif char in "`\"'":
            quote, text = char, []
            index += 1
            while index < len(sql):
                char = sql[index]
                index += 1
                if char == "\\":
                    if index == len(sql) or sql[index] not in (quote, "\\"):
                        raise ValueError("unsupported_design_escape")
                    text.append(sql[index])
                    index += 1
                elif char == quote:
                    if index < len(sql) and sql[index] == quote:
                        text.append(quote)
                        index += 1
                    else:
                        break
                else:
                    text.append(char)
            else:
                raise ValueError("unterminated_design_quote")
            result.append(_Token("literal" if quote == "'" else "identifier", "".join(text)))
        elif match := re.match(r"[A-Za-z_][A-Za-z0-9_]*|[0-9]+", sql[index:]):
            word = match[0]
            result.append(_Token("number" if word.isdigit() else "word", word))
            index += len(word)
        else:
            raise ValueError("unsupported_design_token")
    return result


class _Parser:
    def __init__(self, sql: str):
        self.tokens = _tokens(sql)
        self.index = 0

    def pop(self) -> _Token:
        if self.index >= len(self.tokens):
            raise ValueError("incomplete_table_design")
        token = self.tokens[self.index]
        self.index += 1
        return token

    def take(self, expected: str) -> bool:
        if self.index < len(self.tokens):
            token = self.tokens[self.index]
            if token.kind not in {"literal", "identifier"} and token.value.upper() == expected.upper():
                self.index += 1
                return True
        return False

    def need(self, expected: str) -> None:
        if not self.take(expected):
            raise ValueError("unsupported_table_design")

    def identifier(self) -> str:
        token = self.pop()
        if token.kind not in {"identifier", "word"}:
            raise ValueError("expected_design_identifier")
        return token.value

    def type_name(self, depth: int = 0) -> str:
        if depth > 1:
            raise ValueError("unsupported_nested_design_type")
        token = self.pop()
        if token.kind != "word":
            raise ValueError("expected_design_type")
        root = token.value
        if not self.take("("):
            return root
        if root == "Nullable":
            inner = self.type_name(depth + 1)
        else:
            values = []
            while True:
                item = self.pop()
                if item.kind == "number":
                    values.append(item.value)
                elif item.kind == "literal":
                    values.append("'" + item.value.replace("'", "''") + "'")
                else:
                    raise ValueError("unsupported_design_type_argument")
                if not self.take(","):
                    break
            inner = ", ".join(values)
        self.need(")")
        return f"{root}({inner})"

    def key(self) -> tuple[str, ...]:
        if self.take("tuple"):
            self.need("(")
        elif not self.take("("):
            return (self.identifier(),)
        if self.take(")"):
            return ()
        names = [self.identifier()]
        while self.take(","):
            names.append(self.identifier())
        self.need(")")
        return tuple(names)

    def design(self) -> CandidateDesign:
        self.need("CREATE")
        self.need("TABLE")
        self.identifier()
        if self.take("."):
            self.identifier()
        if self.take("UUID"):
            token = self.pop()
            if token.kind != "literal":
                raise ValueError("invalid_design_uuid")
            UUID(token.value)
        self.need("(")
        columns = [CandidateColumn(self.identifier(), self.type_name())]
        while self.take(","):
            columns.append(CandidateColumn(self.identifier(), self.type_name()))
        self.need(")")
        self.need("ENGINE")
        self.need("=")
        self.need("MergeTree")
        if self.take("("):
            self.need(")")
        keys: dict[str, tuple[str, ...]] = {}
        while self.index < len(self.tokens):
            clause = self.pop()
            if clause.kind != "word" or clause.value.upper() not in {"ORDER", "PRIMARY", "PARTITION"}:
                raise ValueError("unsupported_design_clause")
            name = clause.value.upper()
            if name in keys:
                raise ValueError("duplicate_design_clause")
            self.need("KEY" if name == "PRIMARY" else "BY")
            keys[name] = self.key()
        if "ORDER" not in keys:
            raise ValueError("explicit_sorting_key_required")
        return CandidateDesign(
            tuple(columns), keys["ORDER"], keys.get("PRIMARY", keys["ORDER"]), keys.get("PARTITION", ())
        )


def parse_table_design(create_sql: str) -> CandidateDesign:
    """Parse the entire supported definition; names/UUID are separate identity."""
    if type(create_sql) is not str:
        raise ValueError("table_design_requires_sql_text")
    return _Parser(create_sql).design()
