"""Resolve read-only SQL query workload definitions."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jinja2 import StrictUndefined
from jinja2.sandbox import SandboxedEnvironment

_MUTATING_PREFIXES = (
    "alter",
    "attach",
    "create",
    "delete",
    "detach",
    "drop",
    "exchange",
    "insert",
    "kill",
    "optimize",
    "rename",
    "replace",
    "system",
    "truncate",
    "update",
)


@dataclass(frozen=True, slots=True)
class ResolvedSqlQuery:
    """Resolved, rendered and fingerprinted SQL query contract."""

    sql: str
    sql_hash: str
    source_path: Path | None
    evidence: dict[str, Any]


class SqlQueryResolver:
    """Resolve inline or file-based SQL into a safe read-only query artifact."""

    def __init__(self, *, repo_root: str | Path | None = None) -> None:
        self._repo_root = Path(repo_root or Path.cwd()).resolve()

    def resolve(
        self,
        query_config: dict[str, Any],
        *,
        manifest_dir: str | Path,
        dialect: str,
    ) -> ResolvedSqlQuery:
        mode = str(query_config.get("mode") or ("sql_file" if query_config.get("sql_file") else "inline"))
        manifest_path = Path(manifest_dir).resolve()
        raw_sql, source_path = self._load_sql(mode, query_config, manifest_path)
        rendered_sql = self._render(raw_sql, query_config).strip().rstrip(";").strip()
        self._validate_readonly_select(rendered_sql)
        sql_hash = "sha256:" + hashlib.sha256(rendered_sql.encode("utf-8")).hexdigest()
        return ResolvedSqlQuery(
            sql=rendered_sql,
            sql_hash=sql_hash,
            source_path=source_path,
            evidence={
                "schema_version": "dpone.runtime.sql_query.v1",
                "mode": mode,
                "dialect": dialect,
                "sql_hash": sql_hash,
                "source_path": str(source_path) if source_path else None,
                "readonly": True,
            },
        )

    def _load_sql(
        self,
        mode: str,
        query_config: dict[str, Any],
        manifest_dir: Path,
    ) -> tuple[str, Path | None]:
        if mode == "inline":
            sql = str(query_config.get("sql") or "").strip()
            if not sql:
                raise ValueError("sql_query_inline_sql_required")
            return sql, None
        if mode != "sql_file":
            raise ValueError(f"unsupported_sql_query_mode:{mode}")
        raw_path = str(query_config.get("sql_file") or "").strip()
        if not raw_path:
            raise ValueError("sql_query_file_required")
        source_path = (manifest_dir / raw_path).resolve()
        if not _is_relative_to(source_path, self._repo_root):
            raise ValueError("sql_file_outside_repo")
        if not source_path.is_file():
            raise FileNotFoundError(f"sql_file_not_found:{source_path}")
        return source_path.read_text(encoding="utf-8"), source_path

    @staticmethod
    def _render(raw_sql: str, query_config: dict[str, Any]) -> str:
        render = query_config.get("render")
        if not isinstance(render, dict):
            return raw_sql
        context = render.get("context")
        if not isinstance(context, dict):
            context = {}
        environment = SandboxedEnvironment(undefined=StrictUndefined, autoescape=False)
        return environment.from_string(raw_sql).render(**context)

    @staticmethod
    def _validate_readonly_select(sql: str) -> None:
        if not sql:
            raise ValueError("sql_query_empty")
        if _has_multiple_statements(sql):
            raise ValueError("sql_query_single_statement_required")
        first_token = _first_sql_token(sql)
        if first_token in _MUTATING_PREFIXES or first_token not in {"select", "with"}:
            raise ValueError("sql_query_must_be_readonly_select")


def _first_sql_token(sql: str) -> str:
    index = 0
    length = len(sql)
    while index < length:
        while index < length and sql[index].isspace():
            index += 1
        if sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            if newline == -1:
                return ""
            index = newline + 1
            continue
        if sql.startswith("/*", index):
            comment_end = sql.find("*/", index + 2)
            if comment_end == -1:
                return ""
            index = comment_end + 2
            continue
        break
    start = index
    while index < length and (sql[index].isalpha() or sql[index] == "_"):
        index += 1
    return sql[start:index].lower()


def _has_multiple_statements(sql: str) -> bool:
    in_single_quote = False
    in_double_quote = False
    for index, char in enumerate(sql):
        previous = sql[index - 1] if index else ""
        if char == "'" and not in_double_quote and previous != "\\":
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote and previous != "\\":
            in_double_quote = not in_double_quote
        elif char == ";" and not in_single_quote and not in_double_quote and sql[index + 1 :].strip():
            return True
    return False


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


__all__ = ["ResolvedSqlQuery", "SqlQueryResolver"]
