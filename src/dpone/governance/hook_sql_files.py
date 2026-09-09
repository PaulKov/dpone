"""Resolve SQL hook file references into executable SQL text."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ResolvedHookSql:
    sql: str | None
    sql_file: str | None
    sql_hash: str | None


def resolve_hook_sql(
    *,
    inline_sql: object,
    sql_file: object,
    manifest_dir: str | Path | None,
    repo_root: str | Path | None,
) -> ResolvedHookSql:
    """Resolve mutually exclusive inline/file SQL hook inputs."""

    sql = _optional_text(inline_sql)
    raw_path = _optional_text(sql_file)
    if sql and raw_path:
        raise ValueError("hook_sql_inline_and_file_conflict")
    if not raw_path:
        return ResolvedHookSql(sql=sql, sql_file=None, sql_hash=_hash(sql))
    if manifest_dir is None:
        raise ValueError("hook_sql_file_manifest_dir_required")
    manifest_path = Path(manifest_dir).resolve()
    root = Path(repo_root or manifest_path).resolve()
    source_path = (manifest_path / raw_path).resolve()
    if not _is_relative_to(source_path, root):
        raise ValueError("sql_file_outside_repo")
    if not source_path.is_file():
        raise FileNotFoundError(f"sql_file_not_found:{source_path}")
    loaded_sql = source_path.read_text(encoding="utf-8").strip()
    return ResolvedHookSql(sql=loaded_sql, sql_file=raw_path, sql_hash=_hash(loaded_sql))


def _optional_text(raw: object) -> str | None:
    if raw is None:
        return None
    value = str(raw).strip()
    return value or None


def _hash(value: str | None) -> str | None:
    if value is None:
        return None
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


__all__ = ["ResolvedHookSql", "resolve_hook_sql"]
