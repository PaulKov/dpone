"""Runtime enforcement for nested normalization explosion guardrails."""

from __future__ import annotations

from collections import defaultdict

from dpone.runtime.normalization.guardrails import NormalizationGuardrails


class ExplosionGuard:
    """Track per-root nested output and fail before unbounded materialization."""

    def __init__(self, guardrails: NormalizationGuardrails, *, root_table: str) -> None:
        self._guardrails = guardrails
        self._root_table = root_table
        self._tables_by_root: dict[str, set[str]] = defaultdict(set)
        self._rows_by_root: dict[str, int] = defaultdict(int)

    def check_array_length(self, path: str, length: int) -> None:
        limit = self._guardrails.max_array_length
        if limit is not None and length > limit:
            self._raise(f"Nested path `{path}` has {length} items, above max_array_length={limit}")

    def record_row(self, *, root_row_id: str, table: str) -> None:
        if table != self._root_table:
            self._tables_by_root[root_row_id].add(table)
        self._rows_by_root[root_row_id] += 1
        self._check_table_limit(root_row_id)
        self._check_row_limit(root_row_id)

    def _check_table_limit(self, root_row_id: str) -> None:
        limit = self._guardrails.max_child_tables
        count = len(self._tables_by_root[root_row_id])
        if limit is not None and count > limit:
            self._raise(f"Root row `{root_row_id}` produced {count} child tables, above max_child_tables={limit}")

    def _check_row_limit(self, root_row_id: str) -> None:
        limit = self._guardrails.max_rows_per_root
        count = self._rows_by_root[root_row_id]
        if limit is not None and count > limit:
            self._raise(f"Root row `{root_row_id}` produced {count} normalized rows, above max_rows_per_root={limit}")

    def _raise(self, message: str) -> None:
        if self._guardrails.on_explosion == "warn":
            return
        raise ValueError(message)
