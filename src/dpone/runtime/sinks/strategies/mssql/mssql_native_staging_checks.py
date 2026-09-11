"""SQL key equivalence and row-count checks shared by native staging paths.

The caller owns the database authority scope and validation ordering. These
checks retain SQL Server equality semantics; they never establish ownership or
issue consumed-payload evidence themselves.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.support.mssql_native_projection import decoded_staging_expression
from dpone.runtime.support.mssql_snapshot_projection import is_text_key_type


class MssqlNativeStagingChecks:
    """Validate required keys, SQL collisions and authoritative COUNT_BIG."""

    def __init__(self, strategy: Any) -> None:
        self._strategy = strategy
        self._connector = strategy.connector

    def validate_required_keys(self, raw: StagingTableArtifact, keys: Sequence[str]) -> None:
        if not keys:
            return
        predicates = [f"{decoded_staging_expression(self._strategy, raw, column, 'r')} IS NULL" for column in keys]
        rows = self._connector.get_records(
            f"SELECT TOP (1) 1 AS invalid_key FROM {self._strategy._staging_name(raw)} AS r "
            f"WHERE {' OR '.join(predicates)}",
            as_dict=True,
        )
        if rows:
            _raise("mssql_native_projection.unique_key_null")

    def count_rows(self, artifact: StagingTableArtifact) -> int:
        """Read back direct-native rows before issuing terminal evidence."""

        rows = self._connector.get_records(f"SELECT COUNT_BIG(*) FROM {self._strategy._staging_name(artifact)}")
        if not rows or isinstance(rows[0][0], bool) or not isinstance(rows[0][0], int):
            _raise("mssql_native_projection.native_row_count_unavailable")
        return int(rows[0][0])

    def validate_key_sql_semantics(
        self,
        native: StagingTableArtifact,
        keys: Sequence[str],
        equality_keys: Sequence[str],
    ) -> None:
        if not equality_keys:
            return
        text_keys = [key for key in equality_keys if is_text_key_type(native.target_column_types.get(key, ""))]
        if text_keys:
            padded = " OR ".join(
                f"DATALENGTH({self._connector.quote_identifier(key)}) <> "
                f"DATALENGTH(RTRIM({self._connector.quote_identifier(key)}))"
                for key in text_keys
            )
            rows = self._connector.get_records(
                f"SELECT TOP (1) 1 FROM {self._strategy._staging_name(native)} WHERE {padded}"
            )
            if rows:
                _raise("mssql_native_projection.text_key_trailing_space_unsupported")
        if not keys:
            return
        grouped = ", ".join(self._connector.quote_identifier(key) for key in keys)
        rows = self._connector.get_records(
            f"SELECT TOP (1) 1 FROM {self._strategy._staging_name(native)} GROUP BY {grouped} HAVING COUNT_BIG(*) > 1"
        )
        if rows:
            _raise("mssql_native_projection.key_sql_equivalence_collision")


def _raise(code: str) -> None:
    from dpone.runtime.incremental_snapshot import SnapshotReconciliationError

    raise SnapshotReconciliationError(code)
