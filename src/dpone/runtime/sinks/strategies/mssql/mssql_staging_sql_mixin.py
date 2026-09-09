"""Cohesive SQL expressions for normalized SQL Server staging artifacts."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.support.mssql_hex_binary import build_mssql_staging_select_expression
from dpone.runtime.support.mssql_native_canonical import canonical_identity_expression


class MSSQLStagingSqlMixin:
    """Render joins and projections from the shared native staging contract."""

    connector: Any
    _SERVICE_COLUMNS: set[str]

    def _key_condition(
        self,
        staging: StagingTableArtifact,
        left_alias: str,
        right_alias: str,
        unique_key: str | Sequence[str],
    ) -> str:
        keys = [unique_key] if isinstance(unique_key, str) else list(unique_key)
        if not keys:
            raise ValueError("unique_key is required for this MSSQL strategy")
        return " AND ".join(self._column_condition(staging, left_alias, right_alias, key) for key in keys)

    def _column_condition(
        self,
        staging: StagingTableArtifact,
        left_alias: str,
        right_alias: str,
        column: str,
    ) -> str:
        return (
            f"{self._key_expression(staging, left_alias, column)} = "
            f"{self._key_expression(staging, right_alias, column)}"
        )

    def _key_expression(self, staging: StagingTableArtifact, alias: str, column: str) -> str:
        quoted = f"{alias}.{self.connector.quote_identifier(column)}"
        dtype = (getattr(staging, "target_column_types", {}) or {}).get(column) or (
            getattr(staging, "column_types", {}) or {}
        ).get(column, "")
        if not dtype:
            raise ValueError(f"MSSQL equality column {column!r} has no resolved native type")
        return canonical_identity_expression(quoted, str(dtype))

    def _data_columns(self, columns: Sequence[str]) -> list[str]:
        return [column for column in columns if column not in self._SERVICE_COLUMNS]

    def _data_schema(self, schema: Sequence[tuple[str, str]]) -> list[tuple[str, str]]:
        return [(column, dtype) for column, dtype in schema if column not in self._SERVICE_COLUMNS]

    def _staging_select_expression(self, staging: StagingTableArtifact, column: str, alias: str) -> str:
        return build_mssql_staging_select_expression(
            column=column,
            alias=alias,
            quote_identifier=self.connector.quote_identifier,
            column_types=getattr(staging, "column_types", {}) or {},
            hex_binary_columns=getattr(staging, "hex_binary_columns", ()) or (),
            target_column_types=getattr(staging, "target_column_types", {}) or {},
            bulk_text_codec=getattr(staging, "bulk_text_codec", None),
        )


__all__ = ["MSSQLStagingSqlMixin"]
