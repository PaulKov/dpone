"""Pinned Parquet codec identity projected through the seal-policy port."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.runtime.semantic_refresh_parquet_codec import (
    DponeParquetV1Codec,
    SemanticRefreshParquetField,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_mssql_authority import MssqlProtectedWritableColumn


class DponeParquetV1SealCodecAuthority:
    """Derive the serializer and ordered schema digests from executable code."""

    def __init__(self, codec: DponeParquetV1Codec | None = None) -> None:
        self._codec = codec or DponeParquetV1Codec()

    @property
    def serializer_sha256(self) -> str:
        return self._codec.serializer_sha256

    def parquet_schema_mapping_sha256(
        self,
        columns: tuple[MssqlProtectedWritableColumn, ...],
    ) -> str:
        fields = tuple(
            SemanticRefreshParquetField(column.name, column.source_type.lower(), column.nullable) for column in columns
        )
        return self._codec.schema_mapping_sha256(fields)


__all__ = ["DponeParquetV1SealCodecAuthority"]
