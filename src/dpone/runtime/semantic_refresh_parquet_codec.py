"""Deterministic ``dpone_parquet_v1`` encoder for sealed refresh artifacts."""

from __future__ import annotations

import hashlib
import importlib
import json
import re
import uuid
from collections.abc import Iterable, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from dpone.ports.semantic_refresh_artifact_codec import SemanticRefreshParquetField
from dpone.ports.semantic_refresh_artifact_seal import ArtifactChunk

TRANSPORT_ORDINAL_COLUMN = "__dpone_seal_ordinal"
PARQUET_FORMAT_VERSION = "2.6"
PARQUET_DATA_PAGE_VERSION = "2.0"
_DECIMAL = re.compile(r"^(?:decimal|numeric)\((\d+),(\d+)\)$")
_TEXT = re.compile(r"^(?:n?varchar|n?char)\((?:max|\d+)\)$")
_UTC = timezone.utc  # noqa: UP017 - package type-checks against Python 3.10


class DponeParquetV1Codec:
    """Encode fixed-size, ordinal chunks with one frozen PyArrow configuration.

    The caller supplies rows in the source-authoritative order. The codec adds a
    contiguous transport ordinal, writes one row group per chunk, and never
    exposes the transport column as part of the business schema.
    """

    codec_id = "dpone_parquet_v1"

    @property
    def serializer_sha256(self) -> str:
        """Return the digest of every frozen byte-affecting writer setting."""

        return _digest(
            {
                "allow_truncated_timestamps": False,
                "codec_id": self.codec_id,
                "coerce_timestamps": "us",
                "compression": None,
                "data_page_version": PARQUET_DATA_PAGE_VERSION,
                "dictionary": False,
                "metadata": "arrow_schema_only",
                "parquet_version": PARQUET_FORMAT_VERSION,
                "row_groups_per_chunk": 1,
                "statistics": False,
                "transport_ordinal": TRANSPORT_ORDINAL_COLUMN,
                "write_page_checksum": True,
            }
        )

    def schema_mapping_sha256(self, fields: tuple[SemanticRefreshParquetField, ...]) -> str:
        """Bind the ordered SQL Server-to-Parquet field mapping."""

        _validate_fields(fields)
        return _digest(
            {
                "codec_id": self.codec_id,
                "fields": [
                    {
                        "mssql_type": field.mssql_type,
                        "name": field.name,
                        "nullable": field.nullable,
                    }
                    for field in fields
                ],
                "transport_ordinal_type": "int64",
            }
        )

    def encode(
        self,
        *,
        fields: tuple[SemanticRefreshParquetField, ...],
        rows: Iterable[Sequence[object]],
        maximum_rows_per_chunk: int,
    ) -> tuple[ArtifactChunk, ...]:
        """Return deterministic Parquet chunks for one materialized ordered scope."""

        _validate_fields(fields)
        if isinstance(maximum_rows_per_chunk, bool) or maximum_rows_per_chunk <= 0:
            raise ValueError("maximum_rows_per_chunk must be a positive integer")
        materialized = tuple(tuple(row) for row in rows)
        if any(len(row) != len(fields) for row in materialized):
            raise ValueError("each semantic-refresh row must match the ordered field closure")
        chunks = []
        for ordinal, offset in enumerate(range(0, len(materialized), maximum_rows_per_chunk), start=1):
            batch = materialized[offset : offset + maximum_rows_per_chunk]
            content = _encode_chunk(fields=fields, rows=batch, ordinal_offset=offset)
            chunks.append(ArtifactChunk(ordinal=ordinal, content=content, row_count=len(batch)))
        return tuple(chunks)


def business_projection_columns(fields: tuple[SemanticRefreshParquetField, ...]) -> tuple[str, ...]:
    """Return the only columns authorized for artifact-to-staging projection."""

    _validate_fields(fields)
    return tuple(field.name for field in fields)


def _encode_chunk(
    *,
    fields: tuple[SemanticRefreshParquetField, ...],
    rows: tuple[tuple[object, ...], ...],
    ordinal_offset: int,
) -> bytes:
    pa = _module("pyarrow")
    pq = _module("pyarrow.parquet")
    schema = pa.schema(
        [pa.field(field.name, _arrow_type(pa, field.mssql_type), nullable=field.nullable) for field in fields]
        + [pa.field(TRANSPORT_ORDINAL_COLUMN, pa.int64(), nullable=False)]
    )
    columns = [
        pa.array(
            [_coerce_value(row[index], field.mssql_type) for row in rows],
            type=schema.field(field.name).type,
            from_pandas=False,
        )
        for index, field in enumerate(fields)
    ]
    columns.append(pa.array(range(ordinal_offset, ordinal_offset + len(rows)), type=pa.int64()))
    table = pa.Table.from_arrays(columns, schema=schema)
    output = pa.BufferOutputStream()
    pq.write_table(
        table,
        output,
        version=PARQUET_FORMAT_VERSION,
        data_page_version=PARQUET_DATA_PAGE_VERSION,
        compression=None,
        use_dictionary=False,
        write_statistics=False,
        write_page_checksum=True,
        row_group_size=max(len(rows), 1),
        coerce_timestamps="us",
        allow_truncated_timestamps=False,
        use_compliant_nested_type=True,
        store_schema=True,
    )
    return output.getvalue().to_pybytes()


def _arrow_type(pa: Any, mssql_type: str) -> Any:
    scalar = {
        "bit": pa.bool_(),
        "tinyint": pa.uint8(),
        "smallint": pa.int16(),
        "int": pa.int32(),
        "bigint": pa.int64(),
        "uniqueidentifier": pa.binary(16),
        "date": pa.date32(),
        "datetime2(6)": pa.timestamp("us", tz="UTC"),
    }.get(mssql_type)
    if scalar is not None:
        return scalar
    decimal = _DECIMAL.fullmatch(mssql_type)
    if decimal is not None:
        precision, scale = (int(value) for value in decimal.groups())
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError("decimal payload type is outside the V2 precision/scale domain")
        return pa.decimal128(precision, scale)
    if _TEXT.fullmatch(mssql_type) is not None:
        return pa.string()
    raise ValueError(f"unsupported dpone_parquet_v1 SQL Server type: {mssql_type}")


def _coerce_value(value: object, mssql_type: str) -> object:
    if value is None:
        return None
    if mssql_type == "uniqueidentifier":
        return uuid.UUID(str(value)).bytes
    if mssql_type == "datetime2(6)":
        if not isinstance(value, datetime):
            raise ValueError("datetime2(6) payload values must be datetime instances")
        if value.tzinfo is None:
            return value.replace(tzinfo=_UTC)
        if value.utcoffset() != _UTC.utcoffset(value):
            raise ValueError("datetime2(6) payload values must already represent UTC")
        return value.astimezone(_UTC)
    if mssql_type == "date" and (not isinstance(value, date) or isinstance(value, datetime)):
        raise ValueError("date payload values must be date instances")
    if _DECIMAL.fullmatch(mssql_type) is not None and not isinstance(value, Decimal):
        raise ValueError("decimal payload values must be Decimal instances")
    return value


def _validate_fields(fields: tuple[SemanticRefreshParquetField, ...]) -> None:
    if not fields or any(not isinstance(field, SemanticRefreshParquetField) for field in fields):
        raise ValueError("semantic-refresh fields must be a non-empty typed tuple")
    names = tuple(field.name for field in fields)
    if len(names) != len(set(names)):
        raise ValueError("semantic-refresh fields must be unique and ordered")
    for field in fields:
        _arrow_type(_module("pyarrow"), field.mssql_type)


def _module(name: str) -> Any:
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency guard
        raise ModuleNotFoundError("dpone_parquet_v1 requires the 'dpone[columnar]' extra") from exc


def _digest(value: object) -> str:
    raw = json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


__all__ = [
    "DponeParquetV1Codec",
    "PARQUET_DATA_PAGE_VERSION",
    "PARQUET_FORMAT_VERSION",
    "SemanticRefreshParquetField",
    "TRANSPORT_ORDINAL_COLUMN",
    "business_projection_columns",
]
