from __future__ import annotations

import io
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from dpone.runtime.semantic_refresh_parquet_codec import (
    DponeParquetV1Codec,
    SemanticRefreshParquetField,
    business_projection_columns,
)

pq = pytest.importorskip("pyarrow.parquet")


def _fields() -> tuple[SemanticRefreshParquetField, ...]:
    return (
        SemanticRefreshParquetField("event_id", "uniqueidentifier", False),
        SemanticRefreshParquetField("occurred_at", "datetime2(6)", False),
        SemanticRefreshParquetField("business_date", "date", False),
        SemanticRefreshParquetField("amount", "decimal(18,2)", False),
        SemanticRefreshParquetField("label", "nvarchar(100)", True),
    )


def _rows() -> tuple[tuple[object, ...], ...]:
    return (
        (
            uuid.UUID("00000000-0000-0000-0000-000000000001"),
            datetime(2026, 8, 8, 0, 0, 0, 1, tzinfo=UTC),
            date(2026, 8, 8),
            Decimal("10.25"),
            "first",
        ),
        (
            uuid.UUID("00000000-0000-0000-0000-000000000002"),
            datetime(2026, 8, 8, 0, 0, 0, 2),
            date(2026, 8, 8),
            Decimal("20.50"),
            None,
        ),
        (
            uuid.UUID("00000000-0000-0000-0000-000000000003"),
            datetime(2026, 8, 8, 0, 0, 0, 3, tzinfo=UTC),
            date(2026, 8, 8),
            Decimal("30.75"),
            "third",
        ),
    )


def test_codec_is_byte_deterministic_and_transport_ordinal_is_not_business_schema() -> None:
    codec = DponeParquetV1Codec()

    first = codec.encode(fields=_fields(), rows=_rows(), maximum_rows_per_chunk=2)
    second = codec.encode(fields=_fields(), rows=_rows(), maximum_rows_per_chunk=2)

    assert tuple(chunk.content for chunk in first) == tuple(chunk.content for chunk in second)
    assert tuple(chunk.row_count for chunk in first) == (2, 1)
    assert business_projection_columns(_fields()) == (
        "event_id",
        "occurred_at",
        "business_date",
        "amount",
        "label",
    )
    tables = [pq.read_table(io.BytesIO(chunk.content)) for chunk in first]
    assert tables[0].column("__dpone_seal_ordinal").to_pylist() == [0, 1]
    assert tables[1].column("__dpone_seal_ordinal").to_pylist() == [2]
    assert tables[0].schema.field("event_id").type.byte_width == 16
    assert str(tables[0].schema.field("occurred_at").type) == "timestamp[us, tz=UTC]"
    assert tables[0].num_rows + tables[1].num_rows == 3
    assert codec.serializer_sha256.startswith("sha256:")
    assert codec.schema_mapping_sha256(_fields()) == codec.schema_mapping_sha256(_fields())


def test_empty_scope_uses_a_manifest_with_no_chunks() -> None:
    assert DponeParquetV1Codec().encode(fields=_fields(), rows=(), maximum_rows_per_chunk=10) == ()


@pytest.mark.parametrize(
    "field",
    [
        SemanticRefreshParquetField("bad", "float", False),
        SemanticRefreshParquetField("bad", "decimal(39,2)", False),
    ],
)
def test_codec_rejects_unapproved_payload_mappings(field: SemanticRefreshParquetField) -> None:
    with pytest.raises(ValueError, match="unsupported|outside"):
        DponeParquetV1Codec().encode(fields=(field,), rows=((1,),), maximum_rows_per_chunk=1)


def test_codec_rejects_non_utc_aware_datetime() -> None:
    field = SemanticRefreshParquetField("occurred_at", "datetime2(6)", False)
    value = datetime.fromisoformat("2026-08-08T03:00:00+03:00")

    with pytest.raises(ValueError, match="must already represent UTC"):
        DponeParquetV1Codec().encode(fields=(field,), rows=((value,),), maximum_rows_per_chunk=1)
