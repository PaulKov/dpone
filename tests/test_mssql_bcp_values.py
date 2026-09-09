from __future__ import annotations

from datetime import UTC, datetime

from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.support.mssql_bcp_values import format_mssql_bcp_scalar


def test_format_mssql_bcp_scalar_converts_lineage_iso_timestamp_to_datetime2_text() -> None:
    rendered = format_mssql_bcp_scalar(
        "2026-06-04T09:31:00+00:00",
        mssql_type="datetime2(6)",
    )

    assert rendered == "2026-06-04 09:31:00.000000"


def test_format_mssql_bcp_scalar_formats_aware_datetime_without_timezone_suffix() -> None:
    rendered = format_mssql_bcp_scalar(
        datetime(2026, 6, 4, 9, 31, tzinfo=UTC),
        mssql_type="datetime2(3)",
    )

    assert rendered == "2026-06-04 09:31:00.000"


def test_format_mssql_bcp_scalar_encodes_text_columns_only_with_codec() -> None:
    codec = BulkTextCodec()

    rendered = format_mssql_bcp_scalar(
        "line\nwith\ttab",
        mssql_type="nvarchar(max)",
        text_codec=codec,
    )

    assert codec.marker_prefix in rendered
    assert "\n" not in rendered
    assert "\t" not in rendered
