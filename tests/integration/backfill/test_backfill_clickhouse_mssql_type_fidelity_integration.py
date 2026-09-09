"""Physical value fidelity: ClickHouse -> MSSQL over the full mapped inventory.

Creates a ClickHouse table exercising every mapped type family with boundary
values (integer min/max, negative decimals, microsecond timestamps, non-ASCII
text, NULLs in Nullable columns), runs the real chunked-backfill transfer and
asserts the values that land in SQL Server. Also proves the negative policy:
an unsupported ClickHouse type fails as a deterministic configuration error
before any target mutation.
"""

from __future__ import annotations

import datetime
import os
import uuid
from decimal import Decimal

import pytest
from backfill_toolkit import BackfillCase, assert_campaign_committed, build_load_config, full_window, run_backfill
from backfill_toolkit import SeedSpec as _ToolkitSeedSpec
from endpoints import ClickHouseEndpoint, MSSQLEndpoint

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_backfill,
    pytest.mark.integration_clickhouse,
    pytest.mark.integration_mssql,
]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

_UUID = "0f8fad5b-d9cb-469f-a165-70867728950e"

_FIDELITY_DDL = """
CREATE TABLE {table} (
    id Int32,
    i8 Int8, i16 Int16, i64 Int64,
    u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64,
    f32 Float32, f64 Float64,
    dec Decimal(18, 4), dec9 Decimal32(2),
    s String, lc LowCardinality(String),
    d Date, d32 Date32,
    dt DateTime, dt64 DateTime64(6),
    b Bool, uid UUID,
    e8 Enum8('red' = 1, 'green' = 2),
    ip4 IPv4, ip6 IPv6,
    ns Nullable(String), ni Nullable(Int32)
) ENGINE = MergeTree ORDER BY id
"""

_FIDELITY_ROWS = f"""
INSERT INTO {{table}} VALUES
(1, -128, -32768, -9223372036854775808,
 0, 0, 0, 0,
 -1.5, -2.5,
 toDecimal64('-12345.6789', 4), toDecimal32('-1.25', 2),
 'plain ascii', 'lc-value',
 toDate('1970-01-01'), toDate32('1900-01-01'),
 toDateTime('2025-01-01 12:34:56'), toDateTime64('2025-01-01 12:34:56.123456', 6),
 true, toUUID('{_UUID}'),
 'red', toIPv4('198.51.100.10'), toIPv6('2001:db8::1'),
 NULL, NULL),
(2, 127, 32767, 9223372036854775807,
 255, 65535, 4294967295, 18446744073709551615,
 1.5, 2.5,
 toDecimal64('99999999999999.9999', 4), toDecimal32('3.5', 2),
 'таблица 名前 emoji 🚀', 'lc-value',
 toDate('2149-06-06'), toDate32('2299-12-31'),
 toDateTime('1970-01-02 00:00:00'), toDateTime64('1970-01-02 00:00:00.000001', 6),
 false, toUUID('{_UUID}'),
 'green', toIPv4('0.0.0.0'), toIPv6('::1'),
 'present', 42)
"""


@pytest.fixture
def fidelity_route(clickhouse_connector, clickhouse_settings, mssql_connector, mssql_schema):
    source = ClickHouseEndpoint(clickhouse_connector, database=clickhouse_settings.database)
    target = MSSQLEndpoint(mssql_connector)
    table = f"bf_ch_ms_fidelity_{uuid.uuid4().hex[:8]}"
    qualified = source.qualified(clickhouse_settings.database, table)
    clickhouse_connector.execute_query(_FIDELITY_DDL.format(table=qualified))
    clickhouse_connector.execute_query(_FIDELITY_ROWS.format(table=qualified))
    yield source, target, clickhouse_settings.database, mssql_schema, table
    source.drop(clickhouse_settings.database, table)
    target.drop(mssql_schema, table)


def _transfer(source, target, source_schema, target_schema, table, tmp_path, *, rows: int = 2):
    case = BackfillCase(
        inner_mode="incremental_merge",
        window=full_window(_ToolkitSeedSpec(rows=rows)),
        unique_key="id",
    )
    load_config = build_load_config(
        case=case,
        source_schema=source_schema,
        source_table=table,
        target_schema=target_schema,
        target_table=table,
        source_type="clickhouse",
        sink_type="mssql",
        state_dir=tmp_path / "ledger",
    )
    return run_backfill(source.create_source(), target.create_sink(), load_config)


def test_clickhouse_to_mssql_value_fidelity_over_full_mapped_inventory(fidelity_route, tmp_path) -> None:
    source, target, source_schema, target_schema, table = fidelity_route

    result = _transfer(source, target, source_schema, target_schema, table, tmp_path)

    assert_campaign_committed(result, chunks=1)
    columns = (
        "id, i8, i16, i64, u8, u16, u32, u64, f32, f64, dec, dec9, s, lc, d, d32, dt, dt64, "
        "b, uid, e8, ip4, ip6, ns, ni"
    )
    rows = target.connector.get_records(f"SELECT {columns} FROM [{target_schema}].[{table}] ORDER BY [id]")
    assert len(rows) == 2
    low, high = rows[0], rows[1]

    # Integer boundaries are exact.
    assert low[0:4] == (1, -128, -32768, -9223372036854775808)
    assert high[0:4] == (2, 127, 32767, 9223372036854775807)
    assert low[4:8] == (0, 0, 0, Decimal("0"))
    assert high[4:7] == (255, 65535, 4294967295)
    assert high[7] == Decimal("18446744073709551615"), "full UInt64 range must survive via decimal(20,0)"

    # Floats and decimals.
    assert (low[8], low[9]) == (-1.5, -2.5)
    assert (high[8], high[9]) == (1.5, 2.5)
    assert low[10] == Decimal("-12345.6789")
    assert high[10] == Decimal("99999999999999.9999")
    assert low[11] == Decimal("-1.25")
    assert high[11] == Decimal("3.50")

    # Strings including non-ASCII, LowCardinality unwrapping.
    assert low[12] == "plain ascii"
    assert high[12] == "таблица 名前 emoji 🚀"
    assert low[13] == high[13] == "lc-value"

    # Temporal fidelity: Date/Date32 exact, DateTime seconds, DateTime64 microseconds.
    assert low[14] == datetime.date(1970, 1, 1)
    assert high[14] == datetime.date(2149, 6, 6)
    assert low[15] == datetime.date(1900, 1, 1)
    assert high[15] == datetime.date(2299, 12, 31)
    assert low[16] == datetime.datetime(2025, 1, 1, 12, 34, 56)
    assert low[17] == datetime.datetime(2025, 1, 1, 12, 34, 56, 123456)
    assert high[17] == datetime.datetime(1970, 1, 2, 0, 0, 0, 1)

    # Bool -> bit, UUID -> uniqueidentifier, Enum -> name, IP -> canonical text.
    assert (low[18], high[18]) == (True, False)
    assert str(low[19]).lower() == _UUID
    assert (low[20], high[20]) == ("red", "green")
    assert (low[21], high[21]) == ("198.51.100.10", "0.0.0.0")
    assert (low[22], high[22]) == ("2001:db8::1", "::1")

    # NULL vs value in Nullable columns.
    assert (low[23], low[24]) == (None, None)
    assert (high[23], high[24]) == ("present", 42)


def test_unsupported_clickhouse_type_fails_as_configuration_error_before_target_mutation(
    clickhouse_connector, clickhouse_settings, mssql_connector, mssql_schema, tmp_path
) -> None:
    source = ClickHouseEndpoint(clickhouse_connector, database=clickhouse_settings.database)
    target = MSSQLEndpoint(mssql_connector)
    table = f"bf_ch_ms_unsupported_{uuid.uuid4().hex[:8]}"
    qualified = source.qualified(clickhouse_settings.database, table)
    clickhouse_connector.execute_query(
        f"CREATE TABLE {qualified} (id Int32, tags Array(String)) ENGINE = MergeTree ORDER BY id"
    )
    clickhouse_connector.execute_query(f"INSERT INTO {qualified} VALUES (1, ['a', 'b'])")
    try:
        result = _transfer(source, target, clickhouse_settings.database, mssql_schema, table, tmp_path, rows=1)

        assert result["status"] == "error"
        message = result["errors"][0]
        assert "Array(String)" in message
        assert "not supported by the MSSQL sink" in message
        assert "toJSONString" in message, "the error must carry the source-side workaround"
        target_exists = mssql_connector.get_records(
            "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "WHERE s.name = ? AND t.name = ?",
            params=(mssql_schema, table),
        )
        assert int(target_exists[0][0]) == 0, "no target table may be created for an unsupported schema"
    finally:
        source.drop(clickhouse_settings.database, table)
