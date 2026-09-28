"""Wire marker matching must not inherit case-insensitive database collation."""

from dpone.runtime.connectors.clickhouse_tsv_codec import ClickHouseTabSeparatedCodec


def test_mssql_marker_escaping_uses_exact_binary_utf8_comparison():
    codec = ClickHouseTabSeparatedCodec()
    expression = codec.mssql_select_expression("v.value", source_type="nvarchar(100) nullable")
    # MSSQL REPLACE follows the input expression's collation. A case-insensitive
    # expression would normalize a distinct user value resembling a wire marker.
    assert "COLLATE Latin1_General_100_BIN2_UTF8" in expression
    assert codec.decode_wire_value("__DPONE__TSV__empty", source_type="nvarchar(100)") == "__DPONE__TSV__empty"
