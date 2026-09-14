"""Mocked source integration and independent native bytes; no live route pass."""

from contextlib import nullcontext

import pytest

from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_mssql import MssqlBcpNativeDecoder, build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_source_values import native_source_rows
from dpone.runtime.sources.clickhouse_native_source import ClickHouseNativeSource
from tests.native_raw_scalar_oracle import VECTORS, scalar_multiset
from tests.test_clickhouse_native_source import Connector
from tests.test_mssql_native_policy import config


@pytest.mark.parametrize("mode", [None, "raw_single_query"])
@pytest.mark.parametrize("vector", VECTORS, ids=lambda vector: vector.name)
def test_source_adaptation_and_encoder_match_frozen_scalar_bytes(tmp_path, mode, vector):
    class ScalarConnector(Connector):
        def execute_iter(self, query, params, **kwargs):
            self.selects.append((query, params, kwargs))
            header_type = "Int64" if vector.source_type.startswith("DateTime") else vector.source_type
            return iter([[("value", header_type)], (vector.source_value,), (vector.source_value,)])

    value = config()
    value.source_schema, value.source_table = "synthetic", "events"
    if mode is not None:
        value.options["native_transfer"]["source_read"] = {"mode": mode}
    connector = ScalarConnector(dtype=vector.source_type)
    extracted = ClickHouseNativeSource(connector, schema_guard_factory=lambda _: nullcontext()).extract(value)
    wire = build_mssql_bcp_native_contract(schema=[("value", vector.target_type)], query="synthetic scalar")
    rows = list(native_source_rows(extracted.artifact.iter_native_rows(), wire, 1024))
    encoder = MssqlNativeEncoder(wire, max_row_bytes=1024)
    actual = b"".join(encoder.encode_row(row) for row in rows)
    assert actual == bytes.fromhex(vector.native_hex) * 2
    path = tmp_path / "synthetic.bcp"
    path.write_bytes(actual)
    decoded = [row["value"] for row in MssqlBcpNativeDecoder(wire).iter_rows(path)]
    assert scalar_multiset(decoded) == scalar_multiset([vector.expected_value] * 2)
    assert len(connector.selects) == 1
    assert extracted.artifact.rows_exported == 2
    with pytest.raises(ValueError, match="source_reextract_required"):
        list(extracted.artifact.iter_native_rows())


def test_oracle_detects_signed_zero_microsecond_text_and_multiplicity_loss():
    from datetime import datetime

    for before, after in [
        ([-0.0], [0.0]),
        ([2.0**-1074], [0.0]),
        (["a\x00 "], ["a"]),
        (["😀"], ["?"]),
        ([1, 1], [1]),
        ([None], [""]),
        ([datetime(2243, 1, 1, microsecond=1)], [datetime(2243, 1, 1)]),
    ]:
        assert scalar_multiset(before) != scalar_multiset(after)


@pytest.mark.parametrize(
    "value,dtype,error",
    [
        (b"\xff", "nvarchar(max)", "invalid_utf8_source_text"),
        (2**63, "bigint", "mssql_native"),
        (0.1, "real", "mssql_native"),
        (float("inf"), "float", "mssql_native"),
    ],
)
def test_source_to_encoder_rejects_invalid_or_lossy_values(value, dtype, error):
    wire = build_mssql_bcp_native_contract(schema=[("value", dtype)], query="synthetic rejection")
    with pytest.raises(ValueError, match=error):
        rows = native_source_rows(iter([(value,)]), wire, 1024)
        for row in rows:
            MssqlNativeEncoder(wire, max_row_bytes=1024).encode_row(row)


def test_text_bound_counts_native_prefix_nul_and_supplementary_unicode():
    wire = build_mssql_bcp_native_contract(schema=[("value", "nvarchar(max)")], query="synthetic bound")
    rows = list(native_source_rows(iter([("\x00😀".encode(),)]), wire, 14))
    assert len(MssqlNativeEncoder(wire, max_row_bytes=14).encode_row(rows[0])) == 14
    with pytest.raises(ValueError, match="row_bytes"):
        list(native_source_rows(iter([("\x00😀".encode(),)]), wire, 13))
