"""Plan eligibility follows the actual MSSQL artifact selection."""

import pytest

from dpone.runtime.native_transfer_capabilities import NativeTransferCapabilityPlanner
from dpone.runtime.native_transfer_transport import NativeTransferTransportPolicy


def plan(export="odbc_row_stream", **wire):
    return NativeTransferCapabilityPlanner().plan(
        source_type="mssql",
        sink_type="clickhouse",
        source_options={"mssql_export_mode": export, "native_transfer": {"wire": {"mode": "typed_binary", **wire}}},
        sink_options={"clickhouse_bulk": {"mode": "http"}},
        transport=NativeTransferTransportPolicy(mode="stream"),
    )


@pytest.mark.parametrize("export", ["row_stream", "odbc_row_stream"])
def test_typed_rows_stream(export):
    assert plan(export).transport == "stream"


@pytest.mark.parametrize("export", ["bcp", "bcp_queryout", "streaming", ""])
def test_file_source_does_not_claim_stream(export):
    with pytest.raises(RuntimeError, match="stream_unsupported"):
        plan(export)


@pytest.mark.parametrize(
    "wire",
    [
        {"binary_format": "native"},
        {"source_native_format": "bcp_native"},
        {"mode": "source_encoded"},
        {"source_native_format": "unknown"},
    ],
)
def test_unsupported_codec_does_not_claim_stream(wire):
    with pytest.raises(RuntimeError, match="stream_unsupported"):
        plan(**wire)


def test_limits_fail_in_plan():
    with pytest.raises(ValueError, match="block_bytes"):
        plan(block_bytes="-1MiB")
