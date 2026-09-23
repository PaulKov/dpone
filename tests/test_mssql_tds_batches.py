"""Input completion is independent of the bulk SDK's success reporting."""

from dataclasses import replace
from datetime import datetime

import pytest

from dpone.adapters.mssql_tds_input import NativeTdsInput
from dpone.runtime.mssql_native_chunks_files import encode_native_frame
from dpone.runtime.mssql_tds_decoder import MssqlTdsRowDecoder
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract


def make_input(file, contract, **options):
    decoder = MssqlTdsRowDecoder(contract, input_mode=options["input_mode"])
    return NativeTdsInput(file, decoder, **options)


def fixture(tmp_path, rows=5, dtype="bigint"):
    contract = build_mssql_bcp_native_contract(schema=[("v", dtype)], query="synthetic")
    values = tuple({"v": i} for i in range(rows))
    file = encode_native_frame(contract, values, tmp_path / "chunk.native", 0, 1024, 65536)
    return contract, file


def test_complete_rows_receipt_and_one_shot(tmp_path):
    contract, file = fixture(tmp_path)
    source = make_input(file, contract, input_mode="rows", batch_rows=2, max_row_bytes=1024)
    with pytest.raises(ValueError, match="incomplete"):
        source.require_complete()
    assert list(source.iter_rows()) == [(i,) for i in range(5)]
    receipt = source.require_complete()
    assert (receipt.rows, receipt.encoded_bytes, receipt.file_sha256) == (
        file.rows,
        file.encoded_bytes,
        file.file_sha256,
    )
    with pytest.raises(ValueError, match="already_consumed"):
        list(source.iter_rows())


def test_swallowed_iteration_failure_cannot_claim_success(tmp_path):
    contract, file = fixture(tmp_path)
    source = make_input(replace(file, rows=4), contract, input_mode="rows", batch_rows=2, max_row_bytes=1024)
    try:
        list(source.iter_rows())
    except ValueError:
        pass  # Model the SDK swallowing an input-generator exception.
    with pytest.raises(ValueError, match="incomplete"):
        source.require_complete()


def test_abandoned_iterator_never_claims_eof(tmp_path):
    contract, file = fixture(tmp_path)
    source = make_input(file, contract, input_mode="rows", batch_rows=2, max_row_bytes=1024)
    iterator = source.iter_rows()
    next(iterator)
    iterator.close()
    with pytest.raises(ValueError, match="incomplete"):
        source.require_complete()


@pytest.mark.parametrize("rows", [0, 1, 5])
def test_arrow_explicit_schema_and_bounded_batches(tmp_path, rows):
    pa = pytest.importorskip("pyarrow")
    contract, file = fixture(tmp_path, rows)
    source = make_input(file, contract, input_mode="arrow", batch_rows=2, max_row_bytes=1024)
    batches = list(source.iter_arrow_batches())
    assert [b.num_rows for b in batches] == ([2, 2, 1] if rows == 5 else [1] if rows else [])
    assert all(b.schema == pa.schema([pa.field("v", pa.int64(), nullable=False)]) for b in batches)
    assert source.require_complete().rows == rows


def test_arrow_temporal_extrema_do_not_use_float_epoch(tmp_path):
    pa = pytest.importorskip("pyarrow")
    contract = build_mssql_bcp_native_contract(schema=[("v", "datetime2(6)")], query="synthetic")
    file = encode_native_frame(
        contract, ({"v": datetime.min}, {"v": datetime.max}), tmp_path / "chunk.native", 0, 1024, 65536
    )
    source = make_input(file, contract, input_mode="arrow", batch_rows=2, max_row_bytes=1024)
    batch = next(iter(source.iter_arrow_batches()))
    assert batch.column(0).cast(pa.int64()).to_pylist() == [-62135596800000000, 253402300799999999]


@pytest.mark.parametrize(
    "field,value",
    [("batch_rows", True), ("batch_rows", 0), ("batch_rows", 65537), ("max_row_bytes", False), ("max_row_bytes", 0)],
)
def test_invalid_bounds_rejected(tmp_path, field, value):
    contract, file = fixture(tmp_path)
    options = {"input_mode": "rows", "batch_rows": 2, "max_row_bytes": 1024, field: value}
    with pytest.raises(ValueError):
        make_input(file, contract, **options)


def test_sealed_input_tamper_is_rejected(tmp_path):
    contract, file = fixture(tmp_path)
    file.path.chmod(0o600)
    file.path.write_bytes(b"x" * file.encoded_bytes)
    source = make_input(file, contract, input_mode="rows", batch_rows=2, max_row_bytes=1024)
    with pytest.raises(ValueError):
        list(source.iter_rows())
    with pytest.raises(ValueError, match="incomplete"):
        source.require_complete()


def test_last_partial_arrow_batch_is_not_consumer_eof(tmp_path):
    pytest.importorskip("pyarrow")
    contract, file = fixture(tmp_path, 1)
    source = make_input(file, contract, input_mode="arrow", batch_rows=2, max_row_bytes=1024)
    iterator = source.iter_arrow_batches()
    assert next(iterator).num_rows == 1
    with pytest.raises(ValueError, match="incomplete"):
        source.require_complete()
    with pytest.raises(StopIteration):
        next(iterator)
    assert source.require_complete().rows == 1


@pytest.mark.parametrize("mode", ["rows", "arrow"])
def test_wide_duplicate_nullable_rows(tmp_path, mode):
    pytest.importorskip("pyarrow")
    contract = build_mssql_bcp_native_contract(
        schema=[(f"c{i}", "nvarchar(max) nullable") for i in range(100)], query="synthetic"
    )
    row = {f"c{i}": None if i % 2 else "😀\0NULL" for i in range(100)}
    file = encode_native_frame(contract, (row, row, row), tmp_path / "chunk.native", 0, 65536, 65536)
    source = make_input(file, contract, input_mode=mode, batch_rows=2, max_row_bytes=65536)
    if mode == "rows":
        assert list(source.iter_rows()) == [tuple(row.values())] * 3
    else:
        assert [r for b in source.iter_arrow_batches() for r in b.to_pylist()] == [row] * 3
    assert source.require_complete().rows == 3


def test_oversized_native_row_fails_independent_receipt(tmp_path):
    contract = build_mssql_bcp_native_contract(schema=[("v", "nvarchar(max)")], query="synthetic")
    file = encode_native_frame(contract, ({"v": "x" * 10000},), tmp_path / "chunk.native", 0, 65536, 65536)
    source = make_input(file, contract, input_mode="rows", batch_rows=1, max_row_bytes=1024)
    with pytest.raises(ValueError):
        list(source.iter_rows())
    with pytest.raises(ValueError, match="incomplete"):
        source.require_complete()


def test_path_replacement_during_consumption_fails(tmp_path):
    contract, file = fixture(tmp_path)
    source = make_input(file, contract, input_mode="rows", batch_rows=2, max_row_bytes=1024)
    iterator = source.iter_rows()
    next(iterator)
    original = file.path.read_bytes()
    file.path.unlink()
    file.path.write_bytes(original)
    file.path.chmod(0o400)
    with pytest.raises(ValueError, match="file_changed"):
        list(iterator)
    with pytest.raises(ValueError, match="incomplete"):
        source.require_complete()


def test_substituted_fifo_is_rejected_without_waiting_for_writer(tmp_path):
    """Bound the regression in a child so a blocking open cannot hang pytest."""
    import os
    import subprocess
    import sys

    contract, file = fixture(tmp_path)
    file.path.unlink()
    os.mkfifo(file.path, 0o600)
    script = """
import sys
from pathlib import Path
from dpone.contracts.mssql_native_chunks import EncodedNativeFile
from dpone.adapters.mssql_tds_input import NativeTdsInput
from dpone.runtime.mssql_tds_decoder import MssqlTdsRowDecoder
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
contract = build_mssql_bcp_native_contract(schema=[("v", "bigint")], query="synthetic")
file = EncodedNativeFile(Path(sys.argv[1]), 0, 5, 40, "unused", "unused")
source = NativeTdsInput(file, MssqlTdsRowDecoder(contract, input_mode="rows"), input_mode="rows", batch_rows=2, max_row_bytes=1024)
try:
    list(source.iter_rows())
except ValueError as error:
    assert str(error) == "mssql_native.tds_input_file_changed"
else:
    raise AssertionError("FIFO accepted")
try:
    source.require_complete()
except ValueError:
    pass
else:
    raise AssertionError("FIFO received completion receipt")
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(file.path)], capture_output=True, text=True, timeout=10, check=False
    )
    assert completed.returncode == 0, completed.stderr


@pytest.mark.parametrize("input_mode", ["arrow", "unknown", None, True])
def test_injected_decoder_mode_mismatch_rejected_before_file_open(tmp_path, input_mode, monkeypatch):
    from pathlib import Path

    contract, file = fixture(tmp_path)
    decoder = MssqlTdsRowDecoder(contract, input_mode="rows")

    def forbidden(*args, **kwargs):
        pytest.fail("mode mismatch must fail before file access")

    monkeypatch.setattr(Path, "open", forbidden)
    with pytest.raises(ValueError, match="input_mode_mismatch"):
        NativeTdsInput(file, decoder, input_mode=input_mode, batch_rows=2, max_row_bytes=1024)
