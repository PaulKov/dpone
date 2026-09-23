"""Transport-specific admission retains the shared decoder's full text domain."""

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.contracts.mssql_native_chunks import EncodedNativeFile
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_mssql import MssqlBcpNativeDecoder, build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_import import admit_native_bcp_file


def sample(tmp_path, values, dtype="nvarchar(max)"):
    contract = build_mssql_bcp_native_contract(schema=[("v", dtype)], query="admission")
    encoder = MssqlNativeEncoder(contract, max_row_bytes=1024)
    payload = b"".join(encoder.encode_row([value]) for value in values)
    path = tmp_path / "input.native"
    path.write_bytes(payload)
    return EncodedNativeFile(path, 0, len(values), len(payload), sha256(payload).hexdigest(), "a" * 64), contract


def test_single_nul_rejected_even_in_late_row(tmp_path):
    file, contract = sample(tmp_path, ["safe", "\0"])
    with pytest.raises(ValueError, match="^mssql_native.single_nul_not_representable$"):
        admit_native_bcp_file(file, contract, max_row_bytes=1024)
    assert list(MssqlBcpNativeDecoder(contract).iter_rows(file.path)) == [{"v": "safe"}, {"v": "\0"}]


@pytest.mark.parametrize("value", ["", "\0\0", "a\0b", "\0a", "a\0", "\0 ", " \0", "Ж😀é  "])
def test_neighbors_remain_admitted(tmp_path, value):
    file, contract = sample(tmp_path, [value])
    admit_native_bcp_file(file, contract, max_row_bytes=1024)


@pytest.mark.parametrize("values", [[], [None], [None, "normal"]])
def test_nullable_and_empty_input(tmp_path, values):
    file, contract = sample(tmp_path, values, "nvarchar(max) nullable")
    admit_native_bcp_file(file, contract, max_row_bytes=1024)


@pytest.mark.parametrize(
    "field,value", [("rows", 0), ("rows", 2), ("rows", True), ("encoded_bytes", 1), ("file_sha256", "f" * 64)]
)
def test_false_file_authority_rejected(tmp_path, field, value):
    file, contract = sample(tmp_path, ["valid"])
    with pytest.raises(ValueError):
        admit_native_bcp_file(replace(file, **{field: value}), contract, max_row_bytes=1024)


def test_truncated_and_oversized_payloads_fail(tmp_path):
    file, contract = sample(tmp_path, ["abcd"])
    with pytest.raises(ValueError, match="row_bytes"):
        admit_native_bcp_file(file, contract, max_row_bytes=9)
    payload = file.path.read_bytes()[:-1]
    file.path.write_bytes(payload)
    file = replace(file, encoded_bytes=len(payload), file_sha256=sha256(payload).hexdigest())
    with pytest.raises(ValueError):
        admit_native_bcp_file(file, contract, max_row_bytes=1024)


def test_numeric_zero_bytes_are_not_text_nul(tmp_path):
    file, contract = sample(tmp_path, [0, 1, 256], "bigint")
    admit_native_bcp_file(file, contract, max_row_bytes=1024)
