"""Strict self-describing canonical codec for MSSQL R1 V3 authority bytes."""

from dpone.contracts.mssql_r1_v3_errors import (
    MssqlR1V3ContractError,
    canonical_bytes,
    canonical_utf8_fields,
    decode_canonical_bytes,
    decode_canonical_utf8_fields,
    encode_artifact_rows,
    expect_bool,
    expect_bytes,
    expect_enum,
    expect_int,
    expect_text,
    expect_tuple,
    expect_uuid,
    validate_detached_command_binding,
    validate_signed_command_bytes,
)

__all__ = [
    "MssqlR1V3ContractError",
    "canonical_bytes",
    "canonical_utf8_fields",
    "decode_canonical_bytes",
    "decode_canonical_utf8_fields",
    "encode_artifact_rows",
    "expect_bool",
    "expect_bytes",
    "expect_enum",
    "expect_int",
    "expect_text",
    "expect_tuple",
    "expect_uuid",
    "validate_detached_command_binding",
    "validate_signed_command_bytes",
]
