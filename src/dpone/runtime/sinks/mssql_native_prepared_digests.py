"""Typed prepared integrity for shared and independent verification readbacks."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from dpone.runtime.mssql_native_chunks_files import native_multiset_digest
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_models import SourceNativeWireContract
from dpone.runtime.sinks.mssql_native_verification import verification_allowance


@dataclass(frozen=True, slots=True)
class PreparedDigests:
    """Existing versioned multiset digests and their shared verified row count."""

    business_digest: str
    full_digest: str
    rows: int


def digest_prepared_projection(
    read_rows: Callable[[], Iterator[Mapping[str, Any]]],
    *,
    contract: SourceNativeWireContract,
    business_contract: SourceNativeWireContract,
    max_row_bytes: int,
    expected_rows: int,
) -> str:
    """Verify one typed projection with one encoding per row and no retention.

    Validate allowance and encoder before requesting rows: an injected connector
    may perform I/O when creating its iterator. The caller supplies a fresh SQL
    readback for each invocation, preserving the independent prepublication
    boundary. Iterator and encoding failures propagate without retry.
    """
    allowance = verification_allowance(contract, business_contract)
    encoder = MssqlNativeEncoder(contract, max_row_bytes=max_row_bytes + allowance.overhead_bytes)
    count, total = 0, 0
    for row in read_rows():
        allowance.require_null_metadata(row)
        count += 1
        total = (total + int.from_bytes(sha256(encoder.encode_row(row)).digest(), "big")) % (1 << 256)
    if count != expected_rows:
        raise ValueError("mssql_native.prepared_count_mismatch")
    return native_multiset_digest(count, total)


def digest_prepared_rows(
    rows: Iterator[Mapping[str, Any]],
    *,
    business_contract: SourceNativeWireContract,
    full_contract: SourceNativeWireContract,
    max_row_bytes: int,
    expected_rows: int,
) -> PreparedDigests:
    """Consume full prepared mappings once, without retaining the row stream.

    Each projection uses its own native encoder: prepared NULL framing may
    differ from the source contract. Only the full encoder receives the finite
    generated-metadata allowance; the business byte limit remains unchanged.
    Extra/missing columns, non-NULL unbounded metadata, invalid native values
    and count mismatches fail closed with existing diagnostic classifications.

    The caller owns the SQL iterator, ownership checks and cleanup, compares
    the business digest with raw receipts, and persists the full digest in the
    existing recovery snapshot. This result cannot replace the independent
    prepublication readback across a mutation/recovery boundary.
    """

    business_encoder = MssqlNativeEncoder(business_contract, max_row_bytes=max_row_bytes)
    allowance = verification_allowance(full_contract, business_contract)
    full_encoder = MssqlNativeEncoder(full_contract, max_row_bytes=max_row_bytes + allowance.overhead_bytes)
    business_names = tuple(column.name for column in business_contract.columns)
    full_names = {column.name for column in full_contract.columns}
    count, business_sum, full_sum = 0, 0, 0
    for row in rows:
        if not isinstance(row, Mapping) or row.keys() != full_names:
            raise ValueError("mssql_native_columns_mismatch")
        allowance.require_null_metadata(row)
        business_row = {name: row[name] for name in business_names}
        business_sum = (
            business_sum + int.from_bytes(sha256(business_encoder.encode_row(business_row)).digest(), "big")
        ) % (1 << 256)
        full_sum = (full_sum + int.from_bytes(sha256(full_encoder.encode_row(row)).digest(), "big")) % (1 << 256)
        count += 1
    if count != expected_rows:
        raise ValueError("mssql_native.prepared_count_mismatch")
    return PreparedDigests(native_multiset_digest(count, business_sum), native_multiset_digest(count, full_sum), count)
