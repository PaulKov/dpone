"""Strict immutable inputs and bounded, lossless canonical observation evidence."""

from __future__ import annotations

import hashlib
import json
import math
import struct
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from typing import ClassVar
from uuid import UUID

from dpone.contracts.clickhouse_candidate import CandidateBatch
from dpone.contracts.clickhouse_observation import CandidateDesign, ObservationLimits, ScalarType, scalar_type
from dpone.contracts.clickhouse_scalar_wire import (
    encode_decimal,
    encode_fixed_string,
    encode_string,
    encode_temporal_integer,
    encode_uuid,
    pack_integer,
)
from dpone.contracts.clickhouse_typed_multiset import TypedMultiset, snapshot_multiset


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    ).hexdigest()


@dataclass(frozen=True)
class ProtectedObservationProfile:
    """Versioned design and evidence family; no native client or side effects.

    Bounds cover canonical encoded bytes. Caller/source objects, SDK buffers,
    Arrow, Parquet and total process RSS are explicitly outside this guarantee.
    """

    design: CandidateDesign
    limits: ObservationLimits
    profile_id: ClassVar[str] = "dpone.clickhouse.observation.v1"

    def __post_init__(self) -> None:
        if type(self.design) is not CandidateDesign or type(self.limits) is not ObservationLimits:
            raise ValueError("observation_requires_typed_design_and_limits")
        if len(self.design.columns) > self.limits.max_columns:
            raise ValueError("observation_column_limit_exceeded")

    @property
    def design_digest(self) -> str:
        return _digest(
            {
                "grammar": "dpone.clickhouse.design.v1",
                "design": asdict(self.design),
                "engine": "MergeTree",
                "storage_policy": "default",
                "settings": {},
            }
        )

    @property
    def profile_digest(self) -> str:
        return _digest(
            {
                "profile": self.profile_id,
                "design": self.design_digest,
                "limits": asdict(self.limits),
                "server": "24.8.14.39",
                "driver": "0.2.10",
                "encoding": "clickhouse-rowbinary-scalar-v1",
                "multiset": "rowbinary-sha256-sum-v1",
            }
        )

    def encode_row(self, values: tuple[object, ...]) -> bytes:
        """Reject loss, mutable/custom scalars and overflow before joining bytes."""
        return self._encode_row(values, self.limits.max_row_bytes)

    def _encode_row(self, values: tuple[object, ...], budget: int) -> bytes:
        if type(values) is not tuple or len(values) != len(self.design.columns):
            raise ValueError("observation_row_width_mismatch")
        pieces: list[bytes] = []
        for column, value in zip(self.design.columns, values, strict=True):
            scalar = scalar_type(column.type_name)
            prefix = b""
            if scalar.nullable:
                _admit_size(1, budget)
                prefix = b"\x01" if value is None else b"\x00"
                budget -= 1
            if value is None:
                if not scalar.nullable:
                    raise ValueError("observation_null_in_nonnullable_column")
                encoded = b""
            else:
                encoded = _encode_scalar(value, scalar, budget)
            pieces.append(prefix + encoded)
            budget -= len(encoded)
        return b"".join(pieces)

    def validate_batch(self, rows: tuple[tuple[object, ...], ...]) -> CandidateBatch:
        """Copy immutable row containers and compute ordered framing and multiset.

        Each payload frame is an unsigned 64-bit little-endian row-byte length
        followed by canonical bytes. Framing is hashed, not allocated per batch.
        DTOs can be forged; the admission gateway must validate again on use.
        """
        if type(rows) is not tuple or len(rows) > self.limits.max_batch_rows:
            raise ValueError("observation_batch_row_limit_or_mutability")
        multiset = TypedMultiset(len(self.design.columns))
        payload = hashlib.sha256()
        frozen: list[tuple[object, ...]] = []
        for row in rows:
            budget = min(self.limits.max_row_bytes, self.limits.max_batch_bytes - multiset.encoded_bytes)
            encoded = self._encode_row(row, budget)
            multiset.add(row, encoded)
            payload.update(len(encoded).to_bytes(8, "little"))
            payload.update(encoded)
            frozen.append(tuple(value for value in row))
        return CandidateBatch(tuple(frozen), snapshot_multiset(multiset), payload.hexdigest())


def _admit_size(size: int, budget: int) -> None:
    if size > budget:
        raise ValueError("observation_encoded_byte_limit_exceeded")


def _string_bytes(value: object, budget: int, fixed: int | None) -> bytes:
    if type(value) is bytes:
        size = len(value)
    elif type(value) is str:
        # Compute UTF-8 size without first allocating an over-limit payload.
        if len(value) > budget:
            raise ValueError("observation_encoded_byte_limit_exceeded")
        size = sum(1 if ord(c) < 128 else 2 if ord(c) < 2048 else 3 if ord(c) < 65536 else 4 for c in value)
    else:
        raise ValueError("observation_requires_immutable_string")
    width = fixed if fixed is not None else size + max(1, (size.bit_length() + 6) // 7)
    _admit_size(width, budget)
    if fixed is not None and size > fixed:
        raise ValueError("observation_fixed_string_overflow")
    return value if isinstance(value, bytes) else value.encode("utf-8")


def _encode_scalar(value: object, scalar: ScalarType, budget: int) -> bytes:
    root = scalar.root
    if root in {"String", "FixedString"}:
        width = scalar.argument if root == "FixedString" else None
        raw = _string_bytes(value, budget, width)
        return encode_string(raw) if width is None else encode_fixed_string(raw, width)
    sizes = {"Bool": 1, "Float32": 4, "Float64": 8, "UUID": 16, "Date": 2, "Date32": 4, "DateTime": 4, "DateTime64": 8}
    if root.startswith(("Int", "UInt")):
        _admit_size(int(root.removeprefix("U")[3:]) // 8, budget)
        if type(value) is not int:
            raise ValueError("observation_requires_integer")
        return pack_integer(value, root.lower())
    if root == "Decimal":
        declaration = scalar.name[9:-1] if scalar.nullable else scalar.name
        _admit_size(
            4 if declaration.startswith("Decimal(9,") else 8 if declaration.startswith("Decimal(18,") else 16, budget
        )
        if type(value) is not Decimal:
            raise ValueError("observation_requires_decimal")
        return encode_decimal(value, declaration)
    _admit_size(sizes[root], budget)
    if root == "Bool" and type(value) is bool:
        return bytes((int(value),))
    if root in {"Float32", "Float64"} and type(value) is float and math.isfinite(value):
        fmt = "<f" if root == "Float32" else "<d"
        try:
            encoded = struct.pack(fmt, value)
        except OverflowError:
            raise ValueError("observation_float_overflow") from None
        if struct.unpack(fmt, encoded)[0] != value:
            raise ValueError("observation_float_precision_loss")
        return encoded
    if root == "UUID" and type(value) is UUID:
        return encode_uuid(value)
    if root in {"Date", "Date32"} and type(value) is date:
        return encode_temporal_integer((value - date(1970, 1, 1)).days, root.lower())
    if root in {"DateTime", "DateTime64"} and type(value) is datetime:
        if type(value.tzinfo) is not timezone or value.utcoffset() != timedelta(0):
            raise ValueError("observation_requires_utc_timestamp")
        scale = scalar.argument
        if value.microsecond % (10 ** (6 - scale)):
            raise ValueError("observation_timestamp_precision_loss")
        delta = value - datetime(1970, 1, 1, tzinfo=UTC)
        ticks = (delta.days * 86400 + delta.seconds) * (10**scale) + value.microsecond // (10 ** (6 - scale))
        return encode_temporal_integer(ticks, root.lower(), scale)
    raise ValueError("observation_unsupported_scalar_value")
