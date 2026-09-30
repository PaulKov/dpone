"""Constant-space typed multiset aggregation, preserving legacy digest bytes.

SHA-256 sums are order independent probabilistic evidence, not exact proof.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field

from dpone.contracts.clickhouse_observation import MultisetState


@dataclass
class TypedMultiset:
    """Constant-size aggregate over canonical typed RowBinary row bytes."""

    columns: int
    count: int = 0
    total: int = 0
    encoded_bytes: int = 0
    nulls: list[int] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.nulls = [0] * self.columns

    def add(self, values: Sequence[object], encoded: bytes) -> None:
        self.count += 1
        self.encoded_bytes += len(encoded)
        self.total = (self.total + int.from_bytes(hashlib.sha256(encoded).digest(), "big")) % (1 << 256)
        for index, value in enumerate(values):
            self.nulls[index] += value is None

    def digest(self) -> str:
        return hashlib.sha256(
            json.dumps(["rowbinary-sha256-sum-v1", self.count, self.total, self.nulls]).encode()
        ).hexdigest()


def snapshot_multiset(value: TypedMultiset) -> MultisetState:
    """Validate and copy state without recomputing or reinterpreting row hashes."""
    if type(value.columns) is not int or value.columns < 0 or len(value.nulls) != value.columns:
        raise ValueError("invalid_multiset_width")
    return MultisetState(value.count, value.total, value.encoded_bytes, tuple(value.nulls))


def restore_multiset(state: MultisetState) -> TypedMultiset:
    """Restore the exact validated aggregate; no recovered value grants authority."""
    state.__post_init__()
    value = TypedMultiset(len(state.nulls))
    value.count, value.total, value.encoded_bytes = state.count, state.total, state.encoded_bytes
    value.nulls = list(state.nulls)
    return value
