"""Constant-space typed multiset aggregation, preserving duplicate multiplicity.

SHA-256 sums are order independent probabilistic evidence, not exact proof.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field


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
