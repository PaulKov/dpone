"""Deterministic typed workloads; values never appear in diagnostic artifacts."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from .artifacts import canonical_json, digest

PROFILES = ("narrow", "wide", "unicode", "decimal", "null", "binary", "skewed")
WINDOW_START = datetime(2026, 1, 1)
WINDOW_END = datetime(2026, 1, 2)


def typed_value(value: object) -> object:
    """Preserve type and representation, including Decimal scale and Unicode form."""
    if value is None:
        return ["null"]
    if type(value) in (bool, int, str):
        return [type(value).__name__, value]
    if isinstance(value, bytes):
        return ["bytes", value.hex()]
    if isinstance(value, Decimal) and value.is_finite():
        return ["decimal", str(value)]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat(timespec="microseconds")]
    raise ValueError("unsupported_fixture_value")


def exact_multiset(rows: Iterable[Mapping[str, object]]) -> Counter[bytes]:
    """Exact equality over typed canonical rows, including duplicate multiplicity.

    Called outside timed delivery. Memory scales with unique rows; resource claims
    describe only the separately sampled delivery interval.
    """
    return Counter(canonical_json({k: typed_value(v) for k, v in row.items()}) for row in rows)


def capture_rows(rows: Iterable[Mapping[str, object]]) -> tuple[dict[str, object], ...]:
    """Freeze a snapshot before execution; driver iterators may reuse row mappings.

    Accepted fixture values are immutable scalars. Reject mutable/unsupported
    values here so later mutation cannot rewrite the before-image evidence.
    """
    captured = []
    for row in rows:
        for value in row.values():
            typed_value(value)
        captured.append(dict(row))
    return tuple(captured)


def multiset_summary(counts: Counter[bytes]) -> dict[str, object]:
    """Non-value diagnostic summary; equality is checked on the full counters."""
    import hashlib

    authority = [[hashlib.sha256(row).hexdigest(), count] for row, count in sorted(counts.items())]
    return {"rows": sum(counts.values()), "distinct": len(counts), "sha256": digest(authority)}


@dataclass(frozen=True)
class Dataset:
    """Repeatable rows and ordered source/target schema for an invocation-owned table.

    UTC-naive datetime2 values represent the explicit UTC half-open day window.
    Adjacent pairs intentionally duplicate the entire business row.
    """

    profile: str
    rows: int = 10000
    seed: int = 7

    def __post_init__(self) -> None:
        if self.profile not in PROFILES:
            raise ValueError("invalid_profile")
        if type(self.rows) is not int or not 0 <= self.rows <= 1000000:
            raise ValueError("invalid_rows")
        if type(self.seed) is not int or not 0 <= self.seed <= 2147483647:
            raise ValueError("invalid_seed")

    def schema(self) -> list[dict[str, str]]:
        """Return explicit wire-relevant types, never inferred from a first row."""
        types = {
            "decimal": ("Nullable(Decimal(18,4))", "decimal(18,4)"),
            "null": ("Nullable(String)", "nvarchar(200)"),
            "binary": ("Nullable(String)", "varbinary(200)"),
        }
        source, target = types.get(self.profile, ("String", "nvarchar(4000)"))
        columns = [
            {"name": "id", "source": "Int64", "target": "bigint"},
            {"name": "event_at", "source": "DateTime64(6, 'UTC')", "target": "datetime2(6)"},
        ]
        columns.extend(
            {"name": f"value_{i}", "source": source, "target": target}
            for i in range(198 if self.profile == "wide" else 1)
        )
        return columns

    def description(self) -> dict[str, object]:
        """Bind generator version, seed, schema and row count as content authority."""
        return {
            "generator": "native-delivery-synthetic-v1",
            "profile": self.profile,
            "seed": self.seed,
            "rows": self.rows,
            "schema": self.schema(),
            "window": [WINDOW_START.isoformat(), WINDOW_END.isoformat()],
        }

    def envelope(self) -> dict[str, object]:
        return {
            "id": self.profile,
            "seed": self.seed,
            "rows": self.rows,
            "columns": len(self.schema()),
            "sha256": digest(self.description()),
        }

    def generate(self) -> Iterator[dict[str, object]]:
        """Yield deterministic rows lazily; no RNG globals or mutable shared buffers."""
        for index in range(self.rows):
            key = index // 2 + self.seed
            row: dict[str, object] = {"id": key, "event_at": WINDOW_START + timedelta(seconds=key % 86400)}
            if self.profile == "decimal":
                value: object = (None, Decimal("-99999999999999.9999"), Decimal("0.0000"), Decimal("1.2300"))[key % 4]
            elif self.profile == "binary":
                value = (None, b"", b"\x00\xff\x80\x00", bytes(range(128)))[key % 4]
            elif self.profile == "null":
                value = (None, "", "NULL", " ")[key % 4]
            elif self.profile == "unicode":
                value = ("Привет 世界 😀", "é", "é", "line\n\ttab", "")[key % 5]
            else:
                value = "x" * (3000 if self.profile == "skewed" and key % 17 == 0 else 8) + str(key % 3)
            for column in self.schema()[2:]:
                row[column["name"]] = value
            yield row
