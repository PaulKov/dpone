"""PostgreSQL -> ClickHouse type mapping profile."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PostgresClickHouseTypeDecision:
    """One explainable PostgreSQL -> ClickHouse type decision."""

    source_type: str
    target_type: str
    transfer_representation: str
    compatible: bool = True
    requires_explicit_contract: bool = False
    lossless: bool = True
    warning: str | None = None


class PostgresClickHouseTypeMapper:
    """Pair profile ``postgres_to_clickhouse_analytics_v1`` for extract schema mapping."""

    def resolve(self, source_type: str) -> PostgresClickHouseTypeDecision:
        normalized = _normalize(source_type)
        if match := re.fullmatch(r"(?:numeric|decimal)\((\d+),(\d+)\)", normalized):
            return PostgresClickHouseTypeDecision(
                source_type=source_type,
                target_type=f"Decimal({match.group(1)},{match.group(2)})",
                transfer_representation="decimal TSV",
            )
        if re.fullmatch(r"(?:character varying|varchar|character|char)\(\d+\)", normalized):
            return PostgresClickHouseTypeDecision(
                source_type=source_type,
                target_type="String",
                transfer_representation="escaped TSV text",
            )
        if normalized in _EXACT:
            target_type, representation = _EXACT[normalized]
            return PostgresClickHouseTypeDecision(
                source_type=source_type,
                target_type=target_type,
                transfer_representation=representation,
                requires_explicit_contract=normalized in {"bytea", "json", "jsonb"},
            )
        return PostgresClickHouseTypeDecision(
            source_type=source_type,
            target_type="String",
            transfer_representation="escaped TSV text",
            requires_explicit_contract=True,
            lossless=False,
            warning="Unknown or custom PostgreSQL type lands as String unless schema_contract overrides it.",
        )


_EXACT: dict[str, tuple[str, str]] = {
    "smallint": ("Int16", "numeric TSV"),
    "int2": ("Int16", "numeric TSV"),
    "integer": ("Int32", "numeric TSV"),
    "int": ("Int32", "numeric TSV"),
    "int4": ("Int32", "numeric TSV"),
    "bigint": ("Int64", "numeric TSV"),
    "int8": ("Int64", "numeric TSV"),
    "real": ("Float32", "float TSV"),
    "double precision": ("Float64", "float TSV"),
    "boolean": ("Bool", "0/1 TSV"),
    "bool": ("Bool", "0/1 TSV"),
    "text": ("String", "escaped TSV text"),
    "varchar": ("String", "escaped TSV text"),
    "character varying": ("String", "escaped TSV text"),
    "uuid": ("UUID", "uuid TSV"),
    "json": ("String", "JSON text"),
    "jsonb": ("String", "JSON text"),
    "bytea": ("String", "hex text"),
    "date": ("Date", "ISO date TSV"),
    "timestamp": ("DateTime64(6)", "ClickHouse-safe timestamp text"),
    "timestamp without time zone": ("DateTime64(6)", "ClickHouse-safe timestamp text"),
    "timestamp with time zone": ("DateTime64(6, 'UTC')", "UTC instant timestamp text"),
    "timestamptz": ("DateTime64(6, 'UTC')", "UTC instant timestamp text"),
    "time": ("String", "time text"),
    "time without time zone": ("String", "time text"),
}


def _normalize(value: str) -> str:
    normalized = " ".join(str(value).strip().lower().split())
    normalized = re.sub(r"\s+nullable$", "", normalized)
    return normalized


__all__ = ["PostgresClickHouseTypeDecision", "PostgresClickHouseTypeMapper"]
