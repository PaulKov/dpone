"""Immutable observed source-column metadata carried beside projected schemas."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

from dpone._compat import StrEnum


class SourceRelationDialect(StrEnum):
    """Typed catalog dialect for one immutable observed relation."""

    POSTGRES = "postgres"
    MSSQL = "mssql"
    MYSQL = "mysql"
    CLICKHOUSE = "clickhouse"
    BIGQUERY = "bigquery"


@dataclass(frozen=True, slots=True)
class SourceColumnProvenance:
    """Catalog identity that cannot be represented by a ``(name, type)`` pair."""

    name: str
    declared_type: str
    nullable: bool | None
    type_schema: str | None = None
    type_name: str | None = None
    type_kind: str | None = None
    type_category: str | None = None
    domain_schema: str | None = None
    domain_name: str | None = None
    datetime_precision: int | None = None
    interval_type: str | None = None
    interval_precision: int | None = None
    character_set: str | None = None
    collation_schema: str | None = None
    collation_name: str | None = None
    has_default: bool | None = None
    default_expression_sha256: str | None = None
    is_identity: bool | None = None
    identity_generation: str | None = None
    generation_kind: str | None = None
    generation_expression_sha256: str | None = None

    @property
    def collation(self) -> str | None:
        if not self.collation_name:
            return None
        return f"{self.collation_schema}.{self.collation_name}" if self.collation_schema else self.collation_name

    @property
    def identity_token(self) -> str:
        """Return deterministic catalog identity for schema/state fingerprints."""

        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def clickhouse_type_nullable(dtype: str) -> bool:
    """Return exact top-level nullability from a ClickHouse type spelling."""

    value = str(dtype).strip()
    while value.startswith("LowCardinality(") and value.endswith(")"):
        value = value[len("LowCardinality(") : -1].strip()
    return value.startswith("Nullable(") and value.endswith(")")


__all__ = ["SourceColumnProvenance", "SourceRelationDialect", "clickhouse_type_nullable"]
