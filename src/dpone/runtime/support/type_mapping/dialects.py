"""Target dialect renderers for canonical type mapping."""

from __future__ import annotations

from dpone.runtime.support.type_mapping.models import CanonicalType, ParsedType


class _BigQueryDialect:
    """Маппинг ParsedType → BigQuery тип."""

    @classmethod
    def to_type(cls, p: ParsedType) -> str:
        if p.base == CanonicalType.INTEGER:
            return "INT64"
        if p.base == CanonicalType.FLOAT:
            return "FLOAT64"
        if p.base == CanonicalType.NUMERIC:
            return "BIGNUMERIC"
        if p.base == CanonicalType.STRING:
            return "STRING"
        if p.base == CanonicalType.TIMESTAMP:
            return "TIMESTAMP"
        if p.base == CanonicalType.DATE:
            return "DATE"
        if p.base == CanonicalType.TIME:
            return "TIME"
        if p.base == CanonicalType.BOOLEAN:
            return "BOOL"
        if p.base == CanonicalType.JSON:
            return "JSON"
        if p.base == CanonicalType.BYTES:
            return "BYTES"
        if p.base == CanonicalType.ARRAY:
            # ClickHouse Array экспортируется в Parquet как nested structure
            # BigQuery требует RECORD<list ARRAY<STRUCT<item TYPE>>> для чтения
            return "RECORD<list ARRAY<STRUCT<item STRING>>>"
        return "STRING"


class _PostgresDialect:
    """Маппинг ParsedType → PostgreSQL тип.

    Нужен для случаев, когда требуется нормализация типов в PG
    """

    @classmethod
    def to_type(cls, p: ParsedType) -> str:
        if p.base == CanonicalType.INTEGER:
            return "integer"
        if p.base == CanonicalType.FLOAT:
            return "double precision"
        if p.base == CanonicalType.NUMERIC:
            return "numeric"
        if p.base == CanonicalType.STRING:
            return "text"
        if p.base == CanonicalType.TIMESTAMP:
            return "timestamp"
        if p.base == CanonicalType.DATE:
            return "date"
        if p.base == CanonicalType.TIME:
            return "time"
        if p.base == CanonicalType.BOOLEAN:
            return "boolean"
        if p.base == CanonicalType.JSON:
            return "jsonb"
        if p.base == CanonicalType.ARRAY:
            elem_pg = cls.to_type(p.element or ParsedType(CanonicalType.STRING))
            if "[]" in elem_pg:
                elem_pg = "text"
            return f"{elem_pg}[]"
        return "text"


__all__ = ["_BigQueryDialect", "_PostgresDialect"]
