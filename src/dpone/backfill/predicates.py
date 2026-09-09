"""Dialect-aware rendering for generated backfill chunk predicates."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.backfill.models import BackfillChunk, BackfillChunkSpec


@dataclass(frozen=True, slots=True)
class BackfillPredicateRenderer:
    """Render generated chunk windows with dialect-specific quoting and literals."""

    dialect: str = "generic"

    @classmethod
    def for_dialect(cls, dialect: str | None) -> BackfillPredicateRenderer:
        return cls((dialect or "generic").strip().lower())

    def render(self, spec: BackfillChunkSpec, chunk: BackfillChunk) -> str:
        column = self._identifier(spec.column)
        start = self._literal(chunk.start, spec.kind)
        end = self._literal(chunk.end, spec.kind)
        if spec.kind == "integer":
            return f"{column} >= {start} AND {column} <= {end}"
        if spec.kind == "uuid":
            operator = "<=" if chunk.portable_scope.upper and chunk.portable_scope.upper.inclusive else "<"
            return f"{column} >= {start} AND {column} {operator} {end}"
        return f"{column} >= {start} AND {column} < {end}"

    def _identifier(self, name: str) -> str:
        if self.dialect == "mssql":
            return "[" + name.replace("]", "]]") + "]"
        if self.dialect == "clickhouse":
            return "`" + name.replace("`", "``") + "`"
        if self.dialect in {"postgres", "postgresql"}:
            return '"' + name.replace('"', '""') + '"'
        return name

    def _literal(self, value: str, kind: str) -> str:
        if kind == "integer":
            return str(int(value))
        if kind == "uuid":
            escaped = value.replace("'", "''")
            if self.dialect == "mssql":
                return f"CAST('{escaped}' AS uniqueidentifier)"
            if self.dialect in {"postgres", "postgresql"}:
                return f"UUID '{escaped}'"
            return f"'{escaped}'"
        escaped = value.replace("T", " ").replace("'", "''")
        if self.dialect == "clickhouse":
            func = (
                "toDateTime64"
                if kind == "timestamp" and "." in escaped
                else "toDateTime"
                if kind == "timestamp"
                else "toDate"
            )
            if func == "toDateTime64":
                return f"{func}('{escaped}', 6)"
            return f"{func}('{escaped}')"
        if self.dialect == "mssql":
            target = "datetime2" if kind == "timestamp" else "date"
            return f"CAST('{escaped}' AS {target})"
        if self.dialect in {"postgres", "postgresql"} and kind == "timestamp":
            return f"TIMESTAMP '{escaped}'"
        if self.dialect in {"postgres", "postgresql"}:
            return f"DATE '{escaped}'"
        return f"'{escaped}'"


__all__ = ["BackfillPredicateRenderer"]
