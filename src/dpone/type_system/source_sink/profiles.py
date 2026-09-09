"""Reusable source -> sink type matrix profiles.

The profiles in this module are diagnostic contracts: they explain how a source
metadata type is expected to land in a target system. They intentionally do not
perform IO and do not import runtime connectors, so they can be reused by CLI,
docs checks, schema evolution diagnostics, and strategy planning.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class PairTypeDecision:
    """One explainable source -> sink type decision."""

    source_type: str
    target_type: str
    native_transport: str
    compatible: bool = True
    lossless: bool = True
    requires_explicit_contract: bool = False
    warning: str | None = None


@dataclass(frozen=True)
class PairTypeProfile:
    """A diagnostic profile for one source -> sink pair."""

    source: str
    sink: str
    profile: str
    default_source_types: tuple[str, ...]
    runbook: str
    resolver: Callable[[str], PairTypeDecision]

    def resolve(self, source_type: str) -> PairTypeDecision:
        return self.resolver(source_type)


class DeclarativePairTypeMapper:
    """Small declarative mapper for non-specialized source -> sink profiles."""

    def __init__(
        self,
        *,
        exact: Mapping[str, tuple[str, str]],
        sink: str,
        fallback_type: str,
        fallback_transport: str,
        complex_types: frozenset[str] = frozenset(),
        regex_resolvers: Sequence[Callable[[str, str], PairTypeDecision | None]] = (),
    ) -> None:
        self._exact = dict(exact)
        self._sink = sink
        self._fallback_type = fallback_type
        self._fallback_transport = fallback_transport
        self._complex_types = complex_types
        self._regex_resolvers = tuple(regex_resolvers)

    def resolve(self, source_type: str) -> PairTypeDecision:
        nullable = _is_nullable(source_type)
        normalized = _normalize(source_type)
        for resolver in self._regex_resolvers:
            decision = resolver(source_type, normalized)
            if decision:
                return self._with_nullable(decision, nullable)
        if normalized in self._exact:
            target_type, transport = self._exact[normalized]
            return PairTypeDecision(
                source_type=source_type,
                target_type=self._target_type(target_type, nullable),
                native_transport=transport,
            )
        if normalized in self._complex_types or _looks_complex(normalized):
            return PairTypeDecision(
                source_type=source_type,
                target_type=self._target_type(self._fallback_type, nullable),
                native_transport=self._fallback_transport,
                lossless=False,
                warning="Source-specific complex type needs schema_contract for strict production semantics.",
            )
        return PairTypeDecision(
            source_type=source_type,
            target_type=self._target_type(self._fallback_type, nullable),
            native_transport=self._fallback_transport,
            lossless=False,
            requires_explicit_contract=True,
            warning="Unknown or custom type requires explicit schema_contract for production loads.",
        )

    def _with_nullable(self, decision: PairTypeDecision, nullable: bool) -> PairTypeDecision:
        return PairTypeDecision(
            source_type=decision.source_type,
            target_type=self._target_type(decision.target_type, nullable),
            native_transport=decision.native_transport,
            compatible=decision.compatible,
            lossless=decision.lossless,
            requires_explicit_contract=decision.requires_explicit_contract,
            warning=decision.warning,
        )

    def _target_type(self, target_type: str, nullable: bool) -> str:
        if self._sink == "clickhouse" and nullable and not target_type.startswith("Nullable("):
            return f"Nullable({target_type})"
        return target_type


def build_default_profiles() -> dict[tuple[str, str], PairTypeProfile]:
    """Return built-in reusable diagnostic profiles."""

    profiles = [
        PairTypeProfile(
            source="mssql",
            sink="mssql",
            profile="mssql_to_mssql_identity_v1",
            default_source_types=("int nullable", "nvarchar(510) nullable", "datetime2(7)", "rowversion"),
            runbook="docs/source-sink/mssql-to-mssql.md#schema-evolution-and-type-mapping",
            resolver=_mssql_identity_mapper().resolve,
        ),
        PairTypeProfile(
            source="mssql",
            sink="postgres",
            profile="mssql_to_postgres_native_v1",
            default_source_types=("int nullable", "nvarchar(255) nullable", "datetime2(3)", "uniqueidentifier"),
            runbook="docs/source-sink/mssql-to-postgres.md#schema-evolution-and-type-mapping",
            resolver=_mssql_postgres_profile_resolver,
        ),
        PairTypeProfile(
            source="mssql",
            sink="bigquery",
            profile="mssql_to_bigquery_analytics_v1",
            default_source_types=("int nullable", "nvarchar(255) nullable", "datetime2(3)", "varbinary(16)"),
            runbook="docs/source-sink/mssql-to-bigquery.md#schema-evolution-and-type-mapping",
            resolver=_mssql_bigquery_profile_resolver,
        ),
        PairTypeProfile(
            source="postgres",
            sink="postgres",
            profile="postgres_to_postgres_identity_v1",
            default_source_types=("integer", "jsonb nullable", "timestamp with time zone", "uuid"),
            runbook="docs/source-sink/postgres-to-postgres.md#schema-evolution-and-type-mapping",
            resolver=_postgres_identity_mapper().resolve,
        ),
        PairTypeProfile(
            source="postgres",
            sink="clickhouse",
            profile="postgres_to_clickhouse_analytics_v1",
            default_source_types=("integer", "jsonb nullable", "timestamp with time zone", "numeric(18,2)"),
            runbook="docs/source-sink/postgres-to-clickhouse.md#schema-evolution-and-type-mapping",
            resolver=_postgres_clickhouse_profile_resolver,
        ),
        PairTypeProfile(
            source="clickhouse",
            sink="mssql",
            profile="clickhouse_to_mssql_landing_v1",
            default_source_types=("Int32", "Nullable(String)", "DateTime64(3)", "UUID"),
            runbook="docs/source-sink/clickhouse-to-mssql.md#schema-evolution-and-type-mapping",
            resolver=_clickhouse_mssql_resolver,
        ),
    ]
    return {(profile.source, profile.sink): profile for profile in profiles}


def _mssql_identity_mapper() -> DeclarativePairTypeMapper:
    exact = {
        "tinyint": ("tinyint", "native SQL/bcp value"),
        "smallint": ("smallint", "native SQL/bcp value"),
        "int": ("int", "native SQL/bcp value"),
        "bigint": ("bigint", "native SQL/bcp value"),
        "bit": ("bit", "native SQL/bcp value"),
        "real": ("real", "native SQL/bcp value"),
        "float": ("float", "native SQL/bcp value"),
        "money": ("money", "native SQL/bcp value"),
        "smallmoney": ("smallmoney", "native SQL/bcp value"),
        "date": ("date", "native SQL/bcp value"),
        "datetime": ("datetime", "native SQL/bcp value"),
        "smalldatetime": ("smalldatetime", "native SQL/bcp value"),
        "datetime2": ("datetime2", "native SQL/bcp value"),
        "datetimeoffset": ("datetimeoffset", "native SQL/bcp value"),
        "time": ("time", "native SQL/bcp value"),
        "uniqueidentifier": ("uniqueidentifier", "native SQL/bcp value"),
        "rowversion": ("rowversion", "binary token"),
        "timestamp": ("rowversion", "binary token"),
        "xml": ("xml", "native XML text"),
    }
    return DeclarativePairTypeMapper(
        exact=exact,
        sink="mssql",
        fallback_type="nvarchar(max)",
        fallback_transport="BulkTextCodec text",
        regex_resolvers=(_mssql_parameterized_identity,),
    )


def _postgres_identity_mapper() -> DeclarativePairTypeMapper:
    exact = {
        "smallint": ("smallint", "native PostgreSQL value"),
        "integer": ("integer", "native PostgreSQL value"),
        "int": ("integer", "native PostgreSQL value"),
        "bigint": ("bigint", "native PostgreSQL value"),
        "boolean": ("boolean", "native PostgreSQL value"),
        "bool": ("boolean", "native PostgreSQL value"),
        "real": ("real", "native PostgreSQL value"),
        "double precision": ("double precision", "native PostgreSQL value"),
        "text": ("text", "native PostgreSQL value"),
        "varchar": ("varchar", "native PostgreSQL value"),
        "character varying": ("character varying", "native PostgreSQL value"),
        "uuid": ("uuid", "native PostgreSQL value"),
        "json": ("json", "native PostgreSQL value"),
        "jsonb": ("jsonb", "native PostgreSQL value"),
        "bytea": ("bytea", "native PostgreSQL binary"),
        "date": ("date", "native PostgreSQL value"),
        "timestamp": ("timestamp", "native PostgreSQL value"),
        "timestamp without time zone": ("timestamp without time zone", "native PostgreSQL value"),
        "timestamp with time zone": ("timestamp with time zone", "native PostgreSQL value"),
        "time": ("time", "native PostgreSQL value"),
        "time without time zone": ("time without time zone", "native PostgreSQL value"),
        "time with time zone": ("time with time zone", "native PostgreSQL value"),
    }
    return DeclarativePairTypeMapper(
        exact=exact,
        sink="postgres",
        fallback_type="text",
        fallback_transport="text/json landing",
        complex_types=frozenset({"json", "jsonb"}),
        regex_resolvers=(_postgres_parameterized_identity,),
    )


def _postgres_clickhouse_profile_resolver(source_type: str) -> PairTypeDecision:
    """Bridge the extractable pair mapper into the diagnostic profile contract."""

    from dpone.type_system.source_sink.postgres_clickhouse import PostgresClickHouseTypeMapper

    decision = PostgresClickHouseTypeMapper().resolve(source_type)
    target_type = decision.target_type
    if "nullable" in str(source_type).lower() and not target_type.startswith("Nullable("):
        target_type = f"Nullable({target_type})"
    return PairTypeDecision(
        source_type=source_type,
        target_type=target_type,
        native_transport=decision.transfer_representation,
        compatible=decision.compatible,
        lossless=decision.lossless,
        requires_explicit_contract=decision.requires_explicit_contract,
        warning=decision.warning,
    )


def _mssql_postgres_profile_resolver(source_type: str) -> PairTypeDecision:
    """Bridge the MSSQL→Postgres pair mapper into the diagnostic profile contract."""

    from dpone.type_system.source_sink.mssql_postgres import MSSQLPostgresTypeMapper

    decision = MSSQLPostgresTypeMapper().resolve(source_type)
    return PairTypeDecision(
        source_type=source_type,
        target_type=decision.target_type,
        native_transport=decision.transfer_representation,
        compatible=decision.compatible,
        lossless=decision.lossless,
        requires_explicit_contract=decision.requires_explicit_contract,
        warning=decision.warning,
    )


def _mssql_bigquery_profile_resolver(source_type: str) -> PairTypeDecision:
    """Bridge the MSSQL→BigQuery pair mapper into the diagnostic profile contract."""

    from dpone.type_system.source_sink.mssql_bigquery import MSSQLBigQueryTypeMapper

    decision = MSSQLBigQueryTypeMapper().resolve(source_type)
    return PairTypeDecision(
        source_type=source_type,
        target_type=decision.target_type,
        native_transport=decision.transfer_representation,
        compatible=decision.compatible,
        lossless=decision.lossless,
        requires_explicit_contract=decision.requires_explicit_contract,
        warning=decision.warning,
    )


def _clickhouse_mssql_resolver(source_type: str) -> PairTypeDecision:
    """Strict allowlist resolver delegating to the canonical classifier.

    Every ClickHouse type is either mapped exactly or explicitly unsupported
    with a source-side workaround; there is no silent fallback (mirrors the
    runtime ``MSSQLTypeMapper`` policy for this route).
    """

    from dpone.type_system.source_sink.clickhouse_mssql import classify_clickhouse_type

    decision = classify_clickhouse_type(re.sub(r"\s+nullable\s*$", "", str(source_type).strip(), flags=re.IGNORECASE))
    if decision is None:
        return PairTypeDecision(
            source_type=source_type,
            target_type="unsupported",
            native_transport="n/a",
            compatible=False,
            lossless=False,
            requires_explicit_contract=True,
            warning="Not a recognized ClickHouse type; declare an explicit schema_contract.",
        )
    if not decision.supported:
        return PairTypeDecision(
            source_type=source_type,
            target_type="unsupported",
            native_transport="n/a",
            compatible=False,
            lossless=False,
            requires_explicit_contract=True,
            warning=f"{decision.reason}. Workaround: {decision.workaround}.",
        )
    return PairTypeDecision(
        source_type=source_type,
        target_type=decision.mssql_type or "unsupported",
        native_transport=_clickhouse_mssql_transport(decision.mssql_type or ""),
        lossless=decision.lossless,
        warning=decision.note,
    )


def _clickhouse_mssql_transport(mssql_type: str) -> str:
    if mssql_type.startswith(("smallint", "tinyint", "int", "bigint")):
        return "numeric text"
    if mssql_type.startswith(("real", "float")):
        return "float text"
    if mssql_type.startswith(("decimal", "numeric")):
        return "decimal text"
    if mssql_type.startswith(("date", "datetime2")):
        return "timestamp text" if mssql_type.startswith("datetime2") else "ISO date text"
    if mssql_type == "bit":
        return "0/1 text"
    if mssql_type == "uniqueidentifier":
        return "uuid text"
    return "BulkTextCodec text"


def _mssql_parameterized_identity(source_type: str, normalized: str) -> PairTypeDecision | None:
    if re.fullmatch(r"(?:n?var)?char\((?:max|\d+)\)", normalized):
        return PairTypeDecision(source_type, normalized, "native SQL/bcp value")
    if re.fullmatch(r"n?char\(\d+\)", normalized):
        return PairTypeDecision(source_type, normalized, "native SQL/bcp value")
    if re.fullmatch(r"(?:decimal|numeric)\(\d+,\d+\)", normalized):
        return PairTypeDecision(source_type, normalized, "native SQL/bcp value")
    if re.fullmatch(r"(?:date)?time(?:2|offset)?\(\d+\)", normalized):
        return PairTypeDecision(source_type, normalized, "native SQL/bcp value")
    if re.fullmatch(r"varbinary\((?:max|\d+)\)|binary\(\d+\)", normalized):
        return PairTypeDecision(source_type, normalized, "binary token/value")
    return None


def _postgres_parameterized_identity(source_type: str, normalized: str) -> PairTypeDecision | None:
    if re.fullmatch(r"(?:numeric|decimal)\(\d+,\d+\)", normalized):
        return PairTypeDecision(source_type, normalized, "native PostgreSQL value")
    if re.fullmatch(r"(?:character varying|varchar|character|char)\(\d+\)", normalized):
        return PairTypeDecision(source_type, normalized, "native PostgreSQL value")
    if re.fullmatch(r"time(?:stamp)?(?: with(?:out)? time zone)?\(\d+\)", normalized):
        return PairTypeDecision(source_type, normalized, "native PostgreSQL value")
    return None


def _normalize(value: str) -> str:
    normalized = " ".join(str(value).strip().lower().split())
    normalized = re.sub(r"\s+nullable$", "", normalized).strip()
    normalized = _unwrap_clickhouse(normalized)
    normalized = re.sub(r"character varying\(\d+\)", lambda match: match.group(0), normalized)
    return normalized


def _unwrap_clickhouse(value: str) -> str:
    current = value.strip()
    changed = True
    while changed:
        changed = False
        for wrapper in ("nullable", "lowcardinality"):
            prefix = f"{wrapper}("
            if current.startswith(prefix) and current.endswith(")"):
                current = current[len(prefix) : -1].strip()
                changed = True
    return current


def _is_nullable(source_type: str) -> bool:
    value = str(source_type).strip().lower()
    return value.endswith(" nullable") or value.startswith("nullable(")


def _looks_complex(normalized: str) -> bool:
    return (
        normalized.endswith("[]")
        or "range" in normalized
        or normalized.startswith(("array(", "map(", "tuple(", "nested("))
        or normalized in {"tsvector", "tsquery", "pg_lsn"}
    )


__all__ = [
    "DeclarativePairTypeMapper",
    "PairTypeDecision",
    "PairTypeProfile",
    "build_default_profiles",
]
