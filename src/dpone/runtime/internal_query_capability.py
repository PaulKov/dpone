"""Runtime authority for executing a source query on the target connection.

``InternalQueryArtifact`` is safe only when runtime composition has proved that
the target connection can execute the exact source relation query.  Logical
connection names are authoring metadata and are intentionally absent from this
module's grant decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

INTERNAL_QUERY_GRANTED = "DPONE_INTERNAL_QUERY_CAPABILITY_GRANTED"
INTERNAL_QUERY_NOT_ISSUED = "DPONE_INTERNAL_QUERY_CAPABILITY_NOT_ISSUED"
INTERNAL_QUERY_CROSS_DIALECT = "DPONE_INTERNAL_QUERY_CROSS_DIALECT_FILE_FALLBACK"
INTERNAL_QUERY_UNRESOLVED_AUTHORITY = "DPONE_INTERNAL_QUERY_UNRESOLVED_AUTHORITY_FILE_FALLBACK"
INTERNAL_QUERY_DIFFERENT_AUTHORITY = "DPONE_INTERNAL_QUERY_DIFFERENT_AUTHORITY_FILE_FALLBACK"
INTERNAL_QUERY_DATABASE_MISMATCH = "DPONE_INTERNAL_QUERY_DATABASE_AUTHORITY_FILE_FALLBACK"
INTERNAL_QUERY_TARGET_PROBE_FAILED = "DPONE_INTERNAL_QUERY_TARGET_PROBE_FILE_FALLBACK"
INTERNAL_QUERY_UNSUPPORTED_DIALECT = "DPONE_INTERNAL_QUERY_UNSUPPORTED_DIALECT_FILE_FALLBACK"

_SUPPORTED_DIALECTS = frozenset({"postgres", "mssql"})
_CAPABILITY_ISSUER_TOKEN = object()


@dataclass(frozen=True, slots=True)
class InternalQueryCapabilityDiagnostic:
    """Safe, typed explanation of an internal-query grant or file fallback."""

    code: str
    eligible: bool
    source_dialect: str
    target_dialect: str
    message: str
    action: str

    def to_log_payload(self) -> dict[str, object]:
        """Render a stable, secret-free runtime diagnostic."""

        return {
            "Code": self.code,
            "Eligible": self.eligible,
            "Source Dialect": self.source_dialect or "unknown",
            "Target Dialect": self.target_dialect or "unknown",
            "Message": self.message,
            "Action": self.action,
        }


@dataclass(frozen=True, slots=True, init=False)
class InternalQueryCapability:
    """Relation-scoped proof issued by the runtime composition root.

    The capability is bound to the hydrated source connector instance and to
    the exact relation that the target session successfully probed.  It is not
    serializable configuration and cannot be inferred from connection aliases.
    """

    dialect: str
    source_database: str
    source_schema: str
    source_table: str
    _source_connector_identity: int
    _target_connector_identity: int

    def __init__(
        self,
        *,
        dialect: str,
        source_database: str,
        source_schema: str,
        source_table: str,
        source_connector_identity: int,
        target_connector_identity: int,
        _issuer_token: object,
    ) -> None:
        if _issuer_token is not _CAPABILITY_ISSUER_TOKEN:
            raise TypeError("InternalQueryCapability can only be issued by runtime hydration")
        object.__setattr__(self, "dialect", dialect)
        object.__setattr__(self, "source_database", source_database)
        object.__setattr__(self, "source_schema", source_schema)
        object.__setattr__(self, "source_table", source_table)
        object.__setattr__(self, "_source_connector_identity", source_connector_identity)
        object.__setattr__(self, "_target_connector_identity", target_connector_identity)

    def authorizes(
        self,
        *,
        source_connector: Any,
        dialect: str,
        source_database: str | None,
        source_schema: str,
        source_table: str,
    ) -> bool:
        """Return whether this proof covers the requested runtime query."""

        normalized_dialect = _dialect(dialect)
        requested_database = (
            self.source_database if source_database is None else _coordinate(source_database, normalized_dialect)
        )
        return (
            id(source_connector) == self._source_connector_identity
            and normalized_dialect == self.dialect
            and requested_database == self.source_database
            and _coordinate(source_schema, normalized_dialect) == self.source_schema
            and _coordinate(source_table, normalized_dialect) == self.source_table
        )


@dataclass(frozen=True, slots=True)
class InternalQueryCapabilityDecision:
    """One immutable grant/fallback decision shared by all source strategies."""

    capability: InternalQueryCapability | None
    diagnostic: InternalQueryCapabilityDiagnostic

    @property
    def eligible(self) -> bool:
        return self.capability is not None

    def authorizes(
        self,
        *,
        source_connector: Any,
        dialect: str,
        source_database: str | None,
        source_schema: str,
        source_table: str,
    ) -> bool:
        capability = self.capability
        return capability is not None and capability.authorizes(
            source_connector=source_connector,
            dialect=dialect,
            source_database=source_database,
            source_schema=source_schema,
            source_table=source_table,
        )

    @classmethod
    def not_issued(cls, *, source_dialect: str = "", target_dialect: str = "") -> InternalQueryCapabilityDecision:
        """Return the fail-closed decision used outside runtime hydration."""

        normalized_source = _dialect(source_dialect)
        normalized_target = _dialect(target_dialect)
        if normalized_source and normalized_target and normalized_source != normalized_target:
            return _denied(
                INTERNAL_QUERY_CROSS_DIALECT,
                normalized_source,
                normalized_target,
                "Cross-dialect endpoints cannot share an executable internal-query artifact.",
                "The route will use its configured file or streaming transfer boundary.",
            )
        return cls(
            capability=None,
            diagnostic=_diagnostic(
                INTERNAL_QUERY_NOT_ISSUED,
                source_dialect=normalized_source,
                target_dialect=normalized_target,
                message="Runtime did not issue same-connection query authority; the source will use a transfer artifact.",
                action="Use canonical runtime connection bindings to enable the internal-query optimization.",
            ),
        )


class InternalQueryCapabilityIssuer:
    """Issue internal-query authority from hydrated, canonical runtime facts."""

    def issue(
        self,
        *,
        resolved_connections: Any,
        source_config: Any,
        sink_config: Any,
        load_config: Any,
        source_connector: Any,
        target_connector: Any,
    ) -> InternalQueryCapabilityDecision:
        """Grant only a same-dialect, same-binding, target-probed relation."""

        source_dialect = _configured_dialect(source_config)
        target_dialect = _configured_dialect(sink_config)
        if source_dialect != target_dialect:
            return _denied(
                INTERNAL_QUERY_CROSS_DIALECT,
                source_dialect,
                target_dialect,
                "Cross-dialect endpoints cannot share an executable internal-query artifact.",
                "The route will use its configured file or streaming transfer boundary.",
            )
        if source_dialect not in _SUPPORTED_DIALECTS:
            return _denied(
                INTERNAL_QUERY_UNSUPPORTED_DIALECT,
                source_dialect,
                target_dialect,
                "This dialect has no internal-query capability contract.",
                "Use the route's configured transfer artifact.",
            )
        if not bool(getattr(resolved_connections, "strict", False)):
            return _denied(
                INTERNAL_QUERY_UNRESOLVED_AUTHORITY,
                source_dialect,
                target_dialect,
                "Legacy/direct connection fields do not prove a canonical physical database authority.",
                "Migrate both endpoints to a canonical connection_ref to enable this optimization.",
            )

        source_binding = getattr(resolved_connections, "source", None)
        target_binding = getattr(resolved_connections, "sink", None)
        if source_binding is None or target_binding is None or source_binding is not target_binding:
            return _denied(
                INTERNAL_QUERY_DIFFERENT_AUTHORITY,
                source_dialect,
                target_dialect,
                "Source and target were not hydrated from the same canonical connection binding.",
                "Use the transfer boundary, or intentionally bind both endpoints to one canonical connection_ref.",
            )
        descriptor = getattr(source_binding, "descriptor", None)
        descriptor_dialect = _dialect(getattr(descriptor, "connection_type", ""))
        if descriptor_dialect != source_dialect:
            return _denied(
                INTERNAL_QUERY_DIFFERENT_AUTHORITY,
                source_dialect,
                target_dialect,
                "The resolved connection type does not match the source and target dialect.",
                "Correct the canonical binding type; the route will use its transfer boundary.",
            )
        if source_connector is None or target_connector is None:
            return _denied(
                INTERNAL_QUERY_DIFFERENT_AUTHORITY,
                source_dialect,
                target_dialect,
                "Hydrated endpoints do not expose both connector instances.",
                "Use a runtime endpoint implementation that exposes its connector capability.",
            )

        binding_database = str(getattr(getattr(source_binding, "credentials", None), "database", "") or "")
        source_database = str(getattr(load_config, "source_database", None) or binding_database)
        target_database = str(getattr(load_config, "target_database", None) or binding_database)
        if _coordinate(source_database, source_dialect) != _coordinate(target_database, source_dialect):
            return _denied(
                INTERNAL_QUERY_DATABASE_MISMATCH,
                source_dialect,
                target_dialect,
                "Source and target database coordinates are not the same canonical database authority.",
                "Use the transfer boundary for cross-database movement.",
            )

        source_schema = str(getattr(load_config, "source_schema", "") or "")
        source_table = str(getattr(load_config, "source_table", "") or "")
        try:
            _probe_target_relation(
                target_connector,
                dialect=source_dialect,
                database=source_database,
                schema=source_schema,
                table=source_table,
            )
        except Exception:  # noqa: BLE001 - a failed optional optimization must safely fall back.
            return _denied(
                INTERNAL_QUERY_TARGET_PROBE_FAILED,
                source_dialect,
                target_dialect,
                "The hydrated target session could not prove read access to the source relation.",
                "The route will use its configured transfer boundary without exposing connector details.",
            )

        capability = InternalQueryCapability(
            dialect=source_dialect,
            source_database=_coordinate(source_database, source_dialect),
            source_schema=_coordinate(source_schema, source_dialect),
            source_table=_coordinate(source_table, source_dialect),
            source_connector_identity=id(source_connector),
            target_connector_identity=id(target_connector),
            _issuer_token=_CAPABILITY_ISSUER_TOKEN,
        )
        return InternalQueryCapabilityDecision(
            capability=capability,
            diagnostic=InternalQueryCapabilityDiagnostic(
                code=INTERNAL_QUERY_GRANTED,
                eligible=True,
                source_dialect=source_dialect,
                target_dialect=target_dialect,
                message="The target session proved access to the source relation under one canonical binding.",
                action="Use the internal-query artifact for this exact hydrated source relation.",
            ),
        )


def log_internal_query_capability(logger: Any, decision: InternalQueryCapabilityDecision) -> None:
    """Publish one safe self-service diagnostic through the existing ETL logger."""

    log_progress = getattr(logger, "log_etl_progress", None)
    if callable(log_progress):
        log_progress("INTERNAL_QUERY_CAPABILITY_DECISION", decision.diagnostic.to_log_payload())


def _probe_target_relation(
    connector: Any,
    *,
    dialect: str,
    database: str,
    schema: str,
    table: str,
) -> None:
    if not schema or not table:
        raise ValueError("source relation coordinates are required")
    get_records = getattr(connector, "get_records", None)
    if not callable(get_records):
        raise TypeError("target connector has no read probe")
    if dialect == "postgres":
        relation = f"{_postgres_identifier(schema)}.{_postgres_identifier(table)}"
        get_records(f"SELECT 1 FROM {relation} WHERE FALSE")
        return
    if dialect == "mssql":
        qualified_name = getattr(connector, "qualified_name", None)
        if not callable(qualified_name):
            raise TypeError("target connector has no qualified-name renderer")
        relation = qualified_name(schema, table, database=database or None)
        get_records(f"SELECT TOP (0) 1 FROM {relation}")
        return
    raise ValueError("unsupported internal-query dialect")


def _postgres_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _configured_dialect(config: Any) -> str:
    getter = getattr(config, "get", None)
    return _dialect(getter("type", "") if callable(getter) else "")


def _dialect(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _coordinate(value: Any, dialect: str) -> str:
    normalized = str(value or "")
    return normalized.casefold() if dialect == "mssql" else normalized


def _diagnostic(
    code: str,
    *,
    source_dialect: str,
    target_dialect: str,
    message: str,
    action: str,
) -> InternalQueryCapabilityDiagnostic:
    return InternalQueryCapabilityDiagnostic(
        code=code,
        eligible=False,
        source_dialect=source_dialect,
        target_dialect=target_dialect,
        message=message,
        action=action,
    )


def _denied(
    code: str,
    source_dialect: str,
    target_dialect: str,
    message: str,
    action: str,
) -> InternalQueryCapabilityDecision:
    return InternalQueryCapabilityDecision(
        capability=None,
        diagnostic=_diagnostic(
            code,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            message=message,
            action=action,
        ),
    )


__all__ = [
    "INTERNAL_QUERY_CROSS_DIALECT",
    "INTERNAL_QUERY_DATABASE_MISMATCH",
    "INTERNAL_QUERY_DIFFERENT_AUTHORITY",
    "INTERNAL_QUERY_GRANTED",
    "INTERNAL_QUERY_NOT_ISSUED",
    "INTERNAL_QUERY_TARGET_PROBE_FAILED",
    "INTERNAL_QUERY_UNRESOLVED_AUTHORITY",
    "INTERNAL_QUERY_UNSUPPORTED_DIALECT",
    "InternalQueryCapability",
    "InternalQueryCapabilityDecision",
    "InternalQueryCapabilityDiagnostic",
    "InternalQueryCapabilityIssuer",
    "log_internal_query_capability",
]
