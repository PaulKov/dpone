"""Repeatable-read verification of signed PostgreSQL source authority."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from psycopg import sql

from dpone.contracts.postgres_source_authority import (
    PostgresSourceAuthority,
    SelectedPostgresSourceAuthority,
    ascii_case_alias,
)
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.runtime.sources.postgres_source_authority_observation import (
    PostgresSourceAuthorityVerificationError,
    raise_postgres_source_authority_query_error,
    read_postgres_identity_observation,
    read_postgres_timeline_authority,
    timeline_id,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
)

POSTGRES_SOURCE_AUTHORITY_SHA256_OPTION = "postgres_source_authority_sha256"
_ASCII_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_ASCII_LOWER = "abcdefghijklmnopqrstuvwxyz"


@dataclass(frozen=True, slots=True)
class PostgresSourceAuthorityVerifier:
    """Select authority without I/O, then prove it on the branded RR session."""

    authority: PostgresSourceAuthority
    _selected_by_route: dict[tuple[str, str], SelectedPostgresSourceAuthority] = field(
        default_factory=dict,
        init=False,
        repr=False,
        compare=False,
    )

    @classmethod
    def from_connection(
        cls,
        connection: ResolvedBindingConnection,
    ) -> PostgresSourceAuthorityVerifier:
        descriptor = connection.descriptor
        if descriptor is None:
            raise PostgresSourceAuthorityVerificationError("postgres_source_authority.descriptor_required")
        return cls(
            PostgresSourceAuthority.from_connection_properties(
                descriptor.properties,
            )
        )

    def preflight(self, load_config: Any) -> SourcePhysicalIdentity:
        """Validate and select the signed route document without source I/O."""

        selected = self._select(load_config)
        identity = _expected_identity(selected)
        load_config.source_schema = identity.schema
        load_config.source_table = identity.relation
        return identity

    def verify_snapshot(
        self,
        *,
        connector: Any,
        snapshot_lease: PostgresRepeatableReadSnapshotLease,
        load_config: Any,
    ) -> SourcePhysicalIdentity:
        """Verify cluster, timeline, DB, principals and relation on one RR."""

        if not isinstance(snapshot_lease, PostgresRepeatableReadSnapshotLease):
            raise PostgresSourceAuthorityVerificationError("postgres_source_authority.repeatable_read_lease_required")
        snapshot_lease.require_for(connector)
        selected = self._select(load_config)
        identity_row = read_postgres_identity_observation(connector, selected)
        relation_row = _read_relation(connector, selected)
        _verified_identity(selected, identity_row, relation_row)
        _lock_relation(connector, selected)
        # The lock lookup itself can race with a DROP/rename that commits after
        # the first catalog read.  Re-reading under the acquired lock proves
        # that the locked object is still the deployment-signed OID.
        locked_relation_row = _read_relation(connector, selected)
        identity = _verified_identity(selected, identity_row, locked_relation_row)
        load_config.source_schema = identity.schema
        load_config.source_table = identity.relation
        return identity

    def _select(self, load_config: Any) -> SelectedPostgresSourceAuthority:
        authored = (
            str(getattr(load_config, "source_schema", "") or ""),
            str(getattr(load_config, "source_table", "") or ""),
        )
        selected = self.authority.select(
            authored_schema=authored[0],
            authored_relation=authored[1],
        )
        existing = self._selected_by_route.get(authored)
        if existing is not None and existing.authority_sha256 != selected.authority_sha256:
            raise PostgresSourceAuthorityVerificationError("postgres_source_authority.selection_drift")
        self._selected_by_route[authored] = selected
        return selected


def _read_relation(
    connector: Any,
    selected: SelectedPostgresSourceAuthority,
) -> dict[str, Any]:
    pin = selected.relation
    try:
        rows = connector.get_records(
            """
            SELECT n.oid::bigint AS namespace_oid, n.nspname AS schema_name,
                   c.oid::bigint AS relation_oid, c.relname AS relation_name
            FROM pg_catalog.pg_namespace AS n
            INNER JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid
            WHERE pg_catalog.translate(n.nspname, %s, %s) = %s
              AND pg_catalog.translate(c.relname, %s, %s) = %s
              AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
            ORDER BY n.oid, c.oid
            """,
            (
                _ASCII_UPPER,
                _ASCII_LOWER,
                _ascii_fold(pin.schema),
                _ASCII_UPPER,
                _ASCII_LOWER,
                _ascii_fold(pin.relation),
            ),
            as_dict=True,
        )
    except Exception as exc:
        raise_postgres_source_authority_query_error(exc)
    candidates = [
        dict(row)
        for row in rows
        if isinstance(row, dict)
        and ascii_case_alias(str(row.get("schema_name") or ""), pin.schema)
        and ascii_case_alias(str(row.get("relation_name") or ""), pin.relation)
    ]
    if len(candidates) != 1:
        code = (
            "postgres_source_authority.relation_missing"
            if not candidates
            else "postgres_source_authority.relation_ascii_case_ambiguous"
        )
        raise PostgresSourceAuthorityVerificationError(code)
    return candidates[0]


def _lock_relation(
    connector: Any,
    selected: SelectedPostgresSourceAuthority,
) -> None:
    """Hold the selected relation against substitution until RR completion."""

    statement = sql.SQL("LOCK TABLE {}.{} IN ACCESS SHARE MODE").format(
        sql.Identifier(selected.relation.schema),
        sql.Identifier(selected.relation.relation),
    )
    try:
        connector.execute_query(statement)
    except Exception as exc:
        sqlstate = str(getattr(exc, "sqlstate", "") or "")
        if sqlstate in {"42P01", "42704"}:
            raise PostgresSourceAuthorityVerificationError("postgres_source_authority.relation_missing") from exc
        raise_postgres_source_authority_query_error(exc)


def _verified_identity(
    selected: SelectedPostgresSourceAuthority,
    identity: dict[str, Any],
    relation: dict[str, Any],
) -> SourcePhysicalIdentity:
    topology_role = "standby" if bool(identity.get("in_recovery")) else "primary"
    checks = [
        (
            str(identity.get("database_name") or "") == selected.database.canonical_name
            and _integer(identity.get("database_oid")) == selected.database.oid,
            "database_identity_mismatch",
        ),
        (
            str(identity.get("effective_principal") or "") == selected.effective_principal.canonical_name
            and _integer(identity.get("effective_principal_oid")) == selected.effective_principal.oid,
            "effective_principal_identity_mismatch",
        ),
        (
            str(identity.get("session_principal") or "") == selected.session_principal.canonical_name
            and _integer(identity.get("session_principal_oid")) == selected.session_principal.oid,
            "session_principal_identity_mismatch",
        ),
        (
            topology_role == selected.topology_role,
            "topology_role_mismatch",
        ),
        (
            str(relation.get("schema_name") or "") == selected.relation.schema
            and str(relation.get("relation_name") or "") == selected.relation.relation
            and _integer(relation.get("namespace_oid")) == selected.relation.namespace_oid
            and _integer(relation.get("relation_oid")) == selected.relation.relation_oid,
            "relation_identity_mismatch",
        ),
    ]
    if selected.verification_profile == "physical_cluster":
        checks[0:0] = [
            (
                str(identity.get("system_identifier") or "") == selected.system_identifier,
                "system_identifier_mismatch",
            ),
            (
                timeline_id(identity.get("timeline_id")) == selected.timeline_id,
                "timeline_id_mismatch",
            ),
        ]
    for valid, suffix in checks:
        if not valid:
            raise PostgresSourceAuthorityVerificationError(f"postgres_source_authority.{suffix}")
    return SourcePhysicalIdentity(
        dialect="postgres",
        cluster_identifier=selected.system_identifier,
        database=selected.database.canonical_name,
        effective_principal=selected.effective_principal.canonical_name,
        session_principal=selected.session_principal.canonical_name,
        server_address=_optional_text(identity.get("server_address")),
        server_port=_optional_integer(identity.get("server_port")),
        topology_role=selected.topology_role,
        version=2 if selected.verification_profile == "physical_cluster" else 3,
        authority_sha256=selected.authority_sha256,
        timeline_id=selected.timeline_id,
        database_oid=selected.database.oid,
        effective_principal_oid=selected.effective_principal.oid,
        session_principal_oid=selected.session_principal.oid,
        schema=selected.relation.schema,
        schema_oid=selected.relation.namespace_oid,
        relation=selected.relation.relation,
        relation_oid=selected.relation.relation_oid,
        verification_profile=selected.verification_profile,
    )


def _expected_identity(
    selected: SelectedPostgresSourceAuthority,
) -> SourcePhysicalIdentity:
    return _verified_identity(
        selected,
        {
            "database_name": selected.database.canonical_name,
            "database_oid": selected.database.oid,
            "effective_principal": selected.effective_principal.canonical_name,
            "effective_principal_oid": selected.effective_principal.oid,
            "session_principal": selected.session_principal.canonical_name,
            "session_principal_oid": selected.session_principal.oid,
            "in_recovery": selected.topology_role == "standby",
            **(
                {
                    "system_identifier": selected.system_identifier,
                    "timeline_id": selected.timeline_id,
                }
                if selected.verification_profile == "physical_cluster"
                else {}
            ),
        },
        {
            "schema_name": selected.relation.schema,
            "relation_name": selected.relation.relation,
            "namespace_oid": selected.relation.namespace_oid,
            "relation_oid": selected.relation.relation_oid,
        },
    )


def _integer(value: Any) -> int:
    return 0 if isinstance(value, bool) or not isinstance(value, int) else value


def _optional_integer(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_text(value: Any) -> str | None:
    return None if value is None else str(value)


def _ascii_fold(value: str) -> str:
    return value.translate(str.maketrans(_ASCII_UPPER, _ASCII_LOWER))


__all__ = [
    "POSTGRES_SOURCE_AUTHORITY_SHA256_OPTION",
    "PostgresSourceAuthorityVerificationError",
    "PostgresSourceAuthorityVerifier",
    "read_postgres_timeline_authority",
]
