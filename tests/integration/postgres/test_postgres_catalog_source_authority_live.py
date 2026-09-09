"""Live least-privilege proof for PostgreSQL catalog source authority."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import pytest
from psycopg import sql

from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.sources.postgres_source_authority import PostgresSourceAuthorityVerifier
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    issue_repeatable_read_snapshot_lease,
)
from tests.integration.postgres.postgres_live_support import (
    postgres_connector,
    postgres_enabled,
    wait_until_ready,
)

pytestmark = pytest.mark.skipif(
    not postgres_enabled(),
    reason="set DPONE_RUN_INTEGRATION=1 for Docker PostgreSQL",
)


def test_catalog_identity_needs_only_read_only_catalog_and_relation_access_live() -> None:
    """Prove the v2 verifier never touches privileged physical/statistics ports."""

    admin = postgres_connector()
    wait_until_ready("postgres", lambda: admin.get_records("SELECT 1"))
    suffix = uuid.uuid4().hex[:12]
    role = f"dpone_catalog_reader_{suffix}"
    table = f"catalog_authority_{suffix}"
    password = f"LocalOnly.{suffix}.Password"
    limited = None
    try:
        admin.execute_query(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(role),
                sql.Literal(password),
            )
        )
        admin.execute_query(
            sql.SQL("CREATE TABLE {}.{} (id integer PRIMARY KEY)").format(
                sql.Identifier("public"),
                sql.Identifier(table),
            )
        )
        admin.execute_query(
            sql.SQL("INSERT INTO {}.{} VALUES (1)").format(
                sql.Identifier("public"),
                sql.Identifier(table),
            )
        )
        admin.execute_query(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(admin.database),
                sql.Identifier(role),
            )
        )
        admin.execute_query(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
        admin.execute_query(
            sql.SQL("GRANT SELECT ON {}.{} TO {}").format(
                sql.Identifier("public"),
                sql.Identifier(table),
                sql.Identifier(role),
            )
        )
        facts = admin.get_records(
            """
            SELECT d.oid::bigint AS database_oid,
                   r.oid::bigint AS principal_oid,
                   n.oid::bigint AS namespace_oid,
                   c.oid::bigint AS relation_oid
            FROM pg_catalog.pg_database AS d
            CROSS JOIN pg_catalog.pg_roles AS r
            CROSS JOIN pg_catalog.pg_namespace AS n
            INNER JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid
            WHERE d.datname = current_database()
              AND r.rolname = %s
              AND n.nspname = 'public'
              AND c.relname = %s
            """,
            (role, table),
            as_dict=True,
        )
        assert len(facts) == 1
        fact = facts[0]
        connection = _resolved_connection(
            database=admin.database,
            role=role,
            table=table,
            fact=fact,
        )
        verifier = PostgresSourceAuthorityVerifier.from_connection(connection)
        config = SimpleNamespace(source_schema="public", source_table=table)

        raw = type(admin)(
            host=admin.host,
            port=admin.port,
            database=admin.database,
            user=role,
            password=password,
            autocommit=False,
        )
        limited = _RecordingConnector(raw)
        limited.execute_query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        token = limited.get_records(
            "SELECT pg_catalog.pg_current_snapshot()::text AS snapshot_token",
            as_dict=True,
        )[0]["snapshot_token"]
        lease = issue_repeatable_read_snapshot_lease(
            connector=limited,
            lifecycle=ExtractionLifecycleAuthority(),
            raw_snapshot_token=str(token),
        )

        observed = verifier.verify_snapshot(
            connector=limited,
            snapshot_lease=lease,
            load_config=config,
        )
        memberships = limited.get_records(
            """
            SELECT pg_catalog.pg_has_role(current_user, 'pg_read_all_stats', 'member') AS read_all_stats,
                   pg_catalog.pg_has_role(current_user, 'pg_monitor', 'member') AS monitor
            """,
            as_dict=True,
        )[0]

        assert memberships == {"read_all_stats": False, "monitor": False}
        assert observed.version == 3
        assert observed.verification_profile == "catalog_identity"
        assert observed.cluster_identifier is None
        assert observed.timeline_id is None
        assert observed.database_oid == int(fact["database_oid"])
        assert observed.relation_oid == int(fact["relation_oid"])
        executed = "\n".join(limited.queries).lower()
        assert "pg_control_" not in executed
        assert "pg_stat_" not in executed
        assert "pg_wal" not in executed
        limited.commit_transaction()
    finally:
        if limited is not None:
            limited.close()
        admin.execute_query(
            sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                sql.Identifier("public"),
                sql.Identifier(table),
            )
        )
        admin.execute_query(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
        admin.execute_query(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))
        admin.close()


class _RecordingConnector:
    """Record statement shapes while preserving one exact physical session."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector
        self.queries: list[str] = []

    @property
    def connection(self) -> Any:
        return self._connector.connection

    def execute_query(self, query: Any, params: Any = None) -> int:
        self.queries.append(str(query))
        return self._connector.execute_query(query, params)

    def get_records(
        self,
        query: Any,
        params: Any = None,
        *,
        as_dict: bool = False,
    ) -> list[Any]:
        self.queries.append(str(query))
        return self._connector.get_records(query, params=params, as_dict=as_dict)

    def commit_transaction(self) -> None:
        self._connector.commit_transaction()

    def close(self) -> None:
        self._connector.close()


def _resolved_connection(
    *,
    database: str,
    role: str,
    table: str,
    fact: dict[str, Any],
) -> ResolvedBindingConnection:
    authority = {
        "version": 2,
        "verification_profile": "catalog_identity",
        "topology_role": "primary",
        "database": {"canonical_name": database, "oid": int(fact["database_oid"])},
        "principals": {
            "effective": {"canonical_name": role, "oid": int(fact["principal_oid"])},
            "session": {"canonical_name": role, "oid": int(fact["principal_oid"])},
        },
        "relations": {
            f"public.{table}": {
                "schema": "public",
                "relation": table,
                "namespace_oid": int(fact["namespace_oid"]),
                "relation_oid": int(fact["relation_oid"]),
            }
        },
    }
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(database=database),
        safe_metadata={},
        descriptor=ResolvedConnectionDescriptor(
            connection_type="postgres",
            properties={"database": database, "postgres_source_authority": authority},
        ),
    )
