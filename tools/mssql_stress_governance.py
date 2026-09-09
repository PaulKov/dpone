"""Disposable vendor fixture for governed PostgreSQL→MSSQL certification runs.

The runtime deliberately never provisions transaction-governance objects.
This operator-owned helper executes the frozen renderer unchanged, binds an
immutable target-local identity, and injects an externally provisioned
``target_atomic`` state storage into the real sink. Every context receives a
fresh state database so stale objects from an earlier certification run
cannot satisfy preflight. It is shared by live tests and release benchmarks;
production runtime code never invokes it implicitly.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING or __package__ == "tools":
    from tools.mssql_stress_governance_mssql import (
        bind_factual_mssql_database_authority,
        bind_target_identity,
        ensure_target_identity_registry,
        mssql_connector_for_database,
    )
else:  # Direct ``python tools/mssql_stress.py`` execution.
    from mssql_stress_governance_mssql import (
        bind_factual_mssql_database_authority,
        bind_target_identity,
        ensure_target_identity_registry,
        mssql_connector_for_database,
    )

from dpone.contracts.mssql_source_checkpoint import MssqlTransactionCheckpointMode
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
)
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sources.postgres import PostgresSource
from dpone.runtime.sources.postgres_source_authority import (
    POSTGRES_SOURCE_AUTHORITY_SHA256_OPTION,
    PostgresSourceAuthorityVerifier,
    read_postgres_timeline_authority,
)
from dpone.runtime.state.mssql_generic_transaction_ddl import (
    render_generic_transaction_catalog_ddl,
)
from dpone.runtime.state.mssql_generic_transaction_storage import (
    MssqlGenericTransactionStateStorage,
)

STATE_SCHEMA = "system"
_GO_SEPARATOR = re.compile(r"^\s*GO\s*$", re.IGNORECASE | re.MULTILINE)
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")


@dataclass(frozen=True, slots=True)
class GovernedMssqlRoute:
    """Exact target/state objects used by one disposable live route."""

    target: Any
    state: Any
    state_database: str
    state_storage: MssqlGenericTransactionStateStorage

    def sink(self, *, logger: Any) -> MSSQLSink:
        return MSSQLSink(self.target, state_storage=self.state_storage, logger=logger)

    def run_context(self, label: str) -> GovernedRunContext:
        """Return scheduler-stable identity for one logical invocation."""

        normalized = _safe(label.replace("-", "_"))
        return GovernedRunContext(
            run_id=f"integration:{normalized}:{uuid.uuid4().hex}",
            config={"pipeline_id": normalized, "task_id": "load"},
        )


@dataclass(frozen=True, slots=True)
class GovernedRunContext:
    """Minimal immutable scheduler identity consumed by admission."""

    run_id: str
    config: dict[str, str]


class GovernedEtlResult(dict[str, Any]):
    """ETL result view compatible with sink-result attribute assertions."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:  # pragma: no cover - ordinary assertion error path.
            raise AttributeError(name) from exc


class GovernedPostgresSnapshotSource(PostgresSource):
    """Real PostgreSQL source with an explicit complete-snapshot boundary.

    Generic SQL Server strategy certification needs to vary the target
    strategy without accidentally changing the source checkpoint protocol.
    This specialization keeps the production PostgreSQL catalog, projection,
    COPY artifact, lifecycle and physical-identity implementations, while
    making the reviewed complete-relation snapshot boundary explicit.  It is
    not used to claim support for PostgreSQL XMin or column-cursor routes.
    """

    def _resolve_strategy(self, _load_config: Any) -> Any:
        return self._full_extract

    def mssql_transaction_checkpoint_mode(
        self,
        _load_config: Any,
    ) -> MssqlTransactionCheckpointMode:
        """Declare the DI adapter's explicit complete snapshot as stateless.

        This override belongs only to the vendor capability harness.  The
        production :class:`PostgresSource` retains its XMin/column-cursor
        checkpoint gates for authored incremental PostgreSQL routes.
        """

        return MssqlTransactionCheckpointMode.STATELESS


class GovernedStandardEtlRunner:
    """Run real PostgreSQL→MSSQL ETL through one external governance route."""

    def __init__(
        self,
        route: GovernedMssqlRoute,
        postgres: Any,
        *,
        logger: Any,
        source_type: type[PostgresSource] = PostgresSource,
    ) -> None:
        from dpone.runtime.etl.processor import ETLProcessor

        self.route = route
        self.postgres = postgres
        self.sink = route.sink(logger=logger)
        self.source = source_type(postgres, None, logger, sink_connector=route.target)
        self._processor = ETLProcessor(
            self.source,
            self.sink,
            etl_logger=logger,
        )

    def run(
        self,
        load_config: Any,
        *,
        label: str,
        run_context: GovernedRunContext | None = None,
    ) -> GovernedEtlResult:
        """Execute one unique admitted invocation and expose its exact result."""

        bind_factual_postgres_source_authority(
            self.source,
            self.postgres,
            load_config=load_config,
        )
        context = run_context or self.route.run_context(label)
        result = self._processor.run(
            load_config,
            run_context=context,
            dag_id=f"DAG__integration__postgres_mssql__{_safe(label)}",
        )
        return GovernedEtlResult(result)


def factual_postgres_source_authority(
    postgres: Any,
    *,
    load_config: Any,
) -> Mapping[str, Any]:
    """Capture one closed authority document while provisioning live fixtures.

    This function is deliberately certification-only operator composition. It reads
    the disposable vendor before the governed invocation, authors a finite
    registry document, and never participates in production discovery or
    runtime self-learning.  Production subsequently verifies the immutable
    document on the exact branded repeatable-read session.
    """

    schema = str(load_config.source_schema)
    relation = str(load_config.source_table)
    rows = postgres.get_records(
        """
        SELECT
            (pg_catalog.pg_control_system()).system_identifier::text AS system_identifier,
            current_database() AS database_name,
            database_row.oid::bigint AS database_oid,
            current_user AS effective_principal,
            effective_role.oid::bigint AS effective_principal_oid,
            session_user AS session_principal,
            session_role.oid::bigint AS session_principal_oid,
            pg_catalog.pg_is_in_recovery() AS in_recovery,
            namespace_row.oid::bigint AS namespace_oid,
            relation_row.oid::bigint AS relation_oid,
            namespace_row.nspname AS schema_name,
            relation_row.relname AS relation_name
        FROM pg_catalog.pg_database AS database_row
        INNER JOIN pg_catalog.pg_roles AS effective_role
          ON effective_role.rolname = current_user
        INNER JOIN pg_catalog.pg_roles AS session_role
          ON session_role.rolname = session_user
        INNER JOIN pg_catalog.pg_namespace AS namespace_row
          ON namespace_row.nspname = %s
        INNER JOIN pg_catalog.pg_class AS relation_row
          ON relation_row.relnamespace = namespace_row.oid
         AND relation_row.relname = %s
         AND relation_row.relkind IN ('r', 'p', 'v', 'm', 'f')
        WHERE database_row.datname = current_database()
        """,
        params=(schema, relation),
        as_dict=True,
    )
    if len(rows) != 1:
        raise AssertionError("PostgreSQL source-authority fixture is not exact")
    row = dict(rows[0])
    if row["schema_name"] != schema or row["relation_name"] != relation:
        raise AssertionError("PostgreSQL source-authority fixture changed identifier spelling")
    timeline_id = read_postgres_timeline_authority(
        postgres,
        in_recovery=bool(row["in_recovery"]),
    )
    database = str(row["database_name"])
    configured_database = str(load_config.source_database or "").strip()
    if configured_database and configured_database != database:
        raise AssertionError("PostgreSQL source-authority fixture database drift")
    load_config.source_database = database
    return {
        "version": 1,
        "system_identifier": str(row["system_identifier"]),
        "timeline_id": timeline_id,
        "topology_role": "standby" if bool(row["in_recovery"]) else "primary",
        "database": {
            "canonical_name": database,
            "oid": int(row["database_oid"]),
        },
        "principals": {
            "effective": {
                "canonical_name": str(row["effective_principal"]),
                "oid": int(row["effective_principal_oid"]),
            },
            "session": {
                "canonical_name": str(row["session_principal"]),
                "oid": int(row["session_principal_oid"]),
            },
        },
        "relations": {
            f"{schema}.{relation}": {
                "schema": schema,
                "relation": relation,
                "namespace_oid": int(row["namespace_oid"]),
                "relation_oid": int(row["relation_oid"]),
            }
        },
    }


def bind_factual_postgres_source_authority(
    source: Any,
    postgres: Any,
    *,
    load_config: Any,
) -> Mapping[str, Any]:
    """Bind fixture-authored facts through the public source authority API."""

    identity_reader = getattr(source, "mssql_transaction_source_physical_identity", None)
    if callable(identity_reader):
        try:
            identity = identity_reader(load_config)
            _bind_source_authority_digest(load_config, identity.authority_sha256)
            return identity.to_dict()
        except RuntimeError as exc:
            if str(exc) != "mssql_transaction.postgres_source_authority_verifier_required":
                raise
    authority = factual_postgres_source_authority(
        postgres,
        load_config=load_config,
    )
    database = str(load_config.source_database)
    verifier = PostgresSourceAuthorityVerifier.from_connection(
        ResolvedBindingConnection(
            credentials=CredentialsConfig(database=database),
            safe_metadata={},
            descriptor=ResolvedConnectionDescriptor(
                connection_type="postgres",
                properties={
                    "database": database,
                    "postgres_source_authority": authority,
                },
            ),
        )
    )
    source.bind_postgres_source_authority(verifier)
    identity = verifier.preflight(load_config)
    _bind_source_authority_digest(load_config, identity.authority_sha256)
    return identity.to_dict()


def _bind_source_authority_digest(load_config: Any, digest: str | None) -> None:
    options = getattr(load_config, "options", None)
    if not isinstance(options, dict) or digest is None:
        raise AssertionError("PostgreSQL source-authority fixture options are unavailable")
    authored = options.get(POSTGRES_SOURCE_AUTHORITY_SHA256_OPTION)
    if authored not in (None, digest):
        raise AssertionError("PostgreSQL source-authority digest is runtime-owned")
    options[POSTGRES_SOURCE_AUTHORITY_SHA256_OPTION] = digest


@dataclass(frozen=True, slots=True)
class GovernedMssqlCampaign:
    """One frozen state authority shared by many isolated target cases."""

    target: Any
    state: Any
    target_database: str
    state_database: str
    state_storage: MssqlGenericTransactionStateStorage

    def route(self, *, target_schema: str, target_table: str) -> GovernedMssqlRoute:
        """Bind one exact target to the campaign state authority."""

        schema = _safe(target_schema)
        table = _safe(target_table)
        bind_target_identity(
            self.target,
            database=self.target_database,
            schema=schema,
            table=table,
        )
        return GovernedMssqlRoute(
            self.target,
            self.state,
            self.state_database,
            self.state_storage,
        )


@contextmanager
def governed_mssql_route(
    target: Any,
    *,
    target_database: str,
    target_schema: str,
    target_table: str,
) -> Iterator[GovernedMssqlRoute]:
    """Provision a fresh exact catalog and immutable target binding.

    Only disposable integration databases are mutated.  The rendered
    governance DDL is split solely on its explicit ``GO`` batch separators;
    no statement is edited or silently made idempotent.
    """

    with governed_mssql_campaign(
        target,
        target_database=target_database,
    ) as campaign:
        yield campaign.route(
            target_schema=target_schema,
            target_table=target_table,
        )


@contextmanager
def governed_mssql_campaign(
    target: Any,
    *,
    target_database: str,
) -> Iterator[GovernedMssqlCampaign]:
    """Provision one external governance catalog for a finite case matrix."""

    target_database = _safe(target_database)
    state_database = _safe(f"dpone_gov_{uuid.uuid4().hex[:16]}")
    master = mssql_connector_for_database(target, "master")
    state = None
    master.execute_query(f"CREATE DATABASE [{state_database}]")
    try:
        state = mssql_connector_for_database(target, state_database)
        state.execute_query(f"CREATE SCHEMA [{STATE_SCHEMA}] AUTHORIZATION [dbo]")
        ddl = render_generic_transaction_catalog_ddl(
            database=state_database,
            schema=STATE_SCHEMA,
        )
        for batch in _GO_SEPARATOR.split(ddl):
            if batch.strip():
                state.execute_query(batch)
        ensure_target_identity_registry(target, database=target_database)
        storage = MssqlGenericTransactionStateStorage(
            state,
            database=state_database,
            schema=STATE_SCHEMA,
        )
        bind_factual_mssql_database_authority(
            storage,
            master,
            target_database=target_database,
            staging_database=target_database,
            state_database=state_database,
        )
        yield GovernedMssqlCampaign(
            target,
            state,
            target_database,
            state_database,
            storage,
        )
    finally:
        if state is not None:
            with suppress(Exception):
                state.close()
        with suppress(Exception):
            master.execute_query(f"ALTER DATABASE [{state_database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        with suppress(Exception):
            master.execute_query(f"DROP DATABASE [{state_database}]")
        with suppress(Exception):
            master.close()


def _safe(value: str) -> str:
    if _SAFE_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"unsafe integration identifier: {value!r}")
    return value


__all__ = [
    "GovernedEtlResult",
    "GovernedMssqlCampaign",
    "GovernedPostgresSnapshotSource",
    "GovernedMssqlRoute",
    "GovernedRunContext",
    "GovernedStandardEtlRunner",
    "STATE_SCHEMA",
    "bind_factual_mssql_database_authority",
    "bind_factual_postgres_source_authority",
    "factual_postgres_source_authority",
    "governed_mssql_campaign",
    "governed_mssql_route",
]
