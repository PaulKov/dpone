"""Isolated runners and probes for the XMin -> MSSQL live failure matrix.

The helpers deliberately reuse the disposable databases provisioned by the
main route fixture while opening independent source, target, and state
sessions.  That makes concurrent attempts and permission failures exercise
real PostgreSQL/SQL Server transactions instead of connector doubles.
"""

from __future__ import annotations

import copy
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sources.postgres import PostgresSource
from dpone.runtime.state.mssql import (
    MSSQLLoadAuditStorage,
    MSSQLRunStateStorage,
    MSSQLXMinStateStorage,
)
from tests.integration.postgres.postgres_live_support import postgres_connector
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    bind_factual_mssql_database_authority,
    bind_factual_postgres_source_authority,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    STATE_SCHEMA,
    QuietIntegrationLogger,
    SnapshotRouteEnvironment,
)


@dataclass(frozen=True, slots=True)
class EphemeralMssqlPrincipal:
    """Attempt-local SQL login used to prove a real permission rollback."""

    login: str
    password: str


class TransactionLockProbe:
    """Hold the first acquired application lock until both attempts request it."""

    def __init__(self) -> None:
        self.first_acquired = threading.Event()
        self.both_requested = threading.Event()
        self.release_first = threading.Event()
        self._guard = threading.Lock()
        self.request_count = 0
        self.acquired_count = 0

    def wrap(self, connector: Any) -> None:
        """Observe the real ``sp_getapplock`` call on one connector session."""

        real_get_records = connector.get_records

        def get_records(query: Any, params: Any = None, as_dict: bool = False) -> list[Any]:
            if "sp_getapplock" not in str(query).lower():
                return real_get_records(query, params, as_dict)
            with self._guard:
                self.request_count += 1
                if self.request_count == 2:
                    self.both_requested.set()
            rows = real_get_records(query, params, as_dict)
            with self._guard:
                self.acquired_count += 1
                acquired_first = self.acquired_count == 1
                if acquired_first:
                    self.first_acquired.set()
            if acquired_first and not self.release_first.wait(timeout=60):
                raise TimeoutError("vendor_live_first_applock_release_timeout")
            return rows

        connector.get_records = get_records


@contextmanager
def fork_snapshot_route(
    route: SnapshotRouteEnvironment,
    work_dir: Path,
    *,
    principal: EphemeralMssqlPrincipal | None = None,
) -> Iterator[SnapshotRouteEnvironment]:
    """Open a fully independent standard runtime against the same live route."""

    postgres = postgres_connector()
    target = _mssql_connector(route.target, route.target_database, principal)
    state = _mssql_connector(route.state, route.state_database, principal)
    try:
        options = copy.deepcopy(route.load_config.options)
        options["work_dir"] = str(work_dir)
        load_config = replace(route.load_config, options=options)
        logger = QuietIntegrationLogger()
        checkpoint = MSSQLXMinStateStorage(
            state,
            database=route.state_database,
            schema=STATE_SCHEMA,
            table="dpone_source_state",
            receipt_table="dpone_commit_receipt",
            run_table="dpone_run_state",
            audit_table="dpone_load_audit",
            atomicity="target_atomic",
            provisioning="external",
        )
        sink = MSSQLSink(target, state_storage=checkpoint, logger=logger)
        source = PostgresSource(postgres, checkpoint, logger, sink_connector=target)
        bind_factual_mssql_database_authority(
            checkpoint,
            route.master,
            target_database=route.target_database,
            staging_database=route.target_database,
            state_database=route.state_database,
            master_connector_factory=lambda _connection: _mssql_connector(route.master, "master", principal),
            staging_connector_factory=lambda _connection, database: _mssql_connector(
                route.target,
                database,
                principal,
            ),
        )
        bind_factual_postgres_source_authority(source, postgres, load_config=load_config)
        processor = ETLProcessor(
            source,
            sink,
            etl_logger=logger,
            run_state_storage=MSSQLRunStateStorage(
                state,
                database=route.state_database,
                schema=STATE_SCHEMA,
                table="dpone_run_state",
                provisioning="external",
            ),
            load_identity_service=LoadIdentityService(
                audit_storage=MSSQLLoadAuditStorage(
                    state,
                    database=route.state_database,
                    schema=STATE_SCHEMA,
                    table="dpone_load_audit",
                    provisioning="external",
                )
            ),
        )
        yield SnapshotRouteEnvironment(
            postgres=postgres,
            target=target,
            state=state,
            master=route.master,
            checkpoint_storage=checkpoint,
            processor=processor,
            load_config=load_config,
            source_schema=route.source_schema,
            target_database=route.target_database,
            state_database=route.state_database,
        )
    finally:
        for connector in (postgres, target, state):
            with suppress(Exception):
                connector.close()


@contextmanager
def provision_limited_principal(route: SnapshotRouteEnvironment) -> Iterator[EphemeralMssqlPrincipal]:
    """Create a non-owner state principal for the permission-denial proof."""

    suffix = uuid.uuid4().hex
    login = f"dpone_it_xmin_{suffix[:12]}"
    password = f"Dpone-{suffix}-A1!"
    quoted_login = f"[{login}]"
    escaped_password = password.replace("'", "''")
    route.master.execute_query(f"CREATE LOGIN {quoted_login} WITH PASSWORD = N'{escaped_password}', CHECK_POLICY = OFF")
    try:
        route.target.execute_query(f"CREATE USER {quoted_login} FOR LOGIN {quoted_login}")
        route.target.execute_query(f"ALTER ROLE [db_owner] ADD MEMBER {quoted_login}")
        route.state.execute_query(f"CREATE USER {quoted_login} FOR LOGIN {quoted_login}")
        route.state.execute_query(f"GRANT CONNECT TO {quoted_login}")
        route.state.execute_query(f"GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::[{STATE_SCHEMA}] TO {quoted_login}")
        route.state.execute_query(f"GRANT VIEW DEFINITION TO {quoted_login}")
        yield EphemeralMssqlPrincipal(login=login, password=password)
    finally:
        with suppress(Exception):
            route.master.execute_query(f"DROP LOGIN IF EXISTS {quoted_login}")


def deny_checkpoint_writes(
    route: SnapshotRouteEnvironment,
    principal: EphemeralMssqlPrincipal,
) -> None:
    """Deny only target-transaction checkpoint writes after live preflight."""

    quoted_login = f"[{principal.login}]"
    route.state.execute_query(f"DENY UPDATE ON OBJECT::[{STATE_SCHEMA}].[dpone_source_state] TO {quoted_login}")
    route.state.execute_query(f"DENY INSERT ON OBJECT::[{STATE_SCHEMA}].[dpone_commit_receipt] TO {quoted_login}")


def _mssql_connector(
    template: Any,
    database: str,
    principal: EphemeralMssqlPrincipal | None,
) -> MSSQLConnector:
    return MSSQLConnector(
        host=template.host,
        port=template.port,
        database=database,
        user=principal.login if principal else template.user,
        password=principal.password if principal else template.password,
        driver=template.driver,
        encrypt=template.encrypt,
        trust_server_certificate=template.trust_server_certificate,
        connect_timeout=template.connect_timeout,
        query_timeout=template.query_timeout,
        autocommit=template.autocommit,
        application_name="dpone-mssql-xmin-live-matrix",
        bcp_path=template.bcp_path,
        odbc_options=template.odbc_options,
    )


__all__ = [
    "EphemeralMssqlPrincipal",
    "TransactionLockProbe",
    "deny_checkpoint_writes",
    "fork_snapshot_route",
    "provision_limited_principal",
]
