"""Pre-source-I/O SQL Server target/state atomicity handshake."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class MssqlAtomicRoutePreflightError(RuntimeError):
    """Fail-closed atomic-route topology or permission mismatch."""


@dataclass(frozen=True, slots=True)
class MssqlSessionIdentity:
    """Server/replica/principal facts resolved by SQL Server itself."""

    server_name: str
    machine_name: str
    instance_name: str
    replica_name: str
    effective_principal: str
    original_login: str

    @classmethod
    def read(cls, connector: Any) -> MssqlSessionIdentity:
        rows = connector.get_records(
            "SELECT "
            "CONVERT(nvarchar(128), SERVERPROPERTY('ServerName')) AS server_name, "
            "CONVERT(nvarchar(128), SERVERPROPERTY('MachineName')) AS machine_name, "
            "COALESCE(CONVERT(nvarchar(128), SERVERPROPERTY('InstanceName')), N'MSSQLSERVER') AS instance_name, "
            "COALESCE(CONVERT(nvarchar(128), SERVERPROPERTY('ComputerNamePhysicalNetBIOS')), "
            "CONVERT(nvarchar(128), SERVERPROPERTY('MachineName'))) AS replica_name, "
            "CONVERT(nvarchar(128), SUSER_SNAME()) AS effective_principal, "
            "CONVERT(nvarchar(128), ORIGINAL_LOGIN()) AS original_login",
            as_dict=True,
        )
        if not rows:
            raise MssqlAtomicRoutePreflightError("mssql_atomic_route_identity_unavailable")
        row = rows[0]
        values = {field: str(row.get(field) or "").strip() for field in cls.__dataclass_fields__}
        if any(not value for value in values.values()):
            raise MssqlAtomicRoutePreflightError("mssql_atomic_route_identity_incomplete")
        return cls(**values)

    @property
    def topology(self) -> tuple[str, str, str, str]:
        return (
            self.server_name.casefold(),
            self.machine_name.casefold(),
            self.instance_name.casefold(),
            self.replica_name.casefold(),
        )

    @property
    def principal(self) -> tuple[str, str]:
        return self.effective_principal.casefold(), self.original_login.casefold()


def require_atomic_mssql_route(target_connector: Any, state_storage: Any) -> MssqlSessionIdentity:
    """Prove one-instance/principal and direct cross-database DML capability.

    The state catalog/permission work happens before any PostgreSQL schema or
    payload read.  Zero-row DML is compiled under a rollback-only target
    transaction, proving the exact three-part path without changing data.
    """

    if getattr(state_storage, "atomicity", None) != "target_atomic":
        raise MssqlAtomicRoutePreflightError("mssql_atomic_route_target_atomic_state_required")
    state_connector = getattr(state_storage, "connector", None)
    if state_connector is None:
        raise MssqlAtomicRoutePreflightError("mssql_atomic_route_state_session_missing")
    if _requires_database_authority(state_storage):
        verify_database_authority = getattr(state_storage, "verify_database_authority", None)
        if not callable(verify_database_authority):
            raise MssqlAtomicRoutePreflightError("mssql_transaction.database_authority_verifier_required")
        verify_database_authority(target_connector)
    if _is_generic_transaction_storage(state_storage):
        target_identity = MssqlSessionIdentity.read(target_connector)
        state_identity = MssqlSessionIdentity.read(state_connector)
        _require_same_session_authority(target_identity, state_identity)
        from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState

        MssqlGenericTransactionState.from_state_storage(state_storage).preflight(target_connector)
        return target_identity
    ensure = getattr(state_storage, "create_state_table", None)
    if not callable(ensure):
        raise MssqlAtomicRoutePreflightError("mssql_atomic_route_state_preflight_missing")
    catalog_preflight = getattr(state_storage, "preflight_atomic_catalog", None)
    if not callable(catalog_preflight):
        raise MssqlAtomicRoutePreflightError("mssql_atomic_route_state_catalog_preflight_missing")
    ensure()
    catalog_preflight()
    target_identity = MssqlSessionIdentity.read(target_connector)
    state_identity = MssqlSessionIdentity.read(state_connector)
    _require_same_session_authority(target_identity, state_identity)
    _probe_cross_database_permissions(target_connector, state_storage)
    _probe_operational_state_permissions(state_connector, state_storage)
    return target_identity


def _is_generic_transaction_storage(state_storage: Any) -> bool:
    from dpone.runtime.state.mssql_generic_transaction_storage import (
        is_mssql_generic_transaction_storage,
    )

    return is_mssql_generic_transaction_storage(state_storage)


def _requires_database_authority(state_storage: Any) -> bool:
    from dpone.runtime.state.mssql import is_mssql_xmin_state_storage

    return _is_generic_transaction_storage(state_storage) or is_mssql_xmin_state_storage(state_storage)


def _require_same_session_authority(
    target_identity: MssqlSessionIdentity,
    state_identity: MssqlSessionIdentity,
) -> None:
    if target_identity.topology != state_identity.topology:
        raise MssqlAtomicRoutePreflightError("mssql_atomic_route_instance_or_replica_mismatch")
    if target_identity.principal != state_identity.principal:
        raise MssqlAtomicRoutePreflightError("mssql_atomic_route_effective_principal_mismatch")


def resolve_atomic_mssql_target(
    target_connector: Any,
    state_storage: Any,
    *,
    database: str,
    schema: str,
    table: str,
) -> Any:
    """Complete topology/catalog preflight and resolve one registry binding."""

    session = require_atomic_mssql_route(target_connector, state_storage)
    from dpone.runtime.state.mssql_target_identity import resolve_mssql_physical_target_identity

    return resolve_mssql_physical_target_identity(
        target_connector,
        session=session,
        database=database,
        schema=schema,
        table=table,
    )


def _probe_cross_database_permissions(target_connector: Any, state_storage: Any) -> None:
    database = str(getattr(state_storage, "database", "") or "").strip()
    schema = str(getattr(state_storage, "schema", "") or "").strip()
    names = {
        "state": (str(getattr(state_storage, "table", "") or ""), "state_key", "write"),
        "receipt": (str(getattr(state_storage, "receipt_table", "") or ""), "receipt_id", "insert"),
        "repair_authority": (
            str(getattr(state_storage, "repair_authority_table", "") or ""),
            "authority_id",
            "read",
        ),
        "repair_consumption": (
            str(getattr(state_storage, "repair_consumption_table", "") or ""),
            "authority_id",
            "insert",
        ),
    }
    if not database or not schema or any(not table for table, _column, _mode in names.values()):
        raise MssqlAtomicRoutePreflightError("mssql_atomic_route_state_location_incomplete")
    statements: list[str] = ["SET NOCOUNT ON;"]
    for table, column, mode in names.values():
        qualified = _qualified(target_connector, database, schema, table)
        quoted_column = target_connector.quote_identifier(column)
        statements.append(f"SELECT TOP (0) {quoted_column} FROM {qualified};")
        if mode == "write":
            statements.append(f"UPDATE {qualified} SET {quoted_column} = {quoted_column} WHERE 1 = 0;")
        if mode in {"write", "insert"}:
            statements.append(
                f"INSERT INTO {qualified} ({quoted_column}) SELECT {quoted_column} FROM {qualified} WHERE 1 = 0;"
            )
    _execute_rollback_probe(
        target_connector,
        statements,
        denial_code="mssql_atomic_route_cross_database_permission_denied",
    )


def _probe_operational_state_permissions(state_connector: Any, state_storage: Any) -> None:
    database = str(getattr(state_storage, "database", "") or "").strip()
    schema = str(getattr(state_storage, "schema", "") or "").strip()
    tables = (
        (str(getattr(state_storage, "run_table", "") or ""), "run_state_key"),
        (str(getattr(state_storage, "audit_table", "") or ""), "load_id"),
    )
    if not database or not schema or any(not table for table, _column in tables):
        raise MssqlAtomicRoutePreflightError("mssql_atomic_route_operational_state_location_incomplete")
    statements = ["SET NOCOUNT ON;"]
    for table, column in tables:
        qualified = _qualified(state_connector, database, schema, table)
        quoted_column = state_connector.quote_identifier(column)
        statements.extend(
            (
                f"SELECT TOP (0) {quoted_column} FROM {qualified};",
                f"UPDATE {qualified} SET {quoted_column} = {quoted_column} WHERE 1 = 0;",
                f"INSERT INTO {qualified} ({quoted_column}) SELECT {quoted_column} FROM {qualified} WHERE 1 = 0;",
            )
        )
    _execute_rollback_probe(
        state_connector,
        statements,
        denial_code="mssql_atomic_route_operational_state_permission_denied",
    )


def _execute_rollback_probe(connector: Any, statements: list[str], *, denial_code: str) -> None:
    started = False
    try:
        connector.begin()
        started = True
        connector.execute_query("\n".join(statements))
    except Exception as exc:
        raise MssqlAtomicRoutePreflightError(denial_code) from exc
    finally:
        if started:
            try:
                connector.rollback()
            except Exception as exc:
                raise MssqlAtomicRoutePreflightError("mssql_atomic_route_probe_rollback_failed") from exc


def _qualified(connector: Any, database: str, schema: str, table: str) -> str:
    try:
        return str(connector.qualified_name(schema, table, database=database))
    except TypeError:
        return str(connector.qualified_name(f"{database}.{schema}", table))


__all__ = [
    "MssqlAtomicRoutePreflightError",
    "MssqlSessionIdentity",
    "resolve_atomic_mssql_target",
    "require_atomic_mssql_route",
]
