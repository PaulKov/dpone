"""SQL Server database authority catalog and staging-lease primitives."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.state.mssql_database_authority_errors import (
    MssqlDatabaseAuthorityVerificationError,
    MssqlStagingVerificationStage,
    detached_verification_error,
    sanitize_staging_verification_error,
)

if TYPE_CHECKING:
    from dpone.runtime.state.mssql_database_authority import MssqlDatabaseAuthorityVerifier

MSSQL_DATABASE_AUTHORITY_CONNECT_TIMEOUT_SECONDS = 10
_CONNECT_TIMEOUT_PARAMETER_NAMES = ("connect_timeout", "login_timeout", "LoginTimeout")
_CONNECT_TIMEOUT_PARAMETER_TOKENS = frozenset({"connecttimeout", "logintimeout"})


def master_connector(connection: ResolvedBindingConnection) -> Any:
    credentials = _authority_credentials(connection, database="master")
    return ResolvedConnectorFactory.create(replace(connection, credentials=credentials), autocommit=True)


def database_connector(connection: ResolvedBindingConnection, database: str) -> Any:
    credentials = _authority_credentials(connection, database=database)
    return ResolvedConnectorFactory.create(replace(connection, credentials=credentials), autocommit=True)


def bounded_authority_connection(
    connection: ResolvedBindingConnection,
    *,
    connect_timeout_seconds: int,
) -> ResolvedBindingConnection:
    """Return a detached connection whose authority login cannot outlive a budget."""

    if (
        isinstance(connect_timeout_seconds, bool)
        or not isinstance(connect_timeout_seconds, int)
        or connect_timeout_seconds < 1
    ):
        raise ValueError("MSSQL authority connect timeout must be a positive integer")
    credentials = _authority_credentials(
        connection,
        database=str(connection.credentials.database or "master"),
        maximum_timeout=connect_timeout_seconds,
    )
    return replace(connection, credentials=credentials)


def _authority_credentials(
    connection: ResolvedBindingConnection,
    *,
    database: str,
    maximum_timeout: int = MSSQL_DATABASE_AUTHORITY_CONNECT_TIMEOUT_SECONDS,
) -> Any:
    credentials = connection.credentials
    additional_params = dict(credentials.additional_params or {})
    configured_timeout = _configured_connect_timeout(credentials.connect_timeout, additional_params)
    bounded_timeout = (
        min(configured_timeout, maximum_timeout)
        if isinstance(configured_timeout, int) and not isinstance(configured_timeout, bool) and configured_timeout > 0
        else maximum_timeout
    )
    additional_params = {
        key: value
        for key, value in additional_params.items()
        if _parameter_token(key) not in _CONNECT_TIMEOUT_PARAMETER_TOKENS
    }
    additional_params["connect_timeout"] = bounded_timeout
    return replace(
        credentials,
        database=database,
        connect_timeout=bounded_timeout,
        additional_params=additional_params,
    )


def _configured_connect_timeout(default: int, additional_params: dict[str, Any]) -> int:
    for name in _CONNECT_TIMEOUT_PARAMETER_NAMES:
        if name in additional_params:
            return int(additional_params[name] or default)
    for key, value in additional_params.items():
        if _parameter_token(key) in _CONNECT_TIMEOUT_PARAMETER_TOKENS:
            return int(value or default)
    return int(default)


def _parameter_token(value: object) -> str:
    return "".join(character for character in str(value or "").lower() if character.isalnum())


@dataclass(slots=True)
class MssqlStagingDatabaseLease:
    """Live session preventing ordinary same-name staging substitution."""

    verifier: MssqlDatabaseAuthorityVerifier
    target_connector: Any = field(repr=False)
    staging_connector: Any = field(repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def assert_current(self, *, deadline: Any | None = None) -> None:
        """Prove the held session and master catalog still match the pin."""

        if self._closed:
            raise MssqlDatabaseAuthorityVerificationError("mssql_transaction.staging_database_lease_closed")
        caught: MssqlDatabaseAuthorityVerificationError | None = None
        try:
            self.verifier._verify_staging_lease(
                target_connector=self.target_connector,
                staging_connector=self.staging_connector,
                deadline=deadline,
            )
        except MssqlDatabaseAuthorityVerificationError as error:
            caught = detached_verification_error(error)
        except Exception:
            caught = MssqlDatabaseAuthorityVerificationError("mssql_transaction.staging_database_session_unavailable")
        if caught is not None:
            raise caught from None

    def close(self) -> None:
        """Close the held staging session exactly once."""

        if self._closed:
            return
        self._closed = True
        close_connector(self.staging_connector)

    def close_strict(self) -> None:
        """Close a retry-owned session or emit detached, fail-fast cleanup evidence."""

        if self._closed:
            return
        self._closed = True
        error = strict_close_connector_error(
            self.staging_connector,
            stage=MssqlStagingVerificationStage.STAGING_CLEANUP,
        )
        if error is not None:
            raise error from None


def pin_document(pin: MssqlDatabaseAuthorityPin) -> dict[str, Any]:
    return {
        "database_name": pin.database_name,
        "database_id": pin.database_id,
        "create_token": pin.create_token,
        "database_guid": str(pin.database_guid),
    }


def verify_pin(
    connector: Any,
    pin: MssqlDatabaseAuthorityPin,
    *,
    role: str,
    query_runner: Callable[[Callable[[], Any]], Any] | None = None,
) -> None:
    prefix = role_prefix(role)
    require_database_metadata_permission(connector, role=role, query_runner=query_runner)
    try:
        rows = _run_query(
            lambda: connector.get_records(
                """
                SELECT d.database_id, d.name AS database_name, d.state_desc,
                       d.user_access_desc, HAS_DBACCESS(d.name) AS has_dbaccess,
                       CONVERT(nvarchar(33), d.create_date, 126) AS create_token,
                       CONVERT(nvarchar(36), r.database_guid) AS database_guid
                FROM master.sys.databases AS d
                LEFT JOIN master.sys.database_recovery_status AS r
                  ON r.database_id = d.database_id
                WHERE d.database_id = ? OR d.name = ?
                """,
                (pin.database_id, pin.database_name),
                as_dict=True,
            ),
            query_runner=query_runner,
        )
    except MssqlDatabaseAuthorityVerificationError:
        raise
    except Exception as exc:
        raise MssqlDatabaseAuthorityVerificationError(f"{prefix}_database_authority_query_failed") from exc
    values = list(rows or ())
    if not values:
        raise MssqlDatabaseAuthorityVerificationError(f"{prefix}_database_missing")
    if len(values) != 1 or not isinstance(values[0], dict):
        raise MssqlDatabaseAuthorityVerificationError(f"{prefix}_database_identity_mismatch")
    row = values[0]
    if str(row.get("state_desc") or "").upper() != "ONLINE":
        raise MssqlDatabaseAuthorityVerificationError(f"{prefix}_database_offline")
    if str(row.get("user_access_desc") or "").upper() != "MULTI_USER":
        raise MssqlDatabaseAuthorityVerificationError(f"{prefix}_database_not_multi_user")
    if int(row.get("has_dbaccess") or 0) != 1:
        raise MssqlDatabaseAuthorityVerificationError(f"{prefix}_database_access_denied")
    actual_guid = str(row.get("database_guid") or "").strip().lower()
    if not actual_guid:
        raise MssqlDatabaseAuthorityVerificationError(f"{prefix}_database_guid_visibility_required")
    exact = (
        int(row.get("database_id") or 0) == pin.database_id
        and str(row.get("database_name") or "").strip() == pin.database_name
        and str(row.get("create_token") or "").strip() == pin.create_token
        and actual_guid == str(pin.database_guid)
    )
    if not exact:
        raise MssqlDatabaseAuthorityVerificationError(f"{prefix}_database_identity_mismatch")


def require_database_metadata_permission(
    connector: Any,
    *,
    role: str,
    query_runner: Callable[[Callable[[], Any]], Any] | None = None,
) -> None:
    """Require exact server metadata visibility before interpreting catalog rows."""

    try:
        rows = _run_query(
            lambda: connector.get_records(
                "SELECT HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW ANY DATABASE') AS permitted, "
                "IS_SRVROLEMEMBER('sysadmin') AS is_sysadmin",
                as_dict=True,
            ),
            query_runner=query_runner,
        )
    except MssqlDatabaseAuthorityVerificationError:
        raise
    except Exception as exc:
        raise MssqlDatabaseAuthorityVerificationError(f"{role_prefix(role)}_database_authority_query_failed") from exc
    values = list(rows or ())
    permitted = (
        len(values) == 1
        and isinstance(values[0], dict)
        and (int(values[0].get("permitted") or 0) == 1 or int(values[0].get("is_sysadmin") or 0) == 1)
    )
    if not permitted:
        raise MssqlDatabaseAuthorityVerificationError(f"{role_prefix(role)}_database_metadata_permission_denied")


def require_current_database(connector: Any, pin: MssqlDatabaseAuthorityPin, *, role: str) -> None:
    prefix = role_prefix(role)
    try:
        rows = connector.get_records("SELECT DB_ID() AS database_id, DB_NAME() AS database_name", as_dict=True)
    except Exception as exc:
        raise MssqlDatabaseAuthorityVerificationError(f"{prefix}_database_session_unavailable") from exc
    values = list(rows or ())
    exact = (
        len(values) == 1
        and isinstance(values[0], dict)
        and int(values[0].get("database_id") or 0) == pin.database_id
        and str(values[0].get("database_name") or "").strip() == pin.database_name
    )
    if not exact:
        raise MssqlDatabaseAuthorityVerificationError(f"{prefix}_database_session_mismatch")


def session_identity(connector: Any, *, role: str) -> Any:
    from dpone.runtime.state.mssql_route_preflight import MssqlSessionIdentity

    try:
        return MssqlSessionIdentity.read(connector)
    except Exception as exc:
        raise MssqlDatabaseAuthorityVerificationError(f"{role_prefix(role)}_database_session_unavailable") from exc


def require_same_session_authority(first: Any, second: Any, *, code: str) -> None:
    if first.topology != second.topology or first.principal != second.principal:
        raise MssqlDatabaseAuthorityVerificationError(code)


def role_prefix(role: str) -> str:
    if role not in {"target", "staging", "state"}:
        raise ValueError("MSSQL database role must be target, staging, or state")
    return f"mssql_transaction.{role}"


def close_connector(connector: Any) -> None:
    closer = getattr(connector, "close", None)
    if callable(closer):
        with suppress(Exception):
            closer()


def strict_close_connector_error(
    connector: Any,
    *,
    stage: MssqlStagingVerificationStage,
) -> MssqlDatabaseAuthorityVerificationError | None:
    """Attempt one close and return only detached evidence when it is unproven."""

    closer = getattr(connector, "close", None)
    if not callable(closer):
        return MssqlDatabaseAuthorityVerificationError(
            "mssql_transaction.staging_database_session_cleanup_failed",
            stage=stage,
        )
    caught: MssqlDatabaseAuthorityVerificationError | None = None
    try:
        closer()
    except MssqlDatabaseAuthorityVerificationError as error:
        caught = detached_verification_error(error)
    except Exception as error:
        caught = sanitize_staging_verification_error(
            error,
            stage=stage,
            default_code="mssql_transaction.staging_database_session_cleanup_failed",
        )
    return caught


def _run_query(
    operation: Callable[[], Any],
    *,
    query_runner: Callable[[Callable[[], Any]], Any] | None,
) -> Any:
    return query_runner(operation) if query_runner is not None else operation()


__all__ = [
    "MSSQL_DATABASE_AUTHORITY_CONNECT_TIMEOUT_SECONDS",
    "MssqlDatabaseAuthorityVerificationError",
    "MssqlStagingDatabaseLease",
    "bounded_authority_connection",
    "close_connector",
    "database_connector",
    "master_connector",
    "pin_document",
    "require_current_database",
    "require_same_session_authority",
    "session_identity",
    "verify_pin",
]
