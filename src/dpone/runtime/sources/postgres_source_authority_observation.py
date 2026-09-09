"""Least-privilege PostgreSQL source-identity observations."""

from __future__ import annotations

from typing import Any, NoReturn, Protocol


class PostgresSourceVerificationProfile(Protocol):
    """Read-only profile view needed by the observation adapter."""

    @property
    def verification_profile(self) -> str:
        """Return the reviewed source verification profile."""
        ...


class PostgresSourceAuthorityVerificationError(RuntimeError):
    """The RR-session source does not equal its signed registry authority."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def read_postgres_identity_observation(
    connector: Any,
    selected: PostgresSourceVerificationProfile,
) -> dict[str, Any]:
    """Read only the identity fields declared by the selected profile."""

    if selected.verification_profile == "physical_cluster":
        rows = _get_records(
            connector,
            """
            SELECT
                (pg_catalog.pg_control_system()).system_identifier::text AS system_identifier,
                current_database() AS database_name,
                d.oid::bigint AS database_oid,
                current_user AS effective_principal,
                effective_role.oid::bigint AS effective_principal_oid,
                session_user AS session_principal,
                session_role.oid::bigint AS session_principal_oid,
                pg_catalog.pg_is_in_recovery() AS in_recovery,
                pg_catalog.inet_server_addr()::text AS server_address,
                pg_catalog.inet_server_port() AS server_port
            FROM pg_catalog.pg_database AS d
            INNER JOIN pg_catalog.pg_roles AS effective_role
              ON effective_role.rolname = current_user
            INNER JOIN pg_catalog.pg_roles AS session_role
              ON session_role.rolname = session_user
            WHERE d.datname = current_database()
            """,
        )
    else:
        rows = _get_records(
            connector,
            """
            SELECT
                current_database() AS database_name,
                d.oid::bigint AS database_oid,
                current_user AS effective_principal,
                effective_role.oid::bigint AS effective_principal_oid,
                session_user AS session_principal,
                session_role.oid::bigint AS session_principal_oid,
                pg_catalog.pg_is_in_recovery() AS in_recovery,
                pg_catalog.inet_server_addr()::text AS server_address,
                pg_catalog.inet_server_port() AS server_port
            FROM pg_catalog.pg_database AS d
            INNER JOIN pg_catalog.pg_roles AS effective_role
              ON effective_role.rolname = current_user
            INNER JOIN pg_catalog.pg_roles AS session_role
              ON session_role.rolname = session_user
            WHERE d.datname = current_database()
            """,
        )
    if len(rows) != 1 or not isinstance(rows[0], dict):
        raise PostgresSourceAuthorityVerificationError("postgres_source_authority.identity_unavailable")
    identity = dict(rows[0])
    if selected.verification_profile == "physical_cluster":
        identity["timeline_id"] = read_postgres_timeline_authority(
            connector,
            in_recovery=bool(identity.get("in_recovery")),
        )
    return identity


def read_postgres_timeline_authority(connector: Any, *, in_recovery: bool) -> int:
    """Read a physical-cluster timeline for the version-1 authority profile."""

    if in_recovery:
        rows = _get_records(
            connector,
            """
            SELECT (pg_catalog.pg_control_checkpoint()).timeline_id::bigint
                   AS timeline_id
            """,
        )
    else:
        rows = _get_records(
            connector,
            """
            SELECT ('x' || substring(
                pg_catalog.pg_walfile_name(pg_catalog.pg_current_wal_lsn()),
                1,
                8
            ))::bit(32)::bigint AS timeline_id
            """,
        )
    if len(rows) != 1 or not isinstance(rows[0], dict):
        raise PostgresSourceAuthorityVerificationError("postgres_source_authority.timeline_unavailable")
    return timeline_id(rows[0].get("timeline_id"))


def raise_postgres_source_authority_query_error(exc: Exception) -> NoReturn:
    """Normalize only PostgreSQL permission denial; preserve all other errors."""

    if str(getattr(exc, "sqlstate", "") or "") == "42501":
        raise PostgresSourceAuthorityVerificationError("postgres_source_authority.metadata_permission_denied") from exc
    raise exc


def timeline_id(value: Any) -> int:
    """Require one positive uint32 PostgreSQL timeline identifier."""

    result = _integer(value)
    if not 0 < result < 2**32:
        raise PostgresSourceAuthorityVerificationError("postgres_source_authority.timeline_unavailable")
    return result


def _get_records(connector: Any, query: str) -> list[Any]:
    try:
        return connector.get_records(query, params=None, as_dict=True)
    except PostgresSourceAuthorityVerificationError:
        raise
    except Exception as exc:
        raise_postgres_source_authority_query_error(exc)


def _integer(value: Any) -> int:
    return 0 if isinstance(value, bool) or not isinstance(value, int) else value


__all__ = [
    "PostgresSourceAuthorityVerificationError",
    "raise_postgres_source_authority_query_error",
    "read_postgres_identity_observation",
    "read_postgres_timeline_authority",
    "timeline_id",
]
