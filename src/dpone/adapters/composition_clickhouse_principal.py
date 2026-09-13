"""Fixed administrator commands for isolated, single-node ClickHouse 24.8 principals.

The injected client must provide bounded complete synchronous responses, no
retries, and redact the supplied authentication material. The gate owns durable
issuance intent and network-isolation authority; this adapter grants neither.
See ClickHouse CREATE/ALTER USER and system.users/system.grants documentation.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from ipaddress import ip_address, ip_network
from typing import Protocol
from uuid import UUID

from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_snapshot import SnapshotTarget


class ClickHouseAdminClient(Protocol):
    """Trusted complete-response administrator connection, never a worker client."""

    def command(self, statement: str, *, parameters: Mapping[str, object], redactions: tuple[str, ...]) -> None:
        """Complete one bounded synchronous statement once; uncertain ACK raises."""

    def query(
        self, statement: str, *, parameters: Mapping[str, object], max_rows: int
    ) -> tuple[tuple[object, ...], ...]:
        """Read complete bounded rows with global catalog/process visibility."""


def require_principal(condition: bool, reason: str) -> None:
    if not condition:
        raise CompositionAdmissionError("clickhouse_principal_" + reason)


def require_user(name: str) -> None:
    require_principal(type(name) is str and re.fullmatch(r"dpone_ch_[0-9a-f]{64}", name) is not None, "name")


def require_uuid(value: str) -> None:
    try:
        valid = str(UUID(value)) == value and UUID(value).int != 0
    except (ValueError, TypeError, AttributeError):
        valid = False
    require_principal(valid, "uuid")


@dataclass(frozen=True, slots=True)
class IssuedClickHouseCredentials:
    """Delivered once to the trusted dispatcher, never persisted in control SQL."""

    username: str
    user_id: str
    password: str = field(repr=False)


class ClickHousePrincipalAdmin:
    """Exact named-user UUID checks around every bounded administrator mutation."""

    def __init__(self, client: ClickHouseAdminClient) -> None:
        self._client = client

    def _command(self, sql: str, parameters: Mapping[str, object], redactions: tuple[str, ...] = ()) -> None:
        try:
            self._client.command(sql, parameters=parameters, redactions=redactions)
        except Exception:
            raise CompositionAdmissionError("clickhouse_principal_operation_unknown") from None

    def _query(self, sql: str, parameters: Mapping[str, object], maximum: int = 64) -> tuple[tuple[object, ...], ...]:
        try:
            rows = self._client.query(sql, parameters=parameters, max_rows=maximum)
        except Exception:
            raise CompositionAdmissionError("clickhouse_principal_observation_unknown") from None
        require_principal(type(rows) is tuple and len(rows) <= maximum and all(type(r) is tuple for r in rows), "rows")
        return rows

    def require_target(self, target: SnapshotTarget) -> None:
        """Bind this administrator connection to the real server and Atomic UUID."""
        target.__post_init__()
        rows = self._query(
            "SELECT version(), toString(serverUUID()), toString(uuid), engine FROM system.databases "
            "WHERE name={database:String} LIMIT 2",
            {"database": target.database},
            2,
        )
        require_principal(
            len(rows) == 1
            and len(rows[0]) == 4
            and type(rows[0][0]) is str
            and re.fullmatch(r"24\.8\.[0-9]+\.[0-9]+", rows[0][0]) is not None
            and rows[0][1:] == (target.service_id, target.database_id, "Atomic"),
            "target_identity",
        )

    def identity(self, name: str) -> str:
        require_user(name)
        rows = self._query("SELECT toString(id) FROM system.users WHERE name={user:String} LIMIT 2", {"user": name}, 2)
        require_principal(len(rows) == 1 and len(rows[0]) == 1 and type(rows[0][0]) is str, "identity")
        value = rows[0][0]
        assert isinstance(value, str)
        require_uuid(value)
        return value

    # 24.8 Access parsers consume literal strings, not query parameter ASTs.
    # Names, table subjects, IPs and generated hexadecimal hashes above/below
    # use closed grammars; only these validated values enter fixed DDL text.
    def create_disabled(self, name: str, password: str) -> str:
        """Create without replacement. Lost ACK leaves the gate intent unresolved."""
        require_user(name)
        require_principal(type(password) is str and bool(password), "password")
        digest = sha256(password.encode()).hexdigest()
        self._command(
            f"CREATE USER `{name}` IDENTIFIED WITH sha256_hash BY '{digest}' HOST NONE DEFAULT ROLE NONE GRANTEES NONE",
            {},
            (password, digest),
        )
        return self.identity(name)

    @staticmethod
    def grants(target: SnapshotTarget, purpose: str) -> tuple[tuple[str, str, str], ...]:
        target.__post_init__()
        require_principal(purpose in {"ingest", "publisher"}, "purpose")
        tables = (target.generation_table,) if purpose == "ingest" else (target.target_table, target.generation_table)
        privileges = (
            ("CREATE TABLE", "INSERT") if purpose == "ingest" else ("SELECT", "DROP TABLE", "CREATE TABLE", "INSERT")
        )
        return tuple(sorted((privilege, target.database, table) for table in tables for privilege in privileges))

    def grant_disabled(self, name: str, user_id: str, target: SnapshotTarget, purpose: str) -> None:
        """Only the exact generation or EXCHANGE pair can receive direct grants."""
        require_principal(self.identity(name) == user_id, "identity_changed")
        for privilege, database, table in self.grants(target, purpose):
            self._command(
                f"GRANT {privilege} ON `{database}`.`{table}` TO `{name}`",
                {},
            )
        self.observe(name, user_id, target, purpose, host=None, revoked=False)

    def enable(self, name: str, user_id: str, host: str) -> None:
        """Called only after durable ENABLING. The caller never retries this step."""
        require_principal(self.identity(name) == user_id, "identity_changed")
        address = str(ip_address(host))
        require_principal(
            address == host and not ip_address(host).is_unspecified and not ip_address(host).is_loopback, "host"
        )
        self._command(f"ALTER USER `{name}` HOST IP '{host}'", {})
        require_principal(self.identity(name) == user_id, "identity_changed")

    def enable_local(self, name: str, user_id: str) -> None:
        """Enable only for an independently enrolled private network namespace.

        LOCAL includes all namespace-local interfaces. The protected supervisor
        must prove loopback-only listeners and exclude workers/proxies first.
        """
        require_principal(self.identity(name) == user_id, "identity_changed")
        self._command(f"ALTER USER `{name}` HOST LOCAL", {})
        require_principal(self.identity(name) == user_id, "identity_changed")

    def revoke(self, name: str, user_id: str) -> None:
        """Monotonic disable and revoke; the UUID is retained and never dropped."""
        require_principal(self.identity(name) == user_id, "identity_changed")
        self._command(f"ALTER USER `{name}` HOST NONE", {})
        self._command(f"REVOKE ALL ON *.* FROM `{name}`", {})
        require_principal(self.identity(name) == user_id, "identity_changed")

    def observe(
        self, name: str, user_id: str, target: SnapshotTarget, purpose: str, *, host: str | None, revoked: bool
    ) -> dict[str, object]:
        """Reopen complete direct rights, auth/host restrictions and no inherited roles."""
        return self._observe(name, user_id, target, purpose, host=host, host_names=(), revoked=revoked)

    def observe_local(self, name: str, user_id: str, target: SnapshotTarget, purpose: str) -> dict[str, object]:
        """Require exact LOCAL catalog rights; no IP-to-LOCAL fallback exists."""
        return self._observe(name, user_id, target, purpose, host=None, host_names=("localhost",), revoked=False)

    def _observe(
        self,
        name: str,
        user_id: str,
        target: SnapshotTarget,
        purpose: str,
        *,
        host: str | None,
        host_names: tuple[str, ...],
        revoked: bool,
    ) -> dict[str, object]:
        require_user(name)
        require_uuid(user_id)
        expected = () if revoked else self.grants(target, purpose)
        rows = self._query(
            "SELECT toString(id), toString(auth_type), host_ip, host_names, "
            "host_names_regexp, host_names_like, default_roles_all, default_roles_list, default_roles_except, "
            "grantees_any, grantees_list, grantees_except FROM system.users WHERE name={user:String} LIMIT 2",
            {"user": name},
            2,
        )
        require_principal(len(rows) == 1 and len(rows[0]) == 12, "user_shape")
        value = rows[0]
        require_principal(value[0] == user_id and value[1] == "sha256_password", "auth_identity")
        ips = value[2]
        require_principal(type(ips) in (list, tuple), "host")
        assert isinstance(ips, (list, tuple))
        try:
            networks = tuple(str(ip_network(ip, strict=False)) for ip in ips)
            expected_hosts = () if host is None else (str(ip_network(host)),)
        except (ValueError, TypeError):
            raise CompositionAdmissionError("clickhouse_principal_host") from None
        require_principal(networks == expected_hosts, "host")
        require_principal(
            value[3] in (list(host_names), host_names)
            and all(value[i] in ([], ()) for i in (4, 5, 7, 8, 10, 11))
            and value[6] == 0
            and value[9] == 0,
            "inherited_access",
        )
        require_principal(
            not self._query(
                "SELECT granted_role_name FROM system.role_grants WHERE user_name={user:String} LIMIT 2",
                {"user": name},
                2,
            ),
            "roles",
        )
        rights = self._query(
            "SELECT toString(access_type), database, table, column, is_partial_revoke, grant_option "
            "FROM system.grants WHERE user_name={user:String} LIMIT 65",
            {"user": name},
            64,
        )
        require_principal(all(len(row) == 6 and row[3:] == (None, 0, 0) for row in rights), "grant_shape")
        require_principal(tuple(sorted(row[:3] for row in rights)) == expected, "grants")
        return {
            "user_id": user_id,
            "username": name,
            "host": "LOCAL" if host_names else host,
            "grants": expected,
            "roles": (),
            "revoked": revoked,
        }

    def require_quiescence(self, name: str) -> None:
        """Supplement the durable dispatcher barrier; empty processes alone is insufficient."""
        require_user(name)
        queries: tuple[tuple[str, Mapping[str, object]], ...] = (
            (
                "SELECT count() FROM system.processes WHERE user={user:String} OR initial_user={user:String}",
                {"user": name},
            ),
            ("SELECT count() FROM system.transactions", {}),
            ("SELECT count() FROM system.asynchronous_inserts", {}),
            ("SELECT count() FROM system.mutations WHERE NOT is_done", {}),
            ("SELECT count() FROM system.distribution_queue", {}),
        )
        for sql, parameters in queries:
            require_principal(self._query(sql, parameters, 1) == ((0,),), "not_quiescent")
