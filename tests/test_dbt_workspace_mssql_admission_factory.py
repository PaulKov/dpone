"""The coordinator rebuilds control admission from the sealed logical binding."""

from __future__ import annotations

from dpone.app.dbt_workspace_mssql_admission_factory import DbtWorkspaceMssqlAdmissionFactory
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig


class _Resolver:
    def __init__(self, connection: ResolvedBindingConnection) -> None:
        self.connection = connection
        self.refs: list[str] = []

    def resolve(self, connection_ref: str) -> ResolvedBindingConnection:
        self.refs.append(connection_ref)
        return self.connection


def _connection(connection_type: str = "mssql") -> ResolvedBindingConnection:
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(
            host="sql.internal",
            database="dpone_control",
            username="runtime",
            password="secret",
        ),
        safe_metadata={"resolver": "test"},
        descriptor=ResolvedConnectionDescriptor(connection_type, {}),
    )


def test_factory_resolves_only_protected_control_binding() -> None:
    resolver = _Resolver(_connection())

    admission = DbtWorkspaceMssqlAdmissionFactory("dpone_control").build(resolver)

    assert admission is not None
    assert resolver.refs == ["dpone_control"]


def test_factory_rejects_non_mssql_authority() -> None:
    resolver = _Resolver(_connection("postgres"))

    try:
        DbtWorkspaceMssqlAdmissionFactory("dpone_control").build(resolver)
    except ValueError as exc:
        assert "SQL Server" in str(exc)
    else:
        raise AssertionError("non-MSSQL authority was accepted")
