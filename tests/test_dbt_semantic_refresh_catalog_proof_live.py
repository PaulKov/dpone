"""Opt-in Docker SQL Server proof for live module metadata and drift.

Run against the repository SQL Server service with::

    DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1 \
      uv run pytest tests/test_dbt_semantic_refresh_catalog_proof_live.py -q

This is local integration evidence, not production route certification.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import pytest

from dpone.adapters.dbt_semantic_refresh_mssql_catalog import (
    MssqlSemanticRefreshCatalogObserver,
)
from dpone.adapters.dbt_semantic_refresh_sql_proof import (
    SqlglotSemanticRefreshSqlProof,
)
from dpone.contracts.dbt_semantic_refresh_catalog_proof import (
    SemanticRefreshCatalogAuthority,
    SemanticRefreshCatalogProofError,
    SemanticRefreshCatalogProofRequest,
    SemanticRefreshCatalogProofService,
)
from dpone.contracts.dbt_semantic_refresh_dependency_proof import (
    SqlServerDependencyLimits,
    SqlServerReadDependencyProver,
)

pytestmark = [pytest.mark.integration_mssql, pytest.mark.integration_live]
_ENABLED = os.environ.get("DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE") == "1"
_SCHEMA = "dpone_sr_catalog"
_USER = "dpone_sr_catalog_no_view"
_DIGESTS = tuple("sha256:" + character * 64 for character in "1234")
_LIMITS = SqlServerDependencyLimits(4, 20, 40, 100_000)


class _ImpersonatedConnection:
    """Revert SQL Server execution context before pyodbc returns a pooled session."""

    def __init__(self, connection: Any) -> None:
        self._connection = connection

    @property
    def autocommit(self) -> bool:
        return bool(self._connection.autocommit)

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        self._connection.autocommit = value

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    def close(self) -> None:
        try:
            self._connection.cursor().execute("REVERT")
            self._connection.commit()
        finally:
            self._connection.close()


class _Verifier:
    def verify(self, _authority: SemanticRefreshCatalogAuthority) -> bool:
        return True


def _connect_factory(*, execute_as_user: str | None = None) -> Callable[[], Any]:
    pyodbc = pytest.importorskip("pyodbc")
    connection_string = (
        f"DRIVER={{{os.environ.get('DPONE_IT_MSSQL_DRIVER', 'ODBC Driver 18 for SQL Server')}}};"
        f"SERVER={os.environ.get('DPONE_IT_MSSQL_HOST', '127.0.0.1')},"
        f"{os.environ.get('DPONE_IT_MSSQL_PORT_FORWARD', '51433')};"
        f"DATABASE={os.environ.get('DPONE_IT_MSSQL_DATABASE', 'dpone_it')};"
        f"UID={os.environ.get('DPONE_IT_MSSQL_USER', 'sa')};"
        f"PWD={os.environ.get('DPONE_IT_MSSQL_PASSWORD', 'Dp0ne.Strong.Pw.2026!')};"
        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=15;"
    )

    def connect() -> Any:
        connection = pyodbc.connect(connection_string, autocommit=False)
        if execute_as_user is not None:
            if execute_as_user != _USER:
                raise ValueError("unexpected SQL Server impersonation identity")
            connection.cursor().execute(f"EXECUTE AS USER = '{_USER}'")
            return _ImpersonatedConnection(connection)
        return connection

    return connect


def _service(factory: Callable[[], Any]) -> SemanticRefreshCatalogProofService:
    return SemanticRefreshCatalogProofService(
        sql_proof=SqlglotSemanticRefreshSqlProof(),
        dependency_prover=SqlServerReadDependencyProver(),
        catalog=MssqlSemanticRefreshCatalogObserver(factory),
        authority_verifier=_Verifier(),
    )


def _request(database: str, relation: str, *, function_call: bool = False) -> SemanticRefreshCatalogProofRequest:
    suffix = "()" if function_call else ""
    sql = f"select event_id from {database}.{_SCHEMA}.{relation}{suffix}"
    return SemanticRefreshCatalogProofRequest(
        model_unique_id=f"model.catalog.{relation}",
        compiled_sql_by_target={"certified_a": sql, "certified_b": sql},
        forbidden_relations=((database, _SCHEMA, "target_events"),),
        target_relation=(database, _SCHEMA, "target_events"),
        limits=_LIMITS,
        policy_sha256=_DIGESTS[2],
        authority=SemanticRefreshCatalogAuthority(database, "mssql-docker", _DIGESTS[0], _DIGESTS[1]),
    )


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_SEMANTIC_REFRESH_MSSQL_LIVE=1")
def test_live_catalog_proves_view_itvf_and_blocks_scalar_encrypted_visibility_and_drift() -> None:
    factory = _connect_factory()
    setup = factory()
    setup.autocommit = True
    try:
        database = str(setup.cursor().execute("SELECT DB_NAME()").fetchone()[0])
        _reset(setup)
        _create_objects(setup)
        service = _service(factory)

        proven = service.prove(_request(database, "events_for_day", function_call=True))
        assert proven.dependency_proof.base_relation_object_ids
        assert len(proven.dependency_proof.dependency_edges) == 2

        with pytest.raises(SemanticRefreshCatalogProofError) as scalar:
            service.prove(_request(database, "event_scalar"))
        assert scalar.value.code == "DPONE_DBT_V2_OBJECT_TYPE_UNSUPPORTED"

        with pytest.raises(SemanticRefreshCatalogProofError) as encrypted:
            service.prove(_request(database, "encrypted_events"))
        assert encrypted.value.code == "DPONE_DBT_V2_MODULE_UNSUPPORTED"

        with pytest.raises(SemanticRefreshCatalogProofError) as invisible:
            _service(_connect_factory(execute_as_user=_USER)).prove(
                _request(database, "events_for_day", function_call=True)
            )
        assert invisible.value.code == "DPONE_DBT_V2_CATALOG_UNVERIFIED"

        _execute(
            setup,
            f"ALTER FUNCTION [{_SCHEMA}].[events_for_day]() RETURNS TABLE WITH SCHEMABINDING AS "
            f"RETURN SELECT event_id FROM [{_SCHEMA}].[event_view] WHERE event_id > 0",
        )
        invoked = False

        def execute() -> None:
            nonlocal invoked
            invoked = True

        with pytest.raises(SemanticRefreshCatalogProofError) as drift:
            service.recheck_then_execute(
                request=_request(database, "events_for_day", function_call=True),
                expected=proven,
                execute=execute,
            )
        assert drift.value.code == "DPONE_DBT_V2_CATALOG_DRIFT"
        assert invoked is False
    finally:
        try:
            _reset(setup)
        finally:
            setup.close()


def _create_objects(connection: Any) -> None:
    for statement in (
        f"CREATE SCHEMA [{_SCHEMA}] AUTHORIZATION [dbo]",
        f"CREATE TABLE [{_SCHEMA}].[base_events] (event_id bigint NOT NULL PRIMARY KEY)",
        f"CREATE TABLE [{_SCHEMA}].[target_events] (event_id bigint NOT NULL PRIMARY KEY)",
        f"CREATE VIEW [{_SCHEMA}].[event_view] WITH SCHEMABINDING AS SELECT event_id FROM [{_SCHEMA}].[base_events]",
        f"CREATE FUNCTION [{_SCHEMA}].[events_for_day]() RETURNS TABLE WITH SCHEMABINDING AS "
        f"RETURN SELECT event_id FROM [{_SCHEMA}].[event_view]",
        f"CREATE FUNCTION [{_SCHEMA}].[event_scalar]() RETURNS bigint WITH SCHEMABINDING AS "
        "BEGIN RETURN CONVERT(bigint, 1) END",
        f"CREATE VIEW [{_SCHEMA}].[encrypted_events] WITH SCHEMABINDING, ENCRYPTION AS "
        f"SELECT event_id FROM [{_SCHEMA}].[base_events]",
        f"CREATE USER [{_USER}] WITHOUT LOGIN",
        f"DENY VIEW DEFINITION TO [{_USER}]",
    ):
        _execute(connection, statement)


def _reset(connection: Any) -> None:
    statements = (
        f"IF OBJECT_ID(N'[{_SCHEMA}].[encrypted_events]', N'V') IS NOT NULL DROP VIEW [{_SCHEMA}].[encrypted_events]",
        f"IF OBJECT_ID(N'[{_SCHEMA}].[event_scalar]') IS NOT NULL DROP FUNCTION [{_SCHEMA}].[event_scalar]",
        f"IF OBJECT_ID(N'[{_SCHEMA}].[events_for_day]') IS NOT NULL DROP FUNCTION [{_SCHEMA}].[events_for_day]",
        f"IF OBJECT_ID(N'[{_SCHEMA}].[event_view]', N'V') IS NOT NULL DROP VIEW [{_SCHEMA}].[event_view]",
        f"IF OBJECT_ID(N'[{_SCHEMA}].[target_events]', N'U') IS NOT NULL DROP TABLE [{_SCHEMA}].[target_events]",
        f"IF OBJECT_ID(N'[{_SCHEMA}].[base_events]', N'U') IS NOT NULL DROP TABLE [{_SCHEMA}].[base_events]",
        f"IF SCHEMA_ID(N'{_SCHEMA}') IS NOT NULL EXEC(N'DROP SCHEMA [{_SCHEMA}]')",
        f"IF USER_ID(N'{_USER}') IS NOT NULL DROP USER [{_USER}]",
    )
    for statement in statements:
        _execute(connection, statement)


def _execute(connection: Any, statement: str) -> None:
    connection.cursor().execute(statement)
