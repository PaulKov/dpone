"""Concrete SQL Server catalog observer and protected proof-service tests."""

from __future__ import annotations

import json
from collections.abc import Sequence

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

_DIGESTS = tuple("sha256:" + character * 64 for character in "1234")
_LIMITS = SqlServerDependencyLimits(4, 20, 40, 100_000)


class _Cursor:
    def __init__(self, source: _CatalogRows) -> None:
        self.source = source
        self.current = ""
        self.closed = False

    def execute(self, sql: str, *_parameters: object) -> _Cursor:
        self.current = sql
        self.source.executed.append(sql)
        self.source.parameters.append(tuple(_parameters))
        return self

    def fetchone(self) -> Sequence[object] | None:
        assert "HAS_PERMS_BY_NAME" in self.current
        return ("DWH", 1 if self.source.metadata_visible else 0)

    def fetchall(self) -> Sequence[Sequence[object]]:
        assert "sys.sql_expression_dependencies" in self.current
        return tuple(self.source.rows)

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self, source: _CatalogRows) -> None:
        self.source = source
        self.autocommit = True
        self.closed = False
        self.committed = False

    def cursor(self) -> _Cursor:
        return _Cursor(self.source)

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        pass

    def close(self) -> None:
        self.closed = True


class _CatalogRows:
    def __init__(self, rows: list[tuple[object, ...]], *, metadata_visible: bool = True) -> None:
        self.rows = rows
        self.metadata_visible = metadata_visible
        self.executed: list[str] = []
        self.parameters: list[tuple[object, ...]] = []
        self.connections = 0

    def connect(self) -> _Connection:
        self.connections += 1
        return _Connection(self)


class _Verifier:
    def __init__(self, allowed: bool = True) -> None:
        self.allowed = allowed

    def verify(self, _authority: SemanticRefreshCatalogAuthority) -> bool:
        return self.allowed


class _UnavailableCatalog:
    def observe(self, **_kwargs: object) -> None:
        raise RuntimeError("private SQL Server permission detail")


def _rows(*, root_type: str = "SQL_INLINE_TABLE_VALUED_FUNCTION") -> list[tuple[object, ...]]:
    return [
        (101, "raw", "events", "USER_TABLE", None, 0, 0, 0, None, None, None, None, 0, 0),
        (
            102,
            "raw",
            "event_view",
            "VIEW",
            "CREATE VIEW raw.event_view WITH SCHEMABINDING AS SELECT event_id FROM raw.events",
            1,
            0,
            0,
            102,
            101,
            None,
            None,
            0,
            0,
        ),
        (
            103,
            "raw",
            "events_for_day",
            root_type,
            (
                "CREATE FUNCTION raw.events_for_day() RETURNS TABLE WITH SCHEMABINDING "
                "AS RETURN SELECT event_id FROM raw.event_view"
            ),
            1,
            0,
            0,
            103,
            102,
            None,
            None,
            0,
            0,
        ),
        (999, "mart", "events", "USER_TABLE", None, 0, 0, 0, None, None, None, None, 0, 0),
    ]


def _authority() -> SemanticRefreshCatalogAuthority:
    return SemanticRefreshCatalogAuthority("DWH", "mssql-prod", _DIGESTS[0], _DIGESTS[1])


def _request() -> SemanticRefreshCatalogProofRequest:
    sql = "select event_id from DWH.raw.events_for_day()"
    return SemanticRefreshCatalogProofRequest(
        model_unique_id="model.analytics.events",
        compiled_sql_by_target={"certified_a": sql, "certified_b": sql},
        forbidden_relations=(("DWH", "mart", "events"),),
        target_relation=("DWH", "mart", "events"),
        limits=_LIMITS,
        policy_sha256=_DIGESTS[2],
        authority=_authority(),
    )


def _service(source: _CatalogRows, *, allowed: bool = True) -> SemanticRefreshCatalogProofService:
    return SemanticRefreshCatalogProofService(
        sql_proof=SqlglotSemanticRefreshSqlProof(),
        dependency_prover=SqlServerReadDependencyProver(),
        catalog=MssqlSemanticRefreshCatalogObserver(source.connect),
        authority_verifier=_Verifier(allowed),
    )


def test_catalog_observer_queries_required_sys_catalogs_and_proves_view_itvf_closure() -> None:
    source = _CatalogRows(_rows())

    receipt = _service(source).prove(_request())

    proof = receipt.dependency_proof
    assert proof.base_relation_object_ids == (101,)
    assert tuple((edge.from_object_id, edge.to_object_id) for edge in proof.dependency_edges) == (
        (102, 101),
        (103, 102),
    )
    executed = "\n".join(source.executed)
    assert "sys.objects" in executed
    assert "sys.sql_modules" in executed
    assert "sys.sql_expression_dependencies" in executed
    assert "sys.columns" in executed
    assert "VIEW DEFINITION" in executed
    assert "SET NOCOUNT ON;" in source.executed[-1]
    assert "OPENJSON" in executed
    assert "TOP (@maximum_rows)" in executed
    parameters = source.parameters[-1]
    assert parameters[:4] == (61, 20, 4, 100_000)
    assert json.loads(str(parameters[4])) == [
        {"database": "DWH", "name": "events", "schema": "mart"},
        {"database": "dwh", "name": "events_for_day", "schema": "raw"},
    ]


@pytest.mark.parametrize(
    ("root_type", "encrypted", "expected_code"),
    [
        ("SQL_SCALAR_FUNCTION", False, "DPONE_DBT_V2_OBJECT_TYPE_UNSUPPORTED"),
        ("VIEW", True, "DPONE_DBT_V2_MODULE_UNSUPPORTED"),
    ],
)
def test_scalar_and_encrypted_modules_fail_closed(
    root_type: str,
    encrypted: bool,
    expected_code: str,
) -> None:
    rows = _rows(root_type=root_type)
    if encrypted:
        root = list(rows[2])
        root[4] = None
        root[6] = 1
        rows[2] = tuple(root)

    with pytest.raises(SemanticRefreshCatalogProofError) as raised:
        _service(_CatalogRows(rows)).prove(_request())

    assert raised.value.code == expected_code


def test_missing_view_definition_or_protected_ddl_authority_never_becomes_empty_proof() -> None:
    invisible = _CatalogRows(_rows(), metadata_visible=False)
    with pytest.raises(SemanticRefreshCatalogProofError) as raised:
        _service(invisible).prove(_request())
    assert raised.value.code == "DPONE_DBT_V2_CATALOG_UNVERIFIED"

    uncalled = _CatalogRows(_rows())
    with pytest.raises(SemanticRefreshCatalogProofError) as raised:
        _service(uncalled, allowed=False).prove(_request())
    assert raised.value.code == "DPONE_DBT_V2_CATALOG_UNVERIFIED"
    assert uncalled.connections == 0


def test_catalog_adapter_failure_is_translated_to_stable_fail_closed_error() -> None:
    service = SemanticRefreshCatalogProofService(
        sql_proof=SqlglotSemanticRefreshSqlProof(),
        dependency_prover=SqlServerReadDependencyProver(),
        catalog=_UnavailableCatalog(),
        authority_verifier=_Verifier(),
    )

    with pytest.raises(SemanticRefreshCatalogProofError) as raised:
        service.prove(_request())

    assert raised.value.code == "DPONE_DBT_V2_CATALOG_UNVERIFIED"
    assert isinstance(raised.value.__cause__, RuntimeError)


def test_unresolved_same_database_dependency_row_is_unverified_not_dropped() -> None:
    rows = _rows()
    unresolved = list(rows[2])
    unresolved[9] = None
    rows[2] = tuple(unresolved)

    with pytest.raises(SemanticRefreshCatalogProofError) as raised:
        _service(_CatalogRows(rows)).prove(_request())

    assert raised.value.code == "DPONE_DBT_V2_CATALOG_UNVERIFIED"


def test_catalog_observation_row_and_definition_budgets_fail_before_proof() -> None:
    overflow_rows = [
        (index, "raw", f"event_{index}", "USER_TABLE", None, 0, 0, 0, None, None, None, None, 0, 0)
        for index in range(100, 161)
    ]
    with pytest.raises(SemanticRefreshCatalogProofError) as raised:
        _service(_CatalogRows(overflow_rows)).prove(_request())
    assert raised.value.code == "DPONE_DBT_V2_CATALOG_UNVERIFIED"

    oversized_definition = _rows()
    oversized = list(oversized_definition[2])
    oversized[4] = None
    oversized[13] = 1
    oversized_definition[2] = tuple(oversized)
    with pytest.raises(SemanticRefreshCatalogProofError) as raised:
        _service(_CatalogRows(oversized_definition)).prove(_request())
    assert raised.value.code == "DPONE_DBT_V2_CATALOG_UNVERIFIED"


def test_runtime_catalog_drift_blocks_immediately_before_dbt_callback() -> None:
    source = _CatalogRows(_rows())
    service = _service(source)
    expected = service.prove(_request())
    changed = list(source.rows[1])
    changed[4] = str(changed[4]) + " WHERE event_id > 0"
    source.rows[1] = tuple(changed)
    invoked = False

    def execute() -> None:
        nonlocal invoked
        invoked = True

    with pytest.raises(SemanticRefreshCatalogProofError) as raised:
        service.recheck_then_execute(request=_request(), expected=expected, execute=execute)

    assert raised.value.code == "DPONE_DBT_V2_CATALOG_DRIFT"
    assert invoked is False
