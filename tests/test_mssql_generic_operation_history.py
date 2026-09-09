"""Read-only MSSQL operation-history migration contracts."""

from __future__ import annotations

from collections.abc import Iterable

import pytest

from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlOperationRequest,
    operation_owner_digest,
    operation_scope_hash,
)
from dpone.runtime.state.mssql_fresh_session import MssqlFreshSessionFactory
from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState


def test_operation_history_is_fresh_serializable_paged_and_read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    request = _request()
    scopes = tuple(operation_scope_hash({"chunk": index}) for index in range(3))
    ordered = tuple(sorted(scopes))
    operation_rows = [
        {
            "scope_hash": scope,
            "operation_key": (
                key := MssqlOperationRequest(scope, operation_owner_digest("irrelevant")).operation_key(request)
            ),
            "receipt_operation_key": key,
            "receipt_scope_hash": scope,
        }
        for scope in ordered
    ]
    fresh = _Session([[_attempt_row(request)], operation_rows[:2], operation_rows[2:]])
    monkeypatch.setattr("dpone.runtime.state.mssql_generic_operation_history._PAGE_SIZE", 2)
    catalog_checks: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_generic_operation_history.require_generic_transaction_catalog",
        lambda _connector, *, database, schema: catalog_checks.append((database, schema)),
    )
    state = MssqlGenericTransactionState(
        _Session([]),
        database="Example_System",
        schema="dbo",
        fresh_session_factory=MssqlFreshSessionFactory(lambda _template: fresh),
    )

    history = state.operation_history(request)

    assert history is not None
    assert history.attempt_key == request.attempt_key
    assert history.scope_hashes == frozenset(scopes)
    assert history.receipt_scope_hashes == frozenset(scopes)
    assert fresh.begins == fresh.rollbacks == 1
    assert fresh.commits == 0
    assert fresh.statements == ["SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;"]
    assert fresh.closed
    assert "HOLDLOCK" in fresh.calls[0][0]
    assert "TOP (2)" in fresh.calls[1][0]
    assert "LEFT JOIN" in fresh.calls[1][0]
    assert len(fresh.calls[1][1]) == 1 and len(fresh.calls[2][1]) == 2
    assert catalog_checks == [("Example_System", "dbo")]


def test_operation_history_rejects_wrong_attempt_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_generic_operation_history.require_generic_transaction_catalog",
        lambda *_args, **_kwargs: None,
    )
    request = _request()
    row = _attempt_row(request)
    row["invocation_digest"] = b"x" * 32
    fresh = _Session([[row]])
    state = MssqlGenericTransactionState(
        _Session([]),
        database="Example_System",
        schema="dbo",
        fresh_session_factory=MssqlFreshSessionFactory(lambda _template: fresh),
    )

    with pytest.raises(RuntimeError, match="attempt_identity_collision"):
        state.operation_history(request)

    assert fresh.rollbacks == 1
    assert fresh.commits == 0
    assert fresh.statements == ["SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;"]


def test_operation_history_absent_attempt_is_not_allocated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "dpone.runtime.state.mssql_generic_operation_history.require_generic_transaction_catalog",
        lambda *_args, **_kwargs: None,
    )
    fresh = _Session([[]])
    state = MssqlGenericTransactionState(
        _Session([]),
        database="Example_System",
        schema="dbo",
        fresh_session_factory=MssqlFreshSessionFactory(lambda _template: fresh),
    )

    assert state.operation_history(_request()) is None
    assert fresh.begins == fresh.rollbacks == 1
    assert fresh.commits == 0
    assert fresh.statements == ["SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;"]


def _request() -> MssqlAttemptRequest:
    return MssqlAttemptRequest(
        invocation=InvocationIdentity("dpone-backfill:legacy", "dag", "pipeline:task"),
        target_identity=b"t" * 32,
        route_fingerprint=b"r" * 32,
        load_id="proof",
        target_database="DWH",
        target_schema="dbo",
        target_table="events__shadow",
        strategy="backfill",
    )


def _attempt_row(request: MssqlAttemptRequest) -> dict[str, object]:
    return {
        "attempt_key": request.attempt_key,
        "target_identity": request.target_identity,
        "generation": 1,
        "invocation_digest": request.invocation.invocation_digest,
        "route_fingerprint": request.route_fingerprint,
        "target_database": request.target_database,
        "target_schema": request.target_schema,
        "target_table": request.target_table,
        "strategy": request.strategy,
    }


class _Session:
    def __init__(self, responses: Iterable[list[dict[str, object]]]) -> None:
        self._responses = iter(responses)
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.begins = 0
        self.rollbacks = 0
        self.commits = 0
        self.statements: list[str] = []
        self.closed = False

    def begin(self) -> None:
        self.begins += 1

    def get_records(
        self,
        sql: str,
        params: tuple[object, ...] = (),
        *,
        as_dict: bool,
    ) -> list[dict[str, object]]:
        assert as_dict
        self.calls.append((sql, params))
        return next(self._responses)

    def execute_query(self, sql: str, _params: object = None) -> None:
        self.statements.append(sql)

    def commit_transaction(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True
