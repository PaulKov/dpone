"""Registration callbacks cannot change its transaction or current trust revision.

These DB-API observations are offline doubles, not SQL transaction certification.
The original registration, trust, codec and membership policies run unchanged.
"""

from __future__ import annotations

from typing import Any

import pytest

from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from tests.nonproduction_authority_helpers import NOW, execution, qualification
from tests.nonproduction_signature_helpers import github_policy
from tests.test_nonproduction_mssql_registration import inputs, register, setup


def change_boundary(database: Any, change: str) -> None:
    if change == "transaction":
        database.transaction_id += 1
    elif change == "revision":
        database.snapshots[database.revision + 1] = database.snapshots[database.revision]
        database.revision += 1
    elif change == "lock":
        database.lock = (1, 1, "NoLock")
    else:
        database.service = "00000000-0000-0000-0000-000000000000"


def reason(change: str) -> str:
    return {"revision": "trust_revision_changed", "authority": "trust_control_authority"}.get(
        change, "trust_ledger_lock"
    )


def invoke(database: Any, ledger: Any, store: Any, mode: str) -> Any:
    if mode in {"present", "missing"}:
        return store.read_in(
            ledger, execution(github_policy()).consumption_subject_sha256, expected_revision=database.expected
        )
    if mode == "qualification":
        return store.consume_qualification_in(
            ledger, **inputs(qualification(github_policy()), github_policy()), expected_revision=database.expected
        )
    return register(store, ledger, database)


@pytest.mark.parametrize(
    "mode,clock_call",
    [("present", 1), ("missing", 1), ("repeat", 1), ("new", 1), ("new", 2), ("qualification", 1), ("qualification", 2)],
)
@pytest.mark.parametrize("change", ["transaction", "revision", "lock", "authority"])
def test_each_clock_rechecks_actual_boundary_before_read_return_or_insert(
    mode: str, clock_call: int, change: str
) -> None:
    armed, calls = False, 0

    def clock() -> Any:
        nonlocal calls
        if armed:
            calls += 1
            if calls == clock_call:
                change_boundary(database, change)
        return NOW

    database, ledger, store = setup(clock=clock)
    if mode in {"present", "repeat"}:
        register(store, ledger, database)
    baseline = database.grants[:], database.members[:]
    database.commands.clear()
    armed = True
    with pytest.raises(NonproductionAuthorityError, match=reason(change)):
        invoke(database, ledger, store, mode)
    assert baseline == (database.grants, database.members)
    assert not any(sql.startswith("INSERT") for sql, _ in database.commands)


@pytest.mark.parametrize("point", ["catalog", "exact", "members", "page", "historical", "options", "readback"])
@pytest.mark.parametrize("change", ["transaction", "revision", "lock", "authority"])
def test_observation_callbacks_cannot_cross_registration_boundary(
    monkeypatch: pytest.MonkeyPatch, point: str, change: str
) -> None:
    database, ledger, store = setup()
    if point in {"exact", "members", "page", "historical"}:
        register(store, ledger, database)
    if point == "historical":
        change_boundary(database, "revision")
    original = database.execute
    triggered = False

    def execute(sql: str, *parameters: object) -> Any:
        nonlocal triggered
        observed = original(sql, *parameters)
        selected = {
            "catalog": "sys.security_predicates" in sql
            and isinstance(parameters[0], str)
            and parameters[0].endswith("_memberships]"),
            "exact": "WHERE consumption_subject_sha256 = ?" in sql,
            "members": "FROM [dpone_control].[composition_nonproduction_memberships]" in sql,
            "page": sql.startswith("SELECT TOP (1) consumption_subject_sha256"),
            "historical": "AND revision = ?" in sql,
            "options": sql == "SELECT @@OPTIONS;",
            "readback": "WHERE consumption_subject_sha256 = ?" in sql and bool(database.grants),
        }[point]
        if selected and not triggered:
            triggered = True
            change_boundary(database, change)
        return observed

    monkeypatch.setattr(database, "execute", execute)
    before = database.grants[:], database.members[:]
    database.commands.clear()
    with pytest.raises(NonproductionAuthorityError, match=reason(change)):
        invoke(database, ledger, store, "present" if point in {"exact", "members", "page", "historical"} else "new")
    assert triggered
    if point != "readback":
        assert before == (database.grants, database.members)
        assert not any(sql.startswith("INSERT") for sql, _ in database.commands)
    else:
        assert len(database.grants) == 1 and len(database.members) == 3  # Caller must roll back.


@pytest.mark.parametrize("mode", ["present", "missing", "repeat", "new", "qualification"])
@pytest.mark.parametrize("change", ["transaction", "revision"])
def test_successful_exit_rechecks_after_complete_readback(
    monkeypatch: pytest.MonkeyPatch, mode: str, change: str
) -> None:
    database, ledger, store = setup()
    if mode in {"present", "repeat"}:
        register(store, ledger, database)
    original = store._read
    changed = False

    def read(*args: Any, **kwargs: Any) -> Any:
        nonlocal changed
        result = original(*args, **kwargs)
        if not changed and (result is not None or mode == "missing"):
            changed = True
            change_boundary(database, change)
        return result

    monkeypatch.setattr(store, "_read", read)
    with pytest.raises(NonproductionAuthorityError, match=reason(change)):
        invoke(database, ledger, store, mode)
    assert changed


@pytest.mark.parametrize("change", ["transaction", "revision", "lock", "authority"])
def test_capture_precedes_initial_trust_callback(monkeypatch: pytest.MonkeyPatch, change: str) -> None:
    database, ledger, store = setup()
    compare = store._trust.require_revision_in
    calls = 0

    def changed_compare(*args: Any) -> Any:
        nonlocal calls
        observed = compare(*args)
        calls += 1
        if calls == 1:
            change_boundary(database, change)
        return observed

    monkeypatch.setattr(store._trust, "require_revision_in", changed_compare)
    with pytest.raises(NonproductionAuthorityError, match=reason(change)):
        register(store, ledger, database)
    assert not database.grants and not database.members


@pytest.mark.parametrize("written", [1, 2, 3, 4])
@pytest.mark.parametrize("change", ["transaction", "revision", "lock", "authority"])
def test_changed_boundary_after_each_append_stops_subsequent_writes(
    monkeypatch: pytest.MonkeyPatch, written: int, change: str
) -> None:
    database, ledger, store = setup()
    original = database.execute
    writes = 0

    def execute(sql: str, *parameters: object) -> Any:
        nonlocal writes
        result = original(sql, *parameters)
        if sql.startswith("INSERT"):
            writes += 1
            if writes == written:
                change_boundary(database, change)
        return result

    monkeypatch.setattr(database, "execute", execute)
    with pytest.raises(NonproductionAuthorityError, match=reason(change)):
        register(store, ledger, database)
    assert writes == written and len(database.grants) == 1 and len(database.members) == written - 1
    # The caller owns rollback, including a failure after the final membership.


@pytest.mark.parametrize("change", ["transaction", "revision", "lock", "authority"])
def test_stream_resumption_rechecks_captured_boundary(monkeypatch: pytest.MonkeyPatch, change: str) -> None:
    from dpone.adapters import nonproduction_mssql_registration as registration

    database, ledger, store = setup()
    record = register(store, ledger, database)
    original = registration.require_membership_history
    changed = False

    def history(rows: Any, records: Any) -> Any:
        def stream() -> Any:
            nonlocal changed
            for item in records:
                yield item
                if not changed:
                    change_boundary(database, change)
                    changed = True

        return original(rows, stream())

    monkeypatch.setattr(registration, "require_membership_history", history)
    with pytest.raises(NonproductionAuthorityError, match=reason(change)):
        store.read_in(ledger, record.originals.grant.consumption_subject_sha256, expected_revision=database.expected)
    assert changed


def test_failed_context_exit_keeps_original_error_without_new_observation(monkeypatch: pytest.MonkeyPatch) -> None:
    database, ledger, store = setup()
    marker = NonproductionAuthorityError("membership_budget")
    observed_at_failure = 0

    def fail(*args: Any, **kwargs: Any) -> Any:
        nonlocal observed_at_failure
        change_boundary(database, "transaction")
        observed_at_failure = len(database.commands)
        raise marker

    monkeypatch.setattr(store, "_pool", fail)
    with pytest.raises(NonproductionAuthorityError) as caught:
        register(store, ledger, database)
    assert caught.value is marker and len(database.commands) == observed_at_failure
    assert not database.grants and not database.members


def test_new_invocation_recaptures_actual_transaction_without_beginning_ledger() -> None:
    database, ledger, store = setup()
    assert ledger._expected_service_id is None
    first = register(store, ledger, database)
    database.transaction_id = 19
    assert register(store, ledger, database) == first
    assert ledger._expected_service_id is None and len(database.grants) == 1 and len(database.members) == 3
    assert not any("sp_getapplock" in sql or "BEGIN TRANSACTION" in sql for sql, _ in database.commands)


def test_original_readback_failure_stays_unacknowledged_with_caller_owned_rows() -> None:
    database, ledger, store = setup()
    database.fail = lambda sql: len(database.members) == 3 and "WHERE consumption_subject_sha256 = ?" in sql
    with pytest.raises(NonproductionAuthorityError, match="registration_unavailable"):
        register(store, ledger, database)
    assert len(database.grants) == 1 and len(database.members) == 3
