from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from threading import Event, Lock

import pytest

from dpone.adapters.mssql_sqlclient_input_custody_journal import (
    SQLiteSqlClientInputCustodyJournal,
)
from dpone.ports.mssql_native_route_backend import NativeInputCustodyReceipt


def _receipt(request_sha256: str, release: str = "b" * 64) -> NativeInputCustodyReceipt:
    return NativeInputCustodyReceipt(request_sha256, release)


def test_receipt_survives_restart_without_repeating_release(tmp_path):
    path = tmp_path / "custody.sqlite3"
    request = "a" * 64
    calls: list[str] = []

    def release() -> NativeInputCustodyReceipt:
        calls.append("release")
        return _receipt(request)

    first = SQLiteSqlClientInputCustodyJournal(path).observe_or_advance(
        request,
        release,
    )
    restarted = SQLiteSqlClientInputCustodyJournal(path)

    assert restarted.observe_or_advance(request, lambda: pytest.fail("release replayed")) == first
    assert calls == ["release"]


def test_concurrent_instances_invoke_release_once(tmp_path):
    path = tmp_path / "custody.sqlite3"
    request = "a" * 64
    started = Event()
    finish = Event()
    calls: list[str] = []
    lock = Lock()

    def advance() -> NativeInputCustodyReceipt:
        with lock:
            calls.append("release")
        started.set()
        assert finish.wait(timeout=5)
        return _receipt(request)

    first_journal = SQLiteSqlClientInputCustodyJournal(path)
    second_journal = SQLiteSqlClientInputCustodyJournal(path)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(first_journal.observe_or_advance, request, advance)
        assert started.wait(timeout=5)
        second = pool.submit(second_journal.observe_or_advance, request, advance)
        with pytest.raises(RuntimeError, match="mssql_native.input_custody_outcome_unknown"):
            second.result(timeout=5)
        finish.set()
        assert first.result(timeout=5) == _receipt(request)

    assert calls == ["release"]


def test_callback_failure_leaves_intent_and_replay_refuses_effect(tmp_path):
    path = tmp_path / "custody.sqlite3"
    request = "a" * 64
    journal = SQLiteSqlClientInputCustodyJournal(path)

    with pytest.raises(OSError, match="unknown release"):
        journal.observe_or_advance(request, lambda: (_ for _ in ()).throw(OSError("unknown release")))
    with pytest.raises(RuntimeError, match="mssql_native.input_custody_outcome_unknown"):
        SQLiteSqlClientInputCustodyJournal(path).observe_or_advance(
            request,
            lambda: pytest.fail("release replayed"),
        )


def test_mismatched_receipt_leaves_intent_and_fails_closed(tmp_path):
    path = tmp_path / "custody.sqlite3"
    request = "a" * 64
    journal = SQLiteSqlClientInputCustodyJournal(path)

    with pytest.raises(ValueError, match="mssql_native.input_custody_receipt_invalid"):
        journal.observe_or_advance(request, lambda: _receipt("c" * 64))
    with pytest.raises(RuntimeError, match="mssql_native.input_custody_outcome_unknown"):
        journal.observe_or_advance(request, lambda: pytest.fail("release replayed"))


@pytest.mark.parametrize("request_sha", ["", "A" * 64, "a" * 63, "g" * 64])
def test_invalid_request_digest_is_rejected_before_callback(tmp_path, request_sha):
    journal = SQLiteSqlClientInputCustodyJournal(tmp_path / "custody.sqlite3")

    with pytest.raises(ValueError, match="mssql_native.input_custody_request_invalid"):
        journal.observe_or_advance(request_sha, lambda: pytest.fail("release invoked"))


@pytest.mark.parametrize(
    ("phase", "receipt_json"),
    [
        ("receipt", "not-json"),
        ("receipt", '{"request_sha256":"' + "a" * 64 + '","release_sha256":"short"}'),
    ],
)
def test_corrupt_durable_receipt_fails_closed(tmp_path, phase, receipt_json):
    path = tmp_path / "custody.sqlite3"
    request = "a" * 64
    SQLiteSqlClientInputCustodyJournal(path)
    with sqlite3.connect(path) as db:
        db.execute(
            "INSERT INTO sqlclient_input_custody(request_sha256, phase, receipt_json) VALUES (?, ?, ?)",
            (request, phase, receipt_json),
        )

    with pytest.raises(ValueError, match="mssql_native.input_custody_receipt_invalid"):
        SQLiteSqlClientInputCustodyJournal(path).observe_or_advance(
            request,
            lambda: pytest.fail("release invoked"),
        )


def test_independent_request_keys_advance_separately(tmp_path):
    journal = SQLiteSqlClientInputCustodyJournal(tmp_path / "custody.sqlite3")
    calls: list[str] = []

    def release(request_sha: str) -> NativeInputCustodyReceipt:
        calls.append(request_sha)
        return _receipt(request_sha)

    for request in ("a" * 64, "c" * 64):
        assert journal.observe_or_advance(
            request,
            partial(release, request),
        ) == _receipt(request)

    assert calls == ["a" * 64, "c" * 64]


def test_operator_reconciliation_requires_existing_unknown_intent(tmp_path):
    request = "a" * 64
    journal = SQLiteSqlClientInputCustodyJournal(
        tmp_path / "custody.sqlite3", reconciliation_observer=lambda key: _receipt(key)
    )

    with pytest.raises(RuntimeError, match="mssql_native.input_custody_outcome_unknown"):
        journal.reconcile_unknown(request)


def test_operator_reconciliation_rejects_receipt_for_another_request(tmp_path):
    path = tmp_path / "custody.sqlite3"
    request = "a" * 64
    journal = SQLiteSqlClientInputCustodyJournal(path, reconciliation_observer=lambda _key: _receipt("c" * 64))
    with pytest.raises(OSError, match="unknown release"):
        journal.observe_or_advance(request, lambda: (_ for _ in ()).throw(OSError("unknown release")))

    with pytest.raises(ValueError, match="mssql_native.input_custody_receipt_invalid"):
        journal.reconcile_unknown(request)
    with pytest.raises(RuntimeError, match="mssql_native.input_custody_outcome_unknown"):
        journal.observe_or_advance(request, lambda: pytest.fail("release replayed"))


def test_operator_reconciliation_survives_restart_and_avoids_release_callback(tmp_path):
    path = tmp_path / "custody.sqlite3"
    request = "a" * 64
    observed = _receipt(request)
    journal = SQLiteSqlClientInputCustodyJournal(path, reconciliation_observer=lambda _key: observed)
    with pytest.raises(OSError, match="unknown release"):
        journal.observe_or_advance(request, lambda: (_ for _ in ()).throw(OSError("unknown release")))

    assert journal.reconcile_unknown(request) == observed
    restarted = SQLiteSqlClientInputCustodyJournal(path)
    assert restarted.observe_or_advance(request, lambda: pytest.fail("release replayed")) == observed


def test_concurrent_operator_reconciliation_is_idempotent(tmp_path):
    path = tmp_path / "custody.sqlite3"
    request = "a" * 64
    observed = _receipt(request)
    journal = SQLiteSqlClientInputCustodyJournal(path)
    with pytest.raises(OSError, match="unknown release"):
        journal.observe_or_advance(request, lambda: (_ for _ in ()).throw(OSError("unknown release")))

    journals = (
        SQLiteSqlClientInputCustodyJournal(path, reconciliation_observer=lambda _key: observed),
        SQLiteSqlClientInputCustodyJournal(path, reconciliation_observer=lambda _key: observed),
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda item: item.reconcile_unknown(request), journals))

    assert results == [observed, observed]


def test_operator_reconciliation_rejects_conflicting_durable_receipt(tmp_path):
    path = tmp_path / "custody.sqlite3"
    request = "a" * 64
    first = _receipt(request, "b" * 64)
    journal = SQLiteSqlClientInputCustodyJournal(path, reconciliation_observer=lambda _key: first)
    with pytest.raises(OSError, match="unknown release"):
        journal.observe_or_advance(request, lambda: (_ for _ in ()).throw(OSError("unknown release")))
    assert journal.reconcile_unknown(request) == first

    conflicting = SQLiteSqlClientInputCustodyJournal(
        path, reconciliation_observer=lambda _key: _receipt(request, "c" * 64)
    )
    assert conflicting.reconcile_unknown(request) == first


def test_operator_reconciliation_rejects_corrupt_durable_receipt(tmp_path):
    path = tmp_path / "custody.sqlite3"
    request = "a" * 64
    journal = SQLiteSqlClientInputCustodyJournal(path, reconciliation_observer=lambda key: _receipt(key))
    with sqlite3.connect(path) as db:
        db.execute(
            "INSERT INTO sqlclient_input_custody(request_sha256, phase, receipt_json) VALUES (?, 'receipt', ?)",
            (request, "not-json"),
        )

    with pytest.raises(ValueError, match="mssql_native.input_custody_receipt_invalid"):
        journal.reconcile_unknown(request)
