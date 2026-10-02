"""DBAPI fault tests: emitted rows are never proof of an acknowledged commit.

The scripted transport is necessary to place failures between wire results and
commit ACK. It is not SQL Server concurrency or live route certification.
"""

from importlib import import_module

import pytest


def api():
    return import_module("dpone.runtime.state.mssql_publication_transaction")


class ScriptedSession:
    def __init__(self, failure=None, *, extra_result=False):
        self.failure = failure
        self.extra_result = extra_result
        self.autocommit = True
        self.events = []
        self.description = (("receipt",),)
        self.advances = 0
        self.rows = [("write-id", 1)]

    def step(self, name):
        self.events.append(name)
        if name == self.failure:
            raise RuntimeError("synthetic-sensitive-driver-detail")

    def cursor(self):
        self.step("cursor")
        return self

    def execute(self, sql, params):
        assert not self.autocommit
        assert params == ("bound-value",)
        assert "SET XACT_ABORT ON" in sql
        self.step("execute")

    def fetchall(self):
        self.step("fetch")
        return self.rows

    def nextset(self):
        self.step("nextset")
        self.advances += 1
        return self.extra_result and self.advances == 1

    def commit(self):
        self.step("commit")

    def rollback(self):
        self.step("rollback")

    def close(self):
        self.step("close")


def run(session, *, validate=None):
    def check(rows):
        session.step("validate")
        assert "commit" not in session.events
        assert rows == [("write-id", 1)]
        return "validated-receipt"

    return api().execute_publication_transaction(
        lambda: session, "SELECT ?", ("bound-value",), validate=validate or check
    )


def test_success_requires_result_drain_validation_and_commit_ack():
    session = ScriptedSession()
    assert run(session) == "validated-receipt"
    assert session.events.index("nextset") < session.events.index("validate") < session.events.index("commit")
    assert session.events[-1] == "close"
    assert "rollback" not in session.events


@pytest.mark.parametrize("failure", ["cursor", "execute", "fetch", "nextset", "validate", "commit", "close"])
def test_failure_never_retries_or_returns_the_emitted_receipt(failure):
    session = ScriptedSession(failure)
    with pytest.raises(api().PublicationTransactionUnknown) as error:
        run(session)
    assert "synthetic-sensitive-driver-detail" not in str(error.value)
    assert session.events.count("execute") <= 1
    assert session.events.count("commit") <= 1
    assert "close" in session.events


def test_lost_commit_ack_is_unknown_even_if_server_committed():
    session = ScriptedSession("commit")
    with pytest.raises(api().PublicationTransactionUnknown):
        run(session)
    assert "validate" in session.events
    assert session.events.count("fetch") == 1  # no recovery read can manufacture a permit


def test_error_after_output_rolls_back_before_commit():
    session = ScriptedSession("nextset")
    with pytest.raises(api().PublicationTransactionUnknown):
        run(session)
    assert "fetch" in session.events
    assert "rollback" in session.events
    assert "commit" not in session.events


def test_extra_rowset_is_not_silently_discarded():
    session = ScriptedSession(extra_result=True)
    with pytest.raises(api().PublicationTransactionUnknown):
        run(session)
    assert "commit" not in session.events


def test_validator_failure_is_not_committed():
    session = ScriptedSession()

    def reject(rows):
        raise ValueError("bad exact write identity")

    with pytest.raises(api().PublicationTransactionUnknown):
        run(session, validate=reject)
    assert "rollback" in session.events
    assert "commit" not in session.events


def test_connection_failure_is_redacted_and_not_retried():
    calls = []

    def factory():
        calls.append(1)
        raise RuntimeError("synthetic-sensitive-driver-detail")

    with pytest.raises(api().PublicationTransactionUnknown) as error:
        api().execute_publication_transaction(factory, "SELECT ?", (), validate=lambda rows: rows)
    assert calls == [1]
    assert "synthetic-sensitive-driver-detail" not in str(error.value)


def test_owned_operation_validates_multiple_batches_before_single_commit():
    session = ScriptedSession()

    def provision(cursor):
        for _ in range(2):
            rows = api().execute_publication_statement(cursor, "SET XACT_ABORT ON; SELECT ?", ("bound-value",))
            assert rows == [("write-id", 1)]
            assert "commit" not in session.events
        return "catalog-admitted"

    result = api().run_publication_transaction(lambda: session, provision)
    assert result == "catalog-admitted"
    assert session.events.count("execute") == 2
    assert session.events.count("commit") == 1


def test_owned_operation_post_ddl_validation_failure_rolls_back_without_commit():
    session = ScriptedSession()

    def reject_catalog(cursor):
        api().execute_publication_statement(cursor, "SET XACT_ABORT ON; SELECT ?", ("bound-value",))
        raise ValueError("synthetic-sensitive-catalog-detail")

    with pytest.raises(api().PublicationTransactionUnknown) as error:
        api().run_publication_transaction(lambda: session, reject_catalog)
    assert "synthetic-sensitive" not in str(error.value)
    assert "rollback" in session.events
    assert "commit" not in session.events


def test_ddl_statement_consumes_transport_errors_even_without_a_rowset():
    session = ScriptedSession("nextset")
    session.description = None
    session.autocommit = False
    with pytest.raises(RuntimeError):
        api().execute_publication_statement(
            session, "SET XACT_ABORT ON; SELECT ?", ("bound-value",), rows_required=False
        )
    assert session.events == ["execute", "nextset"]


def test_ddl_statement_accepts_no_rows_but_rejects_an_unexpected_result():
    session = ScriptedSession()
    session.description = None
    session.autocommit = False
    assert (
        api().execute_publication_statement(
            session, "SET XACT_ABORT ON; SELECT ?", ("bound-value",), rows_required=False
        )
        == []
    )
    session.description = (("unexpected",),)
    with pytest.raises(ValueError):
        api().execute_publication_statement(
            session, "SET XACT_ABORT ON; SELECT ?", ("bound-value",), rows_required=False
        )
