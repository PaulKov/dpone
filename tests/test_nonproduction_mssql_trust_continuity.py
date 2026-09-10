"""Shared SQL transaction continuity is independent of cached trust revisions."""

import pytest

from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from tests.test_nonproduction_mssql_trust import Connection, provider, reads
from tests.test_nonproduction_mssql_trust import offline_catalog as offline_catalog


@pytest.mark.parametrize("boundary", ["catalog", "original"])
@pytest.mark.parametrize("change", ["rebegin", "closed", "lost_lock", "authority"])
def test_trust_observer_rejects_commit_rebegin_even_with_same_lock(boundary, change, monkeypatch):
    connection = Connection()
    connection.answers = reads()
    original = connection.execute

    def execute(sql, *parameters):
        result = original(sql, *parameters)
        selected = "sys.security_predicates" if boundary == "catalog" else "ORDER BY revision DESC"
        if selected in sql:
            if change == "rebegin":
                connection.transaction_id = 8
            elif change == "closed":
                connection.lock = (0, 0, "NoLock")
            elif change == "lost_lock":
                connection.lock = (1, 1, "NoLock")
            else:
                connection.authority = []
        return result

    monkeypatch.setattr(connection, "execute", execute)
    reason = "trust_control_authority" if change == "authority" else "trust_ledger_lock"
    with pytest.raises(NonproductionAuthorityError, match=reason):
        provider(lambda: connection).read_revision_in(CompositionMssqlLedger(connection, "dpone_control"))
    assert not connection.events
    if boundary == "catalog":
        assert not any("ORDER BY revision DESC" in sql for sql, _ in connection.commands)


@pytest.mark.parametrize("transaction_id", [None, True, 0, -1, "7", 2**63])
def test_invalid_actual_identity_never_reads_authority_or_trust(transaction_id):
    connection = Connection()
    connection.transaction_id = transaction_id
    with pytest.raises(NonproductionAuthorityError, match="trust_ledger_lock"):
        provider(lambda: connection).read_revision_in(CompositionMssqlLedger(connection, "dpone_control"))
    assert len(connection.commands) == 1 and not connection.events


def test_same_ledger_success_rechecks_one_actual_identity_and_keeps_payload_version():
    connection = Connection()
    value = provider(lambda: pytest.fail("no new connection"))
    revision = value.read_revision_in(CompositionMssqlLedger(connection, "dpone_control"))
    assert revision.revision == 1 and not connection.answers and not connection.events
    probes = [sql for sql, _ in connection.commands if "CURRENT_TRANSACTION_ID" in sql]
    assert len(probes) == 3


def test_fresh_read_requires_complete_core_catalog_before_optional_trust(monkeypatch):
    connection = Connection()
    original = connection.execute

    def execute(sql, *parameters):
        result = original(sql, *parameters)
        if "composition_schema:columns" in sql and parameters[-1].endswith("composition_owners]"):
            connection.rows = []
        return result

    monkeypatch.setattr(connection, "execute", execute)
    with pytest.raises(NonproductionAuthorityError, match="trust_read_unavailable"):
        provider(lambda: connection).read()
    assert connection.events == ["cursor", "rollback", "close", "close"]
    assert not any("composition_nonproduction_trust]" in sql for sql, _ in connection.commands)


def test_old_control_version_cannot_authorize_unchanged_optional_payload():
    connection = Connection()
    connection.authority[0] = (1, 1, connection.authority[0][2])
    with pytest.raises(NonproductionAuthorityError, match="trust_control_authority"):
        provider(lambda: connection).read_revision_in(CompositionMssqlLedger(connection, "dpone_control"))
    assert len(connection.commands) == 2 and not connection.events
