"""Exact control-user permissions for the cross-database transfer fence."""

from types import SimpleNamespace

import pytest

from dpone.adapters import composition_mssql_transfer_access as access
from dpone.adapters.composition_mssql_issuance import MssqlIssuedCredentials
from dpone.contracts.composition_identity import CompositionAdmissionError


class Cursor:
    def __init__(self, records):
        self.records = list(records)
        self.calls = []

    def execute(self, sql, *parameters):
        self.calls.append((sql, parameters))

    def fetchone(self):
        return self.records.pop(0) if self.records else None


def context(monkeypatch, records):
    audits = []
    monkeypatch.setattr(access, "require_transaction_fence_schema", lambda cursor, schema: audits.append(schema))
    ledger = SimpleNamespace(schema="control", cursor=Cursor(records), require_transaction=lambda *args: 42)
    return ledger, audits


def credentials():
    return MssqlIssuedCredentials("dpone_v3_" + "a" * 64, b"a" * 16, "not-a-live-password")


def expected():
    value = credentials()
    return value.login_name, value.login_sid, "S", 1, 0, 0, 1, 0


def test_transfer_access_grants_only_exact_procedure(monkeypatch):
    ledger, audits = context(monkeypatch, [expected()])
    access.MssqlCompositionTransferAccess().grant(ledger, credentials())
    assert audits == ["control", "control"]
    statement, parameters = ledger.cursor.calls[0]
    assert "GRANT EXECUTE ON OBJECT::" in statement
    assert "ALTER ROLE" not in statement and "GRANT SELECT" not in statement
    assert parameters == (credentials().login_name, credentials().login_sid, "control", "composition_require_transfer")
    assert credentials().password not in repr(ledger.cursor.calls)


@pytest.mark.parametrize(
    "index,value", [(0, "foreign"), (1, b"b" * 16), (2, "E"), (3, 2), (4, 1), (5, 1), (6, 0), (7, 1)]
)
def test_transfer_access_rejects_identity_or_permission_drift(monkeypatch, index, value):
    record = list(expected())
    record[index] = value
    ledger, _ = context(monkeypatch, [tuple(record)])
    with pytest.raises(CompositionAdmissionError, match="transfer_control_user_policy"):
        access.MssqlCompositionTransferAccess().require(ledger, credentials())


def test_transfer_access_rejects_duplicate_identity(monkeypatch):
    ledger, _ = context(monkeypatch, [expected(), expected()])
    with pytest.raises(CompositionAdmissionError, match="transfer_control_user_policy"):
        access.MssqlCompositionTransferAccess().require(ledger, credentials())


def test_transfer_access_does_not_grant_after_catalog_rejection(monkeypatch):
    ledger, _ = context(monkeypatch, [])

    def reject(*args):
        raise CompositionAdmissionError("module_drift")

    monkeypatch.setattr(access, "require_transaction_fence_schema", reject)
    with pytest.raises(CompositionAdmissionError, match="module_drift"):
        access.MssqlCompositionTransferAccess().grant(ledger, credentials())
    assert ledger.cursor.calls == []
