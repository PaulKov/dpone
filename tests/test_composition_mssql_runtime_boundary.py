"""Real adapter transaction guards over explicit offline SQL/catalog observations."""

from hashlib import sha256

import pytest

from dpone.adapters import composition_mssql_check_definitions as reference
from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.adapters.composition_mssql_layout import COMPOSITION_TABLES
from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema
from dpone.contracts.composition_activation import CompositionAdmissionError
from tests.composition_mssql_store_helpers import SERVICE_ID
from tests.test_composition_mssql_catalog import Cursor as CatalogCursor
from tests.test_composition_mssql_catalog import expected_rows
from tests.test_composition_mssql_ownership import SharedSql


class BoundaryCursor(SharedSql):
    """Model observed SQL state; no patch replaces the production transaction guard."""

    def __init__(self):
        super().__init__()
        self.catalog = CatalogCursor(expected_rows("dpone_control"))
        self.closed = False

    def execute(self, sql, *parameters):
        if sql.startswith("SET XACT_ABORT"):
            assert "SERIALIZABLE" in sql
            assert "IF @@TRANCOUNT = 0 BEGIN TRANSACTION" in sql
            self.results = []
        elif "sp_getapplock" in sql:
            assert parameters == ("dpone:composition-control:v1",)
            self.results = [(0,)]
        elif "/* composition_schema:" in sql:
            self.catalog.execute(sql, *parameters)
            self.results = list(self.catalog.fetchall())
        else:
            return super().execute(sql, *parameters)
        return self

    def fetchone(self):
        return self.results.pop(0) if self.results else None

    def close(self):
        self.closed = True


class BoundaryConnection:
    def __init__(self):
        self.value = BoundaryCursor()
        self.autocommit = True
        self.commits = self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self.value

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@pytest.fixture
def connection(monkeypatch):
    definitions = {check.name: "OFFLINE ONLY " + check.name for table in COMPOSITION_TABLES for check in table.checks}
    monkeypatch.setattr(reference, "CHECK_DEFINITIONS", definitions)
    monkeypatch.setattr(
        reference, "CHECK_DDL_SHA256", "sha256:" + sha256(render_composition_mssql_schema().encode()).hexdigest()
    )
    return BoundaryConnection()


def test_same_observed_transaction_commits_once_after_body(connection):
    observed = []
    with composition_control_transaction(lambda: connection, "dpone_control", SERVICE_ID) as ledger:
        observed.append(ledger.require_transaction())
        assert connection.commits == 0
    assert observed == [7]
    assert connection.commits == 1 and connection.rollbacks == 0
    assert connection.closed and connection.value.closed
    assert len(connection.value.catalog.calls) == 2 + len(COMPOSITION_TABLES) * 9


@pytest.mark.parametrize(
    "observed",
    [
        (1, 1, "Exclusive", 8),
        (0, 0, "NoLock", None),
        (1, -1, "NoLock", None),
        (1, 1, "Shared", 7),
        (1, 1, "NoLock", 7),
    ],
)
def test_replaced_or_invalid_transaction_cannot_be_committed_as_success(connection, observed):
    with pytest.raises(CompositionAdmissionError, match="shared_transaction"):
        with composition_control_transaction(lambda: connection, "dpone_control", SERVICE_ID):
            connection.value.transaction = observed
    assert connection.commits == 0 and connection.rollbacks == 1
    assert connection.closed and connection.value.closed


def test_changed_authority_after_body_cannot_be_committed(connection):
    with pytest.raises(CompositionAdmissionError, match="control_authority"):
        with composition_control_transaction(lambda: connection, "dpone_control", SERVICE_ID):
            connection.value.authority = []
    assert connection.commits == 0 and connection.rollbacks == 1


def test_unavailable_catalog_never_enters_caller_body(connection):
    connection.value.catalog.rows["[dpone_control].[composition_owners]", "columns"] = ()
    entered = []
    with pytest.raises(CompositionAdmissionError, match="control_schema_columns"):
        with composition_control_transaction(lambda: connection, "dpone_control", SERVICE_ID):
            entered.append(True)
    assert entered == [] and connection.commits == 0 and connection.rollbacks == 1
