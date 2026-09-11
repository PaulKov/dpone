"""Reject SQL mutations that the offline fault model does not actually interpret."""

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from tests.composition_mssql_attempt_fixtures import active as active
from tests.composition_mssql_gate_helpers import attempt
from tests.composition_mssql_store_fault_model import Cursor


@pytest.mark.parametrize(
    "table,before,after",
    [
        ("operations", "'RUNNING'", "'FAILED'"),
        ("operations", "'execution'", "'qualification'"),
        ("operations", "owner_key, owner_subject_sha256", "owner_subject_sha256, owner_key"),
        ("operation_domains", "guard_id, fencing_epoch", "fencing_epoch, guard_id"),
    ],
)
def test_unmodeled_sql_shape_cannot_be_reported_as_success(active, monkeypatch, table, before, after):
    database, store = active
    original = Cursor.execute
    changed = []

    def execute(self, sql, *parameters):
        if sql.startswith(f"INSERT INTO [dpone_control].[composition_{table}]"):
            assert before in sql
            sql = sql.replace(before, after)
            changed.append(sql)
        return original(self, sql, *parameters)

    monkeypatch.setattr(Cursor, "execute", execute)
    with pytest.raises(CompositionAdmissionError):
        store.admit_once(attempt())
    assert changed and not database.data["operations"] and not database.data["operation_domains"]
    assert all(connection.commits == 0 and connection.rollbacks == 1 for connection in database.connections)
