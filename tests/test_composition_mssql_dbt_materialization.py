"""SQL catalog doubles verify boundaries; no live schema/data certification."""

import json
from uuid import UUID

import pytest

from dpone.adapters.composition_mssql_dbt_materialization import MssqlDbtMaterializationObserver
from dpone.contracts.composition_dbt_outcome import EVIDENCE_SUBJECT_FIELDS, DbtCaptureError, DbtOutcomeExpectation
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from tests.test_composition_dbt_capture import intent
from tests.test_composition_dbt_materialization import derive


class Catalog:
    autocommit = True

    def __init__(self, contract):
        self.pin = MssqlDatabaseAuthorityPin(
            contract.write.database, 9, "2026-09-11T00:00:00", UUID("10000000-0000-4000-8000-000000000001")
        )
        self.state = (self.pin.database_name, 9, str(self.pin.database_guid), self.pin.create_token, 1, 1, 42, 1, 0)
        self.objects = (
            (
                19,
                contract.write.schema,
                contract.write.relation,
                contract.kind,
                "2026-09-11T00:00:00",
                "2026-09-11T01:00:00",
                None,
            ),
        )
        self.columns = (
            (1, "id", "int", "int", 0, 4, 10, 0, 0, None, 0, 0, 0, 0, None),
            (2, "extra", "nvarchar", "nvarchar", 0, 40, 0, 0, 1, "Latin1_General_100_BIN2", 0, 0, 0, 0, None),
        )
        self.calls = []
        self.rollbacks = 0
        self.closed = False
        self.object_reads = 0
        self.drift = False

    def cursor(self):
        return self

    def execute(self, sql, *parameters):
        self.calls.append(sql)
        if "FROM sys.databases" in sql:
            self.rows = (self.state,)
        elif "FROM sys.objects" in sql:
            self.object_reads += 1
            self.rows = self.objects
            if self.drift and self.object_reads == 2:
                self.rows = ((*self.objects[0][:5], "2026-09-11T02:00:00", self.objects[0][6]),)
        elif "FROM sys.columns" in sql:
            self.rows = self.columns
        else:
            self.rows = ()

    def fetchall(self):
        return self.rows

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def setup(tmp_path, columns=None, callback=None):
    contract = derive(columns or {"id": {"name": "id", "data_type": "int"}})[0]
    db = Catalog(contract)
    value = intent(tmp_path)
    expected = DbtOutcomeExpectation(
        "sha256:" + "a" * 64,
        (contract.write.resource_id,),
        (contract.write.resource_id,),
        "1.12.3",
        "v12",
        "v6",
        tuple((key, "synthetic") for key in sorted(EVIDENCE_SUBJECT_FIELDS)),
        (contract.expectation,),
    )
    callback = callback or (lambda *args: None)
    observer = MssqlDbtMaterializationObserver(
        read_contracts=lambda attempt: (contract,), open_target=lambda *args: (db, db.pin), require_target=callback
    )
    return observer, db, value, expected


def test_original_contains_actual_extra_columns_and_catalog_identity(tmp_path):
    observer, db, value, expected = setup(tmp_path)
    original = json.loads(observer(value.attempt, value, expected, "invocation"))
    observed = original["catalog"][0]
    assert observed["columns"][1]["name"] == "extra"
    assert observed["columns"][1]["max_length"] == 40
    assert observed["object"]["object_id"] == 19 and observed["transaction_id"] == 42
    assert observed["database_authority"]["database_guid"] == str(db.pin.database_guid)
    assert db.object_reads == 2 and db.rollbacks and db.closed
    assert not any(sql.startswith(("INSERT", "UPDATE", "GRANT", "CREATE")) for sql in db.calls)


@pytest.mark.parametrize(
    "field,value", [(0, "foreign"), (1, 10), (2, "10000000-0000-4000-8000-000000000002"), (5, -1), (7, 0), (8, 1)]
)
def test_wrong_database_or_hidden_metadata_rejects(tmp_path, field, value):
    observer, db, intent_value, expected = setup(tmp_path)
    db.state = (*db.state[:field], value, *db.state[field + 1 :])
    with pytest.raises(DbtCaptureError):
        observer(intent_value.attempt, intent_value, expected, "invocation")
    assert db.closed and db.object_reads == 0


@pytest.mark.parametrize(
    "fault",
    ["missing", "duplicate", "wrong_kind", "columns_missing", "ambiguous_columns", "truncated", "type", "changed"],
)
def test_missing_ambiguous_truncated_or_changed_catalog_rejects(tmp_path, fault):
    observer, db, value, expected = setup(tmp_path)
    if fault == "missing":
        db.objects = ()
    elif fault == "duplicate":
        db.objects *= 2
    elif fault == "wrong_kind":
        db.objects = ((*db.objects[0][:3], "view", *db.objects[0][4:]),)
    elif fault == "columns_missing":
        db.columns = (db.columns[1],)
    elif fault == "ambiguous_columns":
        db.columns = (db.columns[0], (2, "ID", *db.columns[0][2:]))
    elif fault == "truncated":
        db.columns = tuple((index + 1, f"c{index}", *db.columns[0][2:]) for index in range(1025))
    elif fault == "type":
        db.columns = ((1, "id", "bigint", "bigint", 0, 8, 19, 0, 0, None, 0, 0, 0, 0, None),)
    else:
        db.drift = True
    with pytest.raises(DbtCaptureError):
        observer(value.attempt, value, expected, "invocation")
    assert db.closed and db.rollbacks


def test_physical_callback_cannot_replace_transaction(tmp_path):
    def drift(connection, *args):
        connection.state = (*connection.state[:6], 43, *connection.state[7:])

    observer, db, value, expected = setup(tmp_path, callback=drift)
    with pytest.raises(DbtCaptureError, match="transaction_changed"):
        observer(value.attempt, value, expected, "invocation")
    assert db.object_reads == 0


@pytest.mark.parametrize(
    "declared,actual",
    [
        ("nvarchar(20)", (2, "label", "nvarchar", "nvarchar", 0, 40, 0, 0, 1, None, 0, 0, 0, 0, None)),
        ("numeric(10,2)", (2, "label", "decimal", "decimal", 0, 9, 10, 2, 1, None, 0, 0, 0, 0, None)),
        ("float(24)", (2, "label", "real", "real", 0, 4, 24, 0, 1, None, 0, 0, 0, 0, None)),
    ],
)
def test_actual_dimensions_use_physical_normalization(tmp_path, declared, actual):
    observer, db, value, expected = setup(tmp_path, {"label": {"name": "label", "data_type": declared}})
    db.columns = (actual,)
    assert observer(value.attempt, value, expected, "invocation")


def test_untyped_column_does_not_invent_type_contract(tmp_path):
    observer, db, value, expected = setup(tmp_path, {"id": {"name": "id"}})
    db.columns = ((1, "id", None, "geography", 1, -1, 0, 0, 1, None, 0, 0, 0, 0, None),)
    assert (
        json.loads(observer(value.attempt, value, expected, "invocation"))["catalog"][0]["columns"][0]["user_type"]
        == "geography"
    )


def test_changed_source_contracts_reject_after_actual_observation(tmp_path):
    observer, db, value, expected = setup(tmp_path)
    read = observer._read_contracts
    calls = []
    observer._read_contracts = lambda attempt: calls.append(1) or (read(attempt) if len(calls) == 1 else ())
    with pytest.raises(DbtCaptureError, match="source_changed"):
        observer(value.attempt, value, expected, "invocation")
