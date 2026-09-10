"""Renderer and catalog doubles; none proves installed SQL permissions or triggers."""

from hashlib import sha256

import pytest

from dpone.adapters.nonproduction_mssql_schema import (
    nonproduction_trust_trigger_sql,
    render_nonproduction_mssql_schema,
    require_nonproduction_mssql_schema,
)
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError

SCHEMA = "dpone_control"
COLLATION = "Latin1_General_100_BIN2"


def catalog():
    columns = [
        ("environment_id", "uniqueidentifier", 16, None),
        ("revision", "bigint", 8, None),
        ("schema_version", "int", 4, None),
        ("policy_document", "varbinary", -1, None),
        ("policy_sha256", "varchar", 71, COLLATION),
        ("verifier_policy_document", "varbinary", -1, None),
        ("verifier_policy_sha256", "varchar", 71, COLLATION),
        ("current_revocation_epoch", "bigint", 8, None),
    ]
    return [
        [(0, 0, 0)],
        [(*item, 0, 0, 0, 1) for item in columns],
        [("environment_id", 1, 0, 0, 1, 1, 0, 1, 0, 0), ("revision", 2, 0, 0, 1, 1, 0, 1, 0, 0)],
        [(0,)],
        [
            (
                "composition_nonproduction_trust_append",
                0,
                1,
                0,
                1,
                1,
                None,
                sha256(nonproduction_trust_trigger_sql(SCHEMA).encode("utf-16le")).digest(),
            )
        ],
        [("DELETE",), ("INSERT",), ("UPDATE",)],
        [(0,)],
    ]


class Cursor:
    def __init__(self, answers=None):
        self.answers = list(catalog() if answers is None else answers)
        self.commands = []

    def execute(self, sql, *parameters):
        self.commands.append((sql, parameters))
        assert sql.startswith("SELECT")
        return self

    def fetchall(self):
        return self.answers.pop(0)

    def fetchone(self):
        pytest.fail("catalog inspection requires complete row sets")

    def close(self):
        pytest.fail("catalog inspection does not own cursor lifetime")


def test_renderer_installs_exact_separate_trigger_batch_and_no_enrollment():
    sql = render_nonproduction_mssql_schema(SCHEMA)
    definition = nonproduction_trust_trigger_sql(SCHEMA)
    assert "EXEC(N'" + definition.replace("'", "''") + "');" in sql
    assert definition.startswith("CREATE TRIGGER ") and definition.endswith("END;")
    assert sql.count("CREATE TABLE ") == 1 and "IF SCHEMA_ID" not in sql
    assert "CREATE USER" not in sql and "GRANT " not in sql and "IF NOT EXISTS" not in sql
    assert "SET ANSI_NULLS ON;" in sql and "SET QUOTED_IDENTIFIER ON;" in sql
    assert sql.endswith("COMMIT TRANSACTION;\n")


def test_trigger_serializes_before_insert_and_closes_every_mutation_event():
    sql = nonproduction_trust_trigger_sql(SCHEMA)
    assert "INSTEAD OF INSERT, UPDATE, DELETE" in sql
    assert "EXISTS (SELECT 1 FROM deleted)" in sql
    assert "COUNT_BIG(*) FROM inserted) <> 1" in sql
    assert sql.index("sys.sp_getapplock") < sql.index("INSERT INTO")
    assert "dpone:composition-control:v1" in sql and "@LockOwner = N'Transaction'" in sql
    assert "@previous_revision + 1" in sql and "current_revocation_epoch < @previous_epoch" in sql
    assert "HASHBYTES('SHA2_256', policy_document)" in sql
    assert "HASHBYTES('SHA2_256', verifier_policy_document)" in sql
    assert "BETWEEN 1 AND 1048576" in sql
    assert "policy_sha256) <> 71" in sql


@pytest.mark.parametrize("schema", ["x.y", "x]", "a; DROP TABLE x", "", "a" * 129, None])
def test_identifiers_reject_before_interpolation(schema):
    with pytest.raises(ValueError):
        render_nonproduction_mssql_schema(schema)


def test_exact_catalog_is_read_only_and_matches_utf16_module_bytes():
    cursor = Cursor()
    require_nonproduction_mssql_schema(cursor, SCHEMA)
    assert not cursor.answers
    assert all("HASHBYTES('SHA2_256', m.definition)" in sql for sql, _ in cursor.commands if "sys.triggers" in sql)
    assert sha256(nonproduction_trust_trigger_sql(SCHEMA).encode()).digest() != catalog()[4][0][-1]


@pytest.mark.parametrize("section", range(7))
@pytest.mark.parametrize("change", ["missing", "extra", "altered"])
def test_missing_changed_or_extra_catalog_objects_reject(section, change):
    answers = catalog()
    if change == "missing":
        answers[section] = []
    elif change == "extra":
        answers[section] += answers[section][:1]
    else:
        answers[section][0] = ("changed", *answers[section][0][1:])
    with pytest.raises(NonproductionAuthorityError):
        require_nonproduction_mssql_schema(Cursor(answers), SCHEMA)


@pytest.mark.parametrize("position,value", [(1, 1), (2, 0), (3, 1), (4, 0), (5, 0), (6, 1), (7, None)])
def test_disabled_changed_or_unobservable_trigger_rejects(position, value):
    answers = catalog()
    changed = list(answers[4][0])
    changed[position] = value
    answers[4][0] = tuple(changed)
    with pytest.raises(NonproductionAuthorityError):
        require_nonproduction_mssql_schema(Cursor(answers), SCHEMA)


def test_catalog_driver_failure_is_sanitized():
    class Broken(Cursor):
        def execute(self, *_):
            raise RuntimeError("secret catalog connection")

    with pytest.raises(NonproductionAuthorityError) as caught:
        require_nonproduction_mssql_schema(Broken(), SCHEMA)
    assert "secret" not in str(caught.value) and caught.value.__suppress_context__


@pytest.mark.parametrize("position,value", [(7, 2), (8, 1), (9, 1)])
def test_changed_index_storage_filter_or_duplicate_policy_rejects(position, value):
    answers = catalog()
    changed = list(answers[2][0])
    changed[position] = value
    answers[2][0] = tuple(changed)
    with pytest.raises(NonproductionAuthorityError, match="primary_key"):
        require_nonproduction_mssql_schema(Cursor(answers), SCHEMA)


def test_row_security_cannot_hide_new_trust_revisions():
    answers = catalog()
    answers[-1] = [(1,)]
    with pytest.raises(NonproductionAuthorityError, match="row_security"):
        require_nonproduction_mssql_schema(Cursor(answers), SCHEMA)
