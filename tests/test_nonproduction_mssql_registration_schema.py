"""Offline schema/trigger audit cases; installation and permissions remain unverified."""

from hashlib import sha256

import pytest

from dpone.adapters.nonproduction_mssql_registration_schema import (
    nonproduction_registration_trigger_sql,
    render_nonproduction_mssql_registration_schema,
    require_nonproduction_registration_insert_options,
    require_nonproduction_registration_schema,
)
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError

SCHEMA = "dpone_control"
BIN = "Latin1_General_100_BIN2"


def catalog(kind):
    """Independent expected installed SQL shape; no SQL engine is emulated."""
    if kind == "grants":
        columns = [
            ("consumption_subject_sha256", "varchar", 71, BIN, 0),
            ("grant_id", "uniqueidentifier", 16, None, 0),
            ("phase", "varchar", 13, BIN, 0),
            ("environment_id", "uniqueidentifier", 16, None, 0),
            ("campaign_id", "uniqueidentifier", 16, None, 0),
            ("phase_subject_id", "uniqueidentifier", 16, None, 0),
            ("schema_version", "int", 4, None, 0),
            ("grant_document", "varbinary", -1, None, 0),
            ("grant_sha256", "varchar", 71, BIN, 0),
            ("signature_bundle", "varbinary", -1, None, 0),
            ("bundle_sha256", "varchar", 71, BIN, 0),
            ("signature_subject_document", "varbinary", -1, None, 0),
            ("signature_subject_sha256", "varchar", 71, BIN, 0),
            ("request_document", "varbinary", -1, None, 1),
            ("request_sha256", "varchar", 71, BIN, 1),
            ("trust_revision", "bigint", 8, None, 0),
            ("registered_at", "varchar", 20, BIN, 0),
        ]
        keys = [
            ("pk_np_grants", True, ("consumption_subject_sha256",)),
            ("uq_np_grant_phase", False, ("grant_id", "phase")),
        ]
        references = [
            ("environment_id", "composition_nonproduction_trust", "environment_id"),
            ("trust_revision", "composition_nonproduction_trust", "revision"),
        ]
    else:
        columns = [
            ("environment_id", "uniqueidentifier", 16, None, 0),
            ("campaign_id", "uniqueidentifier", 16, None, 0),
            ("phase", "varchar", 13, BIN, 0),
            ("workload_sha256", "varchar", 71, BIN, 0),
            ("workload_document", "varbinary", -1, None, 0),
            ("first_grant_sha256", "varchar", 71, BIN, 0),
        ]
        keys = [("pk_np_memberships", True, ("environment_id", "campaign_id", "phase", "workload_sha256"))]
        references = [("first_grant_sha256", "composition_nonproduction_grants", "consumption_subject_sha256")]
    index_rows: list[tuple[object, ...]] = [
        (name, key, ordinal, 0, 0, int(primary), 1, 0, 1 if primary else 2, 0, 0, None)
        for name, primary, values in keys
        for ordinal, key in enumerate(values, 1)
    ]
    if kind == "grants":
        index_rows.append(
            ("uq_np_qualification_run", "phase_subject_id", 1, 0, 0, 0, 1, 0, 2, 1, 0, "([phase]='qualification')")
        )
    return [
        [(0, 0, 0)],
        [(*column, 0, 0, 1) for column in columns],
        index_rows,
        [(0,)],
        [(f"fk_np_{kind}", parent, SCHEMA, target, child, 0, 0, 0, 0, 0) for parent, target, child in references],
        [
            (
                f"composition_nonproduction_{kind}_append",
                0,
                1,
                0,
                1,
                1,
                None,
                sha256(nonproduction_registration_trigger_sql(SCHEMA, kind).encode("utf-16le")).digest(),
            )
        ],
        [("DELETE",), ("INSERT",), ("UPDATE",)],
        [(0,)],
    ]


class Cursor:
    def __init__(self, answers=None):
        self.answers = catalog("grants") + catalog("memberships") if answers is None else answers
        self.commands = []

    def execute(self, sql, *parameters):
        assert sql.startswith("SELECT")
        self.commands.append((sql, parameters))

    def fetchall(self):
        return self.answers.pop(0)

    def fetchone(self):
        pytest.fail("schema audit requires complete row sets")

    def close(self):
        pytest.fail("schema audit does not own the cursor")


def test_exact_catalog_reads_both_tables_without_mutation():
    cursor = Cursor()
    require_nonproduction_registration_schema(cursor, SCHEMA)
    assert not cursor.answers and len(cursor.commands) == 16


@pytest.mark.parametrize("section", range(16))
@pytest.mark.parametrize("change", ["missing", "extra", "altered"])
def test_rejects_catalog_drift_in_either_table(section, change):
    answers = catalog("grants") + catalog("memberships")
    if change == "missing":
        answers[section] = []
    elif change == "extra":
        answers[section] += answers[section][:1]
    else:
        answers[section][0] = ("changed", *answers[section][0][1:])
    with pytest.raises(NonproductionAuthorityError):
        require_nonproduction_registration_schema(Cursor(answers), SCHEMA)


def test_driver_error_contains_no_connection_or_sql_values():
    class Broken(Cursor):
        def execute(self, *_):
            raise RuntimeError("private SQL connection")

    with pytest.raises(NonproductionAuthorityError) as error:
        require_nonproduction_registration_schema(Broken(), SCHEMA)
    assert "private" not in str(error.value) and error.value.__suppress_context__


def test_external_renderer_installs_two_append_only_tables_without_enrollment():
    sql = render_nonproduction_mssql_registration_schema()
    assert sql.count("CREATE TABLE ") == 2
    assert "CREATE USER" not in sql and "GRANT " not in sql and "IF NOT EXISTS" not in sql
    for table in ("grants", "memberships"):
        trigger = nonproduction_registration_trigger_sql("dpone_control", table)
        assert "EXEC(N'" + trigger.replace("'", "''") + "');" in sql
        assert "INSTEAD OF INSERT, UPDATE, DELETE" in trigger
        assert trigger.index("sys.sp_getapplock") < trigger.index("INSERT INTO")
        assert "EXISTS (SELECT 1 FROM deleted)" in trigger
        assert "@LockOwner = N'Transaction'" in trigger
    assert "SET ANSI_NULLS ON;" in sql and "SET QUOTED_IDENTIFIER ON;" in sql


@pytest.mark.parametrize("schema", ["x]", "a.b", "a;drop", "", None, "a" * 129])
def test_invalid_schema_is_rejected_before_rendering(schema):
    with pytest.raises(ValueError):
        render_nonproduction_mssql_registration_schema(schema)


def test_append_module_hashes_original_binary_bytes_and_never_normalizes_them():
    sql = nonproduction_registration_trigger_sql(SCHEMA, "grants")
    for document in ("grant_document", "signature_bundle", "signature_subject_document", "request_document"):
        assert f"HASHBYTES('SHA2_256', {document})" in sql
    assert "BETWEEN 1 AND 8388608" in sql and "DATALENGTH(consumption_subject_sha256) <> 71" in sql
    assert "schema_version <> 1" in sql and "phase_subject_id" in sql
    assert "CONVERT(nvarchar" not in sql.lower()


def test_qualification_only_filter_and_all_required_install_options_are_explicit():
    sql = render_nonproduction_mssql_registration_schema()
    assert "(phase_subject_id) WHERE ([phase]='qualification');" in sql
    assert "uq_np_phase_subject" not in sql
    for option in (
        "ANSI_NULLS",
        "ANSI_PADDING",
        "ANSI_WARNINGS",
        "ARITHABORT",
        "CONCAT_NULL_YIELDS_NULL",
        "QUOTED_IDENTIFIER",
    ):
        assert f"SET {option} ON;" in sql
    assert "SET NUMERIC_ROUNDABORT OFF;" in sql


@pytest.mark.parametrize(
    "predicate", [None, "([phase]='execution')", "([phase]='qualification' AND [trust_revision]>1)"]
)
def test_changed_filter_definition_cannot_weaken_run_uniqueness(predicate):
    answers = catalog("grants") + catalog("memberships")
    changed = list(answers[2][-1])
    changed[-1] = predicate
    answers[2][-1] = tuple(changed)
    with pytest.raises(NonproductionAuthorityError, match="registration_schema_keys"):
        require_nonproduction_registration_schema(Cursor(answers), SCHEMA)


@pytest.mark.parametrize("mask", [0, 4472 | 8192, True, *(4472 & ~bit for bit in (8, 16, 32, 64, 256, 4096))])
def test_new_insert_requires_every_filtered_index_session_option(mask):
    cursor = Cursor([[(mask,)]])
    with pytest.raises(NonproductionAuthorityError, match="registration_session"):
        require_nonproduction_registration_insert_options(cursor)
    assert cursor.commands == [("SELECT @@OPTIONS;", ())]
