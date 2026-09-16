"""Exact catalog inventory rejection, independent of a live SQL permission claim."""

import pytest

from dpone.adapters.dbt_mssql_physical_enrollment_tables import verify_enrollment_tables


class Cursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, *args):
        self.statements.append((sql, args))

    def fetchone(self):
        return None


def test_absent_table_is_not_a_verified_inventory():
    cursor = Cursor()
    with pytest.raises(RuntimeError, match="inventory"):
        verify_enrollment_tables(cursor)
    assert len(cursor.statements) == 1


def inventory():
    """SQL Server catalog contract fixture, with all eight identities explicit."""
    result = []
    for table, columns, checks in (
        (
            "physical_plan_enrollments_v1",
            "generation_id:uniqueidentifier:16:0 executor_invocation_id:uniqueidentifier:16:0 registration_id:uniqueidentifier:16:0 registration_digest:varbinary:71:0 payload_digest:varbinary:71:0 payload:varbinary:-1:0",
            [
                (4, "registration_digest", "datalengthregistration_digest=71"),
                (5, "payload_digest", "datalengthpayload_digest=71"),
                (6, "payload", "datalengthpayload>=1anddatalengthpayload<=1048576"),
            ],
        ),
        (
            "physical_model_sessions_v1",
            "session_registration_id:uniqueidentifier:16:0 generation_id:uniqueidentifier:16:0 model_unique_id_hash:binary:32:0 model_unique_id:varbinary:-1:0 model_plan_digest:varbinary:71:0 enrollment_digest:varbinary:71:0 connection_id:uniqueidentifier:16:0 connect_time:datetime2:8:7 session_id:int:4:0 login_time:datetime2:8:7 build_model_principal_id:int:4:0 build_model_principal_sid:varbinary:85:0 build_control_principal_id:int:4:0 build_control_principal_sid:varbinary:85:0 attached_at_utc:datetime2:8:7",
            [
                (4, "model_unique_id", "datalengthmodel_unique_id>=1anddatalengthmodel_unique_id<=4096"),
                (5, "model_plan_digest", "datalengthmodel_plan_digest=71"),
                (6, "enrollment_digest", "datalengthenrollment_digest=71"),
                (9, "session_id", "session_id>0"),
                (
                    12,
                    "build_model_principal_sid",
                    "datalengthbuild_model_principal_sid>=1anddatalengthbuild_model_principal_sid<=85",
                ),
                (
                    14,
                    "build_control_principal_sid",
                    "datalengthbuild_control_principal_sid>=1anddatalengthbuild_control_principal_sid<=85",
                ),
            ],
        ),
    ):
        parsed = [c.split(":") for c in columns.split()]
        result.append([(1, 1, *([0] * 12))])
        result.append(
            [
                (i, name, kind, int(length), int(scale), *([0] * 12), None, 0)
                for i, (name, kind, length, scale) in enumerate(parsed, 1)
            ]
        )
        result.append(
            sorted((f"CK_{table}_{name}", ordinal, 0, 0, 0, expression) for ordinal, name, expression in checks)
        )
        indexes = [(f"PK_{table}", 1, 1, 1, 0, 0, 0, 0, 0, parsed[0][0], 1, 0, 0)]
        if "sessions" in table:
            indexes.extend(
                (f"UQ_{table}_generation_model", 2, 1, 0, 1, 0, 0, 0, 0, name, ordinal, 0, 0)
                for ordinal, name in enumerate(("generation_id", "model_unique_id_hash"), 1)
            )
        result.extend([indexes, [(1 if "enrollments" in table else 2,)]])
    return result


class InventoryCursor(Cursor):
    def __init__(self, rows):
        super().__init__()
        self.results = iter(rows)
        self.rows = []

    def execute(self, sql, *args):
        super().execute(sql, *args)
        self.rows = list(next(self.results))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


def test_exact_read_only_catalog_observation_is_accepted():
    cursor = InventoryCursor(inventory())
    verify_enrollment_tables(cursor)
    assert len(cursor.statements) == 10
    assert all(sql.startswith("SELECT") for sql, _ in cursor.statements)
    assert {args for _, args in cursor.statements} == {
        ("[dpone_physical].[physical_plan_enrollments_v1]",),
        ("[dpone_physical].[physical_model_sessions_v1]",),
    }


@pytest.mark.parametrize("section", range(10))
@pytest.mark.parametrize("mutation", ["absent", "extra", "changed"])
def test_missing_extra_or_changed_catalog_facts_reject(section, mutation):
    rows = inventory()
    if mutation == "absent":
        rows[section].pop()
    elif mutation == "extra":
        rows[section].append(rows[section][0])
    else:
        rows[section][0] = ("changed", *rows[section][0][1:])
    with pytest.raises(RuntimeError, match="inventory"):
        verify_enrollment_tables(InventoryCursor(rows))


@pytest.mark.parametrize("position", range(19))
def test_each_column_property_is_authoritative(position):
    rows = inventory()
    column = list(rows[1][0])
    column[position] = "changed"
    rows[1][0] = tuple(column)
    with pytest.raises(RuntimeError, match="inventory"):
        verify_enrollment_tables(InventoryCursor(rows))


def test_catalog_fixture_matches_real_canonical_table_source():
    """Read producer-owned source; never substitute synthetic procedure bodies."""
    import os
    import re
    from pathlib import Path

    path = Path(
        os.environ.get("DPONE_ENROLLMENT_SQL_SOURCE", "packages/dbt-dpone/control/sqlserver/physical-v1/enrollment.sql")
    )
    if not path.is_file():
        pytest.skip("real enrollment SQL dependency not integrated")
    table_source = path.read_text().split("\n-- DPONE ENROLLMENT TABLE BOUNDARY\n")[0]
    blocks = re.findall(r"CREATE TABLE \[dpone_physical\]\.\[(\w+)\]\((.*?)\n\);", table_source, re.S)
    assert len(blocks) == 2
    observed = inventory()
    for offset, (name, body) in zip((0, 5), blocks, strict=True):
        columns = re.findall(
            r"^ (\w+) (uniqueidentifier|varbinary|binary|datetime2|int)(?:\((max|\d+)\))? NOT NULL", body, re.M
        )
        projected = []
        for ordinal, (column, kind, size) in enumerate(columns, 1):
            length = {"uniqueidentifier": 16, "datetime2": 8, "int": 4}.get(kind)
            if length is None:
                length = -1 if size == "max" else int(size)
            projected.append((ordinal, column, kind, length, 7 if kind == "datetime2" else 0))
        assert projected == [row[:5] for row in observed[offset + 1]]
        assert set(re.findall(r"CONSTRAINT \[(CK_\w+)\]", body)) == {row[0] for row in observed[offset + 2]}
        assert set(re.findall(r"CONSTRAINT \[((?:PK|UQ)_\w+)\]", body)) == {row[0] for row in observed[offset + 3]}
        assert "DEFAULT" not in body.upper()
        assert all(name in row[0] for row in observed[offset + 2])


@pytest.mark.parametrize(
    "source",
    [
        None,
        "text",
        b"",
        b"CREATE TABLE",
        b"a\n-- DPONE ENROLLMENT TABLE BOUNDARY\nb\n-- DPONE ENROLLMENT TABLE BOUNDARY\nc",
    ],
)
def test_table_extractor_requires_finite_real_package_boundary(source):
    from dpone.adapters.dbt_mssql_physical_enrollment_tables import enrollment_tables

    with pytest.raises(ValueError):
        enrollment_tables(enrollment_sql=source)
