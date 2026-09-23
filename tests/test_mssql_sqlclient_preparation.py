"""Closed preparation records do not turn fixture data into live authority."""

import pytest


def test_ownership_counts_are_complete_ordered_int64_not_legacy_int32():
    from dpone.contracts.mssql_sqlclient_observe_rows import OWNERSHIP_LABELS, validate_rows

    rows = [[label, 0] for label in OWNERSHIP_LABELS]
    assert len(validate_rows("PREP_OWNERSHIP_COUNTS", rows)) == 25
    for changed in (rows[:-1], rows[::-1], rows + [rows[0]]):
        with pytest.raises(ValueError):
            validate_rows("PREP_OWNERSHIP_COUNTS", changed)
    for value in (True, 0.0, -1, 2**63):
        changed = [list(row) for row in rows]
        changed[0][1] = value
        with pytest.raises(ValueError):
            validate_rows("PREP_OWNERSHIP_COUNTS", changed)
    rows[0][1] = 2**40
    assert validate_rows("PREP_OWNERSHIP_COUNTS", rows)[0][1] == 2**40


def test_preparation_opcodes_are_closed_and_no_argument():
    from dpone.contracts.mssql_sqlclient_observe_wire import command
    from tests.test_mssql_sqlclient_observe import request

    for opcode in (
        "PREP_ENV",
        "PREP_OWNERSHIP_COUNTS",
        "PREP_SERVER_PERMISSIONS",
        "PREP_EFFECTIVE",
        "PREP_STAGE_SECURITY",
    ):
        command(request(), b"n" * 32, 1, opcode, {})
        with pytest.raises(ValueError):
            command(request(), b"n" * 32, 1, opcode, {"sql": "SELECT 1"})


def test_live_baseline_constructor_cannot_admit_a_populated_document():
    from dpone.contracts.mssql_sqlclient_preparation import admit_preparation_baseline

    for value in ({}, {"qualified": True}, {"provenance": "fixture"}):
        with pytest.raises(ValueError, match="qualification_required"):
            admit_preparation_baseline(value)


def test_database_securable_query_resolves_hidden_negative_system_object_ids():
    from dpone.adapters.mssql_sqlclient_preparation_sql import IDENTITY_SQL

    query = next(item[2] for item in IDENTITY_SQL if item[0] == "database_securables")
    assert "COALESCE(o.name,OBJECT_NAME(k.major_id))" in query
    assert "COALESCE(os.name,OBJECT_SCHEMA_NAME(k.major_id))" in query
    assert "OBJECTPROPERTYEX(k.major_id,N'IsMSShipped')" in query
    assert "COALESCE(c.name,COL_NAME(k.major_id,k.minor_id))" in query


def profile_fixture():
    """Explicit synthetic controls, independent expected semantic permission rows."""
    from dataclasses import replace
    from types import SimpleNamespace

    from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientGrantPrincipal, SqlClientPermissionRow
    from dpone.contracts.mssql_sqlclient_observe_rows import OWNERSHIP_LABELS
    from tests.test_mssql_sqlclient_grant_catalog import Cursor
    from tests.test_mssql_sqlclient_observe import request
    from tests.test_mssql_sqlclient_observer_incarnation_adapter import own_row

    value = request()
    value = replace(
        value,
        writer_admission=replace(
            value.writer_admission, login=replace(value.writer_admission.login, is_sysadmin=False)
        ),
    )
    login, stage = value.writer_admission.login, value.selected_stage
    owner_sid = bytes.fromhex(value.management_admission.database.owner_sid)
    inventory = SimpleNamespace(
        writer=value.writer_principal,
        public=SqlClientGrantPrincipal(0, "public", "00", "DATABASE_ROLE", "NONE"),
        members=(),
        permissions=(SqlClientPermissionRow(1, -100, 1, 0, 1, "SL", "SELECT", "G"),),
    )
    profile = {
        "PREP_ENV": [
            (
                "16.synthetic",
                "fixture",
                3,
                16,
                160,
                "fixture",
                stage.database_id,
                stage.database_name,
                0,
                0,
                0,
                login.principal_id,
                login.name,
                bytes.fromhex(login.sid),
                "SQL_LOGIN",
                0,
                5,
                "writer_user",
                b"\xaa",
                "SQL_USER",
                "INSTANCE",
                owner_sid,
            )
        ],
        "PREP_OWNERSHIP_COUNTS": [(label, 0) for label in OWNERSHIP_LABELS],
        "PREP_STAGE_SECURITY": [(stage.object_id, 0)],
        "PREP_SERVER_PERMISSIONS": [(105, 2, 2, 1, "CO", "CONNECT", "G", "ENDPOINT", "TSQL Default TCP")],
        "PREP_PERMISSION_IDENTITIES": [
            {
                "server_principals": [
                    (1, "sa", b"\x01", "SQL_LOGIN", 0),
                    (2, "public", b"\x02", "SERVER_ROLE", 0),
                    (login.principal_id, login.name, bytes.fromhex(login.sid), "SQL_LOGIN", 0),
                ],
                "database_principals": [
                    (0, "public", b"\x00", "DATABASE_ROLE", "NONE"),
                    (1, "dbo", owner_sid, "SQL_USER", "INSTANCE"),
                    (5, "writer_user", b"\xaa", "SQL_USER", "INSTANCE"),
                ],
                "server_endpoints": [(2, "TSQL Default TCP", "TSQL", "TCP", "STARTED", 0, 1)],
                "database_securables": [(1, -100, 1, "OBJECT_OR_COLUMN", "sys", "objects", "object_id", 1)],
            }
        ],
        "PREP_EFFECTIVE": [
            {
                "subject": (
                    login.name,
                    bytes.fromhex(login.sid),
                    5,
                    "writer_user",
                    stage.database_id,
                    stage.database_name,
                    0,
                    0,
                    value.management_admission.login.name,
                ),
                "login_token": [
                    (login.principal_id, bytes.fromhex(login.sid), login.name, "SQL LOGIN", "GRANT OR DENY")
                ],
                "user_token": [(5, b"\xaa", "writer_user", "SQL USER", "GRANT OR DENY")],
                "server_permissions": [],
                "database_permissions": [],
                "restored_management": own_row(Cursor().own),
            }
        ],
    }
    baseline = dict(
        server_build=dict(product_version="16.synthetic", edition="fixture", engine_edition=3),
        database_profile=dict(
            compatibility_level=160, collation="fixture", containment=0, trustworthy=0, database_chaining=0
        ),
        effective_server_permissions=[],
        effective_database_permissions=[],
        token_rules={
            name: [dict(subject="writer", type=kind, usage="GRANT OR DENY")]
            for name, kind in (("login", "SQL LOGIN"), ("user", "SQL USER"))
        },
        stock_server_permissions=[
            dict(
                class_id=105,
                securable=["ENDPOINT", "TSQL Default TCP", "TSQL", "TCP", "STARTED", 0, 1],
                grantee="public",
                grantor=dict(name="sa", type="SQL_LOGIN", detail=0, sid_binding="fixed", sid="01"),
                type="CO",
                permission="CONNECT",
                state="G",
            )
        ],
        stock_database_permissions=[
            dict(
                class_id=1,
                securable=["OBJECT_OR_COLUMN", "sys", "objects", "object_id", 1],
                grantee="public",
                grantor=dict(name="dbo", type="SQL_USER", detail="INSTANCE", sid_binding="database_owner", sid=None),
                type="SL",
                permission="SELECT",
                state="G",
            )
        ],
    )
    return baseline, value, inventory, profile


def test_exact_stock_endpoint_and_signed_system_column_bind_independent_controls():
    from dpone.contracts.mssql_sqlclient_preparation_profile import validate_profile

    validate_profile(*profile_fixture())


def test_token_admission_uses_identity_not_dynamic_principal_id_order():
    """SQL orders public before a newly created writer on normal installations."""
    from dpone.contracts.mssql_sqlclient_preparation_profile import validate_profile

    baseline, request, inventory, profile = profile_fixture()
    baseline["token_rules"]["login"].append(dict(subject="public", type="SERVER ROLE", usage="GRANT OR DENY"))
    baseline["token_rules"]["user"].append(dict(subject="public", type="ROLE", usage="GRANT OR DENY"))
    effective = profile["PREP_EFFECTIVE"][0]
    effective["login_token"] = [
        (2, b"\x02", "public", "SERVER ROLE", "GRANT OR DENY"),
        *effective["login_token"],
    ]
    effective["user_token"] = [
        (0, b"\x00", "public", "ROLE", "GRANT OR DENY"),
        *effective["user_token"],
    ]

    validate_profile(baseline, request, inventory, profile)


@pytest.mark.parametrize(
    "fault",
    [
        "same_name_user_endpoint",
        "endpoint_type",
        "endpoint_state",
        "endpoint_admin",
        "missing_endpoint",
        "extra_principal",
        "duplicate_principal",
        "fake_sa_sid",
        "fake_dbo_sid",
        "missing_column",
        "user_same_name_object",
        "unknown_class",
        "bool_system",
        "impossible_minor",
    ],
)
def test_permission_resolver_rejects_unresolved_or_changed_identity(fault):
    from dpone.contracts.mssql_sqlclient_preparation_profile import validate_profile

    baseline, request, inventory, profile = profile_fixture()
    values = profile["PREP_PERMISSION_IDENTITIES"][0]
    if fault.startswith("endpoint_") or fault == "same_name_user_endpoint":
        row = list(values["server_endpoints"][0])
        field, value = {
            "same_name_user_endpoint": (6, 0),
            "endpoint_type": (2, "OTHER"),
            "endpoint_state": (4, "STOPPED"),
            "endpoint_admin": (5, 1),
        }[fault]
        row[field] = value
        values["server_endpoints"] = [row]
    elif fault == "missing_endpoint":
        values["server_endpoints"] = []
    elif fault == "extra_principal":
        values["server_principals"].append((999, "other", b"\xff", "SQL_LOGIN", 0))
    elif fault == "duplicate_principal":
        values["server_principals"].append(values["server_principals"][0])
    elif fault in ("fake_sa_sid", "fake_dbo_sid"):
        key, index = ("server_principals", 0) if fault == "fake_sa_sid" else ("database_principals", 1)
        row = list(values[key][index])
        row[2] = b"\xff"
        values[key][index] = row
    else:
        row = list(values["database_securables"][0])
        index, value = {
            "missing_column": (6, None),
            "user_same_name_object": (7, 0),
            "unknown_class": (0, 9),
            "bool_system": (7, True),
            "impossible_minor": (2, -1),
        }[fault]
        row[index] = value
        values["database_securables"] = [row]
    with pytest.raises(ValueError):
        validate_profile(baseline, request, inventory, profile)


def test_resolver_combined_byte_limit_and_row_overflow_are_not_truncated():
    from dpone.contracts.mssql_sqlclient_observe_rows import validate_rows

    value = profile_fixture()[3]["PREP_PERMISSION_IDENTITIES"][0]
    value["server_principals"] = [(i, "n" * 128, b"a" * 85, "t" * 128, 0) for i in range(8194)]
    with pytest.raises(ValueError):
        validate_rows("PREP_PERMISSION_IDENTITIES", [value])
    value["server_principals"] = [(i, "n", b"a", "t", 0) for i in range(8195)]
    with pytest.raises(ValueError):
        validate_rows("PREP_PERMISSION_IDENTITIES", [value])


def test_exact_combined_preparation_envelope_limit_never_truncates():
    from dpone.contracts.mssql_sqlclient_preparation import MAX_PREPARATION_BYTES, PREPARATION_KEYS, preparation_bytes

    value = {key: {} for key in PREPARATION_KEYS}
    value.update(schema="dpone.sqlclient.preparation.v1", empty=1, operation_deadline_ns=1, policy={"padding": ""})
    used = len(preparation_bytes(value))
    value["policy"]["padding"] = "x" * (MAX_PREPARATION_BYTES - used)
    assert len(preparation_bytes(value)) == MAX_PREPARATION_BYTES
    value["policy"]["padding"] += "x"
    with pytest.raises(ValueError):
        preparation_bytes(value)
