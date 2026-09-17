"""Real finite connection-observer SQL shape; live SQL remains separately qualified."""

from pathlib import Path

import pytest

from dpone.adapters.dbt_mssql_physical_session_queries import connection_observer_procedure

RESOURCE = Path("packages/dbt-dpone/control/sqlserver/physical-v1/session.sql")


def test_connection_observer_queries_only_current_non_mars_authenticated_session():
    sql = connection_observer_procedure(session_sql=RESOURCE.read_bytes(), connection_certificate_thumbprint=b"c" * 20)
    assert "CREATE PROCEDURE [dpone_physical].[physical_observe_current_connection_v1]" in sql
    assert "WHERE c.session_id=@@SPID" in sql
    assert "c.parent_connection_id IS NULL" in sql
    assert "s.original_security_id=SUSER_SID(ORIGINAL_LOGIN())" in sql
    assert "COUNT_BIG(*)" in sql
    assert "DPONE_SESSION_CONNECTION_UNOBSERVABLE" in sql
    assert "HAS_PERMS_BY_NAME(NULL,'SERVER','VIEW SERVER PERFORMANCE STATE')" not in sql
    assert "BEGIN TRY" in sql and "BEGIN CATCH" in sql
    assert "0x" + (b"c" * 20).hex() in sql
    assert "{{" not in sql
    assert "EXECUTE AS" not in sql
    assert "@connection_id uniqueidentifier OUTPUT" in sql
    assert "@session_id int OUTPUT" in sql


@pytest.mark.parametrize("bad", [None, b"", b"c" * 19, b"c" * 21, "c" * 20])
def test_connection_certificate_requires_exact_observed_thumbprint(bad):
    with pytest.raises(ValueError):
        connection_observer_procedure(session_sql=RESOURCE.read_bytes(), connection_certificate_thumbprint=bad)


def test_observer_template_must_have_exact_finite_markers():
    raw = RESOURCE.read_bytes()
    for malformed in (raw.replace(b"{{CONNECTION_CERTIFICATE}}", b"0x00"), raw + b"{{UNKNOWN}}"):
        with pytest.raises(ValueError):
            connection_observer_procedure(session_sql=malformed, connection_certificate_thumbprint=b"c" * 20)


def test_attach_uses_current_model_namespace_and_commits_durable_child_before_attach15():
    from dpone.adapters.dbt_mssql_physical_session_queries import ATTACH, CONNECTION_OBSERVER, session_procedures
    from tests.support.dbt_mssql_physical_registration import registration_inputs

    root = RESOURCE.parent
    modules = session_procedures(
        session_sql=RESOURCE.read_bytes(),
        enrollment_sql=(root / "enrollment.sql").read_bytes(),
        discovery_sql=(root / "discovery.sql").read_bytes(),
        model_database=registration_inputs()["model_database"],
        model_schema="models",
        model_schema_id=10,
        control_database="example",
        control_schema="runtime_control",
        enrollment_certificate_thumbprint=b"e" * 20,
        connection_certificate_thumbprint=b"c" * 20,
    )
    assert set(modules) == {ATTACH, CONNECTION_OBSERVER}
    sql = modules[ATTACH]
    assert "WHERE models.position=@selected_position" in sql
    assert "physical_control_require_owner_v1" not in sql
    assert sql.index("INSERT @source EXEC") < sql.index("physical_plan_enrollments_v1")
    assert sql.index("physical_observe_current_connection_v1") < sql.index("physical_model_sessions_v1")
    assert sql.index("INSERT [dpone_physical].[physical_model_sessions_v1]") < sql.index("COMMIT TRANSACTION;")
    assert "DPONE_SESSION_ALREADY_REGISTERED" in sql
    assert "DATALENGTH(@retained_model)<>DATALENGTH(@model_bytes)" in sql
    assert "JSON_QUERY(@enrollment,'$.executor') AS [executor_json],@selected_model AS [plan_json]" in sql
    assert "{{" not in sql


def test_attach_projection_names_match_actual_managed_macro_wire():
    import re

    from tests.test_dbt_mssql_managed_admission_macros import harness

    context, calls = harness(execute=False)
    expected = context["dpone_managed_columns"]("attach")
    projection = RESOURCE.read_text().rsplit(" COMMIT TRANSACTION;", 1)[1].split("END TRY", 1)[0]
    assert re.findall(r"\bAS \[([a-z0-9_]+)\]", projection) == expected
    assert calls == []
