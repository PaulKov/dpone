"""Finite package expansion preserves caller identity and native lock policy."""

import re
from pathlib import Path

import pytest

from dpone.adapters.dbt_mssql_physical_registration_schema import COLUMNS
from dpone.adapters.dbt_mssql_physical_source_queries import ENTRY, HELPER, source_procedures
from dpone.adapters.native_generation_mssql_owner import physical_owner

PACKAGE = Path(__file__).parents[1] / "packages/dbt-dpone/control/sqlserver/physical-v1/admission.sql"


def modules(**changes):
    arguments = dict(
        admission_sql=PACKAGE.read_bytes(),
        model_database="model",
        local_schema="local_control",
        control_database="control",
        control_schema="native_control",
    )
    arguments.update(changes)
    return source_procedures(**arguments)


def test_exact_modules_and_public_signature():
    bodies = modules()
    assert set(bodies) == {ENTRY, HELPER}
    assert bodies[ENTRY].split("AS\n")[0].count("uniqueidentifier") == 3
    assert "EXEC [control].[native_control].[physical_control_require_source_v1]" in bodies[ENTRY]
    for body in bodies.values():
        assert "{{" not in body
        assert "@@TRANCOUNT<>1 OR XACT_STATE()<>1" in body
        assert "ORIGINAL_LOGIN()" in body
        assert "EXECUTE AS" not in body
        assert re.search(r"\b(COMMIT|ROLLBACK)\b|BEGIN TRANSACTION", body) is None


def test_complete_frozen_projection_and_p_before_g():
    bodies = modules()
    for name, _, _ in COLUMNS:
        assert "r." + name in bodies[ENTRY]
    helper = bodies[HELPER]
    owner = physical_owner("native_control")
    assert owner in helper
    assert helper.index("WITH (READCOMMITTEDLOCK)") < helper.index(owner)
    assert helper.index(owner) < helper.index("[native_generations_v1] WITH (UPDLOCK,HOLDLOCK)")
    assert "writer_admission IN ('OPEN','CLOSED')" in helper
    assert "phase='BUILDING' AND outcome='ACTIVE'" in helper


def test_coordinate_escaping_and_determinism():
    assert modules() == modules()
    assert "EXEC [contro]]l]." in modules(control_database="contro]l")[ENTRY]
    with pytest.raises(ValueError):
        modules(control_schema="evil]; DROP TABLE x")
    with pytest.raises(ValueError):
        modules(admission_sql=b"{{unrecognized}}")
