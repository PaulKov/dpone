"""Opt-in source-identity proof for both layouts, not model/route qualification."""

import json
import os
from dataclasses import replace
from uuid import uuid4

import pytest

from dpone.adapters.dbt_mssql_physical_source import PhysicalSourceReadError
from dpone.adapters.dbt_workspace_mssql_attempt_admission import MssqlDbtWorkspaceAttemptAdmission
from dpone.contracts.native_source_custody_codec import encode_source_executor_binding
from tests.support.dbt_mssql_physical_source_authority import LOCAL, SCHEMA
from tests.support.dbt_mssql_physical_source_fixture import CERTIFICATE, CERTIFICATE_USER, SourceFixture

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_PHYSICAL_SOURCE_LIVE") != "1",
        reason="isolated source proof disabled",
    ),
]


@pytest.fixture(params=["same_database", "two_database"])
def source(request):
    fixture = SourceFixture(pytest.importorskip("pyodbc"), layout=request.param)
    try:
        fixture.install()
        # A rejection is evidence only after this exact fixture proved usable.
        before = fixture.snapshot()
        for role in ("metadata", "build"):
            assert fixture.read(role).reservation == fixture.request.reservation
        assert fixture.snapshot() == before
        yield fixture
    finally:
        fixture.cleanup()


@pytest.mark.parametrize("role", ["metadata", "build"])
def test_live_actual_login_retains_namespace_identity_and_native_rows(source, role, record_property):
    before = source.snapshot()
    value = source.read(role)
    mapping = getattr(source.registration.principals, role)
    assert value.observed_model_principal == mapping.model
    assert value.observed_control_principal == mapping.control
    assert (mapping.model.principal_id == mapping.control.principal_id) == (source.layout == "same_database")
    assert value.reservation == source.request.reservation
    assert value.source_revision == 2 and value.guard_epoch == source.request.guard.fencing_epoch
    assert source.snapshot() == before
    record_property("source_bridge_inventory", json.dumps(source.evidence(), sort_keys=True))
    record_property(
        "retained_fixture_inputs",
        json.dumps(
            {locator: payload.decode("utf-8") for locator, payload in source.authority.payloads.items()},
            sort_keys=True,
        ),
    )


@pytest.mark.parametrize("role", ["metadata", "build"])
def test_live_direct_helper_and_native_tables_remain_inaccessible(source, role):
    before = source.snapshot()
    with source.connection("control", role) as connection:
        for sql in (
            f"EXEC {SCHEMA}.physical_control_require_source_v1",
            f"SELECT * FROM {SCHEMA}.native_generations_v1",
        ):
            with pytest.raises(source.pyodbc.Error, match="permission|Permission|denied"):
                connection.execute(sql)
            connection.rollback()
    if role == "build":
        with source.connection("control", role) as connection:
            with pytest.raises(source.pyodbc.Error, match="permission|Permission|denied"):
                connection.execute(f"EXEC {SCHEMA}.native_source_writer_bind_v1")
    assert source.snapshot() == before


@pytest.mark.parametrize("damage", ["signature", "countersignature", "cert_grant", "explicit_deny", "entry_altered"])
def test_live_signature_and_permission_damage_respects_topology(source, damage):
    namespace = "model" if damage in {"signature", "entry_altered"} else "control"
    statements = {
        "signature": f"DROP SIGNATURE FROM OBJECT::{LOCAL}.physical_require_source_v1 BY CERTIFICATE {CERTIFICATE}",
        "countersignature": f"DROP COUNTER SIGNATURE FROM OBJECT::{SCHEMA}.physical_control_require_source_v1 BY CERTIFICATE {CERTIFICATE}",
        "cert_grant": f"REVOKE EXECUTE ON OBJECT::{SCHEMA}.physical_control_require_source_v1 FROM {CERTIFICATE_USER}",
        "explicit_deny": f"DENY EXECUTE ON OBJECT::{SCHEMA}.physical_control_require_source_v1 TO [{source.credentials['build'][0]}]",
        "entry_altered": f"ALTER PROCEDURE {LOCAL}.physical_require_source_v1 AS SELECT 1",
    }
    with source.connection(namespace, autocommit=True) as admin:
        admin.execute(statements[damage])
    before = source.snapshot()
    if source.layout == "same_database" and damage in {"cert_grant", "explicit_deny"}:
        # A local dbo ownership chain bypasses this permission hop, not the
        # explicit signature inventory or native source/owner predicates.
        observed = source.read("build")
        assert observed.reservation == source.request.reservation
        assert observed.executor_payload == encode_source_executor_binding(source.executor)
        assert observed.observed_model_principal == source.registration.principals.build.model
        assert observed.observed_control_principal == source.registration.principals.build.control
    else:
        with pytest.raises(PhysicalSourceReadError):
            source.read("build")
    assert source.snapshot() == before
    if damage in {"cert_grant", "explicit_deny"}:
        with source.connection("control", "build") as connection:
            with pytest.raises(source.pyodbc.Error, match="permission|Permission|denied"):
                connection.execute(f"EXEC {SCHEMA}.physical_control_require_source_v1")
        if damage == "cert_grant":
            assert source.provisioner.apply(source.registration) == source.registration
            repaired = source.snapshot()
            if source.layout == "same_database":
                # Intentional privileged GRANT DDL advances the native DDL
                # observer. It is not a mutation by the read-only bridge.
                epoch = "semantic_refresh_ddl_epoch"
                assert repaired[epoch][0][0] == before[epoch][0][0]
                assert repaired[epoch][0][1] > before[epoch][0][1]
                assert repaired[epoch][0][2] == "GRANT_DATABASE"
                assert {key: rows for key, rows in repaired.items() if key != epoch} == {
                    key: rows for key, rows in before.items() if key != epoch
                }
            else:
                assert repaired == before
            before = repaired
            assert_certificate_grants(source)
            assert source.read("build").reservation == source.request.reservation
        else:
            with pytest.raises(Exception, match="RUNTIME_PRINCIPAL_UNSAFE"):
                source.provisioner.apply(source.registration)
    if damage == "entry_altered":
        with pytest.raises(Exception, match="MODULE_INVENTORY_MISMATCH"):
            source.provisioner.apply(source.registration)
    assert source.snapshot() == before


def assert_certificate_grants(source):
    """Observe the complete finite grant set, including absence of CONNECT."""
    entry = (1, LOCAL, "physical_require_source_v1", 0, "VIEW DEFINITION", "G")
    helper = {
        (1, SCHEMA, "physical_control_require_source_v1", 0, permission, "G")
        for permission in ("EXECUTE", "VIEW DEFINITION")
    }
    inventory = {}
    for namespace in ("model", "control"):
        expected = {entry} | helper if source.layout == "same_database" else {entry} if namespace == "model" else helper
        with source.connection(namespace) as admin:
            rows = admin.execute(
                "SELECT class,OBJECT_SCHEMA_NAME(major_id),OBJECT_NAME(major_id),minor_id,permission_name,state "
                "FROM sys.database_permissions WHERE grantee_principal_id=DATABASE_PRINCIPAL_ID(?)",
                CERTIFICATE_USER,
            ).fetchall()
        inventory[namespace] = sorted(tuple(row) for row in rows)
        assert set(inventory[namespace]) == expected
        assert len(rows) == len(expected)
    return inventory


def test_live_certificate_user_has_exact_topology_grants(source, record_property):
    before = source.snapshot()
    record_property("certificate_grants", json.dumps(assert_certificate_grants(source), sort_keys=True))
    assert source.snapshot() == before


def test_live_unexpected_certificate_grant_is_not_repaired(source):
    with source.connection(autocommit=True) as admin:
        admin.execute(f"GRANT VIEW DEFINITION ON SCHEMA::{LOCAL} TO {CERTIFICATE_USER}")
    before = source.snapshot()
    with pytest.raises(Exception, match="CERTIFICATE_USER_UNSAFE"):
        source.provisioner.apply(source.registration)
    with source.connection() as admin:
        assert (
            admin.execute(
                "SELECT COUNT(*) FROM sys.database_permissions WHERE class=3 AND major_id=SCHEMA_ID(?) "
                "AND grantee_principal_id=DATABASE_PRINCIPAL_ID(?) AND permission_name='VIEW DEFINITION' AND state='G'",
                LOCAL,
                CERTIFICATE_USER,
            ).fetchone()[0]
            == 1
        )
    assert source.snapshot() == before


@pytest.mark.parametrize("mode", ["extra", "foreign"])
def test_live_entry_rejects_non_exact_signature_inventory(source, mode):
    with source.connection(autocommit=True) as admin:
        admin.execute("CREATE CERTIFICATE unrelated_fixture_certificate WITH SUBJECT='Unrelated fixture signer'")
        if mode == "foreign":
            admin.execute(
                f"DROP SIGNATURE FROM OBJECT::{LOCAL}.physical_require_source_v1 BY CERTIFICATE {CERTIFICATE}"
            )
        admin.execute(
            f"ADD SIGNATURE TO OBJECT::{LOCAL}.physical_require_source_v1 BY CERTIFICATE unrelated_fixture_certificate"
        )
    before = source.snapshot()
    with pytest.raises(PhysicalSourceReadError):
        source.read("build")
    assert source.snapshot() == before


@pytest.mark.parametrize("mode", ["extra", "foreign"])
def test_live_helper_rejects_non_exact_countersignature_inventory(source, mode):
    with source.connection("control", autocommit=True) as admin:
        admin.execute("CREATE CERTIFICATE unrelated_fixture_certificate WITH SUBJECT='Unrelated fixture signer'")
        if mode == "foreign":
            admin.execute(
                f"DROP COUNTER SIGNATURE FROM OBJECT::{SCHEMA}.physical_control_require_source_v1 BY CERTIFICATE {CERTIFICATE}"
            )
        admin.execute(
            f"ADD COUNTER SIGNATURE TO OBJECT::{SCHEMA}.physical_control_require_source_v1 BY CERTIFICATE unrelated_fixture_certificate"
        )
    before = source.snapshot()
    with pytest.raises(PhysicalSourceReadError):
        source.read("build")
    assert source.snapshot() == before


@pytest.mark.parametrize(
    "damage", ["stale_epoch", "unknown_outcome", "wrong_profile", "missing_generation", "wrong_invocation"]
)
def test_live_source_custody_rejects_stale_or_wrong_identity(source, damage):
    sql = {
        "stale_epoch": f"UPDATE {SCHEMA}.semantic_refresh_guards SET fencing_epoch=fencing_epoch+1",
        "unknown_outcome": f"UPDATE {SCHEMA}.native_generations_v1 SET outcome='UNKNOWN'",
        "wrong_profile": f"UPDATE {SCHEMA}.native_generation_profiles_v1 SET max_generation_bytes=max_generation_bytes-1",
    }
    if damage in sql:
        with source.connection("control", autocommit=True) as admin:
            admin.execute(sql[damage])
    before = source.snapshot()
    arguments = (
        {"generation": str(uuid4())}
        if damage == "missing_generation"
        else {"invocation": str(uuid4())}
        if damage == "wrong_invocation"
        else {}
    )
    with pytest.raises(PhysicalSourceReadError):
        source.read(**arguments)
    assert source.snapshot() == before


def test_live_closed_launch_admission_still_allows_admitted_executor(source):
    snapshot = source.provider.read_custody(source.executor.generation_id)
    source.provider.close_writer_admission(source.reserved, expected_revision=snapshot.revision)
    before = source.snapshot()
    assert source.read().source_revision == 3
    assert source.snapshot() == before


@pytest.mark.parametrize("depth", [0, 2])
def test_live_transaction_depth_is_owned_by_caller(source, depth):
    before = source.snapshot()
    with source.connection("model", "build", autocommit=True) as connection:
        try:
            for _ in range(depth):
                connection.execute("BEGIN TRANSACTION")
            with pytest.raises(source.pyodbc.Error, match="TRANSACTION_OR_ID_INVALID"):
                connection.execute(
                    f"EXEC {LOCAL}.physical_require_source_v1 ?,?,?",
                    source.registration.registration_id,
                    str(source.executor.generation_id),
                    str(source.executor.invocation_id),
                )
        finally:
            connection.execute("IF @@TRANCOUNT>0 ROLLBACK")
    assert source.snapshot() == before


def test_live_observer_cannot_claim_build_role(source):
    before = source.snapshot()
    with pytest.raises(PhysicalSourceReadError):
        source.read("observer")
    assert source.snapshot() == before


@pytest.mark.parametrize("namespace", ["model", "control"])
def test_live_replaced_user_cannot_reuse_registered_principal_number(source, namespace):
    login = source.credentials["build"][0]
    with source.connection(namespace, autocommit=True) as admin:
        admin.execute(f"DROP USER [{login}]")
        admin.execute(f"CREATE USER [{login}] WITHOUT LOGIN")
        if namespace == "model":
            admin.execute(f"GRANT EXECUTE ON OBJECT::{LOCAL}.physical_require_source_v1 TO [{login}]")
    before = source.snapshot()
    with pytest.raises(PhysicalSourceReadError):
        source.read("build")
    assert source.snapshot() == before


def test_live_impersonation_is_not_an_independent_authenticated_login(source):
    before = source.snapshot()
    with source.connection(autocommit=True) as admin:
        admin.execute(f"EXECUTE AS LOGIN='{source.credentials['build'][0]}'")
        try:
            admin.execute("BEGIN TRANSACTION")
            with pytest.raises(source.pyodbc.Error, match="CALLER"):
                admin.execute(
                    f"EXEC {LOCAL}.physical_require_source_v1 ?,?,?",
                    source.registration.registration_id,
                    str(source.executor.generation_id),
                    str(source.executor.invocation_id),
                )
        finally:
            admin.execute("IF @@TRANCOUNT>0 ROLLBACK; REVERT")
    assert source.snapshot() == before


@pytest.mark.parametrize("damage", ["digest", "projection", "missing_registration"])
def test_live_registration_corruption_never_authorizes_source_read(source, damage):
    statements = {
        "digest": f"UPDATE {LOCAL}.physical_runtime_registrations_v1 SET registration_digest="
        "CONVERT(varbinary(71),'sha256:'+REPLICATE('0',64))",
        "projection": f"UPDATE {LOCAL}.physical_runtime_registrations_v1 SET max_catalog_rows=max_catalog_rows+1",
        "missing_registration": f"DELETE FROM {LOCAL}.physical_runtime_registrations_v1",
    }
    with source.connection(autocommit=True) as admin:
        admin.execute(statements[damage])
    before = source.snapshot()
    with pytest.raises(PhysicalSourceReadError):
        source.read("build")
    assert source.snapshot() == before


def test_live_terminal_attempt_cannot_reuse_admitted_generation(source):
    admission = MssqlDbtWorkspaceAttemptAdmission(lambda: source.connect("control"), control_schema=SCHEMA)
    receipt = admission.terminalize(source.request.workspace_attempt, state="FAILED")
    assert receipt.state == "FAILED"
    before = source.snapshot()
    with pytest.raises(PhysicalSourceReadError):
        source.read("build")
    assert source.snapshot() == before


def test_live_changed_retained_executor_is_not_the_expected_invocation(source):
    changed = replace(source.executor, invocation_id=uuid4())
    with source.connection("control", autocommit=True) as admin:
        admin.execute(f"UPDATE {SCHEMA}.native_generations_v1 SET executor=?", encode_source_executor_binding(changed))
    before = source.snapshot()
    with pytest.raises(PhysicalSourceReadError):
        source.read("build")
    assert source.snapshot() == before


@pytest.mark.parametrize("phase", ["BUILDING", "FROZEN"])
def test_live_untrusted_completion_metadata_cannot_pass_active_source_boundary(source, phase):
    """Deliberate privileged corruption is not a genuine completion/freeze claim."""
    source.provider.close_writer_admission(source.reserved, expected_revision=2)
    reference = source.authority.retain("negative-completion", {"negative_test_only": True, "not_a_completion": True})
    payload = source.authority.payloads[reference.locator]
    with source.connection("control", autocommit=True) as admin:
        admin.execute(
            f"UPDATE {SCHEMA}.native_generations_v1 SET revision=4,completion_payload=?,completion_locator=?,completion_digest=?",
            payload,
            reference.locator.encode(),
            reference.sha256.encode(),
        )
        if phase == "FROZEN":
            admin.execute(
                f"UPDATE {SCHEMA}.native_generations_v1 SET phase='FROZEN',revision=5,frozen_payload=?,frozen_locator=?,frozen_digest=?",
                payload,
                reference.locator.encode(),
                reference.sha256.encode(),
            )
    before = source.snapshot()
    with pytest.raises(PhysicalSourceReadError):
        source.read("build")
    assert source.snapshot() == before
