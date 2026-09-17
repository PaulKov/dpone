"""Enrollment deployment checks; doubles never certify effective SQL rights."""

import pytest

from dpone.adapters.dbt_mssql_physical_enrollment_schema import MssqlPhysicalEnrollmentSchemaProvisioner


@pytest.mark.parametrize(
    "field",
    [
        "enrollment_sql",
        "session_sql",
        "discovery_sql",
        "enrollment_certificate_public_bytes",
        "connection_certificate_public_bytes",
    ],
)
@pytest.mark.parametrize("invalid", [b"", "text", None])
def test_missing_authenticated_inputs_reject_before_connection(field, invalid):
    inputs = dict(
        enrollment_sql=b"enrollment",
        session_sql=b"session",
        discovery_sql=b"discovery",
        enrollment_certificate_public_bytes=b"E",
        connection_certificate_public_bytes=b"C",
    )
    inputs[field] = invalid
    with pytest.raises(ValueError):
        MssqlPhysicalEnrollmentSchemaProvisioner(connection_factory=lambda: pytest.fail("must not connect"), **inputs)


def test_nonliteral_runtime_schema_rejects_before_loading_sql_or_connecting():
    from tests.test_dbt_mssql_physical_catalog_binding_store import values

    registration, binding = values()
    provisioner = MssqlPhysicalEnrollmentSchemaProvisioner(
        connection_factory=lambda: pytest.fail("must not connect"),
        enrollment_sql=b"enrollment",
        session_sql=b"session",
        discovery_sql=b"discovery",
        enrollment_certificate_public_bytes=b"E",
        connection_certificate_public_bytes=b"C",
    )
    with pytest.raises(ValueError, match="literal dpone_physical"):
        provisioner.apply(registration, binding)


def installed_fixture(*, same=True):
    from dataclasses import replace

    from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
    from tests.test_dbt_mssql_physical_discovery_schema import fixture

    registration, binding = fixture(same=same)
    registration = replace(registration, local_schema="dpone_physical")
    return registration, replace(binding, registration_sha256=physical_runtime_registration_digest(registration))


def provisioner(connection):
    return MssqlPhysicalEnrollmentSchemaProvisioner(
        connection_factory=lambda: connection,
        enrollment_sql=b"enrollment",
        session_sql=b"session",
        discovery_sql=b"discovery",
        enrollment_certificate_public_bytes=b"E",
        connection_certificate_public_bytes=b"C",
    )


def modules(registration):
    from dpone.adapters.dbt_mssql_physical_enrollment_schema import ATTACH, CONTROL, ENROLL, OBSERVER, READ, C, E

    return [
        ("model", "dpone_physical", name, C if name == OBSERVER else E, "SPVC")
        for name in (ENROLL, READ, ATTACH, OBSERVER)
    ] + [("control", registration.control_schema, CONTROL, E, "CPVC")]


@pytest.mark.parametrize("same", [True, False])
def test_finite_signed_permissions_are_new_inventory_only(same):
    from dpone.adapters.dbt_mssql_physical_enrollment_permissions import CL, CU, EU
    from tests.test_dbt_mssql_physical_source_schema import CatalogConnection

    registration, _ = installed_fixture(same=same)
    connection = CatalogConnection()
    provisioner(connection)._inventories(connection, registration, modules(registration), create=True, before=False)
    statements = [sql for sql, _ in connection.statements]
    grants = [sql for sql in statements if sql.startswith("GRANT")]
    assert set(grants) >= {
        f"GRANT VIEW DEFINITION TO [{EU}];",
        f"GRANT EXECUTE ON OBJECT::[dpone_physical].[physical_observe_current_connection_v1] TO [{EU}];",
        f"GRANT VIEW DEFINITION ON OBJECT::[dpone_physical].[physical_observe_current_connection_v1] TO [{CU}];",
        "GRANT EXECUTE ON OBJECT::[dpone_physical].[physical_attach_session_v1] TO [runtime_6];",
        "GRANT EXECUTE ON OBJECT::[dpone_physical].[physical_enroll_plan_set_v1] TO [runtime_5];",
        "GRANT EXECUTE ON OBJECT::[dpone_physical].[physical_read_plan_enrollment_v1] TO [runtime_5];",
    }
    assert not any(
        "physical_require_source_v1" in sql
        or "physical_discover_absent_v1" in sql
        or "physical_catalog_bindings_v1" in sql
        or "physical_runtime_registrations_v1" in sql
        for sql in grants
    )
    assert not any("UPDATE" in sql or "DELETE" in sql or "runtime_7" in sql for sql in grants)
    table_grants = [
        sql for sql in grants if "physical_plan_enrollments_v1" in sql or "physical_model_sessions_v1" in sql
    ]
    assert len(table_grants) == 4 and all(f"TO [{EU}]" in sql for sql in table_grants)
    assert sum("CREATE LOGIN" in sql for sql in statements) == 1
    assert any(f"GRANT VIEW SERVER PERFORMANCE STATE TO [{CL}]" in sql for sql in statements)
    assert any("sys.server_permissions" in sql and "EXCEPT SELECT 100,0" in sql for sql in statements)
    assert any("sys.server_role_members" in sql for sql in statements)
    assert len(grants) == len(set(grants))


@pytest.mark.parametrize("same", [True, False])
def test_inventory_replay_does_not_repair_grants_users_or_signatures(same):
    from tests.test_dbt_mssql_physical_source_schema import CatalogConnection

    registration, _ = installed_fixture(same=same)
    connection = CatalogConnection(existing=True)
    provisioner(connection)._inventories(connection, registration, modules(registration), create=False, before=False)
    assert not any(sql.startswith(("CREATE", "GRANT", "ADD", "ALTER", "REVOKE")) for sql, _ in connection.statements)


@pytest.mark.parametrize("row", [[], [(None,)], [(b"x" * 19,)], [(b"x" * 21,)], [(b"x" * 20,), (b"x" * 20,)]])
def test_malformed_or_ambiguous_observed_certificate_rejects(row):
    from dpone.adapters.dbt_mssql_physical_enrollment_schema import E
    from tests.test_dbt_mssql_physical_source_schema import CatalogConnection

    connection = CatalogConnection(thumbprints=[row])
    with pytest.raises(RuntimeError):
        provisioner(connection)._certificate(connection, E, b"E", private=True)


def test_certificate_identity_is_observed_not_derived_from_public_bytes():
    from dpone.adapters.dbt_mssql_physical_enrollment_schema import E
    from tests.test_dbt_mssql_physical_source_schema import CatalogConnection

    connection = CatalogConnection(thumbprints=[[(b"t" * 20,)]])
    assert provisioner(connection)._certificate(connection, E, b"E", private=True) == b"t" * 20
    assert any(args == (b"E",) and "CERTENCODED" in sql for sql, args in connection.statements)


def real_provisioner(connection):
    from pathlib import Path

    from dpone.adapters import dbt_mssql_physical_discovery_schema

    if not hasattr(dbt_mssql_physical_discovery_schema, "verify_catalog_binding_context"):
        pytest.skip("reviewed public catalog context dependency not integrated")
    root = Path("packages/dbt-dpone/control/sqlserver/physical-v1")
    if not all((root / name).is_file() for name in ("enrollment.sql", "session.sql", "discovery.sql")):
        pytest.skip("real SQL producer dependencies not integrated")
    return MssqlPhysicalEnrollmentSchemaProvisioner(
        connection_factory=lambda: connection,
        enrollment_sql=(root / "enrollment.sql").read_bytes(),
        session_sql=(root / "session.sql").read_bytes(),
        discovery_sql=(root / "discovery.sql").read_bytes(),
        enrollment_certificate_public_bytes=b"E",
        connection_certificate_public_bytes=b"C",
    )


def deployment_connection(registration, binding, **kwargs):
    from tests.test_dbt_mssql_physical_discovery_schema import Connection
    from tests.test_dbt_mssql_physical_enrollment_tables import inventory

    class DeploymentConnection(Connection):
        def __init__(self):
            kwargs.setdefault("thumbprints", [[(b"e" * 20,)], [(b"c" * 20,)], [(b"e" * 20,)], [(b"c" * 20,)]])
            super().__init__(registration, binding, **kwargs)
            self.table_rows = iter(inventory())

        def execute(self, sql, *args):
            super().execute(sql, *args)
            if sql.startswith(
                (
                    "SELECT s.principal_id",
                    "SELECT c.column_id",
                    "SELECT name,parent_column_id",
                    "SELECT i.name",
                    "SELECT COUNT(*) FROM sys.indexes",
                )
            ):
                self.rows = list(next(self.table_rows))
            return self

    return DeploymentConnection()


@pytest.mark.parametrize("same", [True, False])
@pytest.mark.parametrize("existing", [True, False])
def test_real_five_modules_and_tables_install_or_exact_replay(same, existing):
    from hashlib import sha256

    registration, binding = installed_fixture(same=same)
    connection = deployment_connection(registration, binding, existing=existing)
    observed = real_provisioner(connection).apply(registration, binding)
    assert observed.enrollment_certificate_thumbprint == b"e" * 20
    assert observed.connection_certificate_thumbprint == b"c" * 20
    assert len(observed.module_sha256) == 5 and len(observed.source_sha256) == 3
    assert connection.events == ["commit", "close", "close"]
    statements = [sql for sql, _ in connection.statements]
    definitions = [sql for sql in statements if sql.startswith("CREATE PROCEDURE")]
    if existing:
        assert not any(sql.startswith(("CREATE", "GRANT", "ADD", "ALTER", "REVOKE")) for sql in statements)
    else:
        assert len(definitions) == 5
        assert sum(sql.count("CREATE TABLE ") for sql in statements if sql.startswith("CREATE TABLE")) == 2
        assert {digest for _, digest in observed.module_sha256} == {
            "sha256:" + sha256(body.encode("utf-16le")).hexdigest() for body in definitions
        }
        assert sum(sql.startswith("ADD COUNTER SIGNATURE") for sql in statements) == 1
        assert sum(sql.startswith("ADD SIGNATURE") for sql in statements) == 4
    assert all("{{" not in definition for definition in definitions)
    if not existing:
        joined = "\n".join(definitions)
        assert "CROSS APPLY OPENJSON(models.document)" in joined
        assert "model_identity.model_unique_id" in joined
        assert "STRING_ESCAPE((SELECT value FROM OPENJSON(models.document)" not in joined
        assert "FOR XML PATH" not in joined
        assert "ROW_NUMBER() OVER(ORDER BY CONVERT(varchar(max),[key]" in joined
        assert joined.count("STRING_AGG(") == 2


@pytest.mark.parametrize(
    "failure",
    [
        "DPONE_ENROLLMENT_DATABASE_UNSAFE",
        "DPONE_ENROLLMENT_CERTIFICATE_UNSAFE",
        "DPONE_ENROLLMENT_SIGNATURE_INVENTORY_MISMATCH",
        "DPONE_ENROLLMENT_GRANT_INVENTORY_MISMATCH",
        "DPONE_ENROLLMENT_CERTIFICATE_USER_UNSAFE",
        "DPONE_ENROLLMENT_CONNECTION_LOGIN_UNSAFE",
        "DPONE_SOURCE_MODULE_INVENTORY_MISMATCH",
        "commit",
    ],
)
def test_full_apply_failure_or_unknown_commit_rolls_back_closes_without_success(failure):
    registration, binding = installed_fixture()
    connection = deployment_connection(registration, binding, existing=True, fail=failure)
    with pytest.raises(RuntimeError):
        real_provisioner(connection).apply(registration, binding)
    assert "rollback" in connection.events
    assert connection.events[-2:] == ["close", "close"]
    assert connection.events.count("commit") == (1 if failure == "commit" else 0)


@pytest.mark.parametrize("position", range(7))
def test_partial_deployment_rejects_before_creation(position):
    registration, binding = installed_fixture()
    objects = [None] * 7
    objects[position] = 100
    connection = deployment_connection(registration, binding, objects=objects)
    with pytest.raises(RuntimeError, match="partial"):
        real_provisioner(connection).apply(registration, binding)
    assert not any(sql.startswith(("CREATE", "GRANT", "ADD")) for sql, _ in connection.statements)
    assert connection.events == ["rollback", "close", "close"]


def test_cancellation_is_not_retried_or_returned_as_acknowledged_deployment():
    registration, binding = installed_fixture()
    connection = deployment_connection(registration, binding, existing=True)

    def cancelled():
        connection.events.append("commit")
        raise KeyboardInterrupt

    connection.commit = cancelled
    with pytest.raises(KeyboardInterrupt):
        real_provisioner(connection).apply(registration, binding)
    assert connection.events == ["commit", "rollback", "close", "close"]


@pytest.mark.parametrize(
    "identities",
    [(b"e" * 20,) * 4, (b"e" * 20, b"c" * 20, b"x" * 20, b"c" * 20), (b"e" * 20, b"c" * 20, b"e" * 20, b"x" * 20)],
)
def test_full_apply_conflicting_certificate_identities_reject_before_ddl(identities):
    registration, binding = installed_fixture()
    connection = deployment_connection(registration, binding, thumbprints=[[(value,)] for value in identities])
    with pytest.raises(RuntimeError, match="identities"):
        real_provisioner(connection).apply(registration, binding)
    assert connection.events == ["rollback", "close", "close"]
    assert not any(sql.startswith(("CREATE", "GRANT", "ADD")) for sql, _ in connection.statements)


@pytest.mark.parametrize("mutation", ["registration", "binding-absent", "binding-conflict"])
def test_full_apply_requires_actual_protected_rows_before_any_ddl(mutation):
    registration, binding = installed_fixture()
    options = (
        {"stored_registration": ("other",)}
        if mutation == "registration"
        else {"stored_binding": () if mutation == "binding-absent" else (b"wrong", b"wrong", b"wrong")}
    )
    connection = deployment_connection(registration, binding, **options)
    with pytest.raises(RuntimeError):
        real_provisioner(connection).apply(registration, binding)
    assert connection.events == ["rollback", "close", "close"]
    assert not any(sql.startswith(("CREATE", "GRANT", "ADD")) for sql, _ in connection.statements)


def test_full_apply_table_inventory_drift_prevents_acknowledgement():
    from tests.test_dbt_mssql_physical_enrollment_tables import inventory

    registration, binding = installed_fixture()
    connection = deployment_connection(registration, binding, existing=True)
    rows = inventory()
    rows[1].pop()
    connection.table_rows = iter(rows)
    with pytest.raises(RuntimeError, match="table inventory"):
        real_provisioner(connection).apply(registration, binding)
    assert connection.events == ["rollback", "close", "close"]
