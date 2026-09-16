"""Discovery provisioning choreography; actual SQL permissions require live proof."""

from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest

from dpone.adapters.dbt_mssql_physical_discovery_queries import ENTRY, HELPER, discovery_procedures
from dpone.adapters.dbt_mssql_physical_discovery_schema import MssqlPhysicalDiscoverySchemaProvisioner
from dpone.adapters.dbt_mssql_physical_registration_store import registration_columns
from dpone.contracts.dbt_mssql_physical_catalog_binding import catalog_binding_digest, encode_catalog_binding
from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
from tests.test_dbt_mssql_physical_catalog_binding_store import values
from tests.test_dbt_mssql_physical_source_schema import CatalogConnection


class Connection(CatalogConnection):
    """DB-API lifecycle/failure double; SQL text assertions are not live grants."""

    def __init__(self, registration, binding, *, stored_registration=None, stored_binding=None, objects=None, **kwargs):
        super().__init__(**kwargs)
        self.registration = (
            tuple(registration_columns(registration).values()) if stored_registration is None else stored_registration
        )
        self.binding = (
            (
                binding.registration_sha256.encode("ascii"),
                encode_catalog_binding(binding),
                catalog_binding_digest(binding).encode("ascii"),
            )
            if stored_binding is None
            else stored_binding
        )
        self.objects = iter(objects) if objects is not None else None

    def execute(self, sql, *parameters):
        super().execute(sql, *parameters)
        if sql.startswith("SELECT registration_id,") and "physical_runtime_registrations_v1" in sql:
            self.rows = [self.registration]
        elif sql.startswith("SELECT registration_digest,payload,binding_digest"):
            self.rows = [self.binding] if self.binding else []
        elif sql.startswith("SELECT OBJECT_ID") and self.objects is not None:
            self.rows = [(next(self.objects),)]
        return self


def fixture(*, same=True):
    registration, binding = values()
    if not same:
        registration = replace(
            registration,
            control_database=replace(
                registration.control_database,
                database_name="control_db",
                database_id=6,
                database_guid=UUID("10000000-0000-0000-0000-000000000002"),
            ),
        )
        binding = replace(binding, registration_sha256=physical_runtime_registration_digest(registration))
    return registration, binding


def provisioner(connection):
    return MssqlPhysicalDiscoverySchemaProvisioner(
        connection_factory=lambda: connection,
        discovery_sql=Path("packages/dbt-dpone/control/sqlserver/physical-v1/discovery.sql").read_bytes(),
        certificate_name="discovery",
        certificate_public_bytes=b"public",
        certificate_user="discovery_user",
    )


@pytest.mark.parametrize("same", [True, False])
def test_public_binding_context_preserves_caller_lifecycle(same):
    from dpone.adapters.dbt_mssql_physical_discovery_schema import verify_catalog_binding_context

    registration, binding = fixture(same=same)
    connection = Connection(registration, binding)
    verify_catalog_binding_context(connection, registration, binding)
    assert connection.events == []
    statements = [sql for sql, _ in connection.statements]
    assert "DPONE_DISCOVERY_BINDING_DEPLOYMENT_MISMATCH" in statements[-1]
    assert "WITH (HOLDLOCK)" in statements[-2]
    assert connection.statements[-1][1] == (
        binding.model_schema,
        binding.model_schema_id,
        binding.model_schema_owner_id,
        binding.catalog_module_sha256,
    )


@pytest.mark.parametrize("stored", [(), (b"wrong", b"wrong", b"wrong")])
def test_public_binding_context_rejects_absent_or_changed_binding_without_settlement(stored):
    from dpone.adapters.dbt_mssql_physical_discovery_schema import verify_catalog_binding_context

    registration, binding = fixture()
    connection = Connection(registration, binding, stored_binding=stored)
    with pytest.raises(RuntimeError, match="exact protected catalog binding"):
        verify_catalog_binding_context(connection, registration, binding)
    assert connection.events == []
    assert not any("DPONE_DISCOVERY_BINDING_DEPLOYMENT_MISMATCH" in sql for sql, _ in connection.statements)


@pytest.mark.parametrize("same", [True, False])
def test_actual_producer_pair_and_exact_grants_are_installed_without_registration_mutation(same):
    registration, binding = fixture(same=same)
    connection = Connection(registration, binding)
    observed = provisioner(connection).apply(registration, binding)
    definitions = discovery_procedures(
        discovery_sql=Path("packages/dbt-dpone/control/sqlserver/physical-v1/discovery.sql").read_bytes(),
        model_database=registration.model_database,
        local_schema=registration.local_schema,
        control_database=registration.control_database.database_name,
        control_schema=registration.control_schema,
        model_schema=binding.model_schema,
        model_schema_id=binding.model_schema_id,
        model_schema_owner_id=binding.model_schema_owner_id,
        discovery_certificate_thumbprint=b"c" * 20,
    )
    from hashlib import sha256

    assert observed.entry_sha256 == "sha256:" + sha256(definitions[ENTRY].encode("utf-16le")).hexdigest()
    assert observed.helper_sha256 == "sha256:" + sha256(definitions[HELPER].encode("utf-16le")).hexdigest()
    assert observed.certificate_thumbprint == b"c" * 20
    assert connection.events == ["commit", "close", "close"]
    sqls = [sql for sql, _ in connection.statements]
    assert {sql for sql in sqls if sql.startswith("CREATE PROCEDURE")} == set(definitions.values())
    grants = [sql for sql in sqls if sql.startswith("GRANT")]
    assert set(grants) == {
        f"GRANT EXECUTE ON OBJECT::[runtime_local].[{ENTRY}] TO [runtime_5];",
        "GRANT VIEW DEFINITION TO [discovery_user];",
        f"GRANT EXECUTE ON OBJECT::[runtime_control].[{HELPER}] TO [discovery_user];",
        f"GRANT VIEW DEFINITION ON OBJECT::[runtime_control].[{HELPER}] TO [discovery_user];",
    }
    assert len(grants) == 4
    assert f"ADD COUNTER SIGNATURE TO OBJECT::[runtime_control].[{HELPER}] BY CERTIFICATE [discovery];" in sqls
    assert f"ADD SIGNATURE TO OBJECT::[runtime_local].[{ENTRY}] BY CERTIFICATE [discovery];" in sqls
    assert all("INSERT [" not in sql and "UPDATE [" not in sql and "DELETE [" not in sql for sql in sqls)
    assert not any(sql.startswith(("ALTER PROCEDURE", "DROP ", "CREATE TABLE")) for sql in sqls)
    inventory = [
        (sql, args) for sql, args in connection.statements if "DPONE_DISCOVERY_BINDING_DEPLOYMENT_MISMATCH" in sql
    ]
    assert len(inventory) == 1 and inventory[0][1][-1] == binding.catalog_module_sha256
    assert "[physical_catalog_v2]" in inventory[0][0]
    assert next(i for i, sql in enumerate(sqls) if sql.startswith("SELECT registration_digest")) < next(
        i for i, sql in enumerate(sqls) if sql.startswith("CREATE PROCEDURE")
    )
    signatures = [sql for sql in sqls if "DPONE_SOURCE_SIGNATURE_INVENTORY_MISMATCH" in sql]
    assert signatures and all("physical_require_source_v1" not in sql for sql in signatures)


@pytest.mark.parametrize("same", [True, False])
def test_exact_replay_only_verifies_without_grant_or_signature_repair(same):
    registration, binding = fixture(same=same)
    connection = Connection(registration, binding, existing=True)
    provisioner(connection).apply(registration, binding)
    assert connection.events == ["commit", "close", "close"]
    sqls = [sql for sql, _ in connection.statements]
    assert not any(sql.startswith(("CREATE", "GRANT", "ADD", "ALTER")) for sql in sqls)
    assert sum("DPONE_DISCOVERY_GRANT_INVENTORY_MISMATCH" in sql for sql in sqls) == 2


@pytest.mark.parametrize("objects", [(None, 100), (100, None)])
def test_partial_pair_rejects_before_any_creation(objects):
    registration, binding = fixture()
    connection = Connection(registration, binding, objects=objects)
    with pytest.raises(RuntimeError, match="incomplete"):
        provisioner(connection).apply(registration, binding)
    assert "rollback" in connection.events
    assert not any(sql.startswith("CREATE") for sql, _ in connection.statements)


@pytest.mark.parametrize(
    "failure",
    [
        "DPONE_SOURCE_DATABASE_UNSAFE",
        "DPONE_SOURCE_CERTIFICATE_MISMATCH",
        "DPONE_DISCOVERY_CONTEXT_UNSAFE",
        "DPONE_DISCOVERY_BINDING_DEPLOYMENT_MISMATCH",
        "DPONE_SOURCE_SIGNATURE_INVENTORY_MISMATCH",
        "DPONE_SOURCE_RUNTIME_PRINCIPAL_UNSAFE",
        "DPONE_SOURCE_MODULE_INVENTORY_MISMATCH",
        "DPONE_DISCOVERY_VISIBILITY_UNSAFE",
        "DPONE_DISCOVERY_CERTIFICATE_USER_UNSAFE",
        "DPONE_DISCOVERY_GRANT_INVENTORY_MISMATCH",
        "DPONE_CATALOG_BINDING_PROTECTION_MISMATCH",
        "commit",
    ],
)
def test_inventory_or_uncertain_commit_never_returns_success(failure):
    registration, binding = fixture()
    connection = Connection(registration, binding, existing=True, fail=failure)
    with pytest.raises(RuntimeError):
        provisioner(connection).apply(registration, binding)
    assert "rollback" in connection.events
    assert connection.events[-2:] == ["close", "close"]
    assert connection.events.count("commit") == (1 if failure == "commit" else 0)
    assert not any(sql.startswith(("CREATE", "GRANT", "ADD", "ALTER")) for sql, _ in connection.statements)


@pytest.mark.parametrize("rows", [[], [(None,)], [(b"c" * 19,)], [(b"d" * 20,)], [(b"c" * 20,), (b"c" * 20,)]])
def test_certificate_missing_mismatch_or_ambiguous_rejects(rows):
    registration, binding = fixture()
    connection = Connection(registration, binding, thumbprints=[[(b"c" * 20,)], rows])
    with pytest.raises(RuntimeError):
        provisioner(connection).apply(registration, binding)
    assert "rollback" in connection.events
    assert not any(sql.startswith("CREATE") for sql, _ in connection.statements)


@pytest.mark.parametrize("change", ["registration", "binding-absent", "binding-conflict"])
def test_actual_protected_rows_are_required_not_a_matching_constructed_binding(change):
    registration, binding = fixture()
    options = (
        {"stored_registration": ("other",)}
        if change == "registration"
        else {
            "stored_binding": ()
            if change == "binding-absent"
            else (binding.registration_sha256.encode(), b"other", b"other")
        }
    )
    connection = Connection(registration, binding, **options)
    with pytest.raises(RuntimeError):
        provisioner(connection).apply(registration, binding)
    assert not any(sql.startswith("CREATE") for sql, _ in connection.statements)


def test_permission_inventory_is_exact_same_database_union_and_roles_stay_separate():
    registration, binding = fixture()
    connection = Connection(registration, binding)
    provisioner(connection).apply(registration, binding)
    checks = [sql for sql, _ in connection.statements if "DPONE_DISCOVERY_CERTIFICATE_USER_UNSAFE" in sql]
    assert checks and all("N'SELECT'" not in sql for sql in checks)
    assert any("(0,0,0,N'VIEW DEFINITION',N'G')" in sql and "N'EXECUTE'" in sql for sql in checks)
    principal_checks = [
        (sql, args[0]) for sql, args in connection.statements if "SELECT @name;" in sql and ENTRY in sql
    ]
    assert principal_checks
    assert all(
        ("AND NOT (state='G' AND permission_name='EXECUTE')" in sql) == (principal_id == 5)
        for sql, principal_id in principal_checks
    )
    assert any("sys.server_role_members" in sql for sql, _ in connection.statements)
    assert any("VIEW ANY SECURITY DEFINITION" in sql for sql, _ in connection.statements)
    assert any("class<>1" in sql and "sys.crypt_properties" in sql for sql, _ in connection.statements)


def test_missing_discovery_source_is_rejected_before_connection():
    with pytest.raises(ValueError):
        MssqlPhysicalDiscoverySchemaProvisioner(
            connection_factory=lambda: pytest.fail("must not connect"),
            discovery_sql=b"",
            certificate_name="discovery",
            certificate_public_bytes=b"public",
            certificate_user="discovery_user",
        )


def test_claim_substitution_rejects_before_connection():
    registration, binding = fixture()
    connection = Connection(registration, binding)
    with pytest.raises(ValueError, match="complete registration"):
        provisioner(connection).apply(registration, replace(binding, registration_sha256="sha256:" + "c" * 64))
    assert connection.statements == []


def test_cancellation_rolls_back_closes_and_does_not_retry():
    registration, binding = fixture()
    connection = Connection(registration, binding)

    def cancelled_commit():
        connection.events.append("commit")
        raise KeyboardInterrupt

    connection.commit = cancelled_commit
    with pytest.raises(KeyboardInterrupt):
        provisioner(connection).apply(registration, binding)
    assert connection.events == ["commit", "rollback", "close", "close"]


@pytest.mark.parametrize("object_id", [True, 0, -1, "100"])
def test_malformed_object_identity_rejects_before_installation(object_id):
    registration, binding = fixture()
    connection = Connection(registration, binding, objects=[object_id, object_id])
    with pytest.raises(RuntimeError, match="object identity"):
        provisioner(connection).apply(registration, binding)
    assert "rollback" in connection.events
    assert not any(sql.startswith("CREATE") for sql, _ in connection.statements)
