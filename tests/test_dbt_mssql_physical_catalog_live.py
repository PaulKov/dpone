"""Opt-in isolated SQL2022 catalog evidence; never model/route qualification.

Parent executor supplies DPONE_RUN_PHYSICAL_CATALOG_LIVE=1 plus the existing
DPONE_NATIVE_SQL_TEST_HOST/PASSWORD synthetic fixture settings. Every case owns
new databases/logins via SourceFixture. No SQL failure is converted to a skip.
"""

import importlib
import json
import os
import time
from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.adapters.dbt_mssql_physical_catalog import MssqlPhysicalCatalogReader, PhysicalCatalogReadError
from dpone.adapters.dbt_mssql_physical_catalog_schema import MssqlPhysicalCatalogSchemaProvisioner
from dpone.contracts.dbt_mssql_physical import (
    AbsentPredecessor,
    PhysicalFilegroup,
    PhysicalModelPlan,
    PhysicalModelSpec,
    PhysicalRelation,
)
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from tests.support.dbt_mssql_physical_source_authority import LOCAL, ROOT
from tests.support.dbt_mssql_physical_source_fixture import SourceFixture

CATALOG_CERTIFICATE = "catalog_fixture_certificate"
CATALOG_USER = "catalog_fixture_certificate_user"
MODEL_SCHEMA = "catalog_models"
TEMPLATE = ROOT / "packages/dbt-dpone/control/sqlserver/physical-v1/catalog.sql"
LAYOUTS = ("rowstore_none", "rowstore_row", "rowstore_page", "columnstore")
pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_PHYSICAL_CATALOG_LIVE") != "1",
        reason="isolated SQL2022 catalog proof disabled",
    ),
]


class CatalogFixture:
    """Add a separate catalog deployment to an unchanged genuine source fixture."""

    def __init__(self, source, layout):
        self.source = source
        with source.connection(autocommit=True) as admin:
            admin.execute(f"CREATE SCHEMA [{MODEL_SCHEMA}] AUTHORIZATION dbo")
            self.schema_id = admin.execute("SELECT SCHEMA_ID(?)", MODEL_SCHEMA).fetchone()[0]
            filegroup = admin.execute("SELECT data_space_id,name FROM sys.filegroups WHERE name='PRIMARY'").fetchone()
            admin.execute(f"CREATE CERTIFICATE [{CATALOG_CERTIFICATE}] WITH SUBJECT='Synthetic catalog fixture'")
            public = bytes(admin.execute(f"SELECT CERTENCODED(CERT_ID('{CATALOG_CERTIFICATE}'))").fetchone()[0])
        spec = PhysicalModelSpec(
            model_unique_id="model.fixture.orders",
            source_graph_sha256="sha256:" + "a" * 64,
            relation=PhysicalRelation(source.databases["model"], MODEL_SCHEMA, "orders"),
            columns=(MssqlCatalogColumn(name="id", dtype="int", nullable=False, collation=None),),
            layout=layout,
            filegroup=PhysicalFilegroup(filegroup[0], filegroup[1]),
            resource_bounds=source.registration.trusted_profile.reference,
        )
        # Local comparison vector only: no graph membership/resource admission.
        self.plan = PhysicalModelPlan(str(source.executor.generation_id), spec, AbsentPredecessor())
        with source.connection(autocommit=True) as admin:
            admin.execute(f"CREATE TABLE [{MODEL_SCHEMA}].[orders] (id int NOT NULL) ON [PRIMARY]")
            admin.execute(f"INSERT INTO [{MODEL_SCHEMA}].[orders] VALUES (1),(7),(19)")
            if layout == "columnstore":
                admin.execute(
                    f"CREATE CLUSTERED COLUMNSTORE INDEX [{self.plan.columnstore_index_name}] "
                    f"ON [{MODEL_SCHEMA}].[orders] ON [PRIMARY]"
                )
            else:
                compression = layout.removeprefix("rowstore_").upper()
                admin.execute(f"ALTER TABLE [{MODEL_SCHEMA}].[orders] REBUILD WITH (DATA_COMPRESSION={compression})")
            self.object_id, self.created = admin.execute(
                "SELECT object_id,CONVERT(char(27),CONVERT(datetime2(7),create_date),126) "
                "FROM sys.tables WHERE schema_id=SCHEMA_ID(?) AND name='orders'",
                MODEL_SCHEMA,
            ).fetchone()
        self.provisioner = MssqlPhysicalCatalogSchemaProvisioner(
            connection_factory=source.connect,
            catalog_sql=TEMPLATE.read_bytes(),
            certificate_name=CATALOG_CERTIFICATE,
            certificate_public_bytes=public,
            certificate_user=CATALOG_USER,
        )
        self.deployment = self.provision()

    def provision(self):
        return self.provisioner.apply(
            self.source.registration,
            model_schema=MODEL_SCHEMA,
            model_schema_id=self.schema_id,
            model_schema_owner_id=1,
        )

    def read(self, role="metadata"):
        return MssqlPhysicalCatalogReader(
            connection_factory=lambda: self.source.connect("model", role),
            registration=self.source.registration,
            operation_timeout_seconds=20,
            clock=time.monotonic,
        ).read(
            plan=self.plan,
            executor_invocation_id=str(self.source.executor.invocation_id),
            object_id=self.object_id,
            expected_object_name="orders",
            expected_object_create_time=self.created,
        )


@pytest.fixture
def catalog(request):
    for name in ("DPONE_NATIVE_SQL_TEST_HOST", "DPONE_NATIVE_SQL_TEST_PASSWORD"):
        if not os.environ.get(name):
            pytest.skip("synthetic catalog fixture environment is unavailable: " + name)
    source = SourceFixture(importlib.import_module("pyodbc"))
    try:
        source.install()
        assert source.registration.limits.max_metadata_bytes >= 16384
        fixture = CatalogFixture(source, getattr(request, "param", "rowstore_none"))
        # A negative is meaningful only after this exact deployment was usable.
        before = source.snapshot()
        assert fixture.read().results["COUNT"][0].row_count_exact == 3
        assert source.snapshot() == before
        yield fixture
    finally:
        source.cleanup()


@pytest.mark.parametrize("catalog", LAYOUTS, indirect=True)
@pytest.mark.parametrize("role", ["metadata", "build"])
def test_live_catalog_four_layouts_exact_rows_and_source_identity(catalog, role, record_property):
    source = catalog.source
    before = source.snapshot()
    value = catalog.read(role)
    assert value.results["COUNT"][0].row_count_exact == 3
    assert value.results["HEADER"][0].object_id == catalog.object_id
    assert value.source.observed_model_principal == getattr(source.registration.principals, role).model
    assert value.source.reservation == source.request.reservation
    assert source.snapshot() == before
    assert catalog.provision() == catalog.deployment
    with source.connection() as admin:
        assert tuple(row[0] for row in admin.execute(f"SELECT id FROM [{MODEL_SCHEMA}].[orders] ORDER BY id")) == (
            1,
            7,
            19,
        )
    record_property(
        "catalog_fixture_evidence",
        json.dumps(
            {
                "layout": catalog.plan.spec.layout,
                "role": role,
                "module_utf16_sha256": catalog.deployment.module_sha256,
                "template_sha256": sha256(TEMPLATE.read_bytes()).hexdigest(),
                "source": source.evidence(),
            },
            sort_keys=True,
        ),
    )


@pytest.mark.parametrize("damage", ["signature", "schema_owner", "rls", "table_permission", "column", "extra_index"])
def test_live_catalog_rejects_drift_after_positive_baseline(catalog, damage):
    source = catalog.source
    statements = {
        "signature": [
            f"DROP SIGNATURE FROM OBJECT::[{LOCAL}].[physical_catalog_v1] BY CERTIFICATE [{CATALOG_CERTIFICATE}]"
        ],
        "schema_owner": [f"ALTER AUTHORIZATION ON SCHEMA::[{MODEL_SCHEMA}] TO [{source.credentials['build'][0]}]"],
        "rls": [
            f"CREATE FUNCTION [{MODEL_SCHEMA}].[filter_rows](@id int) RETURNS TABLE WITH SCHEMABINDING AS RETURN SELECT 1 AS allowed WHERE @id=1",
            f"CREATE SECURITY POLICY [{MODEL_SCHEMA}].[row_policy] ADD FILTER PREDICATE [{MODEL_SCHEMA}].[filter_rows](id) ON [{MODEL_SCHEMA}].[orders] WITH (STATE=ON)",
        ],
        "table_permission": [
            f"GRANT SELECT ON OBJECT::[{MODEL_SCHEMA}].[orders] TO [{source.credentials['build'][0]}]"
        ],
        "column": [f"ALTER TABLE [{MODEL_SCHEMA}].[orders] ADD extra int NULL"],
        "extra_index": [f"CREATE INDEX extra_index ON [{MODEL_SCHEMA}].[orders](id)"],
    }
    with source.connection(autocommit=True) as admin:
        for statement in statements[damage]:
            admin.execute(statement)
    before = source.snapshot()
    with pytest.raises(PhysicalCatalogReadError):
        catalog.read()
    assert source.snapshot() == before


@pytest.mark.parametrize("role", ["metadata", "build"])
def test_live_catalog_certificate_does_not_grant_direct_runtime_select(catalog, role):
    source = catalog.source
    with source.connection("model", role) as runtime:
        with pytest.raises(source.pyodbc.Error, match="permission|Permission|denied"):
            runtime.execute(f"SELECT COUNT_BIG(*) FROM [{MODEL_SCHEMA}].[orders]").fetchone()
    assert catalog.read(role).results["COUNT"][0].row_count_exact == 3


@pytest.mark.parametrize("damage", ["missing", "extra", "foreign"])
def test_live_catalog_provisioner_rejects_signature_inventory_drift(catalog, damage):
    source = catalog.source
    with source.connection(autocommit=True) as admin:
        if damage in {"missing", "foreign"}:
            admin.execute(
                f"DROP SIGNATURE FROM OBJECT::[{LOCAL}].[physical_catalog_v1] BY CERTIFICATE [{CATALOG_CERTIFICATE}]"
            )
        if damage in {"extra", "foreign"}:
            admin.execute("CREATE CERTIFICATE foreign_catalog_signer WITH SUBJECT='Synthetic foreign signer'")
            admin.execute(
                f"ADD SIGNATURE TO OBJECT::[{LOCAL}].[physical_catalog_v1] BY CERTIFICATE foreign_catalog_signer"
            )
    with pytest.raises(Exception, match="SIGNATURE|INVENTORY"):
        catalog.provision()


def test_live_driver_new_cursor_enforces_remaining_statement_timeout(catalog):
    """Exercise actual SQL_ATTR_QUERY_TIMEOUT; no signed producer modification."""
    source = catalog.source
    with source.connection("model", "metadata") as runtime:
        runtime.timeout = 10
        previous = runtime.cursor()
        previous.execute("SELECT 1").fetchone()
        previous.close()
        runtime.timeout = 1
        cursor = runtime.cursor()
        started = time.monotonic()
        try:
            with pytest.raises(source.pyodbc.Error) as failure:
                cursor.execute("WAITFOR DELAY '00:00:05'; SELECT 1").fetchone()
            assert failure.value.args[0] in {"HYT00", "HYT01"}
            assert time.monotonic() - started < 5
        finally:
            cursor.close()
    assert catalog.read().results["COUNT"][0].row_count_exact == 3


@pytest.mark.parametrize("empty", [False, True])
def test_live_catalog_count_lock_is_held_until_caller_settlement(catalog, empty):
    """A real non-COUNT lock probe retains its table lock, including no rows."""
    source = catalog.source
    if empty:
        with source.connection(autocommit=True) as admin:
            admin.execute(f"DELETE FROM [{MODEL_SCHEMA}].[orders]")
    with source.connection("model", "metadata") as runtime:
        runtime.execute("SET NOCOUNT ON; IF @@TRANCOUNT=0 BEGIN TRANSACTION")
        cursor = runtime.execute(
            f"EXEC [{LOCAL}].[physical_catalog_v1] @registration_id=?, @generation=?, "
            "@expected_invocation=?, @object_id=?, @kind=?",
            source.registration.registration_id,
            str(source.executor.generation_id),
            str(source.executor.invocation_id),
            catalog.object_id,
            "HEADER",
        )
        assert cursor.fetchone() is not None
        assert cursor.fetchone() is None
        assert not cursor.nextset()
        cursor.close()
        with source.connection(autocommit=True) as writer:
            writer.execute("SET LOCK_TIMEOUT 500")
            with pytest.raises(source.pyodbc.Error, match="1222|Lock request time out"):
                writer.execute(f"INSERT INTO [{MODEL_SCHEMA}].[orders] VALUES (23)")
            runtime.commit()
            writer.execute(f"INSERT INTO [{MODEL_SCHEMA}].[orders] VALUES (23)")
    assert catalog.read().results["COUNT"][0].row_count_exact == (1 if empty else 4)


def test_live_foreign_dependency_metadata_deny_cannot_become_absence(catalog):
    source = catalog.source
    with source.connection(autocommit=True) as admin:
        admin.execute("CREATE SCHEMA catalog_foreign AUTHORIZATION dbo")
        admin.execute(f"CREATE VIEW catalog_foreign.incoming AS SELECT id FROM [{MODEL_SCHEMA}].[orders]")
    with pytest.raises(PhysicalCatalogReadError):
        catalog.read()
    with source.connection(autocommit=True) as admin:
        principal = admin.execute(
            "SELECT name FROM sys.database_principals WHERE principal_id=?",
            source.registration.principals.metadata.model.principal_id,
        ).fetchone()[0]
        quoted = "[" + principal.replace("]", "]]") + "]"
        admin.execute("DENY VIEW DEFINITION ON OBJECT::catalog_foreign.incoming TO " + quoted)
    # The incoming view may now be filtered from dependency catalog visibility.
    # Runtime token DENY inventory must reject instead of claiming no dependency.
    with pytest.raises(PhysicalCatalogReadError):
        catalog.read()


def test_live_temporal_and_float_catalog_dimensions(catalog):
    """Real sys.columns settles storage-versus-catalog type dimension assumptions."""
    source = catalog.source
    types = [f"{base}({scale})" for base in ("time", "datetime2", "datetimeoffset") for scale in range(8)]
    types.extend(f"float({precision})" for precision in (1, 24, 25, 53))
    names = [f"c{index}" for index in range(len(types))]
    ddl = ",".join(f"[{name}] {dtype} NULL" for name, dtype in zip(names, types, strict=True))
    with source.connection(autocommit=True) as admin:
        admin.execute(f"CREATE TABLE [{MODEL_SCHEMA}].[type_probe] ({ddl}) ON [PRIMARY]")
        object_id, created = admin.execute(
            "SELECT object_id,CONVERT(char(27),CONVERT(datetime2(7),create_date),126) "
            "FROM sys.tables WHERE schema_id=SCHEMA_ID(?) AND name='type_probe'",
            MODEL_SCHEMA,
        ).fetchone()
        observed = [
            tuple(row)
            for row in admin.execute(
                "SELECT name,max_length,precision,scale FROM sys.columns WHERE object_id=? ORDER BY column_id",
                object_id,
            )
        ]
    spec = replace(
        catalog.plan.spec,
        relation=PhysicalRelation(source.databases["model"], MODEL_SCHEMA, "type_probe"),
        columns=tuple(MssqlCatalogColumn(name, dtype, True) for name, dtype in zip(names, types, strict=True)),
    )
    plan = PhysicalModelPlan(catalog.plan.generation_id, spec, AbsentPredecessor())
    reader = MssqlPhysicalCatalogReader(
        connection_factory=lambda: source.connect("model", "metadata"),
        registration=source.registration,
        operation_timeout_seconds=20,
        clock=time.monotonic,
    )
    try:
        result = reader.read(
            plan=plan,
            executor_invocation_id=str(source.executor.invocation_id),
            object_id=object_id,
            expected_object_name="type_probe",
            expected_object_create_time=created,
        )
    except PhysicalCatalogReadError as error:
        pytest.fail(f"Synthetic SQL catalog dimensions {observed!r}; comparison failed: {error.__cause__}")
    assert result.results["COUNT"][0].row_count_exact == 0


@pytest.mark.parametrize("changed_profile", [False, True])
def test_live_second_registration_cannot_replace_fixed_catalog_module(catalog, changed_profile):
    """Current lifecycle limitation is explicit; no silent ALTER or new namespace."""
    from uuid import uuid4

    from dpone.contracts.native_identity import OriginalRef

    source = catalog.source
    second = replace(source.registration, registration_id=str(uuid4()))
    if changed_profile:
        second = replace(
            second,
            trusted_profile=replace(
                second.trusted_profile, reference=OriginalRef("synthetic/second-profile", "sha256:" + "9" * 64)
            ),
        )
    before = source.snapshot()
    with pytest.raises(source.pyodbc.Error, match="MODULE|INVENTORY"):
        catalog.provisioner.apply(
            second, model_schema=MODEL_SCHEMA, model_schema_id=catalog.schema_id, model_schema_owner_id=1
        )
    assert source.snapshot() == before
    assert catalog.read().results["COUNT"][0].row_count_exact == 3
