"""Genuine archive/P-only discovery fixture; execution belongs to the live parent.

No generation migration, reservation, capacity enrollment or model exists here.
Inherited source-module deployment with unresolved G tables is deliberately tested
by SQL Server, never made to succeed by manufacturing generation prerequisites.
"""

import secrets
import time
from copy import deepcopy
from hashlib import sha256
from uuid import uuid4

from dpone.adapters.dbt_mssql_physical_catalog_binding_schema import MssqlPhysicalCatalogBindingSchemaProvisioner
from dpone.adapters.dbt_mssql_physical_catalog_binding_store import MssqlPhysicalCatalogBindingStore
from dpone.adapters.dbt_mssql_physical_catalog_lifecycle import MssqlPhysicalCatalogRegistrationLifecycle
from dpone.adapters.dbt_mssql_physical_catalog_v2_schema import MssqlPhysicalCatalogV2SchemaProvisioner
from dpone.adapters.dbt_mssql_physical_discovery import MssqlPhysicalDiscoveryReader
from dpone.adapters.dbt_mssql_physical_discovery_schema import MssqlPhysicalDiscoverySchemaProvisioner
from dpone.adapters.dbt_mssql_physical_registration_store import MssqlPhysicalRegistrationStore
from dpone.adapters.dbt_workspace_mssql_activation_admission import MssqlDbtWorkspaceActivationAdmission
from dpone.adapters.native_delivery_originals import NativeOriginalVerifier
from dpone.adapters.native_project_documents import NativeProjectDocumentReader
from dpone.contracts.dbt_mssql_physical_discovery import DiscoveryObject, PhysicalDiscoveryRequest
from dpone.contracts.dbt_native_execution_policy import require_physical_filegroup_name
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActiveActivation
from dpone.contracts.native_delivery_json import decode_native_delivery_json
from dpone.manifest.confined_files import read_confined_file
from tests.support import dbt_mssql_physical_source_authority as source_module
from tests.support.dbt_mssql_physical_catalog_v2_live import ArchiveAuthority, TimedSourceFixture
from tests.support.dbt_mssql_physical_source_authority import LOCAL, ROOT, SCHEMA

MODEL_SCHEMA = "catalog_models"
CATALOG_CERTIFICATE = "discovery_catalog_certificate"
DISCOVERY_CERTIFICATE = "discovery_boundary_certificate"


class DiscoveryArchiveAuthority(ArchiveAuthority):
    """Add explicit filegroup to the producer input before encoding or validation."""

    def registration(self, model_pin, control_pin, principals, service_document):
        original = source_module.native_policy

        def policy():
            value = deepcopy(original())
            value["profiles"]["local"]["native_execution"]["physical_filegroup"] = {"name": "PRIMARY"}
            return value

        with self.monkeypatch.context() as patch:
            patch.setattr(source_module, "native_policy", policy)
            return super().registration(model_pin, control_pin, principals, service_document)


class DiscoverySourceFixture(TimedSourceFixture):
    """Reuse authentic workspace admission and stop before the generation path."""

    def _admit(self):
        self.owner = self.authority.admit_physical_owner(
            self.registration, lambda: self.connect("control"), self.credentials["metadata"][0]
        )


class DiscoveryFixture:
    """Real stores, original verifier, module provisioning and ordinary login read."""

    def __init__(self, source):
        self.source = source
        with source.connection(autocommit=True) as admin:
            major = admin.execute("SELECT CONVERT(int,SERVERPROPERTY('ProductMajorVersion'))").fetchone()[0]
            assert major == 16, "discovery fixture requires actual SQL Server 2022"
            assert admin.execute("SELECT SERVERPROPERTY('ProductUpdateLevel')").fetchone()[0] == "CU26", (
                "discovery fixture requires the parent-qualified CU26 cell"
            )
            admin.execute(f"CREATE SCHEMA [{MODEL_SCHEMA}] AUTHORIZATION dbo")
        catalog_public = self._certificate(CATALOG_CERTIFICATE, paired=False)
        with source.authority.reader(source) as reader:
            self.binding = MssqlPhysicalCatalogRegistrationLifecycle(
                policy_reader=reader,
                registration_store=MssqlPhysicalRegistrationStore(
                    connection_factory=source.connect, local_schema=LOCAL
                ),
                binding_schema=MssqlPhysicalCatalogBindingSchemaProvisioner(
                    connection_factory=source.connect, local_schema=LOCAL
                ),
                module_provisioner=MssqlPhysicalCatalogV2SchemaProvisioner(
                    connection_factory=source.connect,
                    catalog_sql=(ROOT / "packages/dbt-dpone/control/sqlserver/physical-v1/catalog-v2.sql").read_bytes(),
                    certificate_name=CATALOG_CERTIFICATE,
                    certificate_public_bytes=catalog_public,
                    certificate_user="discovery_catalog_user",
                ),
                binding_store=MssqlPhysicalCatalogBindingStore(connection_factory=source.connect, local_schema=LOCAL),
                connection_factory=source.connect,
            ).apply(source.authority.archive.refs, registration=source.registration)
        selected_filegroup = self._selected_filegroup()
        public = self._certificate(DISCOVERY_CERTIFICATE, paired=True)
        self.deployment = MssqlPhysicalDiscoverySchemaProvisioner(
            connection_factory=source.connect,
            discovery_sql=(ROOT / "packages/dbt-dpone/control/sqlserver/physical-v1/discovery.sql").read_bytes(),
            certificate_name=DISCOVERY_CERTIFICATE,
            certificate_public_bytes=public,
            certificate_user="discovery_boundary_user",
        ).apply(source.registration, self.binding)
        self.request = PhysicalDiscoveryRequest(
            subject=source.registration.platform_subject,
            workspace_attempt=source.owner.attempt,
            guard=source.owner.receipt.guard_epochs[0],
            generation_id=str(uuid4()),
            invocation_id=str(uuid4()),
            filegroup_name=selected_filegroup,
            objects=tuple(
                DiscoveryObject("model.alpha.orders", role, name)
                for role, name in (("TARGET", "orders"), ("CANDIDATE", "candidate_orders"), ("HELPER", "helper_orders"))
            ),
        )
        self.require_no_generation()

    def _certificate(self, name, *, paired):
        secret = "Aa!9" + secrets.token_hex(24)
        with self.source.connection(autocommit=True) as admin:
            admin.execute(f"CREATE CERTIFICATE [{name}] WITH SUBJECT='Isolated discovery fixture'")
            row = admin.execute(
                f"SELECT CERTENCODED(CERT_ID('{name}')),CERTPRIVATEKEY(CERT_ID('{name}'),?)", secret
            ).fetchone()
            public, private = bytes(row[0]), bytes(row[1])
        if paired and self.source.layout == "two_database":
            with self.source.connection("control", autocommit=True) as admin:
                admin.execute(
                    f"CREATE CERTIFICATE [{name}] FROM BINARY=0x{public.hex()} "
                    f"WITH PRIVATE KEY(BINARY=0x{private.hex()},DECRYPTION BY PASSWORD='{secret}')"
                )
        return public

    def _selected_filegroup(self):
        """Read the authenticated archive member, not the fixture's policy claim."""
        source = self.source
        authority = source.authority
        admission = MssqlDbtWorkspaceActivationAdmission(lambda: source.connect("control"), control_schema=SCHEMA)

        def active():
            return DbtWorkspaceActiveActivation(authority.activation, admission.require_active(authority.activation))

        with NativeOriginalVerifier(
            inputs=authority.archive.inputs,
            invocation_identity=authority.archive.identity,
            require_active=active,
            bundles=authority.archive.bundles,
            read_file=read_confined_file,
            release_root=authority.archive.release_root,
            max_policy_bytes=1048576,
            max_bundle_bytes=16777216,
        ) as verifier:
            original = verifier.resolve(authority.archive.refs)
            payload, intent = NativeProjectDocumentReader(read_file=read_confined_file, max_policy_bytes=1048576).read(
                original.project_directory, original.project_bundle
            )
            assert payload == original.policy_document
            assert "sha256:" + sha256(payload).hexdigest() == self.binding.policy_member.sha256
            assert original.project_bundle.archive_sha256 == self.binding.project_archive_sha256
            policy = decode_native_delivery_json(payload)
            return require_physical_filegroup_name(policy["profiles"][intent["profile"]]["native_execution"])

    def read(self, request=None):
        return MssqlPhysicalDiscoveryReader(
            connection_factory=lambda: self.source.connect("model", "metadata"),
            registration=self.source.registration,
            binding=self.binding,
            operation_timeout_seconds=self.source.timing["read"],
            clock=time.monotonic,
        ).read(request or self.request)

    def require_no_generation(self):
        assert "reservation" not in self.source.authority.references
        assert not hasattr(self.source, "reserved") and not hasattr(self.source, "executor")
        with self.source.connection("control") as admin:
            tables = tuple(
                row[0] for row in admin.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID(?)", SCHEMA)
            )
            assert not any(name.startswith("native_generation") for name in tables)
            assert (
                admin.execute(f"SELECT COUNT_BIG(*) FROM [{SCHEMA}].[native_original_bindings_v1]").fetchone()[0] == 0
            )
        with self.source.connection() as admin:
            assert (
                admin.execute(
                    "SELECT COUNT_BIG(*) FROM sys.objects WHERE schema_id=SCHEMA_ID(?)", MODEL_SCHEMA
                ).fetchone()[0]
                == 0
            )

    def snapshot(self):
        result = {"control": self.source.snapshot()}
        with self.source.connection() as admin:
            result["model"] = {
                table: tuple(
                    tuple(row) for row in admin.execute(f"SELECT * FROM [{LOCAL}].[{table}] ORDER BY registration_id")
                )
                for table in ("physical_runtime_registrations_v1", "physical_catalog_bindings_v1")
            }
        return result

    def evidence(self):
        modules: list[tuple[str, str, str, str, str, bytes]] = []
        environments = {}
        for namespace in ("model", "control"):
            with self.source.connection(namespace) as admin:
                environments[namespace] = {
                    "engine": admin.execute("SELECT @@VERSION").fetchone()[0],
                    "odbc_driver_version": admin.getinfo(self.source.driver.SQL_DRIVER_VER),
                }
                modules.extend(
                    (namespace, *tuple(row))
                    for row in admin.execute(
                        "SELECT s.name,p.name,m.definition,c.crypt_type,c.thumbprint FROM sys.procedures p "
                        "JOIN sys.schemas s ON s.schema_id=p.schema_id JOIN sys.sql_modules m ON m.object_id=p.object_id "
                        "JOIN sys.crypt_properties c ON c.major_id=p.object_id WHERE p.name LIKE ? ORDER BY s.name,p.name",
                        "physical%",
                    )
                )
        return {
            "topology": self.source.layout,
            "environments": environments,
            "timeouts": self.source.timing,
            "pyodbc_version": self.source.driver.version,
            "registration_id": self.source.registration.registration_id,
            "binding": self.binding.to_dict(),
            "P_epoch": self.request.guard.fencing_epoch,
            "modules": [
                {
                    "namespace": namespace,
                    "schema": s,
                    "name": n,
                    "utf16_sha256": sha256(d.encode("utf-16le")).hexdigest(),
                    "signature": k,
                    "thumbprint": bytes(t).hex(),
                }
                for namespace, s, n, d, k, t in modules
            ],
            "qualification": "isolated P-only discovery correctness; no G, launch or production qualification",
        }


def require_ddl_transition(before, after, *, layout, event):
    """Permit only the trigger's exact owned CREATE/DROP epoch transition.

    A model DDL statement changes the control snapshot only in same-database
    topology. This is distinct from P mutation and from discovery read effects.
    Neither input is mutated or stripped of fields before comparison.
    """
    from datetime import datetime

    assert layout in {"same_database", "two_database"}
    assert event in {"CREATE_VIEW", "DROP_VIEW"}
    if layout == "two_database":
        assert after == before
        return
    table = "semantic_refresh_ddl_epoch"
    old_rows, new_rows = before["control"][table], after["control"][table]
    assert type(old_rows) is tuple and type(new_rows) is tuple and len(old_rows) == len(new_rows) == 1
    old, new = old_rows[0], new_rows[0]
    assert type(old) is tuple and type(new) is tuple and len(old) == len(new) == 4
    assert type(old[0]) is int and type(new[0]) is int and old[0] == new[0] == 1
    assert type(old[1]) is int and type(new[1]) is int and old[1] > 0 and new[1] == old[1] + 1
    assert type(new[2]) is str and new[2] == event
    assert type(old[3]) is datetime and type(new[3]) is datetime and new[3] > old[3]
    expected = {**before, "control": {**before["control"], table: new_rows}}
    assert after == expected


def require_sql_rejection(error, *, name, number):
    """Require the original SQL diagnostic pair, not merely the public wrapper."""
    import re

    from dpone.adapters.dbt_mssql_physical_discovery import PhysicalDiscoveryReadError

    assert type(error) is PhysicalDiscoveryReadError and error.__cause__ is not None
    pattern = rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])\s+\({number}\)"
    assert any(type(arg) is str and re.search(pattern, arg) for arg in error.__cause__.args)
