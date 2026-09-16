"""Real SQL admission and archive verification; no dbt or production qualification."""

import json
import os
import time
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from hashlib import sha256
from types import SimpleNamespace
from uuid import uuid4

from dpone.adapters.dbt_mssql_physical_catalog import MssqlPhysicalCatalogReader
from dpone.adapters.dbt_mssql_physical_catalog_binding_schema import MssqlPhysicalCatalogBindingSchemaProvisioner
from dpone.adapters.dbt_mssql_physical_catalog_binding_store import MssqlPhysicalCatalogBindingStore
from dpone.adapters.dbt_mssql_physical_catalog_lifecycle import MssqlPhysicalCatalogRegistrationLifecycle
from dpone.adapters.dbt_mssql_physical_catalog_policy import MssqlPhysicalCatalogPolicyReader
from dpone.adapters.dbt_mssql_physical_catalog_v2_schema import MssqlPhysicalCatalogV2SchemaProvisioner
from dpone.adapters.dbt_mssql_physical_registration_store import MssqlPhysicalRegistrationStore
from dpone.adapters.dbt_workspace_mssql_activation_admission import MssqlDbtWorkspaceActivationAdmission
from dpone.adapters.dbt_workspace_mssql_attempt_admission import MssqlDbtWorkspaceAttemptAdmission
from dpone.adapters.native_delivery_originals import NativeOriginalVerifier
from dpone.adapters.native_project_documents import NativeProjectDocumentReader
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationRequest, DbtWorkspaceActiveActivation
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from dpone.contracts.native_originals import NativePlatformOriginalSubject
from dpone.manifest.confined_files import read_confined_file
from tests.support.dbt_mssql_physical_source_authority import LOCAL, ROOT, SCHEMA, FixtureAuthority
from tests.support.dbt_mssql_physical_source_fixture import SourceFixture
from tests.test_dbt_mssql_physical_catalog_live import CatalogFixture
from tests.test_native_original_verification import _real_native_projection

PROFILES = {
    "legacy-correctness-v1": {"connect": 5, "setup": 15, "read": 20},
    "emulated-host-correctness-v1": {"connect": 15, "setup": 60, "read": 120},
}
CERTIFICATE = "catalog_v2_fixture_certificate"
CERTIFICATE_USER = "catalog_v2_fixture_user"


class TimedSourceFixture(SourceFixture):
    """Test-only finite connection/statement budgets; production defaults unchanged."""

    def __init__(self, pyodbc, profile, *, layout="two_database"):
        self.timing, self.driver = PROFILES[profile], pyodbc
        super().__init__(SimpleNamespace(connect=self._connect_driver), layout=layout)

    def _connect_driver(self, *args, **kwargs):
        return self.driver.connect(*args, **(kwargs | {"timeout": self.timing["connect"]}))

    def connect(self, *args, **kwargs):
        connection = super().connect(*args, **kwargs)
        connection.timeout = self.timing["setup"]
        return connection


class ArchiveAuthority(FixtureAuthority):
    """Author claims before inherited admission; authenticate through real consumer."""

    def __init__(self, root, monkeypatch, revision):
        super().__init__()
        self.root, self.monkeypatch, self.revision = root.resolve(), monkeypatch, revision

    def retain(self, name, document):
        if name in {"profile", "command"}:
            document = document | {"catalog_fixture_revision": self.revision}
        if name in {"profile", "reservation"}:
            retained_name = (
                f"profile-v{self.revision}"
                if name == "profile"
                else "reservation/" + json.loads(document)["subject"]["generation_id"]
            )
            reference = super().retain(retained_name, document)
            self.references[name] = reference
            return reference
        return super().retain(name, document)

    @contextmanager
    def reservation_producer(self):
        """Bind the inherited producer to its actual retained generation bytes.

        Only this fixture's reservation constructor is adapted. Both original
        locator and immutable object key identify the same retained dictionary
        entry; the real ArtifactObjectRef and NativeOriginalBinding remain used.
        """
        from tests.support import dbt_mssql_physical_source_authority as source_module

        constructor = source_module.ArtifactObjectRef

        def object_ref(key, *fields, **kwargs):
            if key != "fixture/reservation":
                raise ValueError("reservation producer received an unrelated object key")
            reference = self.get("reservation")
            value = constructor(reference.locator, *fields, **kwargs)
            if value.size_bytes != len(self.payloads[reference.locator]) or value.sha256 != reference.sha256:
                raise ValueError("reservation storage claim differs from its exact retained bytes")
            return value

        with self.monkeypatch.context() as patch:
            patch.setattr(source_module, "ArtifactObjectRef", object_ref)
            yield

    def registration(self, model_pin, control_pin, principals, service_document):
        from tests import test_dbt_native_policy_v4 as policy_module
        from tests import test_dbt_release_source_reader as archive_module

        claim = super().registration(model_pin, control_pin, principals, service_document)
        policy = deepcopy(self.policy)
        profile = policy["profiles"]["local"]
        profile["authoring_template"] = {
            "project_name": "orders",
            "invocation_target": {"database": model_pin.database_name, "schema": "catalog_models"},
            "source_relation": {"database": model_pin.database_name, "schema": "dbo", "name": "orders"},
        }
        native = profile["native_execution"]
        native["physical_catalog_limits"] = {
            key: value
            for key, value in claim.limits.to_dict().items()
            if key not in {"max_metadata_bytes", "max_generation_bytes"}
        }
        native["physical_catalog_limits"]["max_catalog_rows"] += self.revision - 1
        target = archive_module.DbtInvocationTarget
        self.root.mkdir()
        with self.monkeypatch.context() as patch:
            patch.setattr(policy_module, "native_policy", lambda: deepcopy(policy))
            patch.setattr(
                archive_module, "DbtInvocationTarget", lambda *_: target(model_pin.database_name, "catalog_models")
            )
            self.archive = _real_native_projection(self.root, patch)
        self.archive.identity = replace(self.archive.identity, activation_id=str(uuid4()))
        self.sources = self.archive.inputs.load_sources(
            projection_root=self.archive.refs.projection_root, release_id=self.archive.identity.release_id
        )
        self.authority = self.archive.inputs.load_runtime_authority(
            projection_root=self.archive.refs.projection_root,
            environment="prod",
            release_id=self.archive.identity.release_id,
            deployment_id=self.archive.identity.deployment_id,
        )
        # Producer-authored claim, checked through archive/member/SQL ACTIVE later.
        policy["workflows"] = {"alpha": {"owner": "synthetic"}}
        self.policy = policy
        subject = NativePlatformOriginalSubject(
            self.authority, "sha256:" + sha256(encode_native_delivery_json(policy)).hexdigest()
        )
        owner = next(item for item in self.sources.workflows if item.source.workflow_id == "alpha")
        self.claim = replace(
            claim,
            platform_subject=subject,
            trusted_profile=replace(claim.trusted_profile, subject=subject),
            trusted_toolchain=replace(claim.trusted_toolchain, subject=subject),
            model_connection_ref=owner.execution.invocation_profile().connection_ref,
            limits=replace(claim.limits, max_catalog_rows=native["physical_catalog_limits"]["max_catalog_rows"]),
        )
        return self.claim

    def admit_physical_owner(self, registration, admin, metadata_name):
        from tests.support import dbt_mssql_physical_source_authority as source_module

        def build(**fields):
            fields.update(
                activation_id=self.archive.identity.activation_id,
                environment=self.authority.environment,
                source_inventory_sha256=self.sources.inventory.snapshot_sha256,
            )
            self.activation = DbtWorkspaceActivationRequest.build(**fields)
            return self.activation

        with self.monkeypatch.context() as patch:
            patch.setattr(source_module, "DbtWorkspaceActivationRequest", SimpleNamespace(build=build))
            return super().admit_physical_owner(registration, admin, metadata_name)

    def admit(self, registration, admin, runtime, metadata_name):
        with self.reservation_producer():
            return super().admit(registration, admin, runtime, metadata_name)

    @contextmanager
    def reader(self, source):
        admission = MssqlDbtWorkspaceActivationAdmission(lambda: source.connect("control"), control_schema=SCHEMA)

        def active():
            return DbtWorkspaceActiveActivation(self.activation, admission.require_active(self.activation))

        with NativeOriginalVerifier(
            inputs=self.archive.inputs,
            invocation_identity=self.archive.identity,
            require_active=active,
            bundles=self.archive.bundles,
            read_file=read_confined_file,
            release_root=self.archive.release_root,
            max_policy_bytes=1048576,
            max_bundle_bytes=16777216,
        ) as verifier:
            yield MssqlPhysicalCatalogPolicyReader(
                verifier=verifier,
                documents=NativeProjectDocumentReader(read_file=read_confined_file, max_policy_bytes=1048576),
            )


class CatalogV2Fixture:
    """Concrete SQL stores/provisioners with original v1 deployment coexisting."""

    def __init__(self, source, root, monkeypatch, profile):
        self.source, self.root, self.monkeypatch, self.profile = source, root, monkeypatch, profile
        self.v1 = CatalogFixture(source, "rowstore_none")
        with source.connection(autocommit=True) as connection:
            connection.execute(f"CREATE CERTIFICATE [{CERTIFICATE}] WITH SUBJECT='Isolated v2 lifecycle'")
            public = bytes(connection.execute(f"SELECT CERTENCODED(CERT_ID('{CERTIFICATE}'))").fetchone()[0])
        self.registrations = MssqlPhysicalRegistrationStore(connection_factory=source.connect, local_schema=LOCAL)
        self.bindings = MssqlPhysicalCatalogBindingStore(connection_factory=source.connect, local_schema=LOCAL)
        self.schema = MssqlPhysicalCatalogBindingSchemaProvisioner(
            connection_factory=source.connect, local_schema=LOCAL
        )
        self.module = MssqlPhysicalCatalogV2SchemaProvisioner(
            connection_factory=source.connect,
            catalog_sql=(ROOT / "packages/dbt-dpone/control/sqlserver/physical-v1/catalog-v2.sql").read_bytes(),
            certificate_name=CERTIFICATE,
            certificate_public_bytes=public,
            certificate_user=CERTIFICATE_USER,
        )
        self.first = source.registration
        self.first_binding = self.apply(self.first)

    def apply(self, registration, *, bindings=None):
        with self.source.authority.reader(self.source) as reader:
            return MssqlPhysicalCatalogRegistrationLifecycle(
                policy_reader=reader,
                registration_store=self.registrations,
                binding_schema=self.schema,
                module_provisioner=self.module,
                binding_store=bindings or self.bindings,
                connection_factory=self.source.connect,
            ).apply(self.source.authority.archive.refs, registration=registration)

    def read(self, registration=None, *, version=2, role="metadata"):
        registration = registration or self.source.registration
        plan = replace(
            self.v1.plan,
            generation_id=str(self.source.executor.generation_id),
            spec=replace(self.v1.plan.spec, resource_bounds=registration.trusted_profile.reference),
        )
        return MssqlPhysicalCatalogReader(
            connection_factory=lambda: self.source.connect("model", role),
            registration=registration,
            operation_timeout_seconds=PROFILES[self.profile]["read"],
            clock=time.monotonic,
            catalog_version=version,
        ).read(
            plan=plan,
            executor_invocation_id=str(self.source.executor.invocation_id),
            object_id=self.v1.object_id,
            expected_object_name="orders",
            expected_object_create_time=self.v1.created,
        )

    def inventory(self):
        with self.source.connection() as connection:
            modules = tuple(
                tuple(row)
                for row in connection.execute(
                    "SELECT p.name,m.definition,c.crypt_type,c.thumbprint FROM sys.procedures p "
                    "JOIN sys.sql_modules m ON m.object_id=p.object_id LEFT JOIN sys.crypt_properties c ON c.major_id=p.object_id "
                    "WHERE p.schema_id=SCHEMA_ID(?) ORDER BY p.name,c.crypt_type,c.thumbprint",
                    LOCAL,
                )
            )
            grants = tuple(
                tuple(row)
                for row in connection.execute(
                    "SELECT class,major_id,minor_id,grantee_principal_id,permission_name,state FROM sys.database_permissions "
                    "ORDER BY class,major_id,minor_id,grantee_principal_id,permission_name,state"
                )
            )
            schemas = tuple(
                tuple(row)
                for row in connection.execute("SELECT schema_id,name,principal_id FROM sys.schemas ORDER BY schema_id")
            )
        return modules, grants, schemas

    def successor(self):
        source = self.source
        previous = source.registration
        source.provider.close_writer_admission(source.reserved, expected_revision=2)

        def admin():
            return source.connect("control")

        MssqlDbtWorkspaceAttemptAdmission(admin, control_schema=SCHEMA).terminalize(
            source.request.workspace_attempt, state="FAILED"
        )
        activation = MssqlDbtWorkspaceActivationAdmission(admin, control_schema=SCHEMA)
        activation.begin_retirement(source.authority.activation)
        activation.finalize_retirement(source.authority.activation)
        authority = ArchiveAuthority(self.root / "second", self.monkeypatch, 2)
        service = json.loads(source.authority.payloads[source.authority.get("service").locator])
        registration = authority.registration(
            previous.model_database, previous.control_database, previous.principals, service
        )
        result = authority.admit(
            registration, admin, lambda: source.connect("control", "metadata"), source.credentials["metadata"][0]
        )
        source.authority, source.registration = authority, registration
        source.request, source.executor, source.provider, source.reserved = result
        assert self.registrations.register(registration) == registration
        return registration, self.apply(registration)

    def evidence(self):
        with self.source.connection() as connection:
            engine = connection.execute("SELECT @@VERSION").fetchone()[0]
            driver_version = connection.getinfo(self.source.driver.SQL_DRIVER_VER)
            driver_name = connection.getinfo(self.source.driver.SQL_DRIVER_NAME)
        return {
            "qualification_profile": self.profile,
            "topology": "same-database"
            if self.source.databases["model"] == self.source.databases["control"]
            else "two-database",
            "timeout_seconds": PROFILES[self.profile],
            "timing_qualification": False,
            "pyodbc_version": self.source.driver.version,
            "odbc_driver_version": driver_version,
            "odbc_driver_name": driver_name,
            "first_binding": self.first_binding.to_dict(),
            "module_inventory": [
                {
                    "name": row[0],
                    "definition_utf16_sha256": sha256(row[1].encode("utf-16le")).hexdigest(),
                    "signature": row[2],
                    "thumbprint": bytes(row[3]).hex() if row[3] is not None else None,
                }
                for row in self.inventory()[0]
            ],
            "engine": engine,
            "registration_id": self.source.registration.registration_id,
            "platform_policy_sha256": self.source.registration.platform_subject.platform_policy_sha256,
            "limits": asdict(self.source.registration.limits),
            "active_authority": "actual SQL require_active readback",
            "qualification": "isolated lifecycle correctness only; no dbt execution or production qualification",
        }


def selected_profile():
    profile = os.environ.get("DPONE_PHYSICAL_CATALOG_TEST_PROFILE", "legacy-correctness-v1")
    if profile not in PROFILES:
        raise ValueError("unknown isolated catalog test profile")
    return profile
