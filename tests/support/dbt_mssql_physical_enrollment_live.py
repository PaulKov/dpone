"""Real SQL/permission boundary with SYNTHETIC prerequisite provenance.

This fixture does not call or replace managed membership verification. It calls
fixed SQL endpoints after authenticating actual retained originals and real P/G.
"""

import time

from dpone.adapters.dbt_mssql_physical_catalog_fetch import CatalogReadBudget
from dpone.adapters.dbt_mssql_physical_enrollment import acquire_enrollment_originals
from dpone.adapters.dbt_mssql_physical_enrollment_permissions import ATTACH, CL, CONTROL, ENROLL, OBSERVER, READ, C, E
from dpone.adapters.dbt_mssql_physical_enrollment_schema import MssqlPhysicalEnrollmentSchemaProvisioner
from dpone.contracts.dbt_mssql_physical_catalog_binding import catalog_binding_digest
from dpone.contracts.dbt_mssql_physical_enrollment import encode_enrollment, enrollment_digest, require_enrollment_row
from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
from tests.support.dbt_mssql_physical_discovery_live import DiscoveryFixture
from tests.support.dbt_mssql_physical_source_authority import LOCAL, ROOT, SCHEMA

# Exact closed dpone_managed_columns('attach') wire; tested against its actual macro.
ATTACH_COLUMNS = (
    "wire_version",
    "registration_id",
    "registration_digest",
    "generation_id",
    "executor_invocation_id",
    "plan_set_sha256",
    "model_unique_id",
    "model_plan_sha256",
    "session_registration_id",
    "session_id",
    "guard_epoch",
    "source_revision",
    "control_program_sha256",
    "executor_json",
    "plan_json",
)


def require_attach_description(description):
    """Reject unnamed/reordered columns even when all fifteen values match."""
    assert description is not None
    assert tuple(field[0] for field in description) == ATTACH_COLUMNS


class RetainedOriginals:
    """Finite in-memory exact-version store owned by the synthetic fixture owner."""

    def __init__(self):
        self.entries = {}

    def retain(self, reference, kind, subject, payload):
        if (
            reference.key in self.entries
            or len(payload) != reference.size_bytes
            or enrollment_digest(payload) != reference.sha256
        ):
            raise ValueError("original identity conflict")
        self.entries[reference.key] = (reference, kind, subject, payload)

    def read(self, exactref, *, expected_kind, expected_subject, max_bytes):
        stored, kind, subject, payload = self.entries[exactref.key]
        if (
            exactref != stored
            or kind != expected_kind
            or subject != expected_subject
            or type(max_bytes) is not int
            or max_bytes <= 0
            or len(payload) > max_bytes
        ):
            raise ValueError("original version, subject, kind or bound mismatch")
        if enrollment_digest(payload) != exactref.sha256:
            raise ValueError("retained bytes changed")
        return payload


class EnrollmentFixture:
    """Five real signed procedures; dedicated METADATA/BUILD/OBSERVER SQL logins."""

    def __init__(self, source):
        self.source = source
        self.master_owned = False

    def install(self):
        from tests.support.dbt_mssql_physical_enrollment_originals_live import admit_originals

        source = self.source
        self.discovery = DiscoveryFixture(source)
        self.binding = self.discovery.binding
        assert self.discovery.read().object_count == 3
        self.originals, self.bindings, self.subject, self.plan_ref = admit_originals(source, self.binding)
        from dpone.contracts.native_source_custody import decode_source_executor_binding

        assert decode_source_executor_binding(source.read().executor_payload) == source.executor
        self.old = self.inventory()
        public_e = self.discovery._certificate(E, paired=True)
        public_c = self.discovery._certificate(C, paired=False)
        with source.connection("master", autocommit=True) as admin:
            assert tuple(admin.execute("SELECT CERT_ID(?),SUSER_ID(?)", C, CL).fetchone()) == (None, None)
            admin.execute(f"CREATE CERTIFICATE [{C}] FROM BINARY=0x{public_c.hex()}")
            self.master_owned = True
        root = ROOT / "packages/dbt-dpone/control/sqlserver/physical-v1"
        self.provisioner = MssqlPhysicalEnrollmentSchemaProvisioner(
            connection_factory=source.connect,
            enrollment_sql=(root / "enrollment.sql").read_bytes(),
            session_sql=(root / "session.sql").read_bytes(),
            discovery_sql=(root / "discovery.sql").read_bytes(),
            enrollment_certificate_public_bytes=public_e,
            connection_certificate_public_bytes=public_c,
        )
        self.deployment = self.provisioner.apply(source.registration, self.binding)
        assert len(self.deployment.module_sha256) == 5
        assert self.provisioner.apply(source.registration, self.binding) == self.deployment
        assert source.provisioner.apply(source.registration) == source.registration
        self.require_old_inventory()
        self.expected = self.acquire()
        self.payload = encode_enrollment(self.expected, max_bytes=source.registration.limits.max_metadata_bytes)
        first = self.enroll()
        before_replay = self.snapshot()
        assert self.read() == first
        assert self.enroll() == first
        assert self.snapshot() == before_replay

    def budget(self):
        return CatalogReadBudget(time.monotonic() + self.source.timing["read"], 1048576, time.monotonic)

    def acquire(self):
        return acquire_enrollment_originals(
            originals=self.originals,
            bindings=self.bindings,
            subject=self.subject,
            plan_reference=self.plan_ref,
            executor=self.source.executor,
            registration_sha256=physical_runtime_registration_digest(self.source.registration),
            catalog_binding_sha256=catalog_binding_digest(self.binding),
            max_bytes=self.source.registration.limits.max_metadata_bytes,
            budget=self.budget(),
        )

    def execute(self, module, parameters, *, role="metadata", namespace="model", columns=None):
        budget = self.budget()
        with self.source.connection(namespace, role, autocommit=True) as connection:
            connection.timeout = budget.seconds()
            cursor = connection.cursor()
            try:
                schema = LOCAL if namespace == "model" else SCHEMA
                cursor.execute(f"EXEC [{schema}].[{module}] " + ",".join("?" for _ in parameters), *parameters)
                budget.seconds()
                if columns is None:
                    return None
                if module == ATTACH:
                    require_attach_description(cursor.description)
                row = cursor.fetchone()
                assert row is not None and len(row) == columns
                assert cursor.fetchone() is None and not cursor.nextset()
                budget.seconds()
                return tuple(row)
            finally:
                cursor.close()

    def identity(self):
        return (
            self.source.registration.registration_id,
            str(self.subject.generation_id),
            str(self.source.executor.invocation_id),
        )

    def observe(self, module, *, role="metadata", payload=None, digest=None):
        value = self.payload if payload is None else payload
        checksum = enrollment_digest(value).encode() if digest is None else digest
        parameters = self.identity() + ((value, checksum) if module == ENROLL else (checksum,))
        row = list(self.execute(module, parameters, role=role, columns=8))
        for index in (1, 3, 4):
            row[index] = str(row[index]).lower()
        for index in (2, 6):
            row[index] = bytes(row[index]).decode("ascii")
        row[7] = bytes(row[7])
        return require_enrollment_row(
            tuple(row), expected=self.expected, max_bytes=self.source.registration.limits.max_metadata_bytes
        )

    def enroll(self, **kwargs):
        return self.observe(ENROLL, **kwargs)

    def read(self, **kwargs):
        return self.observe(READ, **kwargs)

    def attach(self, *, role="build", model=None, plan_ref=None):
        reference = plan_ref or self.plan_ref
        return self.execute(
            ATTACH,
            self.identity()
            + (reference.locator, reference.sha256, model or self.expected.plan.models[0].spec.model_unique_id),
            role=role,
            columns=15,
        )

    def inventory(self):
        result = {}
        for namespace in ("model", "control"):
            with self.source.connection(namespace) as admin:
                modules = tuple(
                    tuple(row)
                    for row in admin.execute(
                        "SELECT s.name,p.name,m.definition,c.crypt_type,c.thumbprint FROM sys.procedures p "
                        "JOIN sys.schemas s ON s.schema_id=p.schema_id JOIN sys.sql_modules m ON m.object_id=p.object_id "
                        "LEFT JOIN sys.crypt_properties c ON c.major_id=p.object_id ORDER BY s.name,p.name,c.crypt_type,c.thumbprint"
                    )
                )
                permissions = tuple(
                    tuple(row)
                    for row in admin.execute(
                        "SELECT class,major_id,minor_id,grantee_principal_id,permission_name,state "
                        "FROM sys.database_permissions ORDER BY class,major_id,minor_id,grantee_principal_id,permission_name,state"
                    )
                )
                result[namespace] = (modules, permissions)
        return result

    def require_old_inventory(self):
        after = self.inventory()
        new_names = {ENROLL, READ, ATTACH, OBSERVER, CONTROL}
        for namespace, (modules, permissions) in self.old.items():
            assert tuple(row for row in after[namespace][0] if row[1] not in new_names) == modules
            assert set(permissions) <= set(after[namespace][1])
            # Existing runtime permissions may only gain the three new endpoint grants.
            principals = {
                getattr(mapping, namespace).principal_id
                for mapping in (
                    self.source.registration.principals.metadata,
                    self.source.registration.principals.build,
                    self.source.registration.principals.observer.mapping,
                )
            }
            with self.source.connection(namespace) as admin:
                allowed = {
                    admin.execute("SELECT OBJECT_ID(?)", f"{LOCAL}.{name}").fetchone()[0]
                    for name in (ENROLL, READ, ATTACH)
                }
            for row in set(after[namespace][1]) - set(permissions):
                if row[3] in principals:
                    assert row[0] == 1 and row[1] in allowed and row[2] == 0 and row[4:] == ("EXECUTE", "G")

    def rows(self):
        with self.source.connection() as admin:
            return tuple(
                tuple(tuple(row) for row in admin.execute(f"SELECT * FROM [{LOCAL}].[{table}]"))
                for table in ("physical_plan_enrollments_v1", "physical_model_sessions_v1")
            )

    def snapshot(self):
        """Exact native P/G/capacity/original rows plus both local custody tables."""
        return {"control": self.source.snapshot(), "model": self.rows()}

    def evidence(self):
        environment = self.discovery.evidence()
        environment["qualification"] = (
            "archive/catalog/discovery prerequisite inventory observed after real fixture G admission; "
            "discovery does not qualify generation or enrollment"
        )
        environment["prerequisite_provenance"] = "SYNTHETIC"
        return {
            "scope": "isolated SQL/permissions only",
            "prerequisite_provenance": "SYNTHETIC",
            "managed_source_producer": "UNAVAILABLE",
            "managed_membership": "UNVERIFIED",
            "bind_transaction_receipt": "UNAVAILABLE",
            "timing_qualification": False,
            "environment": environment,
            "retained_originals": {name: ref.sha256 for name, ref in self.source.authority.references.items()},
            "enrollment_sha256": enrollment_digest(self.payload),
            "expanded_modules": dict(self.deployment.module_sha256),
            "E": self.deployment.enrollment_certificate_thumbprint.hex(),
            "C": self.deployment.connection_certificate_thumbprint.hex(),
            "source_bytes": dict(self.deployment.source_sha256),
        }

    def cleanup(self):
        if self.master_owned:
            with self.source.connection("master", autocommit=True) as admin:
                admin.execute(f"IF SUSER_ID(N'{CL}') IS NOT NULL DROP LOGIN [{CL}]")
                admin.execute(f"DROP CERTIFICATE [{C}]")
