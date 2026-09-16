"""Fixture-owner bootstrap and actual P/G admission, not release qualification.

Only the isolated test owner admits these synthetic documents. Digests establish
integrity, never their own authority. No network artifact provider or production
trust chain is simulated by the local retained-byte dictionary.
"""

from dataclasses import asdict, dataclass
from hashlib import sha256
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from uuid import uuid4

from dpone.adapters.dbt_workspace_mssql_activation_admission import MssqlDbtWorkspaceActivationAdmission
from dpone.adapters.dbt_workspace_mssql_attempt_admission import MssqlDbtWorkspaceAttemptAdmission
from dpone.adapters.native_generation_mssql import MssqlNativeGenerationControl
from dpone.adapters.native_generation_mssql_schema import MssqlNativeGenerationSchemaMigration
from dpone.adapters.native_originals_mssql import MssqlNativeOriginalBindings
from dpone.adapters.native_originals_mssql_schema import MssqlNativeOriginalSchemaMigration
from dpone.adapters.semantic_refresh_mssql_schema import MssqlSemanticRefreshSchemaMigration
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_values import PlatformSelection, ProgramAuthority, RegisteredLimits
from dpone.contracts.dbt_publish_schema_contract_v4 import validate_native_policy_v4
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationRequest, DbtWorkspacePhysicalResource
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptReceipt, DbtWorkspaceAttemptRequest
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from dpone.contracts.native_generation_admission import VerifiedGenerationRequest, generation_admission_request_bytes
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import (
    NativeGenerationOriginalSubject,
    NativeOriginalBinding,
    NativePlatformOriginalSubject,
)
from dpone.contracts.native_source_custody import SourceExecutorBinding
from dpone.ports.semantic_refresh_artifact_store import ArtifactObjectRef
from tests.test_dbt_native_policy_v4 import native_policy

ROOT = Path(__file__).resolve().parents[2]
ADMISSION = ROOT / "packages/dbt-dpone/control/sqlserver/physical-v1/admission.sql"
SCHEMA = "dpone_control"
LOCAL = "dpone_physical"


@dataclass(frozen=True)
class PhysicalOwnerAdmission:
    """Actual fixture admission results; no generation has been reserved yet."""

    resource: DbtWorkspacePhysicalResource
    command: OriginalRef
    attempt: DbtWorkspaceAttemptRequest
    receipt: DbtWorkspaceAttemptReceipt


class FixtureAuthority:
    """Retain the bytes admitted by the explicitly authorized isolated owner."""

    def __init__(self):
        self.payloads = {}
        self.references = {}

    def retain(self, name, document):
        payload = document if type(document) is bytes else encode_native_delivery_json(document)
        reference = OriginalRef("fixture/" + name, "sha256:" + sha256(payload).hexdigest())
        assert name not in self.references
        self.payloads[reference.locator] = payload
        self.references[name] = reference
        return reference

    def get(self, name):
        reference = self.references[name]
        assert "sha256:" + sha256(self.payloads[reference.locator]).hexdigest() == reference.sha256
        return reference

    def registration(self, model_pin, control_pin, principals, service_document):
        def pin_document(pin):
            return asdict(pin) | {"database_guid": str(pin.database_guid)}

        distributions = {}
        for distribution in ("dbt-core", "dbt-sqlserver", "pyodbc"):
            try:
                distributions[distribution] = version(distribution)
            except PackageNotFoundError:
                distributions[distribution] = "UNAVAILABLE"
        inputs = {
            "control": {
                "database": pin_document(control_pin),
                "schema": SCHEMA,
                "metadata_principal": asdict(principals.metadata.control),
            },
            "capacity": {"database": pin_document(model_pin), "capacity_bytes": 3 * 16777216},
            "profile": {
                "driver": "ODBC Driver 18 for SQL Server",
                "model_database": pin_document(model_pin),
                "control_database": pin_document(control_pin),
            },
            "toolchain": {
                "installed_distributions": distributions,
                "source_template_sha256": "sha256:" + sha256(ADMISSION.read_bytes()).hexdigest(),
            },
        }
        for name in (
            "control",
            "capacity",
            "profile",
            "toolchain",
            "release",
            "deployment",
            "bindings",
            "connections",
            "credentials",
            "qualification",
            "storage-root",
            "storage-capability",
            "encryption",
            "retention",
        ):
            self.retain(
                name,
                {
                    "fixture_input": name,
                    "scope": "isolated-source-identity-only",
                    "qualification": "UNVERIFIED",
                    "observed_configuration": inputs.get(name, {}),
                },
            )
        # Same canonical service identity used by MssqlDbtWorkspacePhysicalAuthority.
        assert self.retain("service", service_document).sha256 == canonical_fingerprint(service_document)
        policy = native_policy()
        profile = policy["profiles"]["local"]
        native = profile["native_execution"]
        native["control"]["authority"] = asdict(self.get("control"))
        native["generation"]["capacity_authority"] = asdict(self.get("capacity"))
        native["generation"]["storage_root"] = {"reference": asdict(self.get("storage-root")), "subject": None}
        for name in ("profile", "toolchain", "qualification"):
            native["trusted_execution"][name] = {"reference": asdict(self.get(name)), "subject": None}
        storage = native["originals"]["policy"]
        for field, name in (
            ("capability_evidence_sha256", "storage-capability"),
            ("encryption_policy_sha256", "encryption"),
            ("retention_policy_sha256", "retention"),
        ):
            storage[field] = self.get(name).sha256
        native["originals"]["authority"] = asdict(self.retain("storage-policy", storage))
        # These are retained inventory identities, not passed runtime certification.
        image = self.retain("image-inventory", {"purpose": "source-bridge-test", "qualification": "UNVERIFIED"})
        profile["runtime"]["image"] = "example/isolated@" + image.sha256
        profile["runtime"]["xcom_sidecar_image"] = "example/unused@" + image.sha256
        payload = encode_native_delivery_json(policy)
        validate_native_policy_v4(payload, max_bytes=1048576)
        policy_ref = self.retain("policy", payload)
        authority = DbtWorkspaceRuntimeAuthority.build(
            environment="test",
            release_id=self.get("release").sha256,
            deployment_id=self.get("deployment").sha256,
            release_sha256=self.get("release").sha256,
            deployment_sha256=self.get("deployment").sha256,
            binding_set_sha256=self.get("bindings").sha256,
            connection_registry_sha256=self.get("connections").sha256,
            credential_runtime_sha256=self.get("credentials").sha256,
        )
        subject = NativePlatformOriginalSubject(authority, policy_ref.sha256)
        program = self.retain("admission.sql", ADMISSION.read_bytes())
        package = self.retain(
            "source-bridge-package-inventory",
            {
                "scope": "source-bridge-only",
                "members": [asdict(program)],
                "complete_dbt_package": False,
            },
        )
        macro = self.retain(
            "macro-inventory", {"scope": "source-bridge-only", "model_macros": [], "qualification": "UNVERIFIED"}
        )
        self.authority = authority
        self.policy = policy
        return MssqlPhysicalRuntimeRegistration(
            registration_id=str(uuid4()),
            platform_subject=subject,
            control_authority=self.get("control"),
            trusted_profile=PlatformSelection(self.get("profile"), subject),
            trusted_toolchain=PlatformSelection(self.get("toolchain"), subject),
            qualification_policy_id=native["trusted_execution"]["qualification_policy_id"],
            control_connection_ref="control",
            model_connection_ref="source",
            service_authority_sha256=self.get("service").sha256,
            control_database=control_pin,
            model_database=model_pin,
            control_schema=SCHEMA,
            local_schema=LOCAL,
            program=ProgramAuthority(program.sha256, package.sha256, macro.sha256),
            capacity_authority=self.get("capacity"),
            limits=RegisteredLimits(65536, native["generation"]["max_generation_bytes"], 100, 65536, 100, 256),
            principals=principals,
        )

    def admit_physical_owner(self, registration, admin, metadata_name):
        """Run actual workspace admission only; do not create or reserve G.

        This separation lets isolated P-only tests use the same bootstrap as the
        existing source fixture. Returned values are actual admission outputs;
        the fixture does not infer durable ownership from their construction.
        """
        control = registration.control_authority
        MssqlNativeOriginalSchemaMigration(
            connection_factory=admin,
            control_schema=SCHEMA,
            control_authority=control,
            runtime_database_principal=metadata_name,
        ).apply()
        MssqlSemanticRefreshSchemaMigration(admin, control_schema=SCHEMA).apply()
        subject = self.retain(
            "write-subject", {"database": registration.model_database.database_name, "relation": "fixture_model"}
        )
        observation = self.retain(
            "resource-observation",
            asdict(registration.model_database) | {"database_guid": str(registration.model_database.database_guid)},
        )
        resource = DbtWorkspacePhysicalResource(
            "mssql://fixture/model",
            "mssql",
            registration.service_authority_sha256,
            observation.sha256,
            observation.sha256,
            (subject.sha256,),
        )
        activation = DbtWorkspaceActivationRequest.build(
            activation_id=str(uuid4()),
            environment="test",
            release_id=self.authority.release_id,
            deployment_id=self.authority.deployment_id,
            previous_deployment_id=None,
            source_inventory_sha256=observation.sha256,
            runtime_context_sha256=self.authority.authority_subject_sha256,
            write_subjects=resource.write_subjects,
            resources=(resource,),
        )
        admission = MssqlDbtWorkspaceActivationAdmission(admin, control_schema=SCHEMA)
        admission.prepare(activation)
        admission.activate(activation)
        command = self.retain(
            "command", {"scope": "bridge-admission-only", "argv": ["dbt", "build"], "executed": False}
        )
        attempt = DbtWorkspaceAttemptRequest.build(
            activation_id=activation.activation_id,
            attempt_id=command.sha256,
            workflow_id="orders",
            write_subjects=resource.write_subjects,
        )
        receipt = MssqlDbtWorkspaceAttemptAdmission(admin, control_schema=SCHEMA).admit(attempt)
        return PhysicalOwnerAdmission(resource, command, attempt, receipt)

    def admit(self, registration, admin, runtime, metadata_name):
        """Execute the unchanged P admission, then original bind and G bind."""
        owner = self.admit_physical_owner(registration, admin, metadata_name)
        resource, command, attempt, receipt = owner.resource, owner.command, owner.attempt, owner.receipt
        control = registration.control_authority
        MssqlNativeGenerationSchemaMigration(
            connection_factory=admin,
            control_schema=SCHEMA,
            control_authority=control,
            runtime_database_principal=metadata_name,
            physical_guard=resource.guard_id,
            resource_authority=registration.capacity_authority,
            capacity_bytes=registration.limits.max_generation_bytes,
            trusted_profile=registration.trusted_profile.reference,
            max_generation_bytes=registration.limits.max_generation_bytes,
        ).apply()
        arguments = dict(
            subject=NativeGenerationOriginalSubject(self.authority, uuid4()),
            workspace_attempt=attempt,
            guard=receipt.guard_epochs[0],
            profile=registration.trusted_profile.reference,
            command=command,
            requested_bytes=60,
        )
        payload = generation_admission_request_bytes(**arguments)
        reservation = self.retain("reservation", payload)
        binding = NativeOriginalBinding(
            arguments["subject"],
            "generation_stored_file_v1",
            self.get("storage-policy"),
            ArtifactObjectRef(
                "fixture/reservation",
                "fixture-owner-v1",
                len(payload),
                reservation.sha256,
                "fixture-memory-only",
                "2099-01-01T00:00:00Z",
            ),
            reservation.sha256,
            reservation.locator,
        )
        # This exact object is retained above in the local fixture. No S3 claim.
        originals = MssqlNativeOriginalBindings(
            connection_factory=runtime,
            control_schema=SCHEMA,
            control_authority=control,
            max_binding_bytes=1048576,
        )
        assert originals.bind(binding) == reservation
        request = VerifiedGenerationRequest(reservation=reservation, **arguments)
        provider = MssqlNativeGenerationControl(
            connection_factory=runtime, control_schema=SCHEMA, control_authority=control
        )
        reserved = provider.reserve(request)
        executor = SourceExecutorBinding(
            request.subject.generation_id,
            request.guard.fencing_epoch,
            uuid4(),
            reservation,
            request.profile,
            request.command,
        )
        bound = provider.bind_writer_invocation(reserved, executor, expected_revision=reserved.revision)
        assert bound.state == "BUILDING" and bound.revision == 2
        return request, executor, provider, reserved
