"""SYNTHETIC prerequisite producer; exact retained originals and actual P/G SQL.

No complete managed graph producer, runtime registry or launch is represented.
The archive fixture's ordinary alpha source is authenticated only for catalog
binding. The physical plan is explicitly authored by this isolated test owner.
"""

from types import SimpleNamespace
from typing import Literal
from uuid import uuid4

from dpone.adapters.native_generation_mssql import MssqlNativeGenerationControl
from dpone.adapters.native_generation_mssql_schema import MssqlNativeGenerationSchemaMigration
from dpone.adapters.native_originals_mssql import MssqlNativeOriginalBindings
from dpone.contracts.dbt_mssql_physical import (
    AbsentPredecessor,
    PhysicalFilegroup,
    PhysicalModelPlan,
    PhysicalModelSpec,
    PhysicalPlanSet,
    PhysicalRelation,
)
from dpone.contracts.dbt_mssql_physical_invocation import PhysicalManagedInvocation
from dpone.contracts.dbt_mssql_physical_wire import encode_physical_plan_set
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from dpone.contracts.native_generation_admission import VerifiedGenerationRequest, generation_admission_request_bytes
from dpone.contracts.native_generation_invocation import (
    TrustedDbtCommandEntry,
    TrustedDbtCommandPlan,
    encode_trusted_dbt_command_plan,
)
from dpone.contracts.native_original_kinds import NativeOriginalKind
from dpone.contracts.native_originals import NativeGenerationOriginalSubject, NativeOriginalBinding
from dpone.contracts.native_source_custody import SourceExecutorBinding
from dpone.ports.semantic_refresh_artifact_store import ArtifactObjectRef
from tests.support.dbt_mssql_physical_discovery_live import DiscoveryArchiveAuthority
from tests.support.dbt_mssql_physical_source_authority import SCHEMA


class EnrollmentArchiveAuthority(DiscoveryArchiveAuthority):
    """Select alpha in the fixture producer before real workspace admission."""

    def admit_physical_owner(self, registration, admin, metadata_name):
        from tests.support import dbt_mssql_physical_source_authority as source_module

        def build(**fields):
            return DbtWorkspaceAttemptRequest.build(**(fields | {"workflow_id": "alpha"}))

        with self.monkeypatch.context() as patch:
            patch.setattr(source_module, "DbtWorkspaceAttemptRequest", SimpleNamespace(build=build))
            return super().admit_physical_owner(registration, admin, metadata_name)


def plan_and_command(registration, owner, generation, invocation, graph_ref, retain):
    """Author canonical plans and literal equal vars before reservation exists."""
    profile = registration.trusted_profile.reference
    spec = PhysicalModelSpec(
        "model.alpha.orders",
        graph_ref.sha256,
        PhysicalRelation(registration.model_database.database_name, "catalog_models", "orders"),
        (MssqlCatalogColumn("id", "int", False, None),),
        "rowstore_none",
        PhysicalFilegroup(1, "PRIMARY"),
        profile,
    )
    plan = PhysicalPlanSet(
        str(generation),
        registration.registration_id,
        owner.attempt,
        owner.receipt.guard_epochs[0],
        profile,
        registration.model_database,
        (PhysicalModelPlan(str(generation), spec, AbsentPredecessor()),),
    )
    plan_ref = retain("physical-plan", encode_physical_plan_set(plan))
    managed = PhysicalManagedInvocation(str(generation), str(invocation), plan_ref, registration.registration_id)
    vars_text = encode_native_delivery_json({"__dpone_managed": managed.to_dict()}).decode()
    roots = tuple(
        retain(name, {"scope": "SYNTHETIC SQL fixture only", "root": name})
        for name in ("project-root", "profile-root", "output-root")
    )
    verbs: tuple[Literal["parse", "ls", "build"], ...] = ("parse", "ls", "build")
    command = TrustedDbtCommandPlan(
        "dpone.trusted-dbt-command-plan.v1",
        invocation,
        "BUILD",
        tuple(
            TrustedDbtCommandEntry(index, verb, ("dbt", verb, "--vars", vars_text), 10, 2)
            for index, verb in enumerate(verbs)
        ),
        roots[0],
        roots[2],
        roots[1],
        36,
    )
    return plan_ref, retain("physical-command", encode_trusted_dbt_command_plan(command))


def admit_originals(source, binding):
    """Run actual immutable original binding, reservation, and executor binding."""
    from tests.support.dbt_mssql_physical_enrollment_live import RetainedOriginals

    authority, registration, owner = source.authority, source.registration, source.owner
    assert owner.attempt.workflow_id == binding.workflow_id == "alpha"
    control, generation, invocation = registration.control_authority, uuid4(), uuid4()
    subject = NativeGenerationOriginalSubject(registration.platform_subject.authority, generation)
    graph = authority.retain(
        "physical-graph-prerequisite",
        {
            "provenance": "SYNTHETIC",
            "managed_source_producer": "UNAVAILABLE",
            "models": ["model.alpha.orders"],
        },
    )
    plan_ref, command = plan_and_command(registration, owner, generation, invocation, graph, authority.retain)
    MssqlNativeGenerationSchemaMigration(
        connection_factory=lambda: source.connect("control"),
        control_schema=SCHEMA,
        control_authority=control,
        runtime_database_principal=source.credentials["metadata"][0],
        physical_guard=owner.resource.guard_id,
        resource_authority=registration.capacity_authority,
        capacity_bytes=registration.limits.max_generation_bytes,
        trusted_profile=registration.trusted_profile.reference,
        max_generation_bytes=registration.limits.max_generation_bytes,
    ).apply()
    # G tables are created after the inherited P-only install. Apply the same
    # BUILD/OBSERVER direct DENYs that the full source fixture installs for G.
    with source.connection("control", autocommit=True) as admin:
        tables = admin.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID(?)", SCHEMA).fetchall()
        for role in ("build", "observer"):
            login = source.credentials[role][0]
            for row in tables:
                admin.execute(
                    f"DENY SELECT, INSERT, UPDATE, DELETE, ALTER, TAKE OWNERSHIP "
                    f"ON OBJECT::[{SCHEMA}].[{row[0]}] TO [{login}]"
                )
    arguments = dict(
        subject=subject,
        workspace_attempt=owner.attempt,
        guard=owner.receipt.guard_epochs[0],
        profile=registration.trusted_profile.reference,
        command=command,
        requested_bytes=60,
    )
    reservation = authority.retain("reservation", generation_admission_request_bytes(**arguments))
    originals = RetainedOriginals()
    bindings = MssqlNativeOriginalBindings(
        connection_factory=lambda: source.connect("control", "metadata"),
        control_schema=SCHEMA,
        control_authority=control,
        max_binding_bytes=1048576,
    )
    retained: tuple[tuple[ArtifactObjectRef, NativeOriginalKind], ...] = (
        (reservation, "generation_stored_file_v1"),
        (plan_ref, "mssql_physical_plan_set_v1"),
        (command, "trusted_dbt_command_plan_v1"),
    )
    for reference, kind in retained:
        payload = authority.payloads[reference.locator]
        exact = ArtifactObjectRef(
            reference.locator,
            "fixture-owner-v1",
            len(payload),
            reference.sha256,
            "fixture-memory-only",
            "2099-01-01T00:00:00Z",
        )
        originals.retain(exact, kind, subject, payload)
        assert (
            bindings.bind(
                NativeOriginalBinding(
                    subject, kind, authority.get("storage-policy"), exact, reference.sha256, reference.locator
                )
            )
            == reference
        )
    source.request = VerifiedGenerationRequest(reservation=reservation, **arguments)
    source.provider = MssqlNativeGenerationControl(
        connection_factory=lambda: source.connect("control", "metadata"),
        control_schema=SCHEMA,
        control_authority=control,
    )
    source.reserved = source.provider.reserve(source.request)
    source.executor = SourceExecutorBinding(
        generation,
        owner.receipt.guard_epochs[0].fencing_epoch,
        invocation,
        reservation,
        registration.trusted_profile.reference,
        command,
    )
    bound = source.provider.bind_writer_invocation(
        source.reserved, source.executor, expected_revision=source.reserved.revision
    )
    assert bound.state == "BUILDING" and bound.revision == 2
    return originals, bindings, subject, plan_ref
