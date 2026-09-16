"""Acquire retained release bytes before checking physical model membership."""

from pathlib import Path
from typing import Any, cast

from dpone.adapters.dbt_mssql_physical_catalog_policy import MssqlPhysicalCatalogPolicyReader
from dpone.adapters.dbt_publish_artifact_reader import DbtArtifactReader
from dpone.adapters.native_delivery_originals import NativeOriginalVerifier
from dpone.adapters.native_project_documents import NativeProjectDocumentReader
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_mssql_physical import PhysicalPlanSet
from dpone.contracts.dbt_mssql_physical_plan_membership import (
    PhysicalPlanMembershipError,
    require_model_plan_membership,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_wire import encode_physical_plan_set
from dpone.contracts.dbt_release_workload_binding import release_object
from dpone.contracts.dbt_runtime_release_binding import DbtReleaseArtifactIndex
from dpone.contracts.dbt_selected_graph_observation import observe_dbt_selected_graph
from dpone.contracts.dbt_source_inventory_binding import DbtSourcePlan
from dpone.contracts.native_delivery import NativeOriginalsRefV1
from dpone.contracts.native_delivery_json import decode_native_delivery_json
from dpone.contracts.native_originals import NativePlatformOriginalSubject
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader


class MssqlPhysicalPlanMembershipReader:
    """Concrete source consumer; input plan/registration remain comparison claims.

    Keep the injected verifier alive throughout this invocation-confined reader.
    Current catalog validation performs its own bounded verification; the second
    acquisition is compared to that projection, never substituted by a proof DTO.
    No connection, credentials, SQL, source rewriting or managed-policy fallback.
    """

    def __init__(
        self,
        *,
        verifier: NativeOriginalVerifier,
        documents: NativeProjectDocumentReader,
        read_file: ConfinedReleaseFileReader,
        release_root: Path,
        max_release_bytes: int,
    ) -> None:
        if type(verifier) is not NativeOriginalVerifier or type(documents) is not NativeProjectDocumentReader:
            raise ValueError("plan membership requires concrete original and document readers")
        if not isinstance(release_root, Path) or not release_root.is_absolute() or ".." in release_root.parts:
            raise ValueError("plan membership requires an explicit absolute release root")
        if type(max_release_bytes) is not int or max_release_bytes <= 0:
            raise ValueError("plan membership requires a positive release acquisition bound")
        self._verifier = verifier
        self._documents = documents
        self._read = read_file
        self._root = release_root
        self._maximum = max_release_bytes
        self._policy = MssqlPhysicalCatalogPolicyReader(verifier=verifier, documents=documents)

    def require_plan_membership(
        self,
        refs: NativeOriginalsRefV1,
        *,
        registration: MssqlPhysicalRuntimeRegistration,
        plan_set: PhysicalPlanSet,
    ) -> None:
        """Authenticate indexed source/model membership before enrollment mutation.

        Enrollment must first read and authenticate the actual retained plan
        original and compare the complete plan workspace_attempt/guard with the
        authenticated retained generation request. This call neither proves plan
        retention nor activation/G/attempt ownership; resolved originals expose
        no activation identity. Never infer that proof from membership success.
        It never admits the legacy materialization as managed. Character columns
        require exact collation selection from the authenticated policy; matching
        that expectation does not prove live SQL availability or helper output.
        """
        if (
            type(refs) is not NativeOriginalsRefV1
            or type(registration) is not MssqlPhysicalRuntimeRegistration
            or type(plan_set) is not PhysicalPlanSet
        ):
            raise PhysicalPlanMembershipError()
        refs.__post_init__()
        registration.__post_init__()
        plan_bytes = encode_physical_plan_set(plan_set)
        if (
            len(plan_bytes) > registration.limits.max_metadata_bytes
            or plan_set.runtime_registration_id != registration.registration_id
            or plan_set.model_database != registration.model_database
            or plan_set.profile != registration.trusted_profile.reference
            or registration.local_schema != "dpone_physical"
        ):
            raise PhysicalPlanMembershipError()
        projection = self._policy.read(refs, registration=registration)
        original = self._verifier.resolve(refs)
        policy_bytes, intent = self._documents.read(original.project_directory, original.project_bundle)
        if (
            policy_bytes != original.policy_document
            or projection.platform_subject != NativePlatformOriginalSubject(original.authority, original.policy_sha256)
            or projection.policy_member.sha256 != sha256_bytes(policy_bytes)
            or projection.project_archive_sha256 != original.project_bundle.archive_sha256
            or (projection.profile_name, projection.workflow_id) != (intent["profile"], intent["workflow"])
            or plan_set.workspace_attempt.workflow_id != intent["workflow"]
        ):
            raise PhysicalPlanMembershipError()
        owners = tuple(
            owner
            for owner in original.sources.workflows
            if owner.project.project_bundle_sha256 == original.project_bundle.archive_sha256
            and owner.source.workflow_id == intent["workflow"]
        )
        if len(owners) != 1:
            raise PhysicalPlanMembershipError()
        owner = owners[0]
        release_bytes = self._read(self._root, refs.release.locator, max_bytes=self._maximum)
        if (
            type(release_bytes) is not bytes
            or len(release_bytes) > self._maximum
            or sha256_bytes(release_bytes) != refs.release.sha256
        ):
            raise PhysicalPlanMembershipError()
        release = release_object(release_bytes, "membership release")
        source_plan = DbtSourcePlan.from_release(
            release, original.sources.inventory, expected_release_id=original.authority.release_id
        )
        index = source_plan.artifacts
        index.require_workload_trio(owner.source.workload_id, owner.source.runtime_payload_ids)
        pack = index.workloads[owner.source.workload_id]
        if (
            pack["path"] != refs.pack.locator
            or pack["sha256"] != refs.pack.sha256
            or pack["sha256"] != owner.workload_pack_sha256
        ):
            raise PhysicalPlanMembershipError()
        manifest_bytes = self._payload(index, owner.source.runtime_payload_ids[1], "dbt_manifest")
        selection_bytes = self._payload(index, owner.source.runtime_payload_ids[2], "dbt_selection_lock")
        selection = source_plan.decode_selection(selection_bytes)
        if (
            selection != owner.execution.selection_lock
            or sha256_bytes(manifest_bytes) != owner.project.manifest_sha256
            or sha256_bytes(selection_bytes) != owner.source.selection_lock_sha256
        ):
            raise PhysicalPlanMembershipError()
        artifact, issues = DbtArtifactReader().read_payload(
            manifest_bytes, path=self._root / str(index.payloads[owner.source.runtime_payload_ids[1]]["path"])
        )
        if artifact is None or any(issue.severity == "error" for issue in issues):
            raise PhysicalPlanMembershipError("DPONE_PHYSICAL_PLAN_MANIFEST_INVALID")
        manifest = release_object(manifest_bytes, "membership manifest")
        effective = owner.execution.invocation_profile()
        target = (projection.model_database_name, projection.model_schema)
        if (
            effective.database,
            effective.schema,
        ) != target or effective.connection_ref != registration.model_connection_ref:
            raise PhysicalPlanMembershipError()
        resolved_target = (owner.execution.profile.database, owner.execution.profile.schema)
        observation = observe_dbt_selected_graph(manifest, lock=selection, logical_target=resolved_target)
        observation.require_matches(selection, selection.selected_graph_unique_ids)
        policy = cast(dict[str, Any], decode_native_delivery_json(policy_bytes))
        profile = policy["profiles"][intent["profile"]]
        require_model_plan_membership(
            manifest,
            selection=selection,
            plan_set=plan_set,
            profile=profile,
            limits=registration.limits,
            logical_target=resolved_target,
            declared_models=artifact.models,
            resource_bounds=projection.resource_bounds,
        )
        # The captured intent names a publish root only, never all upstream layouts.
        for model in plan_set.models:
            if (
                model.spec.model_unique_id in selection.publish_model_unique_ids
                and model.spec.layout != intent["model_storage"]
            ):
                raise PhysicalPlanMembershipError()

    def _payload(self, index: DbtReleaseArtifactIndex, payload_id: str, kind: str) -> bytes:
        descriptor = index.payloads[payload_id]
        if descriptor["kind"] != kind:
            raise PhysicalPlanMembershipError()
        # Index construction already enforces the existing per-kind read ceilings.
        path, size = descriptor["path"], descriptor["bytes"]
        assert isinstance(path, str) and type(size) is int
        payload = self._read(self._root, path, max_bytes=size)
        if type(payload) is not bytes:
            raise PhysicalPlanMembershipError()
        index.require_bytes(path, payload)
        return payload
