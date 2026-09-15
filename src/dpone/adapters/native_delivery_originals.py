"""Authenticate native inputs across explicit deployment and immutable release roots."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Self

from dpone_airflow_pack.pack_identity import parse_pack_json, verify_pack_fingerprint

from dpone.adapters.native_project_documents import NativeProjectDocumentReader
from dpone.contracts.airflow_deployment import deployment_id, release_id
from dpone.contracts.airflow_run_identity import AirflowDeploymentIdentity
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_release_artifact_limits import MAX_DBT_RELEASE_PACK_BYTES
from dpone.contracts.dbt_release_workload_binding import dbt_execution_from_pack, release_object, release_text
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2
from dpone.contracts.dbt_runtime_release_binding import DbtReleaseArtifactIndex
from dpone.contracts.dbt_source_inventory_binding import DbtReleaseSources, DbtSourcePlan, DbtWorkflowReleaseSource
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActiveActivation
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.native_delivery import NativeOriginalsRefV1, ResolvedNativeOriginals
from dpone.contracts.native_identity import OriginalRef
from dpone.ports.dbt_project_bundle import DbtProjectBundleOperations
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader
from dpone.ports.dbt_workspace_activation import DbtWorkspaceActivationInputPort


class NativeOriginalVerifier:
    """Own verified extracted roots until explicit close or context-manager exit.

    Constructor capabilities are composed by the trusted application. Reference
    values and local roots provide integrity coordinates, never dispatch authority.
    The pinned active callback is invoked only after all offline preflight checks.
    Keep this verifier alive while consuming its returned project directory.
    """

    def __init__(
        self,
        *,
        inputs: DbtWorkspaceActivationInputPort,
        invocation_identity: AirflowDeploymentIdentity,
        require_active: Callable[[], DbtWorkspaceActiveActivation],
        bundles: DbtProjectBundleOperations,
        read_file: ConfinedReleaseFileReader,
        release_root: Path,
        max_policy_bytes: int,
        max_bundle_bytes: int,
    ) -> None:
        if type(invocation_identity) is not AirflowDeploymentIdentity:
            raise ValueError("native verifier requires an exact pinned invocation identity")
        self._identity = AirflowDeploymentIdentity.from_mapping(invocation_identity.to_dict())
        if not isinstance(release_root, Path) or not release_root.is_absolute() or ".." in release_root.parts:
            raise ValueError("native release root must be explicit and absolute without traversal")
        for maximum in (max_policy_bytes, max_bundle_bytes):
            if type(maximum) is not int or maximum <= 0:
                raise ValueError("native original acquisition bounds must be exact positive integers")
        self._inputs = inputs
        self._active = require_active
        self._bundles = bundles
        self._read = read_file
        self._release_root = release_root
        self._metadata_bound = min(max_bundle_bytes, MAX_DBT_RELEASE_PACK_BYTES)
        self._bundle_bound = max_bundle_bytes
        self._documents = NativeProjectDocumentReader(read_file=read_file, max_policy_bytes=max_policy_bytes)
        self._roots: list[TemporaryDirectory[str]] = []
        self._closed = False

    def __enter__(self) -> Self:
        if self._closed:
            raise ValueError("native verifier is closed")
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        """Release only this verifier's owned extraction directories."""
        self._closed = True
        while self._roots:
            self._roots.pop().cleanup()

    def resolve(self, refs: NativeOriginalsRefV1) -> ResolvedNativeOriginals:
        """Verify originals and exact pinned ACTIVE occurrence without dispatch."""
        if self._closed or type(refs) is not NativeOriginalsRefV1:
            raise ValueError("native resolution requires exact references and an open verifier")
        refs.__post_init__()
        deployment_bytes = self._original(refs.projection_root, refs.deployment, self._metadata_bound)
        release_bytes = self._original(self._release_root, refs.release, self._metadata_bound)
        deployment = release_object(deployment_bytes, "native deployment")
        release = release_object(release_bytes, "native release")
        identity = self._identity
        if (
            deployment.get("deployment_id") != identity.deployment_id
            or deployment_id(deployment) != identity.deployment_id
            or deployment.get("release_ref") != identity.release_id
            or release.get("release_id") != identity.release_id
            or release_id(release) != identity.release_id
        ):
            raise ValueError("native original content identity differs from the pinned invocation")
        environment = release_text(deployment.get("environment"), "native environment")
        index = DbtReleaseArtifactIndex.from_release(release)
        matches = tuple((key, item) for key, item in index.workloads.items() if item["path"] == refs.pack.locator)
        if len(matches) != 1:
            raise ValueError("native selected pack is not an exact release workload member")
        workload_id, descriptor = matches[0]
        pack_bytes = self._original(self._release_root, refs.pack, self._metadata_bound)
        index.require_bytes(refs.pack.locator, pack_bytes)
        pack = parse_pack_json(pack_bytes)
        workload = pack.get("workload")
        if (
            verify_pack_fingerprint(pack) != descriptor["pack_fingerprint"]
            or not isinstance(workload, Mapping)
            or workload.get("workload_id") != workload_id
        ):
            raise ValueError("native workload identity differs from the selected release descriptor")
        sources = self._inputs.load_sources(projection_root=refs.projection_root, release_id=identity.release_id)
        if type(sources) is not DbtReleaseSources or sources.release_id != identity.release_id:
            raise ValueError("native source acquisition differs from the pinned release")
        DbtSourcePlan.from_release(release, sources.inventory, expected_release_id=identity.release_id)
        if sources.required_workloads != tuple((key, row["sha256"]) for key, row in sorted(index.workloads.items())):
            raise ValueError("native source workloads differ from the authenticated release inventory")
        owner = _owner(sources, workload_id, pack)
        authority = self._inputs.load_runtime_authority(
            projection_root=refs.projection_root,
            environment=environment,
            release_id=identity.release_id,
            deployment_id=identity.deployment_id,
        )
        if type(authority) is not DbtWorkspaceRuntimeAuthority:
            raise ValueError("native runtime authority requires its exact existing contract")
        authority.__post_init__()
        if (
            authority.environment,
            authority.release_id,
            authority.deployment_id,
            authority.release_sha256,
            authority.deployment_sha256,
        ) != (environment, identity.release_id, identity.deployment_id, refs.release.sha256, refs.deployment.sha256):
            raise ValueError("native runtime authority differs from the full original bytes")
        project = index.payloads[owner.source.runtime_payload_ids[0]]
        relative = release_text(project["path"], "native project archive")
        archive_size = project["bytes"]
        if (
            type(archive_size) is not int
            or project["sha256"] != owner.project.project_bundle_sha256
            or archive_size > self._bundle_bound
        ):
            raise ValueError("native project archive exceeds or differs from its selected source authority")
        archive = self._read(self._release_root, relative, max_bytes=self._bundle_bound)
        index.require_bytes(relative, archive)
        temporary = TemporaryDirectory(prefix="dpone-native-originals-")
        try:
            project_directory = Path(temporary.name) / "project"
            bundle = self._bundles.extract(archive, project_directory)
            if self._bundles.verify(archive, project_directory) != bundle or bundle.archive_sha256 != project["sha256"]:
                raise ValueError("native extracted project differs from the selected archive")
            policy, intent = self._documents.read(project_directory, bundle)
            if intent["workflow"] != owner.source.workflow_id:
                raise ValueError("native project intent belongs to a different workflow")
            active = self._active()
            _require_active_observation(active, identity, authority, sources)
            result = ResolvedNativeOriginals(
                authority,
                sources,
                bundle,
                policy,
                sha256_bytes(policy),
                project_directory,
            )
        except BaseException:
            temporary.cleanup()
            raise
        self._roots.append(temporary)
        return result

    def _original(self, root: Path, reference: OriginalRef, maximum: int) -> bytes:
        payload = self._read(root, reference.locator, max_bytes=maximum)
        if type(payload) is not bytes or len(payload) > maximum or sha256_bytes(payload) != reference.sha256:
            raise ValueError("native original differs from its bounded full-byte reference")
        return payload


def _owner(sources: DbtReleaseSources, workload_id: str, pack: Mapping[str, object]) -> DbtWorkflowReleaseSource:
    owners = tuple(item for item in sources.workflows if workload_id in item.dag.workload_ids)
    if len(owners) != 1:
        raise ValueError("native workload must have exactly one verified project/workflow owner")
    owner = owners[0]
    if workload_id == owner.source.workload_id:
        execution = dbt_execution_from_pack(
            pack,
            expected_runtime_payload_ids=owner.source.runtime_payload_ids,
            wire_contract=DBT_RUNTIME_WIRE_V2,
        )
        if execution != owner.execution:
            raise ValueError("native selected execution pack differs from the source snapshot")
    else:
        DbtSourcePlan.transfer_manifest_bytes(pack, workload_id)
        writes = tuple(
            item for item in sources.relation_writes if item.kind == "transfer" and item.resource_id == workload_id
        )
        if len(writes) != 1 or (writes[0].project_path, writes[0].workflow_id) != (
            owner.project.project_path,
            owner.source.workflow_id,
        ):
            raise ValueError("native transfer differs from its verified source owner")
    return owner


def _require_active_observation(
    active: DbtWorkspaceActiveActivation,
    identity: AirflowDeploymentIdentity,
    authority: DbtWorkspaceRuntimeAuthority,
    sources: DbtReleaseSources,
) -> None:
    if type(active) is not DbtWorkspaceActiveActivation:
        raise ValueError("native verifier requires exact ACTIVE readback")
    active.request.__post_init__()
    active.__post_init__()
    request = active.request
    if (
        request.activation_id,
        request.environment,
        request.release_id,
        request.deployment_id,
        request.source_inventory_sha256,
        request.runtime_context_sha256,
    ) != (
        identity.activation_id,
        authority.environment,
        identity.release_id,
        identity.deployment_id,
        sources.inventory.snapshot_sha256,
        authority.authority_subject_sha256,
    ):
        raise ValueError("native ACTIVE readback differs from pinned preflight observations")
