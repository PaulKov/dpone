"""Confined acquisition for complete immutable workspace source verification."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType
from typing import TYPE_CHECKING

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_source_inventory import MAX_DBT_SOURCE_INVENTORY_BYTES, DbtSourceInventory
from dpone.contracts.dbt_source_inventory_binding import (
    DbtReleaseSources as DbtReleaseSources,
)
from dpone.contracts.dbt_source_inventory_binding import (
    DbtSourcePlan,
)
from dpone.contracts.dbt_source_inventory_binding import (
    DbtWorkflowReleaseSource as DbtWorkflowReleaseSource,
)
from dpone.manifest.bounded_yaml import load_bounded_yaml
from dpone.manifest.dbt_workspace_release_tree import verify_canonical_dbt_workspace_tree
from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService
from dpone.services.dbt_release_workflow_reader import (
    DbtDevEvidenceReleaseError,
    read_dbt_workflow_dags,
    read_dbt_workload_pack,
    release_object,
    release_text,
)

if TYPE_CHECKING:
    from dpone.contracts.dbt_runtime_release_binding import DbtReleaseArtifactIndex
    from dpone.ports.dbt_project_bundle import DbtProjectBundleOperations
    from dpone.ports.dbt_release_files import ConfinedReleaseFileReader

_MAX_RELEASE_BYTES = 8 * 1024 * 1024
_MAX_PROJECT_CONFIG_BYTES = 1024 * 1024


class DbtReleaseSourceReader:
    """Acquire bytes once per semantic observation, through injected capabilities.

    The pure source plan owns metadata, membership and graph/write policy. This
    service retains no-follow file acquisition, framework fingerprint validation,
    bounded YAML decoding and verified bundle extraction. One project's source
    bytes are discarded before acquiring the next; only compact observations
    survive. No dbt command, SQL, signing or caller-tree mutation occurs here.
    """

    def __init__(
        self,
        *,
        bundle_operations: DbtProjectBundleOperations,
        read_file: ConfinedReleaseFileReader,
    ) -> None:
        self._bundles = bundle_operations
        self._read_file = read_file

    def read(self, compiled_root: Path, *, expected_release_id: str) -> DbtReleaseSources:
        try:
            return self._read(Path(compiled_root).absolute(), expected_release_id)
        except DbtDevEvidenceReleaseError:
            raise
        except (OSError, ValueError, RecursionError, DbtPublishingError) as exc:
            raise DbtDevEvidenceReleaseError("dbt release source tree is invalid or incomplete") from exc

    def _read(self, root: Path, expected_release_id: str) -> DbtReleaseSources:
        release = release_object(self._read_file(root, "release-set.json", max_bytes=_MAX_RELEASE_BYTES), "release-set")
        inventory = DbtSourceInventory.from_payload(
            self._read_file(root, "_dbt/dbt-source-snapshot.json", max_bytes=MAX_DBT_SOURCE_INVENTORY_BYTES)
        )
        plan = DbtSourcePlan.from_release(release, inventory, expected_release_id=expected_release_id)
        index = plan.artifacts
        verify_canonical_dbt_workspace_tree(root, index, read_file=self._read_file)
        dags = read_dbt_workflow_dags(root, index.dags, index.workloads, read_file=self._read_file)
        plan.require_dags(dags)
        observations = []
        for project in inventory.projects:
            first = project.workflows[0]
            archive = self._payload(root, index, first.runtime_payload_ids[0])
            manifest = release_object(self._payload(root, index, first.runtime_payload_ids[1]), "manifest")
            project_name = self._project_name(archive)
            executions = {}
            for source in project.workflows:
                selection = plan.decode_selection(self._payload(root, index, source.runtime_payload_ids[2]))
                pack = read_dbt_workload_pack(
                    root,
                    index.workloads[source.workload_id],
                    source.workload_id,
                    read_file=self._read_file,
                )
                executions[source.workflow_id] = (plan.execution_from_pack(source, pack), selection)
                del pack
            observations.append(
                plan.observe_project(
                    project,
                    manifest=manifest,
                    bundled_project_name=project_name,
                    executions=executions,
                    dags=dags,
                )
            )
            del archive, manifest
        owners = {
            workload_id: (item.project.project_path, item.source.workflow_id)
            for project in observations
            for item in project.workflows
            for workload_id in item.dag.workload_ids
        }
        transfers = []
        for workload_id, descriptor in index.workloads.items():
            if not workload_id.startswith("dbt__"):
                pack = read_dbt_workload_pack(root, descriptor, workload_id, read_file=self._read_file)
                manifest = self._transfer_manifest(pack, workload_id)
                transfers.append(plan.observe_transfer(workload_id, manifest, owner=owners[workload_id]))
        return plan.finish(tuple(observations), transfer_writes=tuple(transfers))

    def capture_verified_files(
        self, root: Path, *, release_payload: bytes, expected_release_id: str
    ) -> Mapping[str, bytes]:
        """Capture a bounded installation snapshot, distinct from streaming read.

        Exact authorized metadata is passed by the caller because provenance is
        not bound by release_id alone. Every published byte comes from this map;
        the complete source reader and checksum verifier inspect its private
        stage, not a later version of the mutable input. Cleanup precedes return.
        No subject is generated or repaired, and no cache state is written here.
        """

        if len(release_payload) > _MAX_RELEASE_BYTES:
            raise ValueError("workspace release metadata exceeds its bound")
        release = release_object(release_payload, "release-set")
        source = self._read_file(root, "_dbt/dbt-source-snapshot.json", max_bytes=MAX_DBT_SOURCE_INVENTORY_BYTES)
        plan = DbtSourcePlan.from_release(
            release, DbtSourceInventory.from_payload(source), expected_release_id=expected_release_id
        )
        index = plan.artifacts
        sizes = [len(release_payload), len(source)]
        for row in index.by_path.values():
            size = row["bytes"]
            assert isinstance(size, int)
            sizes.append(size)
        integrity = DbtReleaseIntegrityService()
        integrity.require_capture_budget(sizes)
        files = {
            "release-set.json": release_payload,
            "_dbt/dbt-source-snapshot.json": source,
            "release-subjects.sha256": self._read_file(root, "release-subjects.sha256", max_bytes=8 * 1024 * 1024),
        }
        verify_canonical_dbt_workspace_tree(root, index, read_file=self._read_file)
        for path, row in index.by_path.items():
            size = row["bytes"]
            assert isinstance(size, int)
            body = self._read_file(root, path, max_bytes=size)
            index.require_bytes(path, body)
            files[path] = body
        with TemporaryDirectory(prefix="dpone-dbt-cache-verification-") as temporary:
            stage = Path(temporary)
            for path, body in files.items():
                destination = stage / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(body)
            self.read(stage, expected_release_id=expected_release_id)
            integrity.verify(stage)
        return MappingProxyType(files)

    def _payload(self, root: Path, index: DbtReleaseArtifactIndex, payload_id: str) -> bytes:
        descriptor = index.payloads[payload_id]
        path = release_text(descriptor["path"], "payload path")
        size = descriptor["bytes"]
        assert isinstance(size, int)  # Index enforces the narrower per-kind bounds.
        body = self._read_file(root, path, max_bytes=size)
        index.require_bytes(path, body)
        return body

    def _project_name(self, archive: bytes) -> object:
        with TemporaryDirectory(prefix="dpone-dbt-source-verify-") as temporary:
            destination = Path(temporary) / "project"
            self._bundles.extract(archive, destination)
            self._bundles.verify(archive, destination)
            config = load_bounded_yaml(
                self._read_file(destination, "dbt_project.yml", max_bytes=_MAX_PROJECT_CONFIG_BYTES)
            )
            return config.get("name") if isinstance(config, Mapping) else None

    @staticmethod
    def _transfer_manifest(pack: Mapping[str, object], workload_id: str) -> Mapping[str, object]:
        manifest = load_bounded_yaml(DbtSourcePlan.transfer_manifest_bytes(pack, workload_id))
        if not isinstance(manifest, Mapping):
            raise DbtDevEvidenceReleaseError("transfer manifest must be an object")
        return manifest


__all__ = ["DbtReleaseSourceReader", "DbtReleaseSources", "DbtWorkflowReleaseSource"]
