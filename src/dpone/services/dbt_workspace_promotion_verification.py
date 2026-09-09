"""Read-only, complete workspace mirror comparison against a pinned release."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.dbt_promotion import DbtProdMirrorError, validated_relative_path
from dpone.contracts.dbt_source_inventory import MAX_DBT_SOURCE_INVENTORY_BYTES, DbtSourceInventory
from dpone.contracts.dbt_workspace_promotion import (
    MAX_DBT_WORKSPACE_PROMOTION_BYTES,
    DbtWorkspaceProjectVerification,
    DbtWorkspacePromotionDescriptor,
    DbtWorkspacePromotionVerificationReport,
    validate_workspace_mirror_paths,
)
from dpone.services.dbt_prod_mirror_journal import readable_prod_mirror
from dpone.services.dbt_prod_mirror_paths import confined_mirror_destination
from dpone.services.dbt_prod_promotion_contract import validated_repository_root

if TYPE_CHECKING:
    from dpone.ports.dbt_release_files import ConfinedReleaseFileReader
    from dpone.services.dbt_workspace_mirror_source import DbtWorkspaceMirrorCandidate, DbtWorkspaceMirrorSourceLoader


class DbtWorkspacePromotionVerificationService:
    """Preserve every pinned row and fence readers against installing writers.

    verify() compares sources, not authority. verify_reviewed() additionally
    compares every descriptor field with independently established expected
    identities; its caller must still authenticate the release and DEV evidence.
    Neither entrypoint recovers a journal, installs sources or changes permissions.
    """

    def __init__(
        self,
        *,
        candidate_loader: DbtWorkspaceMirrorSourceLoader,
        read_file: ConfinedReleaseFileReader,
    ) -> None:
        self._candidates = candidate_loader
        self._read_file = read_file

    def verify(
        self,
        *,
        compiled_root: Path,
        repository_root: Path,
        descriptor_path: str,
        expected_release_id: str,
    ) -> DbtWorkspacePromotionVerificationReport:
        return self._verify(
            compiled_root=compiled_root,
            repository_root=repository_root,
            descriptor_path=descriptor_path,
            expected_release_id=expected_release_id,
            expected_descriptor=None,
        )

    def verify_reviewed(
        self,
        *,
        compiled_root: Path,
        repository_root: Path,
        descriptor_path: str,
        expected_release_id: str,
        expected_descriptor: DbtWorkspacePromotionDescriptor,
    ) -> DbtWorkspacePromotionVerificationReport:
        if not isinstance(expected_descriptor, DbtWorkspacePromotionDescriptor):
            raise DbtProdMirrorError("independently reviewed workspace promotion identity is required")
        return self._verify(
            compiled_root=compiled_root,
            repository_root=repository_root,
            descriptor_path=descriptor_path,
            expected_release_id=expected_release_id,
            expected_descriptor=expected_descriptor,
        )

    def _verify(
        self,
        *,
        compiled_root: Path,
        repository_root: Path,
        descriptor_path: str,
        expected_release_id: str,
        expected_descriptor: DbtWorkspacePromotionDescriptor | None,
    ) -> DbtWorkspacePromotionVerificationReport:
        root = validated_repository_root(repository_root)
        relative_descriptor = validated_relative_path(descriptor_path, "promotion descriptor path")
        candidate = self._candidates.read(compiled_root, expected_release_id=expected_release_id)
        with readable_prod_mirror(root):
            try:
                descriptor = DbtWorkspacePromotionDescriptor.from_payload(
                    self._read_file(root, relative_descriptor.as_posix(), max_bytes=MAX_DBT_WORKSPACE_PROMOTION_BYTES)
                )
                mirror, snapshot, _ = validate_workspace_mirror_paths(
                    descriptor.mirror_root, descriptor.source_snapshot_path, descriptor_path
                )
                mirror_path = confined_mirror_destination(root, mirror)
            except (OSError, ValueError):
                return _unavailable(candidate)
            metadata_passed = (
                descriptor.release_id == candidate.release_id
                and descriptor.source_snapshot_sha256 == candidate.inventory.snapshot_sha256
                and (expected_descriptor is None or descriptor == expected_descriptor)
            )
            try:
                mirrored_inventory = DbtSourceInventory.from_payload(
                    self._read_file(root, snapshot.as_posix(), max_bytes=MAX_DBT_SOURCE_INVENTORY_BYTES)
                )
                metadata_passed = metadata_passed and mirrored_inventory == candidate.inventory
            except (OSError, ValueError):
                metadata_passed = False
            projects, tree_passed = candidate.content.observe(mirror_path)
            return DbtWorkspacePromotionVerificationReport(
                candidate.release_id,
                candidate.inventory.snapshot_sha256,
                projects,
                metadata_passed,
                tree_passed,
            )


def _unavailable(candidate: DbtWorkspaceMirrorCandidate) -> DbtWorkspacePromotionVerificationReport:
    projects = tuple(
        DbtWorkspaceProjectVerification(project.project_path, project.project_bundle_sha256, None, False)
        for project in candidate.inventory.projects
    )
    return DbtWorkspacePromotionVerificationReport(
        candidate.release_id, candidate.inventory.snapshot_sha256, projects, False, False
    )


__all__ = ["DbtWorkspacePromotionVerificationService"]
