"""Prepare a complete workspace audit mirror without adopting author-owned paths."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_promotion import DbtProdMirrorError, DbtPromotionTrustDescriptor
from dpone.contracts.dbt_source_inventory import MAX_DBT_SOURCE_INVENTORY_BYTES, DbtSourceInventory
from dpone.contracts.dbt_workspace_promotion import (
    MAX_DBT_WORKSPACE_PROMOTION_BYTES,
    DbtWorkspacePromotionDescriptor,
    validate_workspace_mirror_paths,
)
from dpone.services.dbt_prod_promotion_contract import validated_repository_root

if TYPE_CHECKING:
    from dpone.ports.dbt_release_files import ConfinedReleaseFileReader
    from dpone.services.dbt_prod_mirror_transaction import DbtProdMirrorTransaction
    from dpone.services.dbt_workspace_mirror_source import DbtWorkspaceMirrorSourceLoader


@dataclass(frozen=True, slots=True)
class DbtWorkspaceMirrorPrepareReport:
    release_id: str
    promotion_id: str
    mirror_root: str
    source_snapshot_path: str
    descriptor_path: str
    projects: tuple[str, ...]
    no_op: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "dpone.dbt-workspace-mirror-prepare.v1",
            "release_id": self.release_id,
            "promotion_id": self.promotion_id,
            "mirror_root": self.mirror_root,
            "source_snapshot_path": self.source_snapshot_path,
            "descriptor_path": self.descriptor_path,
            "projects": list(self.projects),
            "no_op": self.no_op,
        }


class DbtWorkspaceMirrorService:
    """Verify all candidate sources, then install under one ownership-fenced lock.

    This prepares local review files. It neither verifies signatures nor activates
    a deployment. Independently verified DEV identities must still be checked by
    the cryptographic promotion gate before using the prepared descriptor.
    """

    def __init__(
        self,
        *,
        candidate_loader: DbtWorkspaceMirrorSourceLoader,
        transaction: DbtProdMirrorTransaction,
        read_file: ConfinedReleaseFileReader,
    ) -> None:
        self._candidates = candidate_loader
        self._transaction = transaction
        self._read_file = read_file

    def prepare(
        self,
        *,
        compiled_root: Path,
        repository_root: Path,
        mirror_root: str,
        source_snapshot_path: str,
        descriptor_path: str,
        expected_release_id: str,
        dev_deployment_id: str,
        dev_evidence_ref: str,
        trust: DbtPromotionTrustDescriptor,
    ) -> DbtWorkspaceMirrorPrepareReport:
        root = validated_repository_root(repository_root)
        mirror, snapshot, descriptor_path_value = validate_workspace_mirror_paths(
            mirror_root, source_snapshot_path, descriptor_path
        )
        try:
            sources = self._candidates.read(compiled_root, expected_release_id=expected_release_id)
            inventory = sources.inventory
            descriptor = DbtWorkspacePromotionDescriptor(
                release_id=sources.release_id,
                mirror_root=mirror_root,
                source_snapshot_path=source_snapshot_path,
                source_snapshot_sha256=inventory.snapshot_sha256,
                dev_deployment_id=dev_deployment_id,
                dev_evidence_ref=dev_evidence_ref,
                trust=trust,
            )
            installed = self._transaction.install_content(
                repository_root=root,
                mirror_path=mirror,
                content=sources.content,
                source_snapshot_path=snapshot,
                source_snapshot_bytes=_json(inventory.to_dict()),
                descriptor_path=descriptor_path_value,
                descriptor_bytes=_json(descriptor.to_dict()),
                ownership=DbtWorkspaceMirrorOwnership(read_file=self._read_file),
            )
        except DbtProdMirrorError:
            raise
        except (OSError, ValueError, DbtPublishingError) as exc:
            raise DbtProdMirrorError("complete workspace release cannot be prepared safely") from exc
        return DbtWorkspaceMirrorPrepareReport(
            sources.release_id,
            descriptor.promotion_id,
            mirror_root,
            source_snapshot_path,
            descriptor_path,
            tuple(project.project_path for project in inventory.projects),
            installed.no_op,
        )


class DbtWorkspaceMirrorOwnership:
    """Admit absent bootstrap or an established v3 bot-owned layout.

    The transaction validates paths and holds the exclusive lock before calling.
    Ownership is a local replacement boundary, not a proof of DEV authorization.
    An installer recovers a prior journal before this check; this class itself
    never recovers, deletes, changes permissions or writes repository files.
    """

    def __init__(self, *, read_file: ConfinedReleaseFileReader) -> None:
        self._read_file = read_file

    def require_owned_or_absent(self, *, repository_root: Path, mirror: Path, snapshot: Path, descriptor: Path) -> None:
        if not any(os.path.lexists(path) for path in (mirror, snapshot, descriptor)):
            return
        try:
            prior = DbtWorkspacePromotionDescriptor.from_payload(
                self._read_file(
                    repository_root,
                    descriptor.relative_to(repository_root).as_posix(),
                    max_bytes=MAX_DBT_WORKSPACE_PROMOTION_BYTES,
                )
            )
            inventory = DbtSourceInventory.from_payload(
                self._read_file(
                    repository_root,
                    snapshot.relative_to(repository_root).as_posix(),
                    max_bytes=MAX_DBT_SOURCE_INVENTORY_BYTES,
                )
            )
            if (
                prior.mirror_root != mirror.relative_to(repository_root).as_posix()
                or prior.source_snapshot_path != snapshot.relative_to(repository_root).as_posix()
                or prior.source_snapshot_sha256 != inventory.snapshot_sha256
            ):
                raise DbtProdMirrorError("prior mirror layout or source identity differs")
        except (OSError, ValueError) as exc:
            raise DbtProdMirrorError(
                "workspace mirror ownership is missing or invalid; automatic adoption is forbidden"
            ) from exc


def _json(value: object) -> bytes:
    return (json.dumps(value, allow_nan=False, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


__all__ = ["DbtWorkspaceMirrorPrepareReport", "DbtWorkspaceMirrorService"]
