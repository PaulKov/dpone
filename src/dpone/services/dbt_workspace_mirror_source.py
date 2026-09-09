"""Freeze one completely verified candidate for audit preparation or comparison."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_promotion import DbtProdMirrorError
from dpone.contracts.dbt_runtime_payloads import (
    DBT_RUNTIME_WIRE_V2,
    MAX_DBT_RUNTIME_PAYLOAD_BYTES,
    dbt_runtime_payload_reference,
)
from dpone.services.dbt_prod_mirror_content import DbtWorkspaceMirrorContent

if TYPE_CHECKING:
    from dpone.contracts.dbt_source_inventory import DbtSourceInventory
    from dpone.ports.dbt_project_bundle import DbtProjectBundleOperations
    from dpone.ports.dbt_release_files import ConfinedReleaseFileReader
    from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader


@dataclass(frozen=True, slots=True)
class DbtWorkspaceMirrorCandidate:
    """Pinned complete sources; not evidence of a valid signature or deployment."""

    release_id: str
    inventory: DbtSourceInventory
    content: DbtWorkspaceMirrorContent


class DbtWorkspaceMirrorSourceLoader:
    """Shared full source/byte verification for prepare and read-only promotion."""

    def __init__(
        self,
        *,
        bundle_operations: DbtProjectBundleOperations,
        source_reader: DbtReleaseSourceReader,
        verify_integrity: Callable[[Path], object],
        read_file: ConfinedReleaseFileReader,
    ) -> None:
        self._bundles = bundle_operations
        self._sources = source_reader
        self._verify_integrity = verify_integrity
        self._read_file = read_file

    def read(self, compiled_root: Path, *, expected_release_id: str) -> DbtWorkspaceMirrorCandidate:
        try:
            self._verify_integrity(compiled_root)
            sources = self._sources.read(compiled_root, expected_release_id=expected_release_id)
            archives = {
                project.project_path: self._read_file(
                    compiled_root,
                    dbt_runtime_payload_reference(
                        project.workflows[0].runtime_payload_ids[0], wire_contract=DBT_RUNTIME_WIRE_V2
                    ).path,
                    max_bytes=MAX_DBT_RUNTIME_PAYLOAD_BYTES,
                )
                for project in sources.inventory.projects
            }
            content = DbtWorkspaceMirrorContent(
                inventory=sources.inventory, project_bundles=archives, bundle_operations=self._bundles
            )
        except DbtProdMirrorError:
            raise
        except (OSError, ValueError, DbtPublishingError) as exc:
            raise DbtProdMirrorError("complete workspace release source is invalid") from exc
        return DbtWorkspaceMirrorCandidate(sources.release_id, sources.inventory, content)


__all__ = ["DbtWorkspaceMirrorCandidate", "DbtWorkspaceMirrorSourceLoader"]
