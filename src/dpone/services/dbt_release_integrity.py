"""Deterministic byte inventory for one compiled dbt release tree."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from dpone.services.bounded_tree_integrity import (
    BoundedTreeIntegrityError,
    BoundedTreeIntegrityPolicy,
    BoundedTreeIntegrityService,
)

DBT_RELEASE_SUBJECTS_FILENAME = "release-subjects.sha256"
_POLICY = BoundedTreeIntegrityPolicy(
    subject_filename=DBT_RELEASE_SUBJECTS_FILENAME,
    schema_header="# dpone.dbt-release-subjects.v1",
    required_paths=("release-set.json",),
    max_files=50_000,
    max_file_bytes=256 * 1024 * 1024,
    max_inventory_bytes=8 * 1024 * 1024,
    max_total_bytes=2 * 1024 * 1024 * 1024,
)


class DbtReleaseIntegrityError(ValueError):
    """A compiled release cannot be represented or verified safely."""

    code = "DPONE_DBT_RELEASE_INTEGRITY_INVALID"


@dataclass(frozen=True, slots=True)
class DbtReleaseIntegrityReport:
    """Safe CI projection for one complete release-tree verification."""

    subject_path: str
    subject_sha256: str
    file_count: int
    total_bytes: int
    no_op: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "dpone.dbt-release-integrity.v1",
            "passed": True,
            "subject_path": self.subject_path,
            "subject_sha256": self.subject_sha256,
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "no_op": self.no_op,
        }


class DbtReleaseIntegrityService:
    """Create and verify the exact checksum subject attested by CI."""

    def __init__(self) -> None:
        self._integrity = BoundedTreeIntegrityService(_POLICY)

    def write(self, compiled_root: Path) -> DbtReleaseIntegrityReport:
        return self._execute(compiled_root, verify=False)

    def verify(self, compiled_root: Path) -> DbtReleaseIntegrityReport:
        return self._execute(compiled_root, verify=True)

    @staticmethod
    def require_capture_budget(sizes: Iterable[int]) -> None:
        """Apply the same subject limits before retaining files in memory.

        Include release/source metadata but exclude the checksum subject itself.
        File lengths must come from validated descriptors or already bounded
        metadata; actual bytes and integrity are still checked independently.
        """

        count = total = 0
        for size in sizes:
            if type(size) is not int or size < 0 or size > _POLICY.max_file_bytes:
                raise DbtReleaseIntegrityError("workspace capture file size exceeds integrity budget")
            count += 1
            total += size
            if count > _POLICY.max_files or total > _POLICY.max_total_bytes:
                raise DbtReleaseIntegrityError("workspace capture exceeds integrity budget")

    def _execute(
        self,
        compiled_root: Path,
        *,
        verify: bool,
    ) -> DbtReleaseIntegrityReport:
        try:
            report = self._integrity.verify(compiled_root) if verify else self._integrity.write(compiled_root)
        except BoundedTreeIntegrityError as exc:
            raise DbtReleaseIntegrityError(str(exc)) from exc
        return DbtReleaseIntegrityReport(
            subject_path=report.subject_path,
            subject_sha256=report.subject_sha256,
            file_count=report.file_count,
            total_bytes=report.total_bytes,
            no_op=report.no_op,
        )


__all__ = [
    "DBT_RELEASE_SUBJECTS_FILENAME",
    "DbtReleaseIntegrityError",
    "DbtReleaseIntegrityReport",
    "DbtReleaseIntegrityService",
]
