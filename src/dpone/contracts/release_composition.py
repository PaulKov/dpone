"""Public request/report values for verified immutable release composition.

These values carry caller intent and service outcomes. They grant neither
artifact trust nor activation authority; the composition service verifies all
source bytes before publishing. No filesystem access occurs on construction.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

COMPOSITION_SCHEMA = "dpone.release-set.v3"
COMPOSITION_PRODUCER = "dpone.release-composition.v1"
COMPOSITION_ADMISSION = "dpone.release-composition-admission.v1"
COMPOSITION_PROFILE = "compact_v2_runtime_connection_context"
MAX_COMPOSITION_FILES = 50_000
MAX_COMPOSITION_FILE_BYTES = 256 * 1024 * 1024
MAX_COMPOSITION_TOTAL_BYTES = 2 * 1024 * 1024 * 1024
MAX_COMPOSITION_METADATA_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class ReleaseCompositionRequest:
    """Compose two explicitly pinned local producer outputs into one destination.

    ``native_root`` must already contain the complete compact native workspace.
    ``expected_inventory_sha256`` pins the ordinary inventory producer's result.
    The service resolves confinement, validates the digest-pinned sidecar, and
    enforces source/output disjointness before acquiring input bytes.
    """

    native_root: Path
    expected_release_id: str
    standalone_root: Path
    expected_inventory_sha256: str
    output_dir: Path
    xcom_sidecar_image: str
    profile: str = COMPOSITION_PROFILE


@dataclass(frozen=True, slots=True)
class ReleaseCompositionReport:
    """Publication outcome, including a visible but not durably confirmed tree.

    ``durability_uncertain`` retains the visible release identity for recovery;
    it is never a passing result. Blockers are stable sanitized diagnostic codes,
    not exception messages or credentials. A report is not live certification.
    """

    status: Literal["passed", "rejected", "durability_uncertain"]
    release_id: str | None
    output_dir: Path
    source_release_id: str | None
    inventory_sha256: str | None
    blockers: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        """Only independently verified, durable publication counts as success."""
        return self.status == "passed"

    def to_dict(self) -> dict[str, Any]:
        """Return the one JSON-ready public report shared by CLI and Python."""
        return {
            "schema": "dpone.release-composition-report.v1",
            "passed": self.passed,
            "status": self.status,
            "release_id": self.release_id,
            "output_dir": str(self.output_dir),
            "source_release_id": self.source_release_id,
            "inventory_sha256": self.inventory_sha256,
            "blockers": list(self.blockers),
        }
