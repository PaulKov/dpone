"""Transform a complete verified workspace into a strict compact release tree."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

from dpone.contracts.dbt_compact_release import CompactWorkspaceReleasePlan
from dpone.gitops.release_set_validation import validate_release_set

if TYPE_CHECKING:
    from dpone.ports.dbt_compact_release import CompactWorkspaceRewriter, ReleaseIntegrityWriter
    from dpone.ports.dbt_release_files import ConfinedReleaseFileReader, VerifiedWorkspaceReleaseCapture


class CompactWorkspaceReleaseBuilder:
    """Validate a frozen input, rewrite transport, then independently recapture it.

    All I/O and framework policy are injected at composition. Neither validation
    pass is optional. Publication belongs to the caller and starts only after the
    private stage has been verified and cleaned up.
    """

    def __init__(
        self,
        *,
        read_file: ConfinedReleaseFileReader,
        capture: VerifiedWorkspaceReleaseCapture,
        integrity: ReleaseIntegrityWriter,
        rewriter: CompactWorkspaceRewriter,
        promotion: Mapping[str, str],
    ) -> None:
        self._read = read_file
        self._capture = capture
        self._integrity = integrity
        self._rewriter = rewriter
        self._promotion = dict(promotion)

    def build(
        self, root: Path, *, xcom_sidecar_image: str, dag_ids: Sequence[str] | None = None
    ) -> Mapping[str, bytes]:
        """Return a complete immutable-publication input or fail without output."""
        payload = self._read(root, "release-set.json", max_bytes=8 * 1024 * 1024)
        plan = CompactWorkspaceReleasePlan(payload, dag_ids=dag_ids)
        if validate_release_set(plan.release).failure is not None:
            raise ValueError("workspace release violates its public contract")
        captured = self._capture.capture_verified_files(
            root, release_payload=payload, expected_release_id=plan.expected_release_id
        )
        derived, files = plan.derive(
            captured,
            rewrite_dag=lambda dag: self._rewriter.dag(
                dag, workload_ids=tuple(node["workload_id"] for node in dag["nodes"] if "workload_id" in node)
            ),
            rewrite_pack=partial(self._rewriter.pack, xcom_sidecar_image=xcom_sidecar_image),
            promotion=self._promotion,
        )
        if validate_release_set(derived).failure is not None:
            raise ValueError("derived workspace release violates its public contract")
        with TemporaryDirectory(prefix="dpone-compact-workspace-") as temporary:
            stage = Path(temporary)
            for path, body in files.items():
                destination = stage / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(body)
            self._integrity.write(stage)
            verified = self._capture.capture_verified_files(
                stage, release_payload=files["release-set.json"], expected_release_id=derived["release_id"]
            )
        return verified
