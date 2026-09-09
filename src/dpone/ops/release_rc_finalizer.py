"""Release-candidate integration finalizer over merge-train and evidence artifacts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.ops.artifact_validation import EvidenceArtifactReader
from dpone.ops.release_rc_models import ReleaseRcFinalizerReport
from dpone.ops.release_rc_payloads import read_merge_train

DEFAULT_RELEASE_RC_OUTPUT_DIR = ".dpone/release-rc-finalize/latest"
DEFAULT_REQUIRED_RC_ARTIFACTS = ("route_release_finalizer", "release_evidence_pack")


class ReleaseRcFinalizerService:
    """Build the final release-candidate go/no-go receipt without live execution."""

    def __init__(
        self,
        *,
        artifact_reader: EvidenceArtifactReader | None = None,
        policy: Any | None = None,
    ) -> None:
        self._artifact_reader = artifact_reader or EvidenceArtifactReader()
        self._policy = policy or _default_policy()

    def finalize(
        self,
        *,
        output_dir: str | Path = DEFAULT_RELEASE_RC_OUTPUT_DIR,
        release: str,
        previous_release: str,
        package_version: str,
        merge_train_json: str | Path,
        artifacts: Mapping[str, str | Path],
        required_artifacts: Sequence[str] = DEFAULT_REQUIRED_RC_ARTIFACTS,
        mode: str = "pre_merge",
    ) -> ReleaseRcFinalizerReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        train = read_merge_train(merge_train_json)
        artifact_statuses = self._artifact_reader.read_many(
            artifacts=artifacts,
            required=tuple(required_artifacts),
        )
        decision = self._policy.evaluate(
            release=release,
            previous_release=previous_release,
            package_version=package_version,
            mode=mode,
            merge_train=train,
            artifacts=artifact_statuses,
        )
        report = ReleaseRcFinalizerReport(
            release=release,
            previous_release=previous_release,
            package_version=package_version,
            mode=mode,
            passed=decision.passed,
            level=decision.level,
            score=decision.score,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            merge_train=train,
            artifacts=artifact_statuses,
            checks=decision.checks,
            output_dir=str(directory),
            json_path=str(directory / "release_rc_finalizer.json"),
            markdown_path=str(directory / "release_rc_finalizer.md"),
        )
        report.write()
        return report


def _default_policy() -> Any:
    return import_module("dpone.ops.release_rc_policy").ReleaseRcFinalizerPolicy()


__all__ = ["DEFAULT_RELEASE_RC_OUTPUT_DIR", "ReleaseRcFinalizerService"]
