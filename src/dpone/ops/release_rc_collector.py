"""Release-candidate input collector for merge-train finalization."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol

from dpone.ops.release_rc_collector_models import ReleaseRcCollectorReport, ReleaseRcEvidenceRef
from dpone.ops.release_rc_payloads import merge_train_from_pull_requests, read_pull_requests

DEFAULT_RELEASE_RC_COLLECT_OUTPUT_DIR = ".dpone/release-rc-collect/latest"
DEFAULT_RELEASE_RC_FINALIZER_OUTPUT_DIR = ".dpone/release-rc-finalize/latest"
DEFAULT_REQUIRED_RC_ARTIFACTS = ("route_release_finalizer", "release_evidence_pack")


class _MergeTrainForCollection(Protocol):
    base_branch: str
    head_branch: str
    pull_requests: Sequence[Any]


class ReleaseRcCollectorService:
    """Collect PR exports and evidence refs into finalizer-ready artifacts."""

    def collect(
        self,
        *,
        output_dir: str | Path = DEFAULT_RELEASE_RC_COLLECT_OUTPUT_DIR,
        release: str,
        previous_release: str,
        package_version: str,
        base_branch: str,
        head_branch: str,
        pull_request_json: Sequence[str | Path],
        artifacts: Mapping[str, str | Path],
        required_artifacts: Sequence[str] = DEFAULT_REQUIRED_RC_ARTIFACTS,
        finalizer_output_dir: str | Path = DEFAULT_RELEASE_RC_FINALIZER_OUTPUT_DIR,
        mode: str = "pre_merge",
    ) -> ReleaseRcCollectorReport:
        directory = Path(output_dir)
        merge_train = merge_train_from_pull_requests(
            base_branch=base_branch,
            head_branch=head_branch,
            pull_requests=read_pull_requests(tuple(pull_request_json)),
        )
        evidence_refs = _evidence_refs(artifacts=artifacts, required_artifacts=required_artifacts)
        merge_train_path = directory / "merge_train.json"
        command = _finalizer_command(
            release=release,
            previous_release=previous_release,
            package_version=package_version,
            mode=mode,
            output_dir=finalizer_output_dir,
            merge_train_path=merge_train_path,
            evidence_refs=evidence_refs,
        )
        blockers = _collection_blockers(merge_train)
        report = ReleaseRcCollectorReport(
            release=release,
            previous_release=previous_release,
            package_version=package_version,
            mode=mode,
            passed=not blockers,
            blockers=blockers,
            warnings=tuple(),
            next_actions=_next_actions(blockers),
            merge_train=merge_train,
            evidence_refs=evidence_refs,
            finalizer_command=command,
            output_dir=str(directory),
            merge_train_path=str(merge_train_path),
            inputs_path=str(directory / "release_rc_inputs.json"),
            json_path=str(directory / "release_rc_collect.json"),
            markdown_path=str(directory / "release_rc_collect.md"),
        )
        report.write()
        return report


def _evidence_refs(
    *,
    artifacts: Mapping[str, str | Path],
    required_artifacts: Sequence[str],
) -> tuple[ReleaseRcEvidenceRef, ...]:
    required = {str(item) for item in required_artifacts}
    names = sorted({*artifacts.keys(), *required})
    return tuple(
        ReleaseRcEvidenceRef(name=name, path=str(artifacts.get(name, "")), required=name in required) for name in names
    )


def _finalizer_command(
    *,
    release: str,
    previous_release: str,
    package_version: str,
    mode: str,
    output_dir: str | Path,
    merge_train_path: Path,
    evidence_refs: Sequence[ReleaseRcEvidenceRef],
) -> tuple[str, ...]:
    command = [
        "uv",
        "run",
        "dpone",
        "ops",
        "release-rc-finalize",
        "--release",
        release,
        "--previous-release",
        previous_release,
        "--package-version",
        package_version,
        "--merge-train-json",
        str(merge_train_path),
        "--mode",
        mode,
        "--output-dir",
        str(output_dir),
    ]
    for item in evidence_refs:
        if item.path:
            command.extend(["--artifact", f"{item.name}={item.path}"])
    for item in evidence_refs:
        if item.required:
            command.extend(["--require-artifact", item.name])
    return tuple(command)


def _collection_blockers(merge_train: _MergeTrainForCollection) -> tuple[str, ...]:
    pull_requests = merge_train.pull_requests
    if not pull_requests:
        return ("release_rc_collect.pull_requests_missing",)
    if pull_requests[0].base_ref != merge_train.base_branch:
        return ("merge_train.base_mismatch",)
    if pull_requests[-1].head_ref != merge_train.head_branch:
        return ("merge_train.head_mismatch",)
    for left, right in zip(pull_requests, pull_requests[1:]):
        if left.head_ref != right.base_ref:
            return ("merge_train.chain_broken",)
    return tuple()


def _next_actions(blockers: Sequence[str]) -> tuple[str, ...]:
    actions: list[str] = []
    for blocker in blockers:
        if blocker == "release_rc_collect.pull_requests_missing":
            actions.append(
                "Export each stacked PR with `gh pr view --json ...` and pass every file in base-to-head order."
            )
        elif blocker.startswith("merge_train."):
            actions.append("Reorder or refresh PR JSON exports so the train runs from base branch to head branch.")
        else:
            actions.append(f"Resolve `{blocker}` before running `release-rc-finalize`.")
    return tuple(dict.fromkeys(actions))


__all__ = ["DEFAULT_RELEASE_RC_COLLECT_OUTPUT_DIR", "ReleaseRcCollectorService"]
