"""Pure policy for release-candidate integration finalization."""

from __future__ import annotations

import re
from collections.abc import Sequence

from dpone.ops.artifact_validation import EvidenceArtifactStatus
from dpone.ops.release_rc_models import (
    ReleaseRcFinalizerCheck,
    ReleaseRcFinalizerDecision,
    ReleaseRcMergeTrain,
    ReleaseRcPullRequest,
)

ALLOWED_PRE_MERGE_STATES = frozenset({"OPEN", "MERGED"})
ALLOWED_CHECK_CONCLUSIONS = frozenset({"SUCCESS", "SKIPPED", "NEUTRAL"})
COMPLETED_STATUS = "COMPLETED"
DEFAULT_REQUIRED_RC_ARTIFACTS = ("route_release_finalizer", "release_evidence_pack")
RC_MODES = ("pre_merge", "post_merge")

_VERSION_RE = re.compile(r"^v?(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)(?:[-+].*)?$")


class ReleaseRcFinalizerPolicy:
    """Evaluate release candidate readiness without filesystem or GitHub API logic."""

    def evaluate(
        self,
        *,
        release: str,
        previous_release: str,
        package_version: str,
        mode: str,
        merge_train: ReleaseRcMergeTrain,
        artifacts: Sequence[EvidenceArtifactStatus],
    ) -> ReleaseRcFinalizerDecision:
        checks = [
            _version_increment_check(release=release, previous_release=previous_release),
            _package_version_check(release=release, package_version=package_version),
            _merge_train_chain_check(merge_train),
            _pull_request_state_check(mode=mode, pull_requests=merge_train.pull_requests),
            _pull_request_checks_check(merge_train.pull_requests),
            _release_artifacts_check(artifacts),
        ]
        blockers = tuple(
            dict.fromkeys(
                (
                    *(check.blocker for check in checks if check.blocker),
                    *(artifact.blocker for artifact in artifacts if artifact.required and artifact.blocker),
                )
            )
        )
        warnings = tuple(
            f"{artifact.name}.optional_missing"
            for artifact in artifacts
            if not artifact.required and not artifact.exists
        )
        return ReleaseRcFinalizerDecision(
            passed=not blockers,
            level="rc_ready" if not blockers else "blocked",
            score=_score(checks=tuple(checks), artifacts=tuple(artifacts)),
            blockers=blockers,
            warnings=warnings,
            next_actions=_next_actions(blockers),
            checks=tuple(checks),
        )


def _version_increment_check(*, release: str, previous_release: str) -> ReleaseRcFinalizerCheck:
    current = _parse_version(release)
    previous = _parse_version(previous_release)
    passed = current is not None and previous is not None and current > previous
    summary = f"release={release}; previous_release={previous_release}"
    return _check(
        name="version_increment",
        domain="release",
        passed=passed,
        summary=summary,
        blocker="release.version_not_incremented",
    )


def _package_version_check(*, release: str, package_version: str) -> ReleaseRcFinalizerCheck:
    release_version = _parse_version(release)
    package = _parse_version(package_version)
    passed = release_version is not None and package is not None and release_version == package
    return _check(
        name="package_version",
        domain="release",
        passed=passed,
        summary=f"package_version={package_version}; release={release}",
        blocker="release.package_version_mismatch",
    )


def _merge_train_chain_check(merge_train: ReleaseRcMergeTrain) -> ReleaseRcFinalizerCheck:
    blockers = _merge_train_chain_blockers(merge_train)
    return _check(
        name="merge_train_chain",
        domain="merge_train",
        passed=not blockers,
        summary=f"pull_requests={len(merge_train.pull_requests)}",
        blocker=blockers[0] if blockers else "",
    )


def _merge_train_chain_blockers(merge_train: ReleaseRcMergeTrain) -> tuple[str, ...]:
    pull_requests = merge_train.pull_requests
    if not pull_requests:
        return ("merge_train.empty",)
    if pull_requests[0].base_ref != merge_train.base_branch:
        return ("merge_train.base_mismatch",)
    if pull_requests[-1].head_ref != merge_train.head_branch:
        return ("merge_train.head_mismatch",)
    for left, right in zip(pull_requests, pull_requests[1:]):
        if left.head_ref != right.base_ref:
            return ("merge_train.chain_broken",)
    return tuple()


def _pull_request_state_check(
    *,
    mode: str,
    pull_requests: Sequence[ReleaseRcPullRequest],
) -> ReleaseRcFinalizerCheck:
    blockers = _pull_request_state_blockers(mode=mode, pull_requests=pull_requests)
    return _check(
        name="pull_request_state",
        domain="merge_train",
        passed=not blockers,
        summary=f"mode={mode}; pull_requests={len(pull_requests)}",
        blocker=blockers[0] if blockers else "",
    )


def _pull_request_state_blockers(
    *,
    mode: str,
    pull_requests: Sequence[ReleaseRcPullRequest],
) -> tuple[str, ...]:
    blockers: list[str] = []
    if mode not in RC_MODES:
        blockers.append(f"release.mode_invalid:{mode}")
    for item in pull_requests:
        state = item.state.upper()
        merge_state = item.merge_state.upper()
        if item.is_draft:
            blockers.append(f"pull_request.{item.number}.is_draft")
        if mode == "post_merge":
            if state != "MERGED":
                blockers.append(f"pull_request.{item.number}.not_merged")
            continue
        if state not in ALLOWED_PRE_MERGE_STATES:
            blockers.append(f"pull_request.{item.number}.state_invalid:{state.lower()}")
        if state == "OPEN" and merge_state != "CLEAN":
            blockers.append(f"pull_request.{item.number}.not_clean")
    return tuple(blockers)


def _pull_request_checks_check(pull_requests: Sequence[ReleaseRcPullRequest]) -> ReleaseRcFinalizerCheck:
    blockers = _pull_request_check_blockers(pull_requests)
    return _check(
        name="pull_request_checks",
        domain="merge_train",
        passed=not blockers,
        summary=f"pull_requests={len(pull_requests)}",
        blocker=blockers[0] if blockers else "",
    )


def _pull_request_check_blockers(pull_requests: Sequence[ReleaseRcPullRequest]) -> tuple[str, ...]:
    blockers: list[str] = []
    for item in pull_requests:
        if not item.checks:
            blockers.append(f"pull_request.{item.number}.checks_missing")
            continue
        for check in item.checks:
            status = check.status.upper()
            conclusion = check.conclusion.upper()
            if status != COMPLETED_STATUS:
                blockers.append(f"pull_request.{item.number}.check_pending:{check.name}")
            elif conclusion not in ALLOWED_CHECK_CONCLUSIONS:
                blockers.append(f"pull_request.{item.number}.check_failed:{check.name}")
    return tuple(blockers)


def _release_artifacts_check(artifacts: Sequence[EvidenceArtifactStatus]) -> ReleaseRcFinalizerCheck:
    required = tuple(item for item in artifacts if item.required)
    passed = bool(required) and all(item.passed for item in required)
    return _check(
        name="release_artifacts",
        domain="evidence",
        passed=passed,
        summary=f"required={len(required)}",
        blocker="release_artifacts.not_passed",
    )


def _check(
    *,
    name: str,
    domain: str,
    passed: bool,
    summary: str,
    blocker: str,
) -> ReleaseRcFinalizerCheck:
    return ReleaseRcFinalizerCheck(
        name=name,
        domain=domain,
        passed=passed,
        required=True,
        summary=summary,
        blocker="" if passed else blocker,
    )


def _parse_version(value: str) -> tuple[int, int, int] | None:
    match = _VERSION_RE.match(value.strip())
    if not match:
        return None
    return (int(match["major"]), int(match["minor"]), int(match["patch"]))


def _score(
    *,
    checks: tuple[ReleaseRcFinalizerCheck, ...],
    artifacts: tuple[EvidenceArtifactStatus, ...],
) -> float:
    units = [check.passed for check in checks if check.required]
    units.extend(artifact.passed for artifact in artifacts if artifact.required)
    if not units:
        return 100.0
    return round((sum(1 for item in units if item) / len(units)) * 100.0, 2)


def _next_actions(blockers: tuple[str, ...]) -> tuple[str, ...]:
    actions: list[str] = []
    for blocker in blockers:
        if blocker == "release.version_not_incremented":
            actions.append("Choose a release version greater than the previous published release.")
        elif blocker == "release.package_version_mismatch":
            actions.append("Update the package version to match the release tag before final RC review.")
        elif blocker.startswith("merge_train."):
            actions.append("Regenerate `merge_train.json` in base-to-head PR order and rerun the finalizer.")
        elif blocker.startswith("pull_request."):
            actions.append("Fix the blocked pull request, rerun GitHub checks, and regenerate `merge_train.json`.")
        elif blocker.endswith(".missing"):
            artifact = blocker.removesuffix(".missing")
            actions.append(f"Generate or attach required release artifact `{artifact}`.")
        elif blocker.endswith(".not_passed"):
            artifact = blocker.removesuffix(".not_passed")
            actions.append(f"Open `{artifact}` and fix the originating red gate before release review.")
        elif blocker == "release_artifacts.not_passed":
            actions.append("Attach all required release evidence artifacts and ensure each required artifact is green.")
        else:
            actions.append(f"Resolve `{blocker}` before creating the release tag.")
    return tuple(dict.fromkeys(actions))


__all__ = [
    "ALLOWED_CHECK_CONCLUSIONS",
    "DEFAULT_REQUIRED_RC_ARTIFACTS",
    "RC_MODES",
    "ReleaseRcFinalizerPolicy",
]
