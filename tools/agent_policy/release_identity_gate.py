"""Validate the immutable source and package identity of one dpone release."""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

CANONICAL_TAG = re.compile(r"^v(?P<version>(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*))$")
FULL_SHA = re.compile(r"^[0-9a-fA-F]{40}$")
SAFE_GIT_REF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")


def _load_sibling(module_name: str, filename: str) -> Any:
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"unable to load {filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


packages = _load_sibling("dpone_agent_release_identity_packages", "release_identity_packages.py")
PACKAGE_FILES = packages.PACKAGE_FILES


@dataclass(frozen=True)
class CommandResult:
    """Bounded result from one local Git command."""

    returncode: int
    stdout: str


@dataclass(frozen=True)
class ReleaseIdentityBlocker:
    """Stable, credential-free release identity blocker."""

    code: str
    message: str
    package: str | None = None


@dataclass(frozen=True)
class ReleaseIdentityReport:
    """Deterministic release preflight report."""

    status: str
    tag: str | None
    version: str | None
    commit_sha: str | None
    remote_ref: str | None
    package_versions: dict[str, str]
    blockers: tuple[ReleaseIdentityBlocker, ...]
    protected_base_sha: str | None = None
    policy_sha256: str | None = None

    @property
    def decision(self) -> str:
        return "GO" if self.status == "PASS" else "NO-GO"

    def to_payload(self) -> dict[str, Any]:
        """Return the stable JSON evidence payload."""

        return {
            "blockers": [asdict(item) for item in self.blockers],
            "commit_sha": self.commit_sha,
            "decision": self.decision,
            "package_versions": dict(sorted(self.package_versions.items())),
            "policy_sha256": self.policy_sha256,
            "protected_base_sha": self.protected_base_sha,
            "remote_ref": self.remote_ref,
            "schema_version": 1,
            "status": self.status,
            "tag": self.tag,
            "version": self.version,
        }


def _run_command(command: tuple[str, ...], cwd: Path) -> CommandResult:
    """Run one argument-safe local command without inheriting output."""

    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="strict",
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError, UnicodeError):
        return CommandResult(returncode=127, stdout="")
    return CommandResult(returncode=completed.returncode, stdout=completed.stdout)


def _read_commit_text(root: Path, commit_sha: str, relative: Path) -> str | None:
    """Read one UTF-8 source blob from the frozen commit, never the worktree."""

    result = _run_command(("git", "show", f"{commit_sha}:{relative.as_posix()}"), root)
    return result.stdout if result.returncode == 0 else None


def _resolve_protected_base_sha(root: Path, remote_ref: str) -> str | None:
    resolved = _run_command(("git", "rev-parse", "--verify", f"{remote_ref}^{{commit}}"), root)
    digest = resolved.stdout.strip().lower()
    return digest if resolved.returncode == 0 and FULL_SHA.fullmatch(digest) else None


def _git_identity_blockers(
    root: Path,
    *,
    tag: str,
    commit_sha: str,
    remote_ref: str,
) -> tuple[list[ReleaseIdentityBlocker], str | None]:
    blockers: list[ReleaseIdentityBlocker] = []
    tag_ref = f"refs/tags/{tag}"
    tag_type = _run_command(("git", "cat-file", "-t", tag_ref), root)
    if tag_type.returncode != 0:
        blockers.append(
            ReleaseIdentityBlocker("RELEASE_TAG_NOT_FOUND", "The canonical release tag is unavailable locally.")
        )
    elif tag_type.stdout.strip() != "tag":
        blockers.append(
            ReleaseIdentityBlocker(
                "RELEASE_TAG_NOT_ANNOTATED",
                "The release tag must be an annotated Git tag object.",
            )
        )

    tag_commit = _run_command(("git", "rev-list", "-n", "1", tag_ref), root)
    observed_commit = tag_commit.stdout.strip().lower()
    if tag_commit.returncode != 0 or FULL_SHA.fullmatch(observed_commit) is None:
        blockers.append(
            ReleaseIdentityBlocker(
                "RELEASE_TAG_TARGET_UNAVAILABLE",
                "The release tag target commit cannot be resolved.",
            )
        )
    elif observed_commit != commit_sha:
        blockers.append(
            ReleaseIdentityBlocker(
                "RELEASE_TAG_COMMIT_MISMATCH",
                "The release tag does not resolve to the workflow commit.",
            )
        )

    protected_base_sha = _resolve_protected_base_sha(root, remote_ref)
    if protected_base_sha is None:
        blockers.append(
            ReleaseIdentityBlocker(
                "RELEASE_BASE_SHA_UNAVAILABLE",
                "The protected base ref cannot be resolved to a full commit SHA.",
            )
        )

    ancestry = _run_command(("git", "merge-base", "--is-ancestor", commit_sha, remote_ref), root)
    if ancestry.returncode == 1:
        blockers.append(
            ReleaseIdentityBlocker(
                "RELEASE_COMMIT_NOT_ON_BASE",
                "The release commit is not reachable from the protected base branch.",
            )
        )
    elif ancestry.returncode != 0:
        blockers.append(
            ReleaseIdentityBlocker(
                "RELEASE_BASE_EVIDENCE_UNAVAILABLE",
                "The protected base branch ancestry cannot be verified.",
            )
        )
    return blockers, protected_base_sha


def _load_frozen_protected_base(
    root: Path, commit_sha: str
) -> tuple[str | None, str | None, ReleaseIdentityBlocker | None]:
    """Return protected remote ref and policy digest via mocked-friendly git show."""

    import hashlib

    frozen = _load_sibling("dpone_agent_release_frozen_policy", "release_frozen_policy.py")
    source = _read_commit_text(root, commit_sha, frozen.DEFAULT_POLICY)
    if source is None:
        return None, None, ReleaseIdentityBlocker("RELEASE_POLICY_UNAVAILABLE", "Frozen policy unavailable in commit.")
    try:
        policy = frozen.parse_policy_requirements(source)
    except (TypeError, ValueError):
        return None, None, ReleaseIdentityBlocker("RELEASE_POLICY_INVALID", "Frozen policy is invalid.")
    return policy.protected_remote_ref, hashlib.sha256(source.encode("utf-8")).hexdigest(), None


def evaluate_release_identity(
    *,
    root: Path,
    tag: str,
    commit_sha: str,
    remote_ref: str | None = None,
) -> ReleaseIdentityReport:
    """Evaluate one release identity without network or secret access."""

    blockers: list[ReleaseIdentityBlocker] = []
    tag_match = CANONICAL_TAG.fullmatch(tag)
    version = tag_match.group("version") if tag_match else None
    safe_tag = tag if tag_match else None
    if tag_match is None:
        blockers.append(
            ReleaseIdentityBlocker(
                "RELEASE_TAG_INVALID",
                "Release tag must use canonical stable SemVer form vX.Y.Z.",
            )
        )

    normalized_commit = commit_sha.lower() if FULL_SHA.fullmatch(commit_sha) else None
    if normalized_commit is None:
        blockers.append(
            ReleaseIdentityBlocker(
                "RELEASE_COMMIT_SHA_INVALID",
                "Release commit must be a full 40-character hexadecimal SHA.",
            )
        )

    policy_sha256: str | None = None
    policy_remote_ref: str | None = None
    if normalized_commit is not None:
        policy_remote_ref, policy_sha256, policy_blocker = _load_frozen_protected_base(root, normalized_commit)
        if policy_blocker is not None:
            blockers.append(policy_blocker)

    if remote_ref is not None and remote_ref != policy_remote_ref:
        blockers.append(
            ReleaseIdentityBlocker(
                "RELEASE_BASE_REF_POLICY_MISMATCH",
                "Caller-selected protected base ref does not match the frozen release policy.",
            )
        )
    safe_remote_ref = policy_remote_ref
    unsafe = safe_remote_ref is not None and (
        not SAFE_GIT_REF.fullmatch(safe_remote_ref)
        or ".." in safe_remote_ref
        or "@{" in safe_remote_ref
        or safe_remote_ref.endswith(("/", "."))
        or safe_remote_ref in {"HEAD", "head"}
    )
    if unsafe:
        blockers.append(
            ReleaseIdentityBlocker(
                "RELEASE_BASE_REF_INVALID",
                "Protected base ref from frozen policy uses an unsupported Git reference form.",
            )
        )
        safe_remote_ref = None

    projects: dict[str, dict[str, Any]] = {}
    package_versions: dict[str, str] = {}
    if normalized_commit is not None:
        projects, package_versions, package_blockers = packages.load_package_projects(
            root,
            normalized_commit,
            read_commit_text=_read_commit_text,
            blocker=ReleaseIdentityBlocker,
        )
        blockers.extend(package_blockers)
    if version is not None and normalized_commit is not None:
        for package, observed_version in sorted(package_versions.items()):
            if observed_version != version:
                blockers.append(
                    ReleaseIdentityBlocker(
                        "RELEASE_PACKAGE_VERSION_MISMATCH",
                        f"{package} version does not match the release tag.",
                        package=package,
                    )
                )
        blockers.extend(packages.dependency_blockers(projects, version, blocker=ReleaseIdentityBlocker))
        blockers.extend(
            packages.changelog_blockers(
                root,
                normalized_commit,
                version,
                read_commit_text=_read_commit_text,
                blocker=ReleaseIdentityBlocker,
            )
        )

    protected_base_sha: str | None = None
    if safe_tag is not None and normalized_commit is not None and safe_remote_ref is not None:
        git_blockers, protected_base_sha = _git_identity_blockers(
            root,
            tag=safe_tag,
            commit_sha=normalized_commit,
            remote_ref=safe_remote_ref,
        )
        blockers.extend(git_blockers)

    return ReleaseIdentityReport(
        status="PASS" if not blockers else "FAIL",
        tag=safe_tag,
        version=version,
        commit_sha=normalized_commit,
        remote_ref=safe_remote_ref,
        package_versions=package_versions,
        blockers=tuple(blockers),
        protected_base_sha=protected_base_sha,
        policy_sha256=policy_sha256,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the release identity gate and emit one credential-free report."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--tag", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument(
        "--remote-ref",
        default=None,
        help="Optional; when set must exactly match the frozen policy protected base (origin/<branch>).",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    report = evaluate_release_identity(
        root=args.root.resolve(),
        tag=args.tag,
        commit_sha=args.commit_sha,
        remote_ref=args.remote_ref,
    )
    rendered = json.dumps(report.to_payload(), indent=2, sort_keys=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    print(rendered)
    return 0 if report.status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
