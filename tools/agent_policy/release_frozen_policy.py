"""Load the exact-commit branch-protection policy from one frozen Git blob."""

from __future__ import annotations

import hashlib
import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import Any, NamedTuple

import yaml

DEFAULT_POLICY = Path(".agents/policy/github-branch-protection.yml")
DEFAULT_REMOTE = "origin"


class PolicyRequirements(NamedTuple):
    ruleset_id: int
    context_names: tuple[str, ...]
    protected_branch: str
    schema_version: int = 1
    ruleset_projection: dict[str, Any] | None = None

    @property
    def protected_remote_ref(self) -> str:
        return f"{DEFAULT_REMOTE}/{self.protected_branch}"


def _load_sibling(module_name: str, filename: str) -> Any:
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def accepted_policy_path(path: Path) -> Path:
    """Reject caller-selected policy paths; only the fixed relative path is allowed."""

    if path != DEFAULT_POLICY:
        raise ValueError(
            f"caller-selected policy path is rejected; use {DEFAULT_POLICY.as_posix()} from the frozen commit"
        )
    return DEFAULT_POLICY


def parse_policy_requirements(text: str) -> PolicyRequirements:
    """Parse checked-in branch-protection policy YAML into gate requirements."""

    payload = yaml.safe_load(text)
    if not isinstance(payload, dict):
        raise ValueError("policy must be a YAML mapping")
    if payload.get("schema_version") == 2:
        return _parse_v2_requirements(payload)
    match payload:
        case {
            "ruleset": {
                "id": int(ruleset_id),
                "branches": list(branches),
                "required_status_checks": {"checks": list(checks)},
            }
        }:
            pass
        case _:
            raise ValueError("policy must define a ruleset id, protected branches, and required status checks")
    context_names = tuple(sorted(item for item in checks if isinstance(item, str)))
    branch = branches[0] if len(branches) == 1 and isinstance(branches[0], str) else None
    if (
        isinstance(ruleset_id, bool)
        or ruleset_id <= 0
        or len(context_names) != len(checks)
        or len(context_names) != len(set(context_names))
        or any(not item or item != item.strip() for item in context_names)
        or branch is None
        or not branch
        or branch != branch.strip()
        or "/" in branch
        or branch in {"HEAD", "head"}
    ):
        raise ValueError(
            "policy ruleset id, exactly one protected branch name, and context names "
            "must be positive, non-empty, and unique"
        )
    return PolicyRequirements(ruleset_id, context_names, branch, 1, None)


def _parse_v2_requirements(payload: dict[str, Any]) -> PolicyRequirements:
    policy_v2 = _load_sibling("dpone_agent_governance_policy_v2", "governance_policy_v2.py")
    projection = _load_sibling("dpone_agent_governance_projection", "governance_projection.py")
    parsed = policy_v2.parse_governance_policy(payload)
    branch = parsed.branch_view.ruleset
    protected = parsed.release_view.repository.protected_base_ref.removeprefix("refs/heads/")
    if not protected or "/" in protected or protected in {"HEAD", "head"}:
        raise ValueError("release_trust.repository.protected_base_ref must name one branch")
    return PolicyRequirements(
        branch.id,
        parsed.branch_view.context_names,
        protected,
        2,
        projection.canonical_ruleset_projection(branch),
    )


def load_frozen_policy(
    *, root: Path, commit_sha: str, policy_path: Path = DEFAULT_POLICY
) -> tuple[PolicyRequirements, str]:
    """Read policy bytes from ``git show <commit>:<path>`` and bind their SHA-256."""

    relative = accepted_policy_path(policy_path).as_posix()
    try:
        completed = subprocess.run(
            ("git", "show", f"{commit_sha}:{relative}"),
            cwd=root,
            capture_output=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        raise ValueError("frozen commit policy is unavailable") from None
    if completed.returncode != 0 or not completed.stdout:
        raise ValueError("frozen commit policy is unavailable")
    digest = hashlib.sha256(completed.stdout).hexdigest()
    return parse_policy_requirements(completed.stdout.decode("utf-8")), digest


def load_frozen_protected_base(*, root: Path, commit_sha: str) -> tuple[str, str]:
    """Return ``origin/<branch>`` and policy digest from the frozen commit policy."""

    policy, digest = load_frozen_policy(root=root, commit_sha=commit_sha)
    return policy.protected_remote_ref, digest
