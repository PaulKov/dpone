from __future__ import annotations

import copy
import hashlib
import os
import shutil
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"
FIXTURE_ROOT = ROOT / "tests/fixtures/ci-shadow-pr3b"
POLICY_PATH = Path(".agents/policy/workflow-security-privileged.yml")
WORKFLOW_DIRECTORY = Path(".github/workflows")

LIMITS = {
    "workflow_files": 256,
    "reusable_workflows_including_root": 50,
    "local_call_edges": 9,
    "workflow_run_edges": 1,
    "jobs": 4096,
    "graph_edges": 8192,
    "roots": 512,
    "routes": 16384,
    "authority_records": 16384,
    "profile_matches": 16384,
    "route_edges": 256,
    "findings": 4096,
    "workflow_bytes": 1048576,
    "total_workflow_bytes": 33554432,
    "policy_bytes": 1048576,
    "expression_bytes": 8192,
    "expression_tokens": 512,
    "yaml_depth": 32,
    "yaml_nodes": 100000,
    "finding_detail_bytes": 2048,
    "report_bytes": 16777216,
    "text_stdout_bytes": 16777216,
}

PERMISSION_ACCESS = {
    "actions": ("none", "read", "write"),
    "artifact-metadata": ("none", "read", "write"),
    "attestations": ("none", "read", "write"),
    "checks": ("none", "read", "write"),
    "code-quality": ("none", "read", "write"),
    "contents": ("none", "read", "write"),
    "deployments": ("none", "read", "write"),
    "discussions": ("none", "read", "write"),
    "id-token": ("none", "write"),
    "issues": ("none", "read", "write"),
    "models": ("none", "read"),
    "packages": ("none", "read", "write"),
    "pages": ("none", "read", "write"),
    "pull-requests": ("none", "read", "write"),
    "security-events": ("none", "read", "write"),
    "statuses": ("none", "read", "write"),
    "vulnerability-alerts": ("none", "read"),
}


def marked_yaml(marker: str) -> dict[str, Any]:
    """Return a detached mapping from one exact approved-spec YAML block."""
    text = SPEC.read_text(encoding="utf-8")
    begin = f"<!-- {marker}:begin -->"
    end = f"<!-- {marker}:end -->"
    if text.count(begin) != 1 or text.count(end) != 1:
        raise ValueError(f"expected one marked YAML block: {marker}")
    marked = text.split(begin, 1)[1].split(end, 1)[0]
    payload = marked.split("```yaml\n", 1)[1].split("\n```", 1)[0]
    value = yaml.safe_load(payload)
    if not isinstance(value, dict):
        raise TypeError(f"marked YAML block is not a mapping: {marker}")
    return copy.deepcopy(value)


def policy_bytes() -> bytes:
    """Return the approved v1 policy as exact UTF-8 YAML bytes."""
    text = SPEC.read_text(encoding="utf-8")
    marked = text.split("<!-- pr3b-policy-v1:begin -->", 1)[1].split("<!-- pr3b-policy-v1:end -->", 1)[0]
    payload = marked.split("```yaml\n", 1)[1].split("\n```", 1)[0]
    return f"{payload}\n".encode()


def policy_value() -> dict[str, Any]:
    """Return a detached copy of the exact approved v1 policy mapping."""
    return marked_yaml("pr3b-policy-v1")


def job_mapping(
    *,
    needs: Sequence[str] = (),
    condition: str | None = None,
    runs_on: Sequence[str] = ("ubuntu-latest",),
    permissions: Mapping[str, str] | None = None,
    environment: str | None = None,
    secrets: str = "NONE",
    uses: str | None = None,
    inputs: Mapping[str, Any] | None = None,
    steps: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Build one detached workflow-job fixture without policy defaults."""
    job: dict[str, Any] = {}
    if needs:
        job["needs"] = list(needs)
    if condition is not None:
        job["if"] = condition
    if permissions is not None:
        job["permissions"] = dict(permissions)
    if environment is not None:
        job["environment"] = environment
    if uses is None:
        job["runs-on"] = runs_on[0] if len(runs_on) == 1 else list(runs_on)
        job["steps"] = [dict(step) for step in steps]
    else:
        job["uses"] = uses
        if inputs is not None:
            job["with"] = dict(inputs)
        if secrets != "NONE":
            job["secrets"] = secrets.lower()
    return job


def workflow_mapping(
    path: str,
    *,
    name: str,
    events: Mapping[str, Any],
    permissions: Mapping[str, str],
    jobs: Mapping[str, Mapping[str, Any]],
    env: Mapping[str, Any] | None = None,
    defaults: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one path-bound workflow fixture suitable for graph or YAML tests."""
    workflow: dict[str, Any] = {
        "path": path,
        "name": name,
        "on": copy.deepcopy(dict(events)),
        "permissions": dict(permissions),
        "jobs": copy.deepcopy(dict(jobs)),
    }
    if env is not None:
        workflow["env"] = copy.deepcopy(dict(env))
    if defaults is not None:
        workflow["defaults"] = copy.deepcopy(dict(defaults))
    return workflow


def workflow_bytes(workflow: Mapping[str, Any]) -> bytes:
    """Serialize a workflow fixture, excluding its test-only path coordinate."""
    document = copy.deepcopy(dict(workflow))
    document.pop("path", None)
    return yaml.safe_dump(document, sort_keys=False).encode()


def write_repository(
    root: Path,
    workflows: Mapping[str, bytes],
    *,
    policy: bytes | None = None,
) -> Path:
    """Create only the fixed scanner inputs below ``root`` and return ``root``."""
    policy_file = root / POLICY_PATH
    workflow_root = root / WORKFLOW_DIRECTORY
    policy_file.parent.mkdir(parents=True, exist_ok=True)
    workflow_root.mkdir(parents=True, exist_ok=True)
    policy_file.write_bytes(policy_bytes() if policy is None else policy)
    for name, content in workflows.items():
        (workflow_root / name).write_bytes(content)
    return root


def copy_repository_fixture(tmp_path: Path, variant: Literal["pre-split", "target"]) -> Path:
    """Copy one immutable full-repository fixture and return the copied root."""
    if variant not in {"pre-split", "target"}:
        raise ValueError(f"unsupported PR3B fixture variant: {variant}")
    destination = tmp_path / variant
    shutil.copytree(FIXTURE_ROOT / variant, destination)
    return destination


def mutated(value: Mapping[str, Any], path: Sequence[str | int], replacement: Any) -> dict[str, Any]:
    """Return a deep-copied mapping with exactly one nested value replaced."""
    result = copy.deepcopy(dict(value))
    cursor: Any = result
    for segment in path[:-1]:
        cursor = cursor[segment]
    cursor[path[-1]] = replacement
    return result


def sha256(content: bytes) -> str:
    """Return a lowercase SHA-256 digest for fixture assertions."""
    return hashlib.sha256(content).hexdigest()


def finding_codes(result: Any) -> tuple[str, ...]:
    """Project ordered finding codes from an acquisition/parse result."""
    return tuple(finding.code for finding in result.findings)


def track_open_descriptors(monkeypatch: pytest.MonkeyPatch, module: Any) -> list[int]:
    """Record every descriptor opened by one module without changing its behavior."""
    opened: list[int] = []
    real_open = module.os.open

    def tracked_open(*args: Any, **kwargs: Any) -> int:
        descriptor = real_open(*args, **kwargs)
        opened.append(descriptor)
        return descriptor

    monkeypatch.setattr(module.os, "open", tracked_open)
    return opened


def assert_descriptors_closed(descriptors: list[int]) -> None:
    """Assert at least one descriptor was acquired and all are now closed."""
    assert descriptors
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)
