"""SS-47 static detector for privileged jobs that still check out repository code."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


boundary = _load("dpone_agent_release_privileged_boundary_tests", "tools/agent_policy/release_privileged_boundary.py")


def test_release_yml_has_no_privileged_checkout_jobs() -> None:
    findings = boundary.find_privileged_checkouts(ROOT / ".github/workflows/release.yml")
    assert findings == ()


def test_runtime_image_yml_has_no_privileged_checkout_jobs() -> None:
    findings = boundary.find_privileged_checkouts(ROOT / ".github/workflows/runtime-image.yml")
    assert findings == ()


def test_action_only_privileged_job_is_clean(tmp_path: Path) -> None:
    path = tmp_path / "release.yml"
    path.write_text(
        "\n".join(
            [
                "jobs:",
                "  publish:",
                "    permissions:",
                "      id-token: write",
                "      contents: read",
                "    steps:",
                "      - uses: actions/download-artifact@sha",
                "",
            ]
        ),
        encoding="utf-8",
    )
    assert boundary.find_privileged_checkouts(path) == ()


def test_privileged_checkout_is_workflow_security_error(tmp_path: Path) -> None:
    workflow_security = _load(
        "dpone_agent_workflow_security_privileged_tests",
        "tools/agent_policy/workflow_security.py",
    )
    root = tmp_path / "repo"
    workflows = root / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "release.yml").write_text(
        "\n".join(
            [
                "name: Release",
                "on: push",
                "permissions:",
                "  contents: read",
                "jobs:",
                "  publish:",
                "    runs-on: ubuntu-latest",
                "    permissions:",
                "      id-token: write",
                "      contents: read",
                "    steps:",
                "      - uses: actions/checkout@0123456789abcdef0123456789abcdef01234567",
                "",
            ]
        ),
        encoding="utf-8",
    )
    (root / ".agents" / "policy").mkdir(parents=True)
    (root / ".agents" / "policy" / "workflow-security.yml").write_text(
        (ROOT / ".agents/policy/workflow-security.yml").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    result = workflow_security.validate_repository(
        root,
        policy_path=root / ".agents/policy/workflow-security.yml",
        workflows_dir=workflows,
    )
    assert any("SS-47" in error and "publish" in error for error in result.errors)
