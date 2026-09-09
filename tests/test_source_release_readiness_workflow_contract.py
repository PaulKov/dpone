from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/source-release-readiness.yml"


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_readiness_cannot_publish_or_scan_caller_selected_source() -> None:
    workflow = _workflow()
    assert workflow.get("on", workflow.get(True)) == {"workflow_dispatch": None}
    assert workflow["permissions"] == {"contents": "read"}
    for job in workflow["jobs"].values():
        assert "permissions" not in job
        for boundary in (
            "github.repository == 'PaulKov/dpone'",
            "github.event_name == 'workflow_dispatch'",
            "github.ref == 'refs/heads/master'",
            "github.ref_protected == true",
        ):
            assert boundary in job["if"]
        checkout = next(step for step in job["steps"] if step.get("uses", "").startswith("actions/checkout@"))
        assert checkout["with"] == {"ref": "${{ github.sha }}", "persist-credentials": False}
    assert "secrets." not in yaml.safe_dump(workflow["jobs"]["build"])


def test_archive_handoff_is_immutable_and_secret_job_does_not_install_candidates() -> None:
    jobs = _workflow()["jobs"]
    build = jobs["build"]
    build_commands = "\n".join(step.get("run", "") for step in build["steps"])
    assert "uv sync --locked --all-extras" in build_commands
    assert build_commands.index("--inventory-only") < build_commands.index("package_archive_gate.py")
    assert build_commands.index("package_archive_gate.py") < build_commands.index("install dist/*.whl")
    assert '"dpone[full,accel]==${RELEASE_VERSION}"' in build_commands
    hygiene = jobs["hygiene"]
    assert hygiene["needs"] == "build"
    download = next(step for step in hygiene["steps"] if step.get("uses", "").startswith("actions/download-artifact@"))
    assert download["with"]["artifact-ids"] == "${{ needs.build.outputs.artifact_id }}"
    assert download["with"]["digest-mismatch"] == "error"
    secret_steps = [step for step in hygiene["steps"] if "secrets." in yaml.safe_dump(step)]
    assert len(secret_steps) == 1
    assert "secrets.TENANT_HYGIENE_POLICY" in secret_steps[0]["env"]["TENANT_HYGIENE_POLICY"]
    for step in hygiene["steps"]:
        assert "pip install" not in step.get("run", "")
        assert "uv sync" not in step.get("run", "")
    for job in jobs.values():
        for step in job["steps"]:
            if step.get("uses", "").startswith("actions/upload-artifact@"):
                assert "github.run_id" in step["with"]["name"]
                assert "github.run_attempt" in step["with"]["name"]
                assert step["with"]["overwrite"] is False


@pytest.mark.parametrize("failure_mode", ["", "source", "archive"])
@pytest.mark.skipif(os.name == "nt" or shutil.which("bash") is None, reason="workflow uses a POSIX runner")
def test_hygiene_propagates_scanner_failure_and_always_removes_private_policy(
    tmp_path: Path, failure_mode: str
) -> None:
    step = next(
        step for step in _workflow()["jobs"]["hygiene"]["steps"] if "TENANT_HYGIENE_POLICY" in step.get("env", {})
    )
    binary = tmp_path / "bin"
    binary.mkdir()
    scanner = binary / "python"
    scanner.write_text(
        f"#!{sys.executable}\n"
        "import os, pathlib, stat, sys\n"
        "policy = pathlib.Path(sys.argv[sys.argv.index('--policy') + 1])\n"
        "assert stat.S_IMODE(policy.stat().st_mode) == 0o600\n"
        "assert policy.read_text() == 'private-test-pattern\\n'\n"
        "assert 'TENANT_HYGIENE_POLICY' not in os.environ\n"
        "with open('observed-policy-paths', 'a') as observed:\n"
        "    observed.write(str(policy) + '\\n')\n"
        'print(\'{"status":"test-stub"}\')\n'
        "sys.exit(2 if os.environ['FAILURE_MODE'] in sys.argv[4:5] else 0)\n",
        encoding="utf-8",
    )
    scanner.chmod(0o755)
    env = {**os.environ, "PATH": f"{binary}:{os.environ['PATH']}", "RUNNER_TEMP": str(tmp_path)}
    env.update(TENANT_HYGIENE_POLICY="private-test-pattern", RELEASE_COMMIT="a" * 40, FAILURE_MODE=failure_mode)
    result = subprocess.run(
        ["bash", "-c", step["run"]], cwd=tmp_path, env=env, capture_output=True, text=True, check=False
    )
    assert result.returncode == (2 if failure_mode else 0), result.stderr
    paths = (tmp_path / "observed-policy-paths").read_text(encoding="utf-8").splitlines()
    assert len(paths) == (1 if failure_mode == "source" else 2)
    assert all(not Path(path).exists() for path in paths)
    assert "private-test-pattern" not in result.stdout + result.stderr
