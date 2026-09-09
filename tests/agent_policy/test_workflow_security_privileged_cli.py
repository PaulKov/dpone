from __future__ import annotations

import hashlib
import importlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import replace
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "tools/agent_policy/workflow_security_privileged.py"
UMBRELLA = ROOT / "tools/agent_policy/workflow_security.py"
LEGACY_POLICY = ROOT / ".agents/policy/workflow-security.yml"
FIXTURES = ROOT / "tests/fixtures/ci-shadow-pr3b"
INTERNAL_ERROR = "PRIVILEGE_INTERNAL_REPORT_INVALID: report construction or schema validation failed\n"


def _copy_fixture(tmp_path: Path, variant: str) -> Path:
    destination = tmp_path / variant
    shutil.copytree(FIXTURES / variant, destination)
    return destination


def _run_script(script: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    return subprocess.run(
        [sys.executable, str(script), *arguments],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


_run = partial(_run_script, SCRIPT)
_run_umbrella = partial(_run_script, UMBRELLA)


def _tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


def test_standalone_help_has_only_the_closed_public_options() -> None:
    completed = _run("--help")

    assert completed.returncode == 0
    assert completed.stderr == ""
    assert completed.stdout.startswith(
        "usage: workflow_security_privileged.py [-h] --root ROOT\n"
        "                                       [--format {text,json}]\n"
    )
    assert completed.stdout.endswith("\n")
    assert completed.stdout.count("--root ROOT") == 2
    assert completed.stdout.count("--format {text,json}") == 2
    assert "repository checkout containing the fixed policy and\n" in completed.stdout
    assert "                        .github/workflows\n" in completed.stdout
    for forbidden in ("--policy", "--workflows-dir", "--ignore", "--write", "--network"):
        assert forbidden not in completed.stdout


def test_umbrella_help_explains_fixed_and_override_input_boundaries() -> None:
    completed = _run_umbrella("--help")

    assert completed.returncode == 0 and completed.stderr == ""
    assert completed.stdout.startswith("usage: workflow_security.py") and completed.stdout.endswith("\n")
    for expected in (
        "repository root (default: .)",
        "legacy general-linter policy override",
        "legacy general-linter workflow-directory override",
        "output format (default: text)",
        "Overrides affect only legacy general lint",
        "semantic inputs and fixed release/runtime boundaries remain below root",
        "Relative overrides use the process working directory",
    ):
        assert expected in " ".join(completed.stdout.split())


def test_umbrella_sanitizes_policy_and_privileged_boundary_failures(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    workflow_security = importlib.import_module("tools.agent_policy.workflow_security")
    root = _copy_fixture(tmp_path, "target")
    policy = tmp_path / "invalid-policy.yml"
    policy.write_bytes(b"owner: dont-print-this-secret: invalid\n\xff")

    completed = _run_umbrella(str(root), "--policy", str(policy), "--format", "json")
    assert completed.returncode == 1 and completed.stderr == ""
    assert "dont-print-this-secret" not in completed.stdout and "Traceback" not in completed.stdout
    assert f"{policy}: cannot load workflow security policy" in json.loads(completed.stdout)["errors"]

    release = root / ".github/workflows/release.yml"
    release.write_text("name: Release\non: push\npermissions: {}\njobs: {}\n")
    original_load = workflow_security.load_sibling

    def assert_sanitized() -> None:
        assert workflow_security.main([str(root), "--policy", str(LEGACY_POLICY), "--format", "json"]) == 1
        captured = capsys.readouterr()
        assert captured.err == "" and "dont-print-this-secret" not in captured.out and "Traceback" not in captured.out
        assert f"{release}: privileged-boundary scan UNVERIFIED" in json.loads(captured.out)["errors"]

    for phase, error in (("scan", ValueError), ("scan", RuntimeError), ("load", OSError), ("load", RuntimeError)):
        boundary = SimpleNamespace(
            find_privileged_checkouts=lambda _path, error=error: (_ for _ in ()).throw(error("dont-print-this-secret"))
        )
        monkeypatch.setattr(
            workflow_security,
            "load_sibling",
            lambda name, filename, phase=phase, error=error: (
                (_ for _ in ()).throw(error("dont-print-this-secret"))
                if phase == "load"
                else boundary
                if filename == "release_privileged_boundary.py"
                else original_load(name, filename)
            ),
        )
        assert_sanitized()


def test_umbrella_preserves_exact_text_and_json_streams(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    workflow_security = importlib.import_module("tools.agent_policy.workflow_security")
    result = workflow_security.WorkflowSecurityValidationResult(errors=["legacy error"], warnings=["legacy warning"])
    monkeypatch.setattr(workflow_security, "validate_repository", lambda *_args, **_kwargs: result)

    assert workflow_security.main([".", "--format", "text"]) == 1
    captured = capsys.readouterr()
    assert captured.out == (
        "WARNING: legacy warning\nERROR: legacy error\nWorkflow security validation: FAILED (1 errors, 1 warnings)\n"
    )
    assert captured.err == ""

    assert workflow_security.main([".", "--format", "json"]) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"errors": ["legacy error"], "status": "failed", "warnings": ["legacy warning"]}
    assert captured.err == ""


def test_standalone_never_writes_python_bytecode(tmp_path: Path) -> None:
    root = _copy_fixture(tmp_path, "target")
    cache = tmp_path / "python-cache"
    environment = {key: value for key, value in os.environ.items() if key != "PYTHONDONTWRITEBYTECODE"}
    environment["PYTHONPYCACHEPREFIX"] = str(cache)

    completed = subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root), "--format", "json"],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0
    assert completed.stderr == ""
    generated = {path.relative_to(cache).as_posix() for path in cache.rglob("*.pyc")}
    assert not any("/tools/agent_policy/" in f"/{path}" for path in generated)


def test_target_json_and_text_are_canonical_replay_safe_and_read_only(tmp_path: Path) -> None:
    root = _copy_fixture(tmp_path, "target")
    before = _tree_digest(root)

    json_run = _run("--root", str(root), "--format", "json")
    json_replay = _run("--root", str(root), "--format", "json")
    text_run = _run("--root", str(root))

    assert json_run.returncode == json_replay.returncode == text_run.returncode == 0
    assert json_run.stderr == json_replay.stderr == text_run.stderr == ""
    assert json_run.stdout == json_replay.stdout
    report = json.loads(json_run.stdout)
    report_module = importlib.import_module("tools.agent_policy.workflow_privilege_report")
    assert json_run.stdout.encode("utf-8") == report_module.canonical_json_bytes(report)
    assert text_run.stdout == report_module.render_text(report)
    assert report["status"] == "PASS"
    assert _tree_digest(root) == before


def test_nonpass_is_stdout_only_with_exit_one(tmp_path: Path) -> None:
    root = _copy_fixture(tmp_path, "pre-split")

    completed = _run("--root", str(root), "--format", "json")

    assert completed.returncode == 1
    assert completed.stderr == ""
    report = json.loads(completed.stdout)
    assert report["status"] in {"FAIL", "UNVERIFIED"}
    assert report["ok"] is False
    assert report["findings"]


def test_empty_workflow_inventory_is_unverified_not_profile_drift(tmp_path: Path) -> None:
    root = _copy_fixture(tmp_path, "target")
    for workflow in (root / ".github/workflows").iterdir():
        workflow.unlink()

    completed = _run("--root", str(root), "--format", "json")

    assert completed.returncode == 1
    assert completed.stderr == ""
    report = json.loads(completed.stdout)
    assert (report["status"], report["inventory"]["complete"], report["inventory"]["workflow_count"]) == (
        "UNVERIFIED",
        False,
        0,
    )
    assert [item["code"] for item in report["findings"]] == ["PRIVILEGE_INVALID_WORKFLOW"]


def test_explicit_null_job_permissions_are_invalid_not_inherited(tmp_path: Path) -> None:
    root = _copy_fixture(tmp_path, "target")
    (root / ".github/workflows/null-permissions.yml").write_text(
        "name: Null permissions\non: pull_request\npermissions: {contents: write}\n"
        "jobs: {publish: {permissions: null, runs-on: ubuntu-latest, steps: []}}\n",
        encoding="utf-8",
    )

    completed = _run("--root", str(root), "--format", "json")

    assert completed.returncode == 1 and completed.stderr == ""
    report = json.loads(completed.stdout)
    assert (report["status"], [item["code"] for item in report["findings"]]) == (
        "UNVERIFIED",
        ["PRIVILEGE_INVALID_WORKFLOW"],
    )


def test_intermediate_route_paths_stop_at_n_plus_one() -> None:
    graph = importlib.import_module("tools.agent_policy.workflow_privilege_graph")
    support = importlib.import_module("tests.ci_shadow_pr3b_report_contract_support")
    limited = support.policy()
    limited["limits"]["routes"] = 3
    root, leaf = ".github/workflows/root.yml", ".github/workflows/leaf.yml"

    def job() -> dict[str, object]:
        return {"uses": f"./{leaf}", "with": {}}

    workflows = {
        root: {
            "path": root,
            "name": "Root",
            "on": {"pull_request": {"types": ["opened"]}},
            "jobs": {f"call-{i}": job() for i in range(4)},
        },
        leaf: {"path": leaf, "name": "Leaf", "on": {"workflow_call": {}}, "jobs": {}},
    }

    result = graph.expand_routes(graph.build_graph(workflows, limited), workflows, limited)

    assert (result.route_count, result.overflow_dimensions, result.routes) == (4, ("routes",), ())
    assert [item.code for item in result.findings][-1] == "PRIVILEGE_RESOURCE_LIMIT"


def test_missing_root_is_repository_relative_and_machine_independent(tmp_path: Path) -> None:
    missing = tmp_path / "private-host-path"

    completed = _run("--root", str(missing), "--format", "json")

    assert completed.returncode == 1
    assert completed.stderr == ""
    assert str(tmp_path) not in completed.stdout
    report = json.loads(completed.stdout)
    assert report["status"] == "UNVERIFIED"
    assert {item["subject"] for item in report["findings"]} == {"."}


def test_argparse_errors_use_exit_two_stderr_and_no_stdout(tmp_path: Path) -> None:
    root = _copy_fixture(tmp_path, "target")
    invocations = (
        (),
        ("--root", str(root), "--format", "yaml"),
        ("--root", str(root), "--policy", "policy.yml"),
        ("--root", str(root), "--workflows-dir", "workflows"),
    )

    for invocation in invocations:
        completed = _run(*invocation)
        assert completed.returncode == 2
        assert completed.stdout == ""
        assert completed.stderr.startswith("usage: workflow_security_privileged.py")
        assert completed.stderr.endswith("\n")


def test_internal_report_failure_has_fixed_exit_three_streams(
    monkeypatch,
    capsys,
    tmp_path: Path,
) -> None:
    cli = importlib.import_module("tools.agent_policy.workflow_security_privileged")
    report_module = importlib.import_module("tools.agent_policy.workflow_privilege_report")
    root = _copy_fixture(tmp_path, "target")

    def fail_scan(_root: Path):
        raise report_module.InternalReportError("sensitive implementation detail")

    monkeypatch.setattr(cli, "scan_repository", fail_scan)

    assert cli.main(["--root", str(root), "--format", "json"]) == 3
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == INTERNAL_ERROR
    assert "Traceback" not in captured.err
    assert "sensitive implementation detail" not in captured.err


@pytest.mark.parametrize("mutation", ("erase-authority", "change-inventory", "mutate-source"))
def test_report_builder_cannot_forge_source_evidence(mutation, monkeypatch, capsys, tmp_path: Path) -> None:
    cli = importlib.import_module("tools.agent_policy.workflow_security_privileged")
    report_module = importlib.import_module("tools.agent_policy.workflow_privilege_report")
    original = report_module.build_report

    def forge_pass(*args, **kwargs):
        if mutation == "mutate-source":
            args[6][0].runner["labels"].append("self-hosted")
        report = original(*args, **kwargs)
        if mutation == "erase-authority":
            for authority in report["route_authority"]:
                authority["declared_permissions"] = authority["effective_permissions"] = {
                    key: "none" for key in authority["effective_permissions"]
                }
                authority["profile_id"] = None
            report.update(status="PASS", ok=True, findings=[], privileged_profiles=[])
        elif mutation == "change-inventory":
            for field in ("workflow_count", "job_count", "edge_count"):
                report["inventory"][field] += 1
        return report

    monkeypatch.setattr(report_module, "build_report", forge_pass)
    root = _copy_fixture(tmp_path, "pre-split" if mutation == "erase-authority" else "target")

    assert cli.main(["--root", str(root), "--format", "json"]) == 3
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == INTERNAL_ERROR


def test_service_uses_the_injected_snapshot_capability(tmp_path: Path) -> None:
    service = importlib.import_module("tools.agent_policy.workflow_privilege_service")
    snapshot = importlib.import_module("tools.agent_policy.workflow_privilege_snapshot")
    composition = importlib.import_module("tools.agent_policy.workflow_privilege_composition")
    root = _copy_fixture(tmp_path, "target")
    calls: list[Path] = []
    reader = snapshot.SnapshotReader(limits=service.V1_LIMITS)

    def acquire(path: Path):
        calls.append(path)
        return reader.acquire(path)

    report = service.WorkflowPrivilegeService(
        policy_schema=json.loads(composition._V1_SCHEMA.read_text()),
        report_schema=json.loads(composition.REPORT_SCHEMA.read_text()),
        acquire_snapshot=acquire,
    ).scan(root)

    assert report["status"] == "PASS" and calls == [root]


def test_route_expansion_cannot_omit_graph_findings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    service = importlib.import_module("tools.agent_policy.workflow_privilege_service")
    contracts = importlib.import_module("tools.agent_policy.workflow_privilege_contracts")
    original_build, original_expand = service.build_graph, service.expand_routes
    finding = contracts.Finding(
        "PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE",
        "UNVERIFIED",
        ".github/workflows",
        "injected graph finding",
        "BOUND_WORKFLOW_EDGE",
    )

    def build(*args, **kwargs):
        graph = original_build(*args, **kwargs)
        return replace(graph, findings=(*graph.findings, finding))

    def expand(graph, *args, **kwargs):
        expansion = original_expand(graph, *args, **kwargs)
        return replace(expansion, findings=tuple(item for item in expansion.findings if item != finding))

    monkeypatch.setattr(service, "build_graph", build)
    monkeypatch.setattr(service, "expand_routes", expand)
    with pytest.raises(service.report_contract.InternalReportError, match="graph result differs"):
        service.scan_repository(_copy_fixture(tmp_path, "target"))
