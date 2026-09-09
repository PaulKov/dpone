from __future__ import annotations

import copy
import importlib
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest
from tests.ci_shadow_pr3b_report_examples import report_example

ROOT = Path(__file__).resolve().parents[2]
LEGACY_POLICY = ROOT / ".agents/policy/workflow-security.yml"
INTERNAL_ERROR = "semantic-pr-privilege-internal=PRIVILEGE_INTERNAL_REPORT_INVALID"


class _FatalSignal(BaseException):
    pass


def _workflow_security():
    return importlib.import_module("tools.agent_policy.workflow_security")


def _finding(
    code: str,
    status: str,
    subject: str,
    recovery_command_id: str,
) -> dict[str, object]:
    return {
        "code": code,
        "status": status,
        "subject": subject,
        "route_id": None,
        "detail": f"{status.lower()} semantic route",
        "recovery_command_id": recovery_command_id,
    }


def _semantic_report() -> dict[str, object]:
    report = report_example()
    report.update(status="FAIL", ok=False)
    report["findings"] = [
        _finding(
            "PRIVILEGE_PULL_REQUEST_TARGET",
            "FAIL",
            ".github/workflows/first.yml",
            "REMOVE_PULL_REQUEST_TARGET",
        ),
        _finding(
            "PRIVILEGE_UNKNOWN_EXPRESSION",
            "UNVERIFIED",
            ".github/workflows/second.yml",
            "SIMPLIFY_PRIVILEGE_GUARD",
        ),
    ]
    return report


def _write_legacy_repository(tmp_path: Path, *, unsafe: bool = False) -> Path:
    workflows = tmp_path / ".github/workflows"
    workflows.mkdir(parents=True)
    action = "actions/checkout@v4" if unsafe else "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0"
    (workflows / "fixture.yml").write_text(
        (
            "name: Fixture\n"
            "on: push\n"
            "permissions: {}\n"
            "jobs:\n"
            "  check:\n"
            "    runs-on: ubuntu-latest\n"
            "    steps:\n"
            f"      - uses: {action}\n"
        ),
        encoding="utf-8",
    )
    return tmp_path


def _canonical(finding: dict[str, object]) -> str:
    return json.dumps(
        finding,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def test_umbrella_appends_all_semantic_findings_after_legacy_errors(tmp_path: Path) -> None:
    workflow_security = _workflow_security()
    root = _write_legacy_repository(tmp_path, unsafe=True)
    report = _semantic_report()

    result = workflow_security.validate_repository(
        root,
        policy_path=LEGACY_POLICY,
        semantic_service=lambda _root: report,
    )

    assert "must pin external actions" in result.errors[0]
    assert result.errors[1:] == [f"semantic-pr-privilege={_canonical(finding)}" for finding in report["findings"]]
    assert result.warnings == []


def test_umbrella_projection_is_atomic_on_second_serialization_failure() -> None:
    workflow_security = _workflow_security()
    result = workflow_security.WorkflowSecurityValidationResult(
        errors=["legacy error"],
        warnings=["legacy warning"],
    )
    calls = 0

    def fail_second(finding: dict[str, object]) -> str:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise TypeError("injected serialization failure")
        return _canonical(finding)

    workflow_security._project_semantic_report(
        result,
        ROOT,
        semantic_service=lambda _root: _semantic_report(),
        serialize_finding=fail_second,
    )

    assert calls == 2
    assert result.errors == ["legacy error", INTERNAL_ERROR]
    assert result.warnings == ["legacy warning"]
    assert not any(error.startswith("semantic-pr-privilege={") for error in result.errors)


@pytest.mark.parametrize(
    "signal",
    (KeyboardInterrupt(), SystemExit(7), GeneratorExit(), _FatalSignal()),
)
def test_umbrella_propagates_every_base_exception(signal: BaseException, tmp_path: Path) -> None:
    workflow_security = _workflow_security()
    root = _write_legacy_repository(tmp_path)

    def interrupted(_root: Path):
        raise signal

    with pytest.raises(type(signal)) as raised:
        workflow_security.validate_repository(
            root,
            policy_path=LEGACY_POLICY,
            semantic_service=interrupted,
        )
    assert raised.value is signal


def test_umbrella_internal_failure_is_one_compatible_error(tmp_path: Path) -> None:
    workflow_security = _workflow_security()
    root = _write_legacy_repository(tmp_path)
    invalid = copy.deepcopy(_semantic_report())
    invalid["findings"][0] = {"code": "partial"}

    result = workflow_security.validate_repository(
        root,
        policy_path=LEGACY_POLICY,
        semantic_service=lambda _root: invalid,
    )

    assert result.errors == [INTERNAL_ERROR]
    assert result.warnings == []


def test_umbrella_rejects_policyless_pass_as_internal_evidence_failure(tmp_path: Path) -> None:
    workflow_security = _workflow_security()
    root = _write_legacy_repository(tmp_path)
    invalid = report_example()
    invalid["policy"].update(sha256=None, schema_version=None)

    result = workflow_security.validate_repository(
        root,
        policy_path=LEGACY_POLICY,
        semantic_service=lambda _root: invalid,
    )

    assert result.errors == [INTERNAL_ERROR]
    assert result.warnings == []


def test_umbrella_scans_yaml_workflow_extensions(tmp_path: Path) -> None:
    workflow_security = _workflow_security()
    root = _write_legacy_repository(tmp_path)
    unsafe = root / ".github/workflows/unpinned.yaml"
    unsafe.write_text("name: Unsafe\non: push\npermissions: {}\njobs: {}\nuses: actions/checkout@v4\n")

    result = workflow_security.validate_repository(
        root,
        policy_path=LEGACY_POLICY,
        semantic_service=lambda _root: report_example(),
    )

    assert result.errors == [f"{unsafe}: actions/checkout@v4 must pin external actions to a full commit SHA"]


def test_umbrella_orders_mixed_workflow_extensions_before_semantic_findings(tmp_path: Path) -> None:
    workflow_security = _workflow_security()
    root = _write_legacy_repository(tmp_path)
    unsafe = root / ".github/workflows"
    for name in ("z-last.yml", "a-first.yaml"):
        (unsafe / name).write_text("name: Unsafe\non: push\npermissions: {}\njobs: {}\nuses: external/action@v1\n")

    result = workflow_security.validate_repository(
        root, policy_path=LEGACY_POLICY, semantic_service=lambda _root: _semantic_report()
    )

    assert [Path(error.split(":", 1)[0]).name for error in result.errors[:2]] == ["a-first.yaml", "z-last.yml"]
    assert result.errors[2:] == [
        f"semantic-pr-privilege={_canonical(finding)}" for finding in _semantic_report()["findings"]
    ]


def test_umbrella_applies_privileged_boundary_to_yaml_extension(tmp_path: Path) -> None:
    workflow_security = _workflow_security()
    root = _write_legacy_repository(tmp_path)
    release = root / ".github/workflows/release.yaml"
    release.write_text(
        "name: Release\non: push\npermissions: {}\njobs:\n  publish:\n"
        "    permissions: {id-token: write}\n    runs-on: ubuntu-latest\n"
        "    steps:\n      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0\n"
    )

    result = workflow_security.validate_repository(
        root, policy_path=LEGACY_POLICY, semantic_service=lambda _root: report_example()
    )

    assert any("SS-47" in error for error in result.errors)

    alternate = tmp_path / "alternate"
    alternate.mkdir()
    (alternate / "safe.yml").write_text(
        "name: Safe\non: push\npermissions: {}\njobs: {check: {runs-on: ubuntu-latest}}\n"
    )
    redirected = workflow_security.validate_repository(
        root,
        policy_path=LEGACY_POLICY,
        workflows_dir=alternate,
        semantic_service=lambda _root: report_example(),
    )
    assert any("SS-47" in error for error in redirected.errors)

    missing = workflow_security.validate_repository(
        root,
        policy_path=LEGACY_POLICY,
        workflows_dir=tmp_path / "missing",
        semantic_service=lambda _root: report_example(),
    )
    assert any("workflows directory is missing" in error for error in missing.errors)
    assert any("SS-47" in error for error in missing.errors)


def test_umbrella_sanitizes_recursive_yaml(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workflow_security = _workflow_security()
    root = _write_legacy_repository(tmp_path)
    invalid = root / ".github/workflows/recursive.yml"
    invalid.write_text("name: Recursive\non: push\npermissions: {}\njobs: &jobs {loop: *jobs}\n")

    exit_code = workflow_security.main([str(root), "--policy", str(LEGACY_POLICY), "--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == 1 and captured.err == ""
    assert f"{invalid}: invalid workflow YAML" in json.loads(captured.out)["errors"]


@pytest.mark.parametrize(
    "invalid_bytes",
    (
        b"name: invalid\nAPI_TOKEN: dont-print-this-secret: invalid\n",
        b"name: invalid\n\xff",
        b"name: invalid\npublished: 0000-01-01\n",
    ),
)
def test_umbrella_sanitizes_invalid_workflow_input(
    invalid_bytes: bytes, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workflow_security = _workflow_security()
    root = _write_legacy_repository(tmp_path)
    invalid = root / ".github/workflows/invalid.yml"
    invalid.write_bytes(invalid_bytes)

    exit_code = workflow_security.main([str(root), "--policy", str(LEGACY_POLICY), "--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == 1 and captured.err == ""
    assert "dont-print-this-secret" not in captured.out and "Traceback" not in captured.out
    assert f"{invalid}: invalid workflow YAML" in json.loads(captured.out)["errors"]


@pytest.mark.parametrize(
    "expression",
    (
        "${{ toJSON(secrets) }}",
        "${{ format('{0}', secrets.API_KEY) }}",
        "${{ format('}} {0}', secrets.API_KEY) }}",
        "${{ format('{{Hello {0}!}}', secrets.API_KEY) }}",
        "${{ secrets['API_KEY'] }}",
        "${{ 'safe\\' || secrets.API_KEY }}",
    ),
)
def test_legacy_scanner_rejects_nested_secret_context(expression: str, tmp_path: Path) -> None:
    workflow_security = _workflow_security()
    workflow = tmp_path / "nested-secret.yml"
    workflow.write_text(
        (
            "name: Nested secret\n"
            "on: pull_request\n"
            "permissions: {}\n"
            "jobs:\n"
            "  inspect:\n"
            "    runs-on: ubuntu-latest\n"
            "    steps:\n"
            "      - run: echo inspect\n"
            "        env:\n"
            f"          TOKEN: {expression}\n"
        ),
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(
        workflow,
        policy=workflow_security.WorkflowSecurityPolicy.empty(),
    )

    assert result.errors == [f"{workflow}: pull_request workflows must not reference repository secrets"]
    assert result.warnings == []


def test_legacy_scanner_ignores_secret_text_inside_quoted_expression(tmp_path: Path) -> None:
    workflow_security = _workflow_security()
    workflow = tmp_path / "quoted-secret-text.yml"
    workflow.write_text(
        "name: Quoted text\non: pull_request\npermissions: {}\njobs:\n  inspect:\n"
        "    runs-on: ubuntu-latest\n    env:\n      VALUE: ${{ 'secrets }} literal' }}\n"
        "    steps: [{run: echo inspect}]\n",
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(
        workflow,
        policy=workflow_security.WorkflowSecurityPolicy.empty(),
    )

    assert result.errors == []


@pytest.mark.parametrize(
    "expression",
    (
        "${{ github.event.no-secrets }}",
        "${{ github.event.items.*.secrets }}",
        "${{ secrets.GITHUB_TOKEN }}",
        "${{ secrets['GITHUB_TOKEN'] }}",
    ),
)
def test_legacy_scanner_ignores_non_repository_secret_expression(expression: str, tmp_path: Path) -> None:
    workflow_security = _workflow_security()
    workflow = tmp_path / "non-repository-secret.yml"
    workflow.write_text(
        "name: Non-repository secret\non: pull_request\npermissions: {}\njobs:\n  inspect:\n"
        f"    runs-on: ubuntu-latest\n    env:\n      VALUE: {expression}\n"
        "    steps: [{run: echo inspect}]\n",
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(
        workflow,
        policy=workflow_security.WorkflowSecurityPolicy.empty(),
    )

    assert result.errors == []


def test_umbrella_import_failure_is_one_compatible_error(monkeypatch: pytest.MonkeyPatch) -> None:
    service_name = "tools.agent_policy.workflow_privilege_service"
    monkeypatch.setitem(sys.modules, service_name, ModuleType(service_name))
    module_name = "dpone_agent_workflow_security_missing_semantic_service"
    path = ROOT / "tools/agent_policy/workflow_security.py"
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    workflow_security = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, workflow_security)
    spec.loader.exec_module(workflow_security)
    result = workflow_security.WorkflowSecurityValidationResult()

    workflow_security._project_semantic_report(result, ROOT)

    assert result.errors == [INTERNAL_ERROR]
    assert result.warnings == []
