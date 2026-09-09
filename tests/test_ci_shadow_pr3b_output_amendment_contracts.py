from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator

from tests.agent_policy._ci_shadow_history_fixtures import commit_files, git, initialize_repository
from tests.ci_shadow_pr3b_report_contract_model import valid_report_shape
from tests.ci_shadow_pr3b_report_examples import report_example

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"
ADR = ROOT / "docs/adr/0037-immutable-agent-pr-merge-closure.md"
TASK = ROOT / "test_artifacts/agent-policy/dpone-ci-shadow-pr3b-public-output-amendment.yml"
TASK_SCHEMA = ROOT / "evals/agent/agent-task-contract.schema.json"
AMENDMENT_BASE = "f18298c14af247225758f7fe8901bc8462994dd4"
APPROVED_SPEC_BLOB = "ec162f1a3d82d785ab680b299005e8bba13cb432"
ACCEPTED_ADR_BLOB = "04705ee802cae434246c779945691a724c5c8851"
SEMANTIC_INTERNAL_ERROR = "semantic-pr-privilege-internal=PRIVILEGE_INTERNAL_REPORT_INVALID"


class _FatalTestSignal(BaseException):
    pass


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _squash(value: str) -> str:
    return " ".join(value.split())


def _canonical_finding(finding: dict[str, Any]) -> str:
    return json.dumps(
        finding,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _project_semantic_report(
    *,
    legacy_errors: list[str],
    legacy_warnings: list[str],
    semantic_service: Callable[[], dict[str, Any]],
    serialize_finding: Callable[[dict[str, Any]], str] = _canonical_finding,
) -> tuple[dict[str, Any], int]:
    errors = [*legacy_errors]
    try:
        report = semantic_service()
        if not valid_report_shape(report):
            raise ValueError("semantic report must satisfy the closed v1 contract")
        projected = [f"semantic-pr-privilege={serialize_finding(finding)}" for finding in report["findings"]]
    except Exception:
        errors.append(SEMANTIC_INTERNAL_ERROR)
    else:
        errors.extend(projected)
    result = {
        "status": "failed" if errors else "passed",
        "errors": errors,
        "warnings": [*legacy_warnings],
    }
    return result, 1 if errors else 0


def _semantic_service_returning(report: dict[str, Any]) -> Callable[[], dict[str, Any]]:
    return lambda: report


def _valid_amendment_lifecycle(spec: str) -> bool:
    metadata = spec.split("## Executive summary", maxsplit=1)[0]
    statuses = [line for line in metadata.splitlines() if line.startswith("- Public-output amendment status:")]
    researched = "- Public-output amendment status: RESEARCHED"
    approved = "- Public-output amendment status: APPROVED"
    pending = "- [ ] Maintainer changed public-output amendment status to `APPROVED` after"
    accepted = "- [x] Maintainer changed public-output amendment status to `APPROVED` after"
    return (statuses == [researched] and spec.count(pending) == 1 and accepted not in spec) or (
        statuses == [approved] and spec.count(accepted) == 1 and pending not in spec
    )


def test_public_output_amendment_lifecycle_and_authority_are_exact(tmp_path: Path) -> None:
    raw = _read(SPEC)
    assert _valid_amendment_lifecycle(raw)
    assert raw.split("## Executive summary", maxsplit=1)[0].count("- Status: APPROVED") == 1
    assert f"- Public-output amendment base: `{AMENDMENT_BASE}`" in raw

    researched = raw.replace(
        "- Public-output amendment status: APPROVED",
        "- Public-output amendment status: RESEARCHED",
        1,
    ).replace(
        "- [x] Maintainer changed public-output amendment status to `APPROVED` after",
        "- [ ] Maintainer changed public-output amendment status to `APPROVED` after",
        1,
    )
    approved = researched.replace(
        "- Public-output amendment status: RESEARCHED",
        "- Public-output amendment status: APPROVED",
        1,
    ).replace(
        "- [ ] Maintainer changed public-output amendment status to `APPROVED` after",
        "- [x] Maintainer changed public-output amendment status to `APPROVED` after",
        1,
    )
    assert _valid_amendment_lifecycle(researched)
    assert _valid_amendment_lifecycle(approved)
    assert not _valid_amendment_lifecycle(
        researched.replace(
            "- [ ] Maintainer changed public-output amendment status to `APPROVED` after",
            "- [x] Maintainer changed public-output amendment status to `APPROVED` after",
            1,
        )
    )

    # Old authority identifiers remain documentation metadata, not locally verified history.
    task = yaml.safe_load(_read(TASK))
    assert task["base_commit"] == AMENDMENT_BASE
    assert APPROVED_SPEC_BLOB in " ".join(task["dependencies"])
    root = tmp_path / "authority-lifecycle"
    initialize_repository(root)
    base = commit_files(root, "synthetic researched amendment", {"design.md": researched, "adr.md": _read(ADR)})
    spec_blob = git(root, "rev-parse", f"{base}:design.md")
    assert git(root, "rev-parse", f"{base}:adr.md") == ACCEPTED_ADR_BLOB
    approved_head = commit_files(root, "synthetic approval only", {"design.md": approved})
    assert git(root, "merge-base", base, approved_head) == base
    assert git(root, "diff", "--name-only", base, approved_head) == "design.md"
    assert git(root, "rev-parse", f"{approved_head}:adr.md") == ACCEPTED_ADR_BLOB
    assert git(root, "rev-parse", f"{approved_head}:design.md") != spec_blob
    approved_blob = git(root, "rev-parse", f"{approved_head}:design.md")
    tampered = commit_files(root, "synthetic invalid amendment", {"design.md": researched + "tamper\n"})
    with pytest.raises(AssertionError):
        assert git(root, "rev-parse", f"{tampered}:design.md") == approved_blob
    assert git(root, "rev-parse", f"{approved_head}:design.md") == approved_blob


def test_public_output_amendment_is_closed_and_fail_safe() -> None:
    spec = _squash(_read(SPEC))
    for expected in (
        "ephemeral internal analysis metadata bounded by the route limit",
        "It is not serialized, logged, counted as a finding, or appended to the umbrella output",
        "`PRIVILEGE_UNKNOWN_EXPRESSION` / `UNVERIFIED` finding",
        "`semantic-pr-privilege=<canonical-finding-json>`",
        "It appends no semantic item to `warnings`",
        'json.dumps(finding, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)',
        'both semantic `FAIL` and `UNVERIFIED` produce the existing umbrella contract with `status="failed"`',
        "The umbrella has no `ok` field",
        "`semantic-pr-privilege-internal=PRIVILEGE_INTERNAL_REPORT_INVALID`",
        "mutates the legacy result only after every finding serializes successfully",
        "The umbrella boundary catches ordinary `Exception` only",
        "standalone `PRIVILEGE_INTERNAL_REPORT_INVALID` still exits `3`",
        "A repeated exit `3` is an implementation defect",
        'integration_method == "squash"',
        "git log --first-parent --reverse",
        "A rerun cannot repair that identity",
        "dpone-ci-shadow-pr3b-public-output-amendment-v2.yml",
        "Only after step 3 is PASS",
        "stop before creating the task or editing production bytes",
    ):
        assert expected in spec
    assert "unknown expression on a read-only job is recorded diagnostically" not in spec
    assert "semantic findings are appended to its errors/warnings" not in spec


def test_public_output_amendment_reference_adapter_is_exact() -> None:
    fail_finding = {
        "code": "PRIVILEGE_PULL_REQUEST_TARGET",
        "status": "FAIL",
        "subject": ".github/workflows/unsafe.yml",
        "route_id": "a" * 64,
        "detail": "privileged route from pull_request_target",
        "recovery_command_id": "REMOVE_PULL_REQUEST_TARGET",
    }
    unknown_finding = {
        "code": "PRIVILEGE_UNKNOWN_EXPRESSION",
        "status": "UNVERIFIED",
        "subject": ".github/workflows/unknown.yml",
        "route_id": None,
        "detail": "неизвестное выражение",
        "recovery_command_id": "SIMPLIFY_PRIVILEGE_GUARD",
    }
    semantic_report = report_example()
    semantic_report.update(status="FAIL", ok=False)
    semantic_report["findings"] = [fail_finding, unknown_finding]

    result, exit_code = _project_semantic_report(
        legacy_errors=["legacy error"],
        legacy_warnings=["legacy warning"],
        semantic_service=lambda: semantic_report,
    )

    assert result == {
        "status": "failed",
        "errors": [
            "legacy error",
            f"semantic-pr-privilege={_canonical_finding(fail_finding)}",
            f"semantic-pr-privilege={_canonical_finding(unknown_finding)}",
        ],
        "warnings": ["legacy warning"],
    }
    assert exit_code == 1
    assert "ok" not in result
    assert result["errors"][1].endswith(
        '{"code":"PRIVILEGE_PULL_REQUEST_TARGET","detail":"privileged route from pull_request_target",'
        '"recovery_command_id":"REMOVE_PULL_REQUEST_TARGET","route_id":"'
        + "a" * 64
        + '","status":"FAIL","subject":".github/workflows/unsafe.yml"}'
    )
    assert "\\u043d\\u0435\\u0438\\u0437\\u0432\\u0435\\u0441\\u0442\\u043d\\u043e\\u0435" in result["errors"][2]
    assert "\n" not in result["errors"][1]
    with pytest.raises(ValueError, match="Out of range float values are not JSON compliant"):
        _canonical_finding({"detail": float("nan")})

    unknown_report = report_example()
    unknown_report.update(status="UNVERIFIED", ok=False)
    unknown_report["findings"] = [unknown_finding]
    unknown_result, unknown_exit = _project_semantic_report(
        legacy_errors=[],
        legacy_warnings=["legacy warning"],
        semantic_service=lambda: unknown_report,
    )
    assert unknown_result == {
        "status": "failed",
        "errors": [f"semantic-pr-privilege={_canonical_finding(unknown_finding)}"],
        "warnings": ["legacy warning"],
    }
    assert unknown_exit == 1
    assert "ok" not in unknown_result

    incomplete_report = copy.deepcopy(unknown_report)
    incomplete_report["findings"] = [{"code": "incomplete"}]
    mismatched_recovery = copy.deepcopy(unknown_report)
    mismatched_recovery["findings"][0]["recovery_command_id"] = "REMOVE_PULL_REQUEST_TARGET"
    for invalid_report in (incomplete_report, mismatched_recovery):
        invalid_result, invalid_exit = _project_semantic_report(
            legacy_errors=["legacy error"],
            legacy_warnings=["legacy warning"],
            semantic_service=_semantic_service_returning(invalid_report),
        )
        assert invalid_result == {
            "status": "failed",
            "errors": ["legacy error", SEMANTIC_INTERNAL_ERROR],
            "warnings": ["legacy warning"],
        }
        assert invalid_exit == 1


def test_public_output_amendment_internal_failures_are_atomic_and_compatible() -> None:
    first = {
        "code": "PRIVILEGE_PULL_REQUEST_TARGET",
        "status": "FAIL",
        "subject": ".github/workflows/first.yml",
        "route_id": "a" * 64,
        "detail": "first",
        "recovery_command_id": "REMOVE_PULL_REQUEST_TARGET",
    }
    second = {
        "code": "PRIVILEGE_UNKNOWN_EXPRESSION",
        "status": "UNVERIFIED",
        "subject": ".github/workflows/second.yml",
        "route_id": None,
        "detail": "second",
        "recovery_command_id": "SIMPLIFY_PRIVILEGE_GUARD",
    }
    report = report_example()
    report.update(status="FAIL", ok=False)
    report["findings"] = [first, second]

    calls = 0

    def fail_on_second_finding(finding: dict[str, Any]) -> str:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise TypeError("injected serialization failure")
        return _canonical_finding(finding)

    partial_result, partial_exit = _project_semantic_report(
        legacy_errors=["legacy error"],
        legacy_warnings=["legacy warning"],
        semantic_service=lambda: report,
        serialize_finding=fail_on_second_finding,
    )
    assert calls == 2
    assert partial_result == {
        "status": "failed",
        "errors": ["legacy error", SEMANTIC_INTERNAL_ERROR],
        "warnings": ["legacy warning"],
    }
    assert partial_exit == 1
    assert "ok" not in partial_result
    assert not any(error.startswith("semantic-pr-privilege={") for error in partial_result["errors"])

    def failed_service() -> dict[str, Any]:
        raise RuntimeError("injected semantic service failure")

    service_result, service_exit = _project_semantic_report(
        legacy_errors=[],
        legacy_warnings=[],
        semantic_service=failed_service,
    )
    assert service_result == {
        "status": "failed",
        "errors": [SEMANTIC_INTERNAL_ERROR],
        "warnings": [],
    }
    assert service_exit == 1
    assert "ok" not in service_result

    json_stdout = f"{json.dumps(partial_result, indent=2, sort_keys=True)}\n"
    text_stdout = (
        "WARNING: legacy warning\n"
        "ERROR: legacy error\n"
        f"ERROR: {SEMANTIC_INTERNAL_ERROR}\n"
        "Workflow security validation: FAILED (2 errors, 1 warnings)\n"
    )
    assert json.loads(json_stdout) == partial_result
    assert set(json.loads(json_stdout)) == {"status", "errors", "warnings"}
    assert "Traceback" not in json_stdout
    assert "Traceback" not in text_stdout
    assert "injected serialization failure" not in json_stdout
    assert "injected serialization failure" not in text_stdout
    assert text_stdout.endswith("Workflow security validation: FAILED (2 errors, 1 warnings)\n")

    replay_result, replay_exit = _project_semantic_report(
        legacy_errors=["legacy error"],
        legacy_warnings=["legacy warning"],
        semantic_service=failed_service,
    )
    assert (replay_result, replay_exit) == (
        {
            "status": "failed",
            "errors": ["legacy error", SEMANTIC_INTERNAL_ERROR],
            "warnings": ["legacy warning"],
        },
        1,
    )

    for interrupt in (KeyboardInterrupt(), SystemExit(2), GeneratorExit(), _FatalTestSignal()):

        def interrupted_service(interrupt: BaseException = interrupt) -> dict[str, Any]:
            raise interrupt

        with pytest.raises(type(interrupt)) as raised:
            _project_semantic_report(
                legacy_errors=[],
                legacy_warnings=[],
                semantic_service=interrupted_service,
            )
        assert raised.value is interrupt
        if isinstance(interrupt, SystemExit):
            assert isinstance(raised.value, SystemExit)
            assert raised.value.code == 2


def test_public_output_amendment_task_contract_is_closed() -> None:
    task = yaml.safe_load(_read(TASK))
    assert isinstance(task, dict)
    schema: dict[str, Any] = json.loads(_read(TASK_SCHEMA))
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(task)

    assert task["base_commit"] == AMENDMENT_BASE
    assert task["integrator"] == task["shared_file_owner"] == "/root"
    assert task["specification"] == SPEC.relative_to(ROOT).as_posix()
    assert set(task["owned_paths"]).isdisjoint(task["integrator_owned_paths"])
    assert {
        "tests/ci_shadow_pr3b_squash_receipt_support.py",
        "tests/test_ci_shadow_pr3b_squash_receipt_contracts.py",
        "tests/test_ci_shadow_pr3b_squash_receipt_failures.py",
    } <= set(task["owned_paths"])
    assert set(task["integrator_owned_paths"]) == {"docs/quality-metrics.md"}
    for path in task["integrator_owned_paths"]:
        assert path in task["forbidden_paths"]
    assert any("before this amendment is APPROVED and merged" in item for item in task["stop_conditions"])
    assert any(
        "internal semantic failure" in item and "partial semantic finding" in item for item in task["stop_conditions"]
    )
    assert any("task-changing scope head" in item and "non-ancestor" in item for item in task["stop_conditions"])
    assert any("cumulative committed scope" in item for item in task["acceptance_criteria"])
    assert any("after squash" in item and "byte-identical" in item for item in task["acceptance_criteria"])
    assert any(
        "MUST be integrated with the squash method" in item and "post-merge receipt" in item
        for item in task["acceptance_criteria"]
    )
    assert any(
        "fail-closed copyable post-merge receipt check" in item and "versioned v2 recovery" in item
        for item in task["acceptance_criteria"]
    )
    assert any("readable non-PASS" in item and "controlled UNVERIFIED" in item for item in task["acceptance_criteria"])
    assert any(
        "exact bytes" in item
        and "isolated interpreter" in item
        and "full closed schema" in item
        and "source_receipt" in item
        for item in task["acceptance_criteria"]
    )
    assert any(
        "Receipt-intrinsic" in item and "commit-header" in item and "cannot be downgraded" in item
        for item in task["acceptance_criteria"]
    )
    assert any(
        "GIT_NO_REPLACE_OBJECTS=1" in item
        and "GIT_GRAFT_FILE" in item
        and "GIT_SHALLOW_FILE" in item
        and "concurrent metadata mutation" in item
        for item in task["acceptance_criteria"]
    )
    assert any(
        "before implementation-task creation" in item and "only receipt PASS" in item
        for item in task["acceptance_criteria"]
    )
    assert any(
        "Executable receipt-command tests" in item
        and "full schema/binding" in item
        and "replacement/graft ancestry" in item
        and "NUL/invalid UTF-8" in item
        and "hidden pre-boundary task reuse" in item
        for item in task["acceptance_criteria"]
    )
    assert any("receipt-proven squash" in item and "canonical first-parent" in item for item in task["stop_conditions"])
    assert any("deterministic integration-identity failure" in item for item in task["stop_conditions"])
    assert any("raw jq/git diagnostics" in item for item in task["stop_conditions"])
    assert any(
        "schema" in item and "shallow-history violation" in item and "later local-object" in item
        for item in task["stop_conditions"]
    )
    assert any("implementation task or production edit" in item for item in task["stop_conditions"])
    assert any("versioned v2 recovery amendment" in item for item in task["stop_conditions"])
