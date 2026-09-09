from __future__ import annotations

import copy
import json
from pathlib import Path

from dpone.gitops.schema_contracts import get_gitops_schema_contract
from dpone.gitops.schema_validation import GitOpsSchemaValidator

ROOT = Path(__file__).resolve().parents[1]


def _valid_report() -> dict[str, object]:
    return {
        "schema": "dpone.selected-safe-sample-report.v1",
        "passed": True,
        "exit_code": 0,
        "sample": 1000,
        "target": "temporary",
        "environment": "development",
        "selection": {
            "schema": "dpone.selection-report.v1",
            "selection_fingerprint": "sha256:" + ("a" * 64),
        },
        "results": [
            {
                "workload_id": "orders",
                "source": "pipelines/orders/pipeline.yaml",
                "run_id": "safe-sample-orders",
                "passed": True,
                "status": "passed",
                "exit_code": 0,
                "errors": [],
                "evidence_path": ".dpone/safe-sample/orders/evidence.json",
            }
        ],
        "unscheduled": [],
        "errors": [],
    }


def test_selected_safe_sample_report_is_registered_and_documented() -> None:
    contract = get_gitops_schema_contract("dpone.selected-safe-sample-report.v1")

    assert contract is not None
    assert contract.name == "selected-safe-sample-report"
    documented = json.loads(
        (ROOT / "docs/schemas/gitops/selected-safe-sample-report.schema.json").read_text(encoding="utf-8")
    )
    assert documented == contract.schema


def test_selected_safe_sample_report_accepts_success_and_preselection_failure() -> None:
    validator = GitOpsSchemaValidator()
    success = _valid_report()
    failure = {
        "schema": "dpone.selected-safe-sample-report.v1",
        "passed": False,
        "exit_code": 2,
        "selection": None,
        "results": [],
        "unscheduled": [],
        "errors": [
            {
                "schema": "dpone.error.v1",
                "code": "DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID",
                "stage": "cli_validation",
                "severity": "error",
                "message": "Safe-sample mode received incompatible options.",
            }
        ],
    }

    assert validator.validate(success, expected_kind="dpone.selected-safe-sample-report.v1") == ()
    assert validator.validate(failure, expected_kind="dpone.selected-safe-sample-report.v1") == ()


def test_selected_safe_sample_report_rejects_missing_exit_code_and_unknown_top_level_field() -> None:
    validator = GitOpsSchemaValidator()
    missing_exit = _valid_report()
    missing_exit.pop("exit_code")
    unknown = copy.deepcopy(_valid_report())
    unknown["password"] = "must-not-be-an-extension"

    missing_exit_issues = validator.validate(
        missing_exit,
        expected_kind="dpone.selected-safe-sample-report.v1",
    )
    unknown_issues = validator.validate(
        unknown,
        expected_kind="dpone.selected-safe-sample-report.v1",
    )

    assert {(issue.code, issue.path) for issue in missing_exit_issues} == {
        ("schema_required_field_missing", "exit_code")
    }
    assert {(issue.code, issue.path) for issue in unknown_issues} == {
        ("schema_additional_property_forbidden", "password")
    }
