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


pr_receipt = _load("dpone_agent_pr_receipt_traceability_test", "tools/agent_policy/pr_receipt.py")


COMPLETE_BODY = """
## Summary

- Problem: agent governance controls changed.

## Design and scope

- Approved specification or issue: #275
- In scope: PR receipt traceability evidence.
- Non-goals: Runtime ETL behavior.

## Validation evidence

| Check | Status | Command/workflow | Artifact/notes |
|---|---|---|---|
| Focused tests | PASS | `uv run pytest tests/agent_policy/test_pr_receipt_traceability.py -q` | test output |
| Agent governance gate | PASS | `uv run python tools/agent_policy/governance_gate.py --base-ref origin/master` | agent_governance_gate.json |

## Owner attestation

- [x] Owner reviewed the final diff and accepts the change.
- [x] Required GitHub checks are green on the reviewed head commit.
- [x] Admin bypass was not used.
- [x] Agent governance receipt is attached when agent controls changed.

Evidence: agent_governance_gate.json
""".strip()


def test_pr_receipt_requires_approved_source_for_control_surface() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY.replace("- Approved specification or issue: #275", "- Approved specification or issue:"),
        changed_paths=["tools/agent_policy/pr_receipt.py"],
    )

    assert result.status == "FAIL"
    assert any("Approved specification or issue" in error for error in result.errors)


def test_pr_receipt_accepts_source_na_with_reason_for_control_surface() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY.replace(
            "- Approved specification or issue: #275",
            "- Approved specification or issue: N/A: internal traceability-only guard with no separate issue",
        ),
        changed_paths=["tools/agent_policy/pr_receipt.py"],
    )

    assert result.status == "PASS"
    assert result.errors == []


def test_pr_receipt_requires_validation_status_for_control_surface() -> None:
    body = COMPLETE_BODY.replace(
        "| Focused tests | PASS | `uv run pytest tests/agent_policy/test_pr_receipt_traceability.py -q` | test output |",
        "| Focused tests | | | |",
    ).replace(
        "| Agent governance gate | PASS | `uv run python tools/agent_policy/governance_gate.py --base-ref origin/master` | agent_governance_gate.json |",
        "| Agent governance gate | | | |",
    )

    result = pr_receipt.validate_pr_receipt(
        body=body,
        changed_paths=["tools/agent_policy/pr_receipt.py"],
    )

    assert result.status == "FAIL"
    assert any("Validation evidence" in error for error in result.errors)


def test_pr_receipt_requires_reason_for_unverified_validation_evidence() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY.replace(
            "| Focused tests | PASS | `uv run pytest tests/agent_policy/test_pr_receipt_traceability.py -q` | test output |",
            "| Focused tests | UNVERIFIED | `uv run pytest tests/agent_policy/test_pr_receipt_traceability.py -q` | |",
        ),
        changed_paths=["tools/agent_policy/pr_receipt.py"],
    )

    assert result.status == "FAIL"
    assert any("UNVERIFIED" in error and "reason" in error for error in result.errors)


def test_pr_receipt_accepts_unverified_validation_evidence_with_reason() -> None:
    result = pr_receipt.validate_pr_receipt(
        body=COMPLETE_BODY.replace(
            "| Focused tests | PASS | `uv run pytest tests/agent_policy/test_pr_receipt_traceability.py -q` | test output |",
            "| Focused tests | UNVERIFIED | `uv run pytest tests/agent_policy/test_pr_receipt_traceability.py -q` | pending approved live environment |",
        ),
        changed_paths=["tools/agent_policy/pr_receipt.py"],
    )

    assert result.status == "PASS"
    assert result.errors == []

    payload = pr_receipt.result_payload(result)
    assert payload["traceability"]["validation_statuses"] == ["UNVERIFIED", "PASS"]
    assert payload["traceability"]["non_pass_reasons"] == [
        {
            "check": "Focused tests",
            "status": "UNVERIFIED",
            "reason": "pending approved live environment",
        }
    ]
