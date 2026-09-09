from __future__ import annotations

import importlib.util
import json
import subprocess
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


pr_body = _load("dpone_agent_pr_body_test", "tools/agent_policy/pr_body.py")


def _finalize(body: str) -> str:
    return (
        body.replace("- [ ] Owner reviewed the final diff", "- [x] Owner reviewed the final diff")
        .replace("- [ ] Required GitHub checks are green", "- [x] Required GitHub checks are green")
        .replace("- [ ] Admin bypass was not used.", "- [x] Admin bypass was not used.")
        .replace("- [ ] Agent governance receipt is attached", "- [x] Agent governance receipt is attached")
    )


def test_rendered_body_uses_receipt_grammar_and_passes_draft_preflight() -> None:
    body = pr_body.render_body(
        approved_source="docs/feature-design-agent-pr-body-preflight.md",
        problem="Agent-control PR bodies are easy to format almost correctly.",
        solution="Generate and preflight the exact receipt grammar locally.",
        impact="Maintainers avoid a CI rerun for body-only grammar mistakes.",
    )

    assert "- Approved specification or issue: docs/feature-design-agent-pr-body-preflight.md" in body
    assert "## Validation evidence" in body
    assert "- [ ] Owner reviewed the final diff and accepts the change." in body

    result = pr_body.check_body(
        body=body,
        changed_paths=["tools/agent_policy/pr_body.py"],
        phase="draft",
    )

    assert result.status == "PASS"
    assert result.errors == []


def test_draft_preflight_rejects_heading_only_approved_source() -> None:
    body = pr_body.render_body(approved_source="docs/feature-design-agent-pr-body-preflight.md").replace(
        "- Approved specification or issue: docs/feature-design-agent-pr-body-preflight.md",
        "## Approved Specification Or Issue\n\ndocs/feature-design-agent-pr-body-preflight.md",
    )

    result = pr_body.check_body(
        body=body,
        changed_paths=["tools/agent_policy/pr_body.py"],
        phase="draft",
    )

    assert result.status == "FAIL"
    assert any("Approved specification or issue" in error for error in result.errors)


def test_final_preflight_requires_checked_owner_attestation() -> None:
    body = pr_body.render_body(
        approved_source="docs/feature-design-agent-pr-body-preflight.md",
        governance_receipt="agent_governance_gate.json",
    )

    draft_result = pr_body.check_body(
        body=body,
        changed_paths=["tools/agent_policy/pr_body.py"],
        phase="draft",
    )
    final_result = pr_body.check_body(
        body=body,
        changed_paths=["tools/agent_policy/pr_body.py"],
        phase="final",
    )
    checked_result = pr_body.check_body(
        body=_finalize(body),
        changed_paths=["tools/agent_policy/pr_body.py"],
        phase="final",
    )

    assert draft_result.status == "PASS"
    assert final_result.status == "FAIL"
    assert any("owner review attestation" in error for error in final_result.errors)
    assert checked_result.status == "PASS"
    assert checked_result.errors == []


def test_preflight_is_not_applicable_for_non_agent_paths() -> None:
    result = pr_body.check_body(
        body="",
        changed_paths=["docs/run.md"],
        phase="draft",
    )

    assert result.status == "N/A"
    assert result.errors == []
    assert result.warnings == ["No agent control-surface paths changed."]


def test_cli_check_emits_json_payload(tmp_path: Path) -> None:
    body_path = tmp_path / "body.md"
    body_path.write_text(
        pr_body.render_body(approved_source="docs/feature-design-agent-pr-body-preflight.md"),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "tools/agent_policy/pr_body.py"),
            "check",
            "--phase",
            "draft",
            "--body-file",
            str(body_path),
            "--changed-paths",
            "tools/agent_policy/pr_body.py",
            "--format",
            "json",
        ],
        check=True,
        cwd=ROOT,
        text=True,
        capture_output=True,
    )

    payload = json.loads(completed.stdout)
    assert payload["status"] == "PASS"
    assert payload["phase"] == "draft"
    assert payload["traceability"]["approved_source"] == "docs/feature-design-agent-pr-body-preflight.md"
