"""Build one auditable receipt for the agent governance drift workflow."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

DEFAULT_EVIDENCE_DIR = "test_artifacts/agent-policy"
SUMMARY_FILENAME = "agent-governance-drift-summary.json"

COMPONENT_FILES = {
    "workflow_security": "workflow-security.json",
    "ruleset_drift": "github-ruleset-drift.json",
    "classic_branch_protection": "github-classic-branch-protection-drift.json",
}
VALID_STATUSES = {"passed", "failed", "unverified"}


def build_summary(evidence_dir: Path) -> dict[str, Any]:
    """Build a normalized summary from governance drift component receipts."""

    components: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    warnings: list[str] = []
    statuses: list[str] = []

    for component_name, filename in COMPONENT_FILES.items():
        receipt = _load_component(evidence_dir / filename, component_name)
        components[component_name] = receipt
        status = str(receipt["status"])
        statuses.append(status)
        errors.extend(f"{component_name}: {error}" for error in receipt["errors"])
        warnings.extend(f"{component_name}: {warning}" for warning in receipt["warnings"])

    overall_status = _overall_status(statuses, errors)
    return {
        "schema_version": 1,
        "overall_status": overall_status,
        "status": overall_status,
        "components": components,
        "errors": errors,
        "warnings": warnings,
    }


def write_summary(summary: dict[str, Any], output: Path) -> None:
    """Write the governance drift summary JSON."""

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_component(path: Path, component_name: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return _failed_component(f"{path}: cannot load receipt: {exc}")
    if not isinstance(payload, dict):
        return _failed_component(f"{path}: receipt must be a JSON object")

    raw_status = payload.get("status")
    status = raw_status if isinstance(raw_status, str) else "failed"
    errors = _string_list(payload.get("errors"))
    warnings = _string_list(payload.get("warnings"))

    if status not in VALID_STATUSES:
        errors.append(f"{path}: {component_name} status must be one of {sorted(VALID_STATUSES)}")
        status = "failed"
    if raw_status is None:
        errors.append(f"{path}: {component_name} receipt is missing status")
    return {"status": status, "errors": errors, "warnings": warnings}


def _failed_component(error: str) -> dict[str, Any]:
    return {"status": "failed", "errors": [error], "warnings": []}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _overall_status(statuses: list[str], errors: list[str]) -> str:
    if errors or any(status == "failed" for status in statuses):
        return "failed"
    if any(status == "unverified" for status in statuses):
        return "unverified"
    return "passed"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", default=DEFAULT_EVIDENCE_DIR, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)

    summary = build_summary(args.evidence_dir)
    output = args.output or args.evidence_dir / SUMMARY_FILENAME
    write_summary(summary, output)

    if args.format == "json":
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        for warning in summary["warnings"]:
            print(f"WARNING: {warning}")
        for error in summary["errors"]:
            print(f"ERROR: {error}")
        print(f"Agent governance drift summary: {summary['overall_status'].upper()} ({output})")
    return 0 if summary["overall_status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
