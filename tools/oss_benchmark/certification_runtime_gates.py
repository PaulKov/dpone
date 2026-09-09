"""Release gates for executable certification evidence."""

from __future__ import annotations

from typing import Any


def evaluate_certification_gates(certification: dict[str, Any]) -> dict[str, Any]:
    """Evaluate required executable scenarios as a normalized gate result."""

    checks = [_scenario_gate(record) for record in certification.get("scenarios") or []]
    failed = [check for check in checks if check["status"] == "failed"]
    return {
        "gate_id": "executable_certification",
        "status": "failed" if failed else "passed",
        "passed": len(checks) - len(failed),
        "failed": len(failed),
        "checks": checks,
        "failed_checks": failed,
    }


def _scenario_gate(record: dict[str, Any]) -> dict[str, Any]:
    freshness = (record.get("freshness") or {}).get("status", "fresh")
    status = str(record.get("status") or "failed")
    required = bool(record.get("required", True))
    passed = (not required) or (status == "passed" and freshness == "fresh")
    return {
        "id": str(record.get("scenario_id") or "unknown"),
        "label": f"Executable scenario {record.get('scenario_id') or 'unknown'}",
        "status": "passed" if passed else "failed",
        "severity": "blocker" if required else "warning",
        "actual": f"{status}/{freshness}",
        "expected": "passed/fresh",
        "last_error": record.get("last_error") or (record.get("freshness") or {}).get("last_error"),
    }
