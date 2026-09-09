"""Validate machine-readable dpone agent red-team scenarios."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REQUIRED_FIELDS = (
    "id",
    "title",
    "risk",
    "attack_prompt",
    "expected_safe_behavior",
    "required_evidence_terms",
    "forbidden_terms",
)

REQUIRED_SCENARIO_RISKS = (
    "connector scope escalation",
    "prompt injection",
    "secret disclosure",
    "workflow tampering",
    "verification laundering",
    "excessive agency",
    "unbounded consumption",
    "unregistered tool use",
    "github settings drift",
)

LIST_FIELDS = ("required_evidence_terms", "forbidden_terms")


@dataclass(frozen=True)
class ValidationReport:
    """Result of validating the red-team scenario catalog."""

    errors: list[str]
    warnings: list[str]

    @property
    def passed(self) -> bool:
        return not self.errors


def _non_empty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _validate_list_field(scenario: dict[str, Any], field: str, prefix: str, errors: list[str]) -> None:
    values = scenario.get(field)
    if not isinstance(values, list) or not values:
        errors.append(f"{prefix}: {field} must be a non-empty list")
        return
    for index, value in enumerate(values):
        if not _non_empty_string(value):
            errors.append(f"{prefix}: {field}[{index}] must be a non-empty string")


def validate_payload(payload: object) -> ValidationReport:
    """Validate a parsed red-team scenario catalog."""

    errors: list[str] = []
    warnings: list[str] = []
    if not isinstance(payload, dict):
        return ValidationReport(errors=["catalog must be a mapping"], warnings=warnings)

    if payload.get("version") != 1:
        errors.append("version must be 1")

    scenarios = payload.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        return ValidationReport(errors=errors + ["scenarios must be a non-empty list"], warnings=warnings)

    seen_ids: set[str] = set()
    scenario_text: list[str] = []
    for index, scenario in enumerate(scenarios):
        prefix = f"scenarios[{index}]"
        if not isinstance(scenario, dict):
            errors.append(f"{prefix}: scenario must be a mapping")
            continue

        for field in REQUIRED_FIELDS:
            if field not in scenario:
                errors.append(f"{prefix}: missing {field}")

        scenario_id = scenario.get("id")
        if not _non_empty_string(scenario_id):
            errors.append(f"{prefix}: id must be a non-empty string")
        else:
            scenario_id_value = str(scenario_id).strip()
            if scenario_id_value in seen_ids:
                errors.append(f"{prefix}: duplicate id {scenario_id_value!r}")
            else:
                seen_ids.add(scenario_id_value)

        for field in ("title", "risk", "attack_prompt", "expected_safe_behavior"):
            if field in scenario and not _non_empty_string(scenario[field]):
                errors.append(f"{prefix}: {field} must be a non-empty string")

        for field in LIST_FIELDS:
            _validate_list_field(scenario, field, prefix, errors)

        scenario_text.append(" ".join(str(scenario.get(field, "")) for field in ("id", "title", "risk")).lower())

    combined = "\n".join(scenario_text)
    for required in REQUIRED_SCENARIO_RISKS:
        if required not in combined:
            errors.append(f"missing required red-team risk: {required}")

    return ValidationReport(errors=errors, warnings=warnings)


def load_payload(path: Path) -> object:
    """Load a YAML scenario catalog."""

    return yaml.safe_load(path.read_text(encoding="utf-8"))


def validate_file(path: Path) -> ValidationReport:
    """Validate a YAML scenario catalog file."""

    try:
        payload = load_payload(path)
    except OSError as exc:
        return ValidationReport(errors=[f"cannot read scenario catalog: {exc}"], warnings=[])
    except yaml.YAMLError as exc:
        return ValidationReport(errors=[f"invalid YAML: {exc}"], warnings=[])
    return validate_payload(payload)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("catalog", type=Path)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args()

    report = validate_file(args.catalog)
    status = "passed" if report.passed else "failed"
    payload = {"status": status, "errors": report.errors, "warnings": report.warnings}
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for warning in report.warnings:
            print(f"WARNING: {warning}")
        for error in report.errors:
            print(f"ERROR: {error}")
        print(f"Agent red-team validation: {status.upper()} ({len(report.errors)} errors)")
    return 0 if report.passed else 1


if __name__ == "__main__":
    sys.exit(main())
