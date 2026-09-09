"""Validate dpone agent task contracts."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

TASK_CONTRACT_TEMPLATE = "docs/agent-templates/agent-task-contract.yml"
TASK_CONTRACT_SCHEMA = Path(__file__).resolve().parents[2] / "evals/agent/agent-task-contract.schema.json"
PUBLIC_CONTRACT_LEVELS = {"none", "compatible", "deprecation", "breaking"}
REQUIRED_COMPLETION_STATUSES = ("PASS", "FAIL", "SKIP", "N/A", "UNVERIFIED")
REQUIRED_OUTPUTS = ("implementation", "tests", "documentation_impact", "completion_report")
STANDARD_FORBIDDEN_PATHS = ("pyproject.toml", "uv.lock", "CHANGELOG.md", "mkdocs.yml", ".github/workflows")
REQUIRED_STOP_CONDITIONS = (
    "Required edit falls outside owned_paths.",
    "Approved specification is missing or contradicted.",
    "Public-contract impact is larger than declared.",
    "A live check requires unapproved credentials or environment.",
    "Another writer owns the same semantic contract.",
)
STRING_FIELDS = ("task_id", "title", "goal", "specification", "base_commit", "integrator", "shared_file_owner")
LIST_FIELDS = ("acceptance_criteria", "owned_paths", "read_only_paths", "forbidden_paths", "dependencies")
OPTIONAL_LIST_FIELDS = ("integrator_owned_paths",)
CONCRETE_NON_EMPTY_LISTS = ("acceptance_criteria", "owned_paths", "forbidden_paths")
PLACEHOLDER_VALUES = {"", "DPONE-000", "FILL" + "_ME", "<" + "TODO" + ">"}


@dataclass
class TaskContractValidationResult:
    """Validation result for a template or concrete task contract."""

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def load_contract(path: Path) -> dict[str, Any]:
    """Load a YAML task contract as a mapping."""

    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("contract must be a YAML mapping")
    return data


def validate_file(path: Path, *, template: bool = False) -> TaskContractValidationResult:
    """Validate a YAML task contract file."""

    try:
        payload = load_contract(path)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        return TaskContractValidationResult(errors=[f"{path}: cannot load task contract: {exc}"])
    return validate_payload(payload, label=str(path), template=template)


def validate_payload(
    payload: dict[str, Any],
    *,
    label: str,
    template: bool = False,
) -> TaskContractValidationResult:
    """Validate a parsed task contract payload."""

    result = TaskContractValidationResult()
    if not isinstance(payload, dict):
        result.errors.append(f"{label}: contract must be a mapping")
        return result

    _validate_closed_schema(payload, label, result)
    _validate_schema_version(payload, label, result)
    _validate_string_fields(payload, label, result, template=template)
    _validate_list_fields(payload, label, result, template=template)
    _validate_public_contract_impact(payload, label, result, template=template)
    _validate_required_checks(payload, label, result, template=template)
    _validate_required_outputs(payload, label, result, template=template)
    _validate_completion_statuses(payload, label, result)
    _validate_stop_conditions(payload, label, result)
    _validate_path_boundaries(payload, label, result)
    return result


def _validate_closed_schema(
    payload: dict[str, Any],
    label: str,
    result: TaskContractValidationResult,
) -> None:
    try:
        schema = json.loads(TASK_CONTRACT_SCHEMA.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
    except (OSError, json.JSONDecodeError, SchemaError, TypeError, ValueError) as exc:
        result.errors.append(f"{label}: cannot load task contract schema: {exc}")
        return
    try:
        errors = sorted(
            Draft202012Validator(schema).iter_errors(payload),
            key=lambda item: [str(value) for value in item.path],
        )
    except Exception as exc:  # fail closed at the canonical schema execution boundary
        result.errors.append(f"{label}: cannot execute task contract schema: {exc}")
        return
    for error in errors:
        location = ".".join(str(item) for item in error.absolute_path) or "<root>"
        result.errors.append(f"{label}: schema {location}: {error.message}")


def _validate_schema_version(
    payload: dict[str, Any],
    label: str,
    result: TaskContractValidationResult,
) -> None:
    if payload.get("schema_version") != 1:
        result.errors.append(f"{label}: schema_version must be 1")


def _validate_string_fields(
    payload: dict[str, Any],
    label: str,
    result: TaskContractValidationResult,
    *,
    template: bool,
) -> None:
    for field_name in STRING_FIELDS:
        value = payload.get(field_name)
        if not isinstance(value, str):
            result.errors.append(f"{label}: {field_name} must be a string")
            continue
        if not template and _is_placeholder(value):
            result.errors.append(f"{label}: {field_name} must not be empty or a template placeholder")


def _validate_list_fields(
    payload: dict[str, Any],
    label: str,
    result: TaskContractValidationResult,
    *,
    template: bool,
) -> None:
    for field_name in LIST_FIELDS:
        values = _string_list(payload, field_name, label, result, template=template)
        if not template and field_name in CONCRETE_NON_EMPTY_LISTS and not values:
            result.errors.append(f"{label}: {field_name} must not be empty")
    for field_name in OPTIONAL_LIST_FIELDS:
        if field_name in payload:
            _string_list(payload, field_name, label, result, template=template)


def _validate_public_contract_impact(
    payload: dict[str, Any],
    label: str,
    result: TaskContractValidationResult,
    *,
    template: bool,
) -> None:
    value = payload.get("public_contract_impact")
    if not isinstance(value, dict):
        result.errors.append(f"{label}: public_contract_impact must be a mapping")
        return
    level = value.get("level")
    if level not in PUBLIC_CONTRACT_LEVELS:
        result.errors.append(f"{label}: public_contract_impact.level must be one of {sorted(PUBLIC_CONTRACT_LEVELS)}")
    surfaces = _string_list(value, "surfaces", label, result, template=template, parent="public_contract_impact")
    if not template and level != "none" and not surfaces:
        result.errors.append(f"{label}: public_contract_impact.surfaces must not be empty when level is not none")
    if not isinstance(value.get("migration_required"), bool):
        result.errors.append(f"{label}: public_contract_impact.migration_required must be boolean")


def _validate_required_checks(
    payload: dict[str, Any],
    label: str,
    result: TaskContractValidationResult,
    *,
    template: bool,
) -> None:
    value = payload.get("required_checks")
    if not isinstance(value, dict):
        result.errors.append(f"{label}: required_checks must be a mapping")
        return
    for field_name in ("focused", "broad", "live"):
        checks = _string_list(value, field_name, label, result, template=template, parent="required_checks")
        if not template and field_name in {"focused", "broad"} and not checks:
            result.errors.append(f"{label}: required_checks.{field_name} must not be empty")


def _validate_required_outputs(
    payload: dict[str, Any],
    label: str,
    result: TaskContractValidationResult,
    *,
    template: bool,
) -> None:
    values = _string_list(payload, "required_outputs", label, result, template=template)
    missing = [value for value in REQUIRED_OUTPUTS if value not in values]
    if missing:
        result.errors.append(f"{label}: required_outputs must include {', '.join(missing)}")


def _validate_completion_statuses(
    payload: dict[str, Any],
    label: str,
    result: TaskContractValidationResult,
) -> None:
    value = payload.get("completion_statuses")
    if not isinstance(value, dict):
        result.errors.append(f"{label}: completion_statuses must be a mapping")
        return
    statuses = _string_list(value, "allowed", label, result, template=True, parent="completion_statuses")
    if set(statuses) != set(REQUIRED_COMPLETION_STATUSES):
        result.errors.append(f"{label}: completion_statuses.allowed must be {', '.join(REQUIRED_COMPLETION_STATUSES)}")


def _validate_stop_conditions(
    payload: dict[str, Any],
    label: str,
    result: TaskContractValidationResult,
) -> None:
    values = _string_list(payload, "stop_conditions", label, result, template=True)
    missing = [value for value in REQUIRED_STOP_CONDITIONS if value not in values]
    if missing:
        result.errors.append(f"{label}: stop_conditions must include {', '.join(missing)}")


def _validate_path_boundaries(
    payload: dict[str, Any],
    label: str,
    result: TaskContractValidationResult,
) -> None:
    owned = _normalized_path_values(payload.get("owned_paths"))
    integrator_owned = _normalized_path_values(payload.get("integrator_owned_paths"))
    read_only = _normalized_path_values(payload.get("read_only_paths"))
    forbidden = _normalized_path_values(payload.get("forbidden_paths"))
    missing_forbidden = [value for value in STANDARD_FORBIDDEN_PATHS if _normalize_path(value) not in forbidden]
    if missing_forbidden:
        result.errors.append(f"{label}: forbidden_paths must include {', '.join(missing_forbidden)}")
    _append_path_overlap_errors(label, result, "owned_paths", owned, "read_only_paths", read_only)
    _append_path_overlap_errors(label, result, "owned_paths", owned, "forbidden_paths", forbidden)
    _append_path_overlap_errors(label, result, "owned_paths", owned, "integrator_owned_paths", integrator_owned)
    _append_path_overlap_errors(
        label,
        result,
        "integrator_owned_paths",
        integrator_owned,
        "read_only_paths",
        read_only,
    )
    if integrator_owned and payload.get("integrator") != payload.get("shared_file_owner"):
        result.errors.append(f"{label}: integrator_owned_paths require integrator == shared_file_owner")
    for path in sorted(integrator_owned):
        if not any(path == prefix or path.startswith(f"{prefix}/") for prefix in forbidden):
            result.errors.append(f"{label}: integrator_owned_paths must remain covered by forbidden_paths: {path}")


def _string_list(
    payload: dict[str, Any],
    field_name: str,
    label: str,
    result: TaskContractValidationResult,
    *,
    template: bool,
    parent: str | None = None,
) -> list[str]:
    display_name = f"{parent}.{field_name}" if parent else field_name
    values = payload.get(field_name)
    if not isinstance(values, list):
        result.errors.append(f"{label}: {display_name} must be a list")
        return []
    strings: list[str] = []
    for index, item in enumerate(values):
        if not isinstance(item, str):
            result.errors.append(f"{label}: {display_name}[{index}] must be a string")
            continue
        stripped = item.strip()
        if not template and _is_placeholder(item):
            result.errors.append(f"{label}: {display_name}[{index}] must not be empty or a template placeholder")
            continue
        if stripped:
            strings.append(stripped)
    return strings


def _normalized_path_values(value: Any) -> set[str]:
    if not isinstance(value, list):
        return set()
    paths: set[str] = set()
    for item in value:
        if isinstance(item, str):
            normalized = _normalize_path(item)
            if normalized:
                paths.add(normalized)
    return paths


def _normalize_path(value: str) -> str:
    normalized = value.strip().replace("\\", "/")
    if normalized.endswith("/**"):
        normalized = normalized[:-3]
    return normalized.rstrip("/")


def _append_path_overlap_errors(
    label: str,
    result: TaskContractValidationResult,
    left_name: str,
    left_values: set[str],
    right_name: str,
    right_values: set[str],
) -> None:
    for left in sorted(left_values):
        for right in sorted(right_values):
            if _paths_overlap(left, right):
                result.errors.append(f"{label}: {left_name} overlaps {right_name}: {left} conflicts with {right}")


def _paths_overlap(left: str, right: str) -> bool:
    return left == right or left.startswith(f"{right}/") or right.startswith(f"{left}/")


def _is_placeholder(value: str) -> bool:
    return value.strip() in PLACEHOLDER_VALUES


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contract", type=Path)
    parser.add_argument("--template", action="store_true", help="Allow empty placeholders in the repository template.")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)

    result = validate_file(args.contract, template=args.template)
    payload = {"status": "passed" if result.ok else "failed", "errors": result.errors, "warnings": result.warnings}
    if args.format == "json":
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for warning in result.warnings:
            print(f"WARNING: {warning}")
        for error in result.errors:
            print(f"ERROR: {error}")
        print(
            f"Agent task contract validation: {payload['status'].upper()} "
            f"({len(result.errors)} errors, {len(result.warnings)} warnings)"
        )
    return 0 if result.ok else 1


if __name__ == "__main__":
    sys.exit(main())
