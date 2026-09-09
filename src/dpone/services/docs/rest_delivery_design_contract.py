"""Strict entry point for the governed REST delivery design authority."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from importlib import import_module
from typing import Any

from dpone.manifest.bounded_yaml import BoundedYamlError, load_bounded_yaml

from .rest_delivery_design_contract_authority import (
    DesignContractIssue,
    DesignContractReport,
    build_contract_report,
    validate_authority_semantics,
)
from .rest_delivery_design_contract_state import validate_state_machine_semantics


def validate_design_contract(*, contract_content: bytes, schema_text: str) -> DesignContractReport:
    """Validate bounded YAML, its strict JSON Schema, and cross-field semantics."""

    try:
        loaded = load_bounded_yaml(contract_content)
    except BoundedYamlError as exc:
        return build_contract_report(DesignContractIssue(f"yaml.{exc.code}", "$", str(exc)))
    if not isinstance(loaded, Mapping):
        return build_contract_report(DesignContractIssue("schema.root_type", "$", "Contract root must be a mapping."))

    try:
        schema = json.loads(schema_text, object_pairs_hook=_unique_json_object)
    except (TypeError, ValueError) as exc:
        return build_contract_report(DesignContractIssue("schema.invalid_json", "$schema", str(exc)))

    contract = dict(loaded)
    schema_issues = tuple(_schema_issues(contract, schema))
    if schema_issues:
        return build_contract_report(*schema_issues)
    return build_contract_report(
        *validate_state_machine_semantics(contract),
        *validate_authority_semantics(contract),
    )


def format_design_contract_report(report: DesignContractReport) -> str:
    """Render a stable human-readable validation report."""

    if report.ok:
        return "REST delivery design contract: PASSED"
    lines = [f"REST delivery design contract: FAILED ({len(report.issues)} issue(s))"]
    lines.extend(f"- [{issue.code}] {issue.path}: {issue.message}" for issue in report.issues)
    return "\n".join(lines)


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key in schema: {key}")
        result[key] = value
    return result


def _schema_issues(contract: dict[str, Any], schema: object) -> Iterable[DesignContractIssue]:
    jsonschema = import_module("jsonschema")
    try:
        validator_type = jsonschema.validators.validator_for(schema)
        validator_type.check_schema(schema)
    except jsonschema.exceptions.SchemaError as exc:
        yield DesignContractIssue("schema.invalid_contract_schema", "$schema", exc.message)
        return
    validator = validator_type(schema)
    errors = sorted(validator.iter_errors(contract), key=lambda error: tuple(str(item) for item in error.path))
    for error in errors:
        path = "$" + "".join(f"[{item!r}]" for item in error.path)
        yield DesignContractIssue("schema.validation", path, error.message)


__all__ = [
    "DesignContractIssue",
    "DesignContractReport",
    "format_design_contract_report",
    "validate_design_contract",
]
