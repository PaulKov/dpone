"""Offline validation against the vendored official dbt run-results schema."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import jsonschema

_SCHEMA_ROOT = Path(__file__).resolve().parents[1] / "schema" / "dbt"
_MAX_REPORTED_VIOLATIONS = 20
_SUPPORTED_VERSION = 6


@dataclass(frozen=True, slots=True)
class DbtRunResultsSchemaViolation:
    path: str
    rule: str
    severity: str = "error"


class OfficialDbtRunResultsValidator:
    """Validate one run-results artifact without network access or importing dbt."""

    def validate(
        self,
        payload: Mapping[str, Any],
        *,
        version: int,
    ) -> tuple[DbtRunResultsSchemaViolation, ...]:
        validator = _validator(version)
        violations: list[DbtRunResultsSchemaViolation] = []
        errors = sorted(
            validator.iter_errors(payload),
            key=_error_sort_key,
        )
        for error in errors[:_MAX_REPORTED_VIOLATIONS]:
            violations.append(
                DbtRunResultsSchemaViolation(
                    path=_json_path(tuple(error.absolute_path)),
                    rule=str(error.validator or "schema"),
                )
            )
        return tuple(violations)


@lru_cache(maxsize=1)
def _validator(version: int) -> jsonschema.protocols.Validator:
    if version != _SUPPORTED_VERSION:
        raise ValueError("unsupported dbt run-results schema version")
    path = _SCHEMA_ROOT / f"run-results-v{version}.schema.json"
    schema = json.loads(path.read_text(encoding="utf-8"))
    validator_type = jsonschema.validators.validator_for(schema)
    validator_type.check_schema(schema)
    return validator_type(schema)


def _error_sort_key(error: jsonschema.ValidationError) -> tuple[tuple[str, ...], str, str]:
    return (
        tuple(str(part) for part in error.absolute_path),
        str(error.validator or "schema"),
        error.message,
    )


def _json_path(parts: tuple[object, ...]) -> str:
    if not parts:
        return "$"
    return "$." + ".".join(str(part) for part in parts)


__all__ = ["DbtRunResultsSchemaViolation", "OfficialDbtRunResultsValidator"]
