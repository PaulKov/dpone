"""Offline validation against vendored official dbt manifest schemas."""

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


@dataclass(frozen=True, slots=True)
class DbtManifestSchemaViolation:
    path: str
    rule: str
    severity: str = "error"


class OfficialDbtManifestValidator:
    """Validate one artifact without network access or importing dbt."""

    def validate(
        self,
        payload: Mapping[str, Any],
        *,
        version: int,
    ) -> tuple[DbtManifestSchemaViolation, ...]:
        validator = _validator(version)
        errors: list[DbtManifestSchemaViolation] = []
        warnings: list[DbtManifestSchemaViolation] = []
        for error in sorted(
            validator.iter_errors(payload),
            key=lambda item: tuple(str(part) for part in item.absolute_path),
        ):
            path = _json_path(tuple(error.absolute_path))
            violation = DbtManifestSchemaViolation(
                path=path,
                rule=str(error.validator or "schema"),
                severity=("warning" if _known_sqlserver_macro_extension(error) else "error"),
            )
            (warnings if violation.severity == "warning" else errors).append(violation)
        return tuple((errors + warnings)[:_MAX_REPORTED_VIOLATIONS])


@lru_cache(maxsize=3)
def _validator(version: int) -> jsonschema.protocols.Validator:
    if version not in {10, 11, 12}:
        raise ValueError("unsupported dbt manifest schema version")
    path = _SCHEMA_ROOT / f"manifest-v{version}.schema.json"
    schema = json.loads(path.read_text(encoding="utf-8"))
    validator_type = jsonschema.validators.validator_for(schema)
    validator_type.check_schema(schema)
    return validator_type(schema)


def _known_sqlserver_macro_extension(error: jsonschema.ValidationError) -> bool:
    path = tuple(error.absolute_path)
    if len(path) == 3 and path[0] == "macros" and path[2] == "supported_languages" and error.validator == "anyOf":
        return True
    # dbt Core 1.12 emits per-macro config under the still-v12 manifest
    # envelope. Macro identity, SQL body and dependency closure remain checked
    # independently by the SQL Server authority contract.
    return (
        len(path) == 2
        and path[0] == "macros"
        and error.validator == "additionalProperties"
        and "'config' was unexpected" in error.message
    )


def _json_path(parts: tuple[object, ...]) -> str:
    if not parts:
        return "$"
    return "$." + ".".join(str(part) for part in parts)


__all__ = ["DbtManifestSchemaViolation", "OfficialDbtManifestValidator"]
