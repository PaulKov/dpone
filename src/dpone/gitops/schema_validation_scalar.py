from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from dpone.gitops.models import GitOpsIssue
from dpone.gitops.schema_validation_primitives import (
    format_expected_type,
    format_name,
    matches_expected_type,
    matches_format,
)


def validate_scalar_constraints(
    value: object,
    schema: Mapping[str, Any],
    *,
    path: str,
    source: str,
) -> tuple[list[GitOpsIssue], bool]:
    issues: list[GitOpsIssue] = []
    expected_type = schema.get("type")
    if not matches_expected_type(value, expected_type):
        return [
            GitOpsIssue(
                code="schema_type_mismatch",
                message=f"Field {_display_path(path)} must be {format_expected_type(expected_type)}",
                path=_display_path(path),
                source=source,
            )
        ], True

    expected_const = schema.get("const")
    if expected_const is not None and value != expected_const:
        issues.append(
            GitOpsIssue(
                code="schema_const_mismatch",
                message=f"Field {_display_path(path)} must be {expected_const!r}",
                path=_display_path(path),
                source=source,
            )
        )

    enum_values = schema.get("enum")
    if isinstance(enum_values, list) and value not in enum_values:
        issues.append(
            GitOpsIssue(
                code="schema_enum_mismatch",
                message=f"Field {_display_path(path)} must be one of the allowed values",
                path=_display_path(path),
                source=source,
            )
        )

    min_length = schema.get("minLength")
    failed_min_length = isinstance(min_length, int) and isinstance(value, str) and len(value) < min_length
    if failed_min_length:
        issues.append(
            GitOpsIssue(
                code="schema_min_length_violation",
                message=f"Field {_display_path(path)} is shorter than {min_length} characters",
                path=_display_path(path),
                source=source,
            )
        )

    max_length = schema.get("maxLength")
    if isinstance(max_length, int) and isinstance(value, str) and len(value) > max_length:
        issues.append(
            GitOpsIssue(
                code="schema_max_length_violation",
                message=f"Field {_display_path(path)} is longer than {max_length} characters",
                path=_display_path(path),
                source=source,
            )
        )

    expected_format = schema.get("format")
    if not failed_min_length and not matches_format(value, expected_format):
        issues.append(
            GitOpsIssue(
                code="schema_format_mismatch",
                message=f"Field {_display_path(path)} must match {format_name(expected_format)} format",
                path=_display_path(path),
                source=source,
            )
        )

    minimum = schema.get("minimum")
    if isinstance(minimum, (int, float)) and isinstance(value, (int, float)) and not isinstance(value, bool):
        if value < minimum:
            issues.append(
                GitOpsIssue(
                    code="schema_minimum_violation",
                    message=f"Field {_display_path(path)} must be greater than or equal to {minimum}",
                    path=_display_path(path),
                    source=source,
                )
            )

    maximum = schema.get("maximum")
    if isinstance(maximum, (int, float)) and isinstance(value, (int, float)) and not isinstance(value, bool):
        if value > maximum:
            issues.append(
                GitOpsIssue(
                    code="schema_maximum_violation",
                    message=f"Field {_display_path(path)} must be less than or equal to {maximum}",
                    path=_display_path(path),
                    source=source,
                )
            )

    pattern = schema.get("pattern")
    if (
        isinstance(pattern, str)
        and isinstance(value, str)
        and not failed_min_length
        and re.search(pattern, value) is None
    ):
        issues.append(
            GitOpsIssue(
                code="schema_pattern_mismatch",
                message=f"Field {_display_path(path)} does not match the required pattern",
                path=_display_path(path),
                source=source,
            )
        )

    not_schema = schema.get("not")
    if isinstance(not_schema, Mapping) and _matches_schema_fragment(value, not_schema):
        issues.append(
            GitOpsIssue(
                code="schema_validation_failed",
                message=f"Field {_display_path(path)} matches a forbidden schema rule",
                path=_display_path(path),
                source=source,
            )
        )

    return issues, False


def _matches_schema_fragment(value: object, schema: Mapping[str, Any]) -> bool:
    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        return any(isinstance(fragment, Mapping) and _matches_schema_fragment(value, fragment) for fragment in any_of)

    required = schema.get("required")
    if isinstance(required, list | tuple):
        return isinstance(value, Mapping) and all(isinstance(field, str) and field in value for field in required)

    pattern = schema.get("pattern")
    if isinstance(pattern, str) and isinstance(value, str):
        return re.search(pattern, value) is not None
    return False


def _display_path(path: str) -> str:
    return path or "$"
