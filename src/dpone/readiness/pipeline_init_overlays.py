"""Strict, allowlisted overrides for built-in beginner recipes."""

from __future__ import annotations

import copy
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import (
    ConfinedFileError,
    project_relative_path,
    read_confined_file_snapshot,
)
from dpone.manifest.domain_identity import DomainId, DomainIdError

_LOCATOR_COMPONENT = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}$")
_ANSWERS_LIMITS = BoundedYamlLimits(max_bytes=64 * 1024, max_tokens=4_000, max_depth=16, max_nodes=2_000)
_ALLOWED_ANSWER_KEYS = frozenset(
    {
        "domain",
        "source_connection_ref",
        "sink_connection_ref",
        "source_schema",
        "source_table",
        "target_schema",
        "target_table",
        "unique_key",
    }
)


class PipelineInitOverlayError(ValueError):
    """A concise locator or answers file violates the beginner contract."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class BuiltinDefaultsResolution:
    """Resolved built-in values and exact project inputs consumed to produce them."""

    values: Mapping[str, Any]
    consumed_files: Mapping[str, str]


def parse_table_locator(value: str) -> tuple[str, str, str]:
    """Parse ``connection_ref:schema.table`` without accepting ambiguous forms."""

    if not isinstance(value, str) or value.count(":") != 1:
        raise PipelineInitOverlayError(
            "DPONE_PIPELINE_LOCATOR_INVALID",
            "Table locator must use connection_ref:schema.table.",
        )
    connection_ref, table_ref = value.split(":", 1)
    if table_ref.count(".") != 1:
        raise PipelineInitOverlayError(
            "DPONE_PIPELINE_LOCATOR_INVALID",
            "Table locator must use connection_ref:schema.table.",
        )
    schema, table = table_ref.split(".", 1)
    values = (connection_ref.strip(), schema.strip(), table.strip())
    if any(_LOCATOR_COMPONENT.fullmatch(item) is None for item in values):
        raise PipelineInitOverlayError(
            "DPONE_PIPELINE_LOCATOR_INVALID",
            "Locator components must use letters, digits, underscore, or hyphen.",
        )
    return values


def resolve_builtin_defaults(
    defaults: Mapping[str, Any],
    *,
    root: Path,
    answers: Path | None,
    domain: str | None,
    from_locator: str | None,
    to_locator: str | None,
    unique_key: str | None,
) -> BuiltinDefaultsResolution:
    """Apply defaults < answers < explicit CLI values with one allowlist."""

    resolved = copy.deepcopy(dict(defaults))
    consumed_files: dict[str, str] = {}
    if answers is not None:
        answer_values, answer_path, answer_sha256 = _load_answers(root, answers)
        resolved.update(answer_values)
        consumed_files[answer_path] = answer_sha256
    if domain is not None:
        resolved["domain"] = domain
    if from_locator is not None:
        connection_ref, schema, table = parse_table_locator(from_locator)
        resolved.update(
            {
                "source_connection_ref": connection_ref,
                "source_schema": schema,
                "source_table": table,
            }
        )
    if to_locator is not None:
        connection_ref, schema, table = parse_table_locator(to_locator)
        resolved.update(
            {
                "sink_connection_ref": connection_ref,
                "target_schema": schema,
                "target_table": table,
            }
        )
    if unique_key is not None:
        key = unique_key.strip()
        if _LOCATOR_COMPONENT.fullmatch(key) is None:
            raise PipelineInitOverlayError(
                "DPONE_PIPELINE_KEY_INVALID",
                "Unique key must use letters, digits, underscore, or hyphen.",
            )
        resolved["unique_key"] = key
    try:
        resolved["domain"] = str(DomainId.parse(str(resolved["domain"])))
    except DomainIdError as exc:
        raise PipelineInitOverlayError(
            "DPONE_DOMAIN_ID_INVALID",
            "Domain must use the canonical project identifier syntax.",
        ) from exc
    return BuiltinDefaultsResolution(
        values=resolved,
        consumed_files=dict(sorted(consumed_files.items())),
    )


def _load_answers(root: Path, answers: Path) -> tuple[dict[str, Any], str, str]:
    try:
        relative = project_relative_path(root, answers)
        snapshot = read_confined_file_snapshot(root, relative, max_bytes=_ANSWERS_LIMITS.max_bytes)
        payload = load_bounded_yaml(snapshot.content, limits=_ANSWERS_LIMITS)
    except (ConfinedFileError, BoundedYamlError) as exc:
        code = getattr(exc, "code", "answers_invalid")
        raise PipelineInitOverlayError(
            "DPONE_RECIPE_ANSWERS_INVALID",
            f"Answers must be a bounded YAML object inside the project ({code}).",
        ) from exc
    if not isinstance(payload, Mapping):
        raise PipelineInitOverlayError(
            "DPONE_RECIPE_ANSWERS_INVALID",
            "Answers must be a YAML object.",
        )
    unknown = sorted(str(key) for key in payload if key not in _ALLOWED_ANSWER_KEYS)
    if unknown:
        raise PipelineInitOverlayError(
            "DPONE_RECIPE_ANSWERS_FIELD_FORBIDDEN",
            f"Built-in answers contain unsupported fields: {', '.join(unknown)}.",
        )
    result: dict[str, Any] = {}
    for raw_key, raw_value in payload.items():
        key = str(raw_key)
        if raw_value is None and key == "unique_key":
            result[key] = None
            continue
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise PipelineInitOverlayError(
                "DPONE_RECIPE_ANSWERS_INVALID",
                f"Built-in answer {key} must be a non-empty string.",
            )
        value = raw_value.strip()
        if _LOCATOR_COMPONENT.fullmatch(value) is None:
            raise PipelineInitOverlayError(
                "DPONE_RECIPE_ANSWERS_INVALID",
                f"Built-in answer {key} has an unsupported value.",
            )
        result[key] = value
    return result, relative, snapshot.sha256


__all__ = [
    "BuiltinDefaultsResolution",
    "PipelineInitOverlayError",
    "parse_table_locator",
    "resolve_builtin_defaults",
]
