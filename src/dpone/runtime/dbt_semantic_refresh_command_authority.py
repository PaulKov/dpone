"""Protected scope-map and command authority for semantic-refresh dbt."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

_SCOPE_MAP_SCHEMA = "dpone.semantic-refresh-mssql-scope-map.v1"
_SCOPE_MAP_FIELDS = {
    "authorities",
    "models",
    "schema",
    "scope_map_sha256",
    "signature_sha256",
    "verification_status",
}


class SemanticRefreshDbtScopeMapLoaderPort(Protocol):
    """Load the complete protected attempt authority for one dbt invocation."""

    def load(
        self,
        *,
        workflow_execution_binding_sha256: str,
        model_unique_ids: tuple[str, ...],
    ) -> Mapping[str, object]: ...


@dataclass(frozen=True, slots=True)
class SemanticRefreshDbtExecutionVariables:
    """Exact interval plus protected scope-map variables consumed by the macro."""

    start: str
    end: str
    scope_map: Mapping[str, object]
    statement_timeout_seconds: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.statement_timeout_seconds, bool)
            or not isinstance(self.statement_timeout_seconds, int)
            or self.statement_timeout_seconds <= 0
        ):
            raise ValueError("semantic-refresh statement timeout must be positive")

    def dbt_vars_json(self) -> str:
        """Return deterministic dbt variables for parse, selection, and build."""

        return _canonical_json(
            {
                "dpone_data_interval_end": self.end,
                "dpone_data_interval_start": self.start,
                "dpone_semantic_refresh_scope_map": self.scope_map,
                "dpone_semantic_refresh_statement_timeout_seconds": (self.statement_timeout_seconds),
            }
        )


class SemanticRefreshDbtCommandRunner:
    """Force exact V2 selection and variables around a shell-free runner."""

    def __init__(self, delegate: Any, *, expected_vars_json: str) -> None:
        self._delegate = delegate
        self._expected_vars_json = expected_vars_json

    def run(
        self,
        args: tuple[str, ...],
        *,
        cwd: Path,
        timeout_seconds: int,
        redactions: tuple[str, ...],
    ) -> Any:
        command = _semantic_command(args, expected_vars_json=self._expected_vars_json)
        return self._delegate.run(
            command,
            cwd=cwd,
            timeout_seconds=timeout_seconds,
            redactions=redactions,
        )


def load_semantic_refresh_scope_map(
    loader: SemanticRefreshDbtScopeMapLoaderPort,
    *,
    workflow_execution_binding_sha256: str,
    model_unique_ids: tuple[str, ...],
) -> Mapping[str, object]:
    """Load and locally close the protected scope-map before any dbt process."""

    _require_digest(workflow_execution_binding_sha256, "workflow execution binding")
    expected_models = _model_ids(model_unique_ids)
    value = loader.load(
        workflow_execution_binding_sha256=workflow_execution_binding_sha256,
        model_unique_ids=expected_models,
    )
    if not isinstance(value, Mapping) or set(value) != _SCOPE_MAP_FIELDS:
        raise ValueError("semantic-refresh scope map fields are not closed")
    if value.get("schema") != _SCOPE_MAP_SCHEMA or value.get("verification_status") != "VERIFIED":
        raise ValueError("semantic-refresh scope map is not verified")
    models = _mapping(value.get("models"), "models")
    authorities = _mapping(value.get("authorities"), "authorities")
    if tuple(sorted(models)) != expected_models or tuple(sorted(authorities)) != expected_models:
        raise ValueError("semantic-refresh scope map model closure differs")
    for model_id in expected_models:
        strategy = _mapping(models[model_id], model_id)
        raw_authority = authorities[model_id]
        if (
            strategy.get("model_unique_id") != model_id
            or strategy.get("workflow_execution_binding_sha256") != workflow_execution_binding_sha256
            or not isinstance(raw_authority, str)
            or _canonical_json(strategy) != raw_authority
        ):
            raise ValueError("semantic-refresh scope map authority differs")
    unsigned = {
        "authorities": authorities,
        "models": models,
        "schema": _SCOPE_MAP_SCHEMA,
    }
    expected_sha = "sha256:" + hashlib.sha256(_canonical_json(unsigned).encode()).hexdigest()
    if value.get("scope_map_sha256") != expected_sha:
        raise ValueError("semantic-refresh scope map digest differs")
    _require_digest(value.get("signature_sha256"), "scope map authority receipt")
    return json.loads(_canonical_json(value), object_pairs_hook=_unique_object)


def _semantic_command(args: tuple[str, ...], *, expected_vars_json: str) -> tuple[str, ...]:
    command = list(args)
    try:
        variables_at = command.index("--vars")
    except ValueError as exc:
        raise ValueError("semantic-refresh dbt command is missing protected variables") from exc
    if variables_at + 1 >= len(command) or command[variables_at + 1] != expected_vars_json:
        raise ValueError("semantic-refresh dbt command variables differ from protected authority")
    if "compile" in command or "ls" in command or "build" in command:
        try:
            indirect_at = command.index("--indirect-selection")
        except ValueError as exc:
            raise ValueError("semantic-refresh dbt command is missing exact selection policy") from exc
        command[indirect_at + 1] = "empty"
    return tuple(command)


def _model_ids(value: object) -> tuple[str, ...]:
    if (
        not isinstance(value, tuple)
        or not value
        or value != tuple(sorted(set(value)))
        or any(not isinstance(item, str) or not item.startswith("model.") for item in value)
    ):
        raise ValueError("semantic-refresh model closure is not canonical")
    return value


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"semantic-refresh {field} must be an object")
    return value


def _require_digest(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 71
        or not value.startswith("sha256:")
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"semantic-refresh {field} must be a canonical digest")
    return value


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _unique_object(items: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


__all__ = [
    "SemanticRefreshDbtCommandRunner",
    "SemanticRefreshDbtExecutionVariables",
    "SemanticRefreshDbtScopeMapLoaderPort",
    "load_semantic_refresh_scope_map",
]
