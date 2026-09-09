"""Columnar execution policy resolver shared by columnar routes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ColumnarExecutionPolicy:
    value: str
    requested: str
    deprecated_alias: str | None = None
    cleanup_policy: str = "eager"

    @property
    def is_chunked(self) -> bool:
        return self.value == "chunked"

    @property
    def is_file(self) -> bool:
        return self.value == "file"

    @property
    def is_streaming(self) -> bool:
        return self.value == "streaming"


ColumnarDirectPushExecutionMode = ColumnarExecutionPolicy


def resolve_columnar_execution_policy(options: Any) -> ColumnarExecutionPolicy:
    values = dict(options or {}) if isinstance(options, Mapping) else {}
    direct_options = _mapping(values.get("direct_push"))
    execution_options = _mapping(values.get("execution"))
    canonical_requested = _first_text(execution_options.get("mode"), values.get("execution_mode"))
    legacy_requested = _first_text(direct_options.get("mode"), direct_options.get("execution_mode"))
    canonical = _resolve(canonical_requested) if canonical_requested is not None else None
    legacy = _resolve(legacy_requested) if legacy_requested is not None else None
    if canonical is not None and legacy is not None and canonical != legacy:
        raise RuntimeError(f"columnar_execution_mode_conflict:{canonical}:{legacy}")
    selected = canonical or legacy or "chunked"
    requested = canonical_requested or legacy_requested or selected
    deprecated = requested if _is_deprecated(requested) else None
    cleanup_policy = str(
        execution_options.get("cleanup_policy")
        or direct_options.get("cleanup_policy")
        or values.get("cleanup_policy")
        or "eager"
    )
    return ColumnarExecutionPolicy(
        value=selected,
        requested=requested,
        deprecated_alias=deprecated,
        cleanup_policy=cleanup_policy,
    )


def resolve_columnar_direct_push_execution_mode(options: Any) -> ColumnarDirectPushExecutionMode:
    return resolve_columnar_execution_policy(options)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _first_text(*values: Any) -> str | None:
    for value in values:
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    return None


def _resolve(value: str) -> str:
    normalized = value.lower().replace("-", "_")
    canonical = _ALIASES.get(normalized, normalized)
    if canonical not in {"chunked", "file", "streaming"}:
        raise RuntimeError(f"columnar_execution_mode_unsupported:{canonical}")
    return canonical


def _is_deprecated(value: str) -> bool:
    return value.lower().replace("-", "_") in _DEPRECATED_ALIASES


_ALIASES = {
    "pipeline": "chunked",
    "pipelined": "chunked",
    "chunk_pipeline": "chunked",
    "chunked_file": "chunked",
    "chunked_files": "chunked",
    "chunks": "chunked",
    "two_phase": "file",
    "spooled": "file",
    "spooled_file": "file",
    "spooled_files": "file",
    "manifest": "file",
    "local_manifest": "file",
    "stream": "streaming",
}
_DEPRECATED_ALIASES = {"pipeline", "pipelined", "two_phase", "spooled", "manifest", "local_manifest"}


__all__ = [
    "ColumnarDirectPushExecutionMode",
    "ColumnarExecutionPolicy",
    "resolve_columnar_direct_push_execution_mode",
    "resolve_columnar_execution_policy",
]
