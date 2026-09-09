"""Bounded, secret-redacted diffs for self-service scaffold plans."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.readiness.airflow_pipeline_source import bounded_redacted_unified_diff
from dpone.readiness.airflow_secret_redaction import is_secret_key
from dpone.security_redaction import REDACTION_TOKEN, redact_text

_STRUCTURED_SUFFIXES = frozenset({".json", ".yaml", ".yml"})
_MAX_STRUCTURED_DEPTH = 32
_MAX_STRUCTURED_ITEMS = 4_096
_STRUCTURED_DIFF_LIMITS = BoundedYamlLimits(
    max_bytes=64 * 1024,
    max_tokens=20_000,
    max_depth=_MAX_STRUCTURED_DEPTH,
    max_nodes=_MAX_STRUCTURED_ITEMS,
)


def scaffold_diff(
    existing: str,
    desired: str,
    *,
    path: Path,
    existing_complete: bool,
    fromfile: str,
    tofile: str,
) -> str:
    """Return one bounded diff that never exposes known credential fields."""

    return bounded_redacted_unified_diff(
        _safe_scaffold_content(
            existing,
            path=path,
            complete=existing_complete,
        ),
        _safe_scaffold_content(
            desired,
            path=path,
            complete=True,
        ),
        fromfile=fromfile,
        tofile=tofile,
    )


def _safe_scaffold_content(
    text: str,
    *,
    path: Path,
    complete: bool,
) -> str:
    if not text:
        return ""
    if not complete:
        return _omitted_content(text, reason="bounded content preview")
    if path.suffix.lower() not in _STRUCTURED_SUFFIXES:
        return redact_text(text)
    try:
        payload = load_bounded_yaml(text.encode("utf-8"), limits=_STRUCTURED_DIFF_LIMITS)
    except BoundedYamlError:
        return _omitted_content(text, reason="unparseable structured content")
    return yaml.safe_dump(
        _redact_structured_value(payload),
        sort_keys=True,
        allow_unicode=True,
    )


def _redact_structured_value(value: Any) -> Any:
    seen: set[int] = set()
    remaining = _MAX_STRUCTURED_ITEMS

    def visit(item: Any, *, depth: int) -> Any:
        nonlocal remaining
        if depth > _MAX_STRUCTURED_DEPTH or remaining <= 0:
            return REDACTION_TOKEN
        remaining -= 1
        if isinstance(item, dict):
            identity = id(item)
            if identity in seen:
                return REDACTION_TOKEN
            seen.add(identity)
            redacted: dict[str, Any] = {}
            for key, raw_value in item.items():
                key_text = redact_text(str(key))
                redacted[key_text] = REDACTION_TOKEN if is_secret_key(str(key)) else visit(raw_value, depth=depth + 1)
            return redacted
        if isinstance(item, list | tuple | set):
            identity = id(item)
            if identity in seen:
                return REDACTION_TOKEN
            seen.add(identity)
            return [visit(raw_value, depth=depth + 1) for raw_value in item]
        if isinstance(item, str):
            return redact_text(item)
        return item

    return visit(value, depth=0)


def _omitted_content(text: str, *, reason: str) -> str:
    encoded = text.encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    suffix = "\n... [diff truncated by dpone]\n" if reason == "bounded content preview" else "\n"
    return f"{REDACTION_TOKEN} [{reason} omitted by dpone; bytes={len(encoded)}; sha256:{digest}]{suffix}"


__all__ = ["scaffold_diff"]
