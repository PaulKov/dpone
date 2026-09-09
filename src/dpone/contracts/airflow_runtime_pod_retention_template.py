"""Canonical identity for one rendered runtime Pod retention Job template."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

RUNTIME_POD_RETENTION_TEMPLATE_SHA_ANNOTATION = "dpone.dev/runtime-pod-retention-template-sha256"
_TEMPLATE_SHA_PATHS = (
    ("metadata", "annotations", RUNTIME_POD_RETENTION_TEMPLATE_SHA_ANNOTATION),
    ("spec", "template", "metadata", "annotations", RUNTIME_POD_RETENTION_TEMPLATE_SHA_ANNOTATION),
)


def runtime_pod_retention_template_sha256(template: Mapping[str, object]) -> str:
    """Hash every rendered template field except the self-referential digest."""

    canonical = json.dumps(
        _without_digest(template),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def verify_runtime_pod_retention_template_identity(template: Mapping[str, object]) -> str:
    """Require the exact digest mirrors and return their verified identity."""

    expected = runtime_pod_retention_template_sha256(template)
    actual_paths = tuple(_digest_paths(template))
    if set(actual_paths) != set(_TEMPLATE_SHA_PATHS) or len(actual_paths) != len(_TEMPLATE_SHA_PATHS):
        raise ValueError("runtime Pod retention template digest mirrors are invalid")
    if any(_value_at(template, path) != expected for path in _TEMPLATE_SHA_PATHS):
        raise ValueError("runtime Pod retention template digest value does not match its template")
    return expected


def _without_digest(value: object, *, path: tuple[object, ...] = ()) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _without_digest(item, path=(*path, str(key)))
            for key, item in value.items()
            if (*path, str(key)) not in _TEMPLATE_SHA_PATHS
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_without_digest(item, path=(*path, index)) for index, item in enumerate(value)]
    return value


def _digest_paths(value: object, *, path: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
    paths: list[tuple[object, ...]] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            item_path = (*path, str(key))
            if str(key) == RUNTIME_POD_RETENTION_TEMPLATE_SHA_ANNOTATION:
                paths.append(item_path)
            paths.extend(_digest_paths(item, path=item_path))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            paths.extend(_digest_paths(item, path=(*path, index)))
    return paths


def _value_at(value: object, path: tuple[object, ...]) -> object:
    current = value
    for key in path:
        if not isinstance(current, Mapping) or not isinstance(key, str):
            raise ValueError("runtime Pod retention template digest path is invalid")
        current = current.get(key)
    return current


__all__ = [
    "RUNTIME_POD_RETENTION_TEMPLATE_SHA_ANNOTATION",
    "runtime_pod_retention_template_sha256",
    "verify_runtime_pod_retention_template_identity",
]
