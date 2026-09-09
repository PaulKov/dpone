"""Build and self-digest ``dpone.release-receipt-envelope.v2`` objects."""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


canonical = _load_sibling("dpone_agent_release_canonical", "release_canonical.py")

SCHEMA = "dpone.release-receipt-envelope.v2"
SCHEMA_VERSION = 2


def build_receipt_envelope(
    *,
    receipt_type: str,
    stream: Mapping[str, Any],
    scope: Mapping[str, Any],
    attempt: Mapping[str, Any],
    lease: Mapping[str, Any],
    producer: Mapping[str, Any],
    payload: Mapping[str, Any],
    observed_at: str,
    committed_at: str,
) -> dict[str, Any]:
    """Return a schema-shaped envelope with ``receipt_id`` and ``payload_sha256``."""

    body: dict[str, Any] = {
        "schema": SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "receipt_type": receipt_type,
        "stream": dict(stream),
        "scope": dict(scope),
        "attempt": dict(attempt),
        "lease": dict(lease),
        "producer": dict(producer),
        "timestamps": {"observed_at": observed_at, "committed_at": committed_at},
        "payload": dict(payload),
        "payload_sha256": canonical.sha256_id("dpone.release.payload.v2", dict(payload)),
    }
    body["receipt_id"] = receipt_id_for(body)
    return body


def receipt_id_for(envelope: Mapping[str, Any]) -> str:
    """Content identity over envelope bytes with ``receipt_id`` omitted."""

    material = {key: value for key, value in envelope.items() if key != "receipt_id"}
    return f"sha256:{canonical.sha256_hex(material)}"


def envelope_bytes(envelope: Mapping[str, Any]) -> bytes:
    """Canonical bytes for durable append."""

    return canonical.canonical_json_bytes(dict(envelope))
