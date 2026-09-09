"""File-IO facade for schema migration bundle diffs."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class MigrationBundleDiffFacade:
    """Loads bundle artifacts and builds provider-neutral diff evidence."""

    def diff(
        self,
        *,
        base_path: str,
        head_path: str,
        base_gate_path: str | None = None,
        head_gate_path: str | None = None,
        require_attestation: bool = False,
    ) -> dict[str, Any]:
        base_bundle = _read_json_object(base_path)
        head_bundle = _read_json_object(head_path)
        base_bytes = _artifact_bytes(base_bundle, base_path)
        head_bytes = _artifact_bytes(head_bundle, head_path)
        verifier = _bundle_verifier()
        diff_module = import_module("dpone.readiness.schema_migration_bundle_diff")
        inputs = diff_module.MigrationBundleDiffInputs(
            base_bundle=base_bundle,
            head_bundle=head_bundle,
            base_artifacts=_artifact_payloads(base_bundle, base_bytes),
            head_artifacts=_artifact_payloads(head_bundle, head_bytes),
            base_verification=verifier.verify(
                bundle=base_bundle,
                artifact_bytes=base_bytes,
                require_attestation=require_attestation,
            ),
            head_verification=verifier.verify(
                bundle=head_bundle,
                artifact_bytes=head_bytes,
                require_attestation=require_attestation,
            ),
            base_gate=_read_json_object(base_gate_path) if base_gate_path else None,
            head_gate=_read_json_object(head_gate_path) if head_gate_path else None,
        )
        return diff_module.MigrationBundleDiffBuilder().build(
            inputs=inputs,
            options=diff_module.MigrationBundleDiffOptions(require_attestation=require_attestation),
        )


def _artifact_bytes(bundle: Mapping[str, Any], bundle_path: str) -> dict[str, bytes]:
    loaded: dict[str, bytes] = {}
    for artifact in bundle.get("artifacts", []):
        if not isinstance(artifact, Mapping) or not artifact.get("path"):
            continue
        path = str(artifact["path"])
        try:
            loaded[path] = _read_bytes(path, bundle_path=bundle_path)
        except FileNotFoundError:
            continue
    return loaded


def _artifact_payloads(bundle: Mapping[str, Any], artifact_bytes: Mapping[str, bytes]) -> dict[str, Mapping[str, Any]]:
    payloads: dict[str, Mapping[str, Any]] = {}
    for artifact in bundle.get("artifacts", []):
        if not isinstance(artifact, Mapping):
            continue
        path = str(artifact.get("path", ""))
        content = artifact_bytes.get(path)
        if content is None:
            continue
        try:
            text = content.decode("utf-8")
            payload = json.loads(text) if path.lower().endswith(".json") else yaml.safe_load(text)
        except (ValueError, yaml.YAMLError):
            continue
        if isinstance(payload, Mapping):
            payloads[str(artifact.get("kind"))] = payload
    return payloads


def _read_bytes(path: str, *, bundle_path: str) -> bytes:
    raw = Path(path)
    if raw.is_absolute() or raw.exists():
        return raw.read_bytes()
    return (Path(bundle_path).parent / raw).read_bytes()


def _read_json_object(path: str | Path) -> dict[str, Any]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return raw


def _bundle_verifier() -> Any:
    return import_module("dpone.readiness.schema_migration_bundle").MigrationBundleVerifier()


__all__ = ["MigrationBundleDiffFacade"]
