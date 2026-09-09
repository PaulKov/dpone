"""File-IO facade for schema migration bundle policy gates."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.migration_control import MigrationPack, read_json_object
from dpone.readiness.schema_migration_bundle import MigrationBundleVerifier
from dpone.readiness.schema_migration_bundle_policy import (
    BUNDLE_GATE_SCHEMA,
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
    policy_error_decision,
)


class MigrationBundleGateFacade:
    """Application facade for offline bundle gate receipts."""

    def gate(
        self,
        *,
        bundle_path: str,
        profile: str | None = None,
        policy_path: str | None = None,
        target_environment: str | None = None,
        trust_verification_path: str | None = None,
    ) -> dict[str, Any]:
        bundle = read_json_object(bundle_path)
        policy_payload = _read_mapping(policy_path) if policy_path else None
        try:
            policy = MigrationBundlePolicyOptions.resolve(
                profile=profile,
                policy_payload=policy_payload,
                target_environment=target_environment,
            )
        except ValueError as exc:
            return policy_error_decision(profile=profile or _policy_profile(policy_payload), message=str(exc))
        artifact_bytes = _artifact_bytes(bundle, bundle_path)
        verification = MigrationBundleVerifier().verify(
            bundle=bundle,
            artifact_bytes=artifact_bytes,
            require_attestation=policy.require_attestation,
        )
        return MigrationBundlePolicyEvaluator().evaluate(
            bundle=bundle,
            verification=verification,
            policy=policy,
            artifact_payloads=_artifact_payloads(bundle, artifact_bytes),
            trust_verification=read_json_object(trust_verification_path) if trust_verification_path else None,
        )


def bundle_gate_apply_blockers(*, pack: MigrationPack, gate_path: str | None) -> tuple[str, ...]:
    if not gate_path:
        return ()
    gate = read_json_object(gate_path)
    blockers: list[str] = []
    if gate.get("schema_version") != BUNDLE_GATE_SCHEMA:
        blockers.append("migration_bundle_gate.invalid_schema")
    if gate.get("pack_id") != pack.pack_id:
        blockers.append("migration_bundle_gate.pack_id_mismatch")
    if gate.get("status") not in {"allowed", "warning"}:
        blockers.append("migration_bundle_gate.not_allowed")
        blockers.extend(str(item) for item in gate.get("blockers", []) if str(item))
    return tuple(dict.fromkeys(blockers))


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


def _read_mapping(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(text) if path.lower().endswith(".json") else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _policy_profile(payload: Mapping[str, Any] | None) -> str | None:
    value = payload.get("profile") if payload else None
    return str(value) if value else None


__all__ = ["MigrationBundleGateFacade", "bundle_gate_apply_blockers"]
