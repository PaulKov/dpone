"""File-IO facade for schema migration trust provenance."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.migration_control import read_json_object
from dpone.readiness.schema_migration_bundle import MigrationBundleVerifier
from dpone.readiness.schema_migration_trust import (
    MigrationTrustAttestor,
    MigrationTrustPolicy,
    MigrationTrustVerifier,
)


class MigrationTrustFacade:
    """Thin CLI facade for bundle provenance and trust verification."""

    def attest(
        self,
        *,
        bundle_path: str,
        output_path: str,
        provenance_source: str,
        repository: str,
        commit_sha: str,
        ref: str,
        signing_key_env: str | None = None,
        signing_key_id: str = "local-hmac",
        run_id: str | None = None,
        workflow: str | None = None,
        actor: str | None = None,
        run_url: str | None = None,
        builder_id: str = "dpone.local",
        protected_ref: bool = False,
    ) -> dict[str, Any]:
        bundle = read_json_object(bundle_path)
        verification = MigrationBundleVerifier().verify(
            bundle=bundle,
            artifact_bytes=_artifact_bytes(bundle, bundle_path),
            require_attestation=True,
        )
        signing_key, key_blocker = _optional_env(signing_key_env)
        payload = MigrationTrustAttestor().attest(
            bundle=bundle,
            verification=verification,
            provenance_source=provenance_source,
            repository=repository,
            commit_sha=commit_sha,
            ref=ref,
            run_id=run_id,
            workflow=workflow,
            actor=actor,
            run_url=run_url,
            builder_id=builder_id,
            protected_ref=protected_ref,
            signing_key=signing_key,
            signing_key_id=signing_key_id,
        )
        if key_blocker:
            payload["status"] = "blocked"
            payload["blockers"] = list(dict.fromkeys([*payload.get("blockers", []), key_blocker]))
        _write_json(Path(output_path), payload)
        return payload

    def verify(
        self,
        *,
        bundle_path: str,
        provenance_path: str,
        policy_path: str | None = None,
        signing_key_env: str | None = None,
        external_attestation_paths: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        bundle = read_json_object(bundle_path)
        provenance = read_json_object(provenance_path)
        verification = MigrationBundleVerifier().verify(
            bundle=bundle,
            artifact_bytes=_artifact_bytes(bundle, bundle_path),
            require_attestation=True,
        )
        try:
            policy = MigrationTrustPolicy.from_mapping(_read_mapping(policy_path) if policy_path else None)
        except ValueError as exc:
            return _policy_error(str(exc))
        signing_key, key_blocker = _optional_env(signing_key_env)
        payload = MigrationTrustVerifier().verify(
            bundle=bundle,
            verification=verification,
            provenance=provenance,
            policy=policy,
            signing_key=signing_key,
            external_attestations=tuple(_read_mapping(path) for path in external_attestation_paths),
        )
        if key_blocker and policy.require_signed_provenance:
            payload["status"] = "blocked"
            payload["blockers"] = list(dict.fromkeys([*payload.get("blockers", []), key_blocker]))
        return payload


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


def _read_bytes(path: str, *, bundle_path: str) -> bytes:
    raw = Path(path)
    if raw.is_absolute() or raw.exists():
        return raw.read_bytes()
    return (Path(bundle_path).parent / raw).read_bytes()


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(text) if path.lower().endswith(".json") else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _optional_env(name: str | None) -> tuple[str | None, str | None]:
    if not name:
        return None, None
    value = os.environ.get(name)
    if value:
        return value, None
    return None, f"migration_trust.signing_key_env_missing:{name}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n", encoding="utf-8")


def _policy_error(message: str) -> dict[str, Any]:
    blocker = f"migration_trust.policy_invalid:{message}"
    return {
        "schema_version": "dpone.schema_migration_trust_verification.v1",
        "status": "blocked",
        "bundle_id": None,
        "pack_id": None,
        "provenance_id": None,
        "checks": [{"name": "policy", "status": "blocked", "details": [blocker]}],
        "blockers": [blocker],
        "warnings": [],
        "recommendations": ["Fix the trust policy file before using it as protected CI evidence."],
    }


__all__ = ["MigrationTrustFacade"]
