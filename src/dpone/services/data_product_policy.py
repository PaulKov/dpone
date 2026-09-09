"""File-IO facade for data product policy commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductPolicyFacade:
    """Thin CLI facade; policy decisions live in provider-neutral readiness modules."""

    def evaluate(
        self,
        *,
        manifest_path: str,
        bundle_path: str | None = None,
        evidence_dir: str | None = None,
    ) -> dict[str, Any]:
        bundle = _read_optional(bundle_path)
        return (
            _policy()
            .DataProductPolicyEvaluator()
            .evaluate(
                manifest=_read_mapping(manifest_path),
                evidence=_evidence(bundle_path=bundle_path, bundle=bundle, evidence_dir=evidence_dir),
                pack_id=bundle.get("pack_id") if bundle else None,
                bundle_id=bundle.get("bundle_id") if bundle else None,
            )
        )

    def waiver_request(
        self,
        *,
        evaluation_path: str,
        rule_id: str,
        reason: str,
        expires_at: str,
        requested_by: str | None = None,
    ) -> dict[str, Any]:
        return (
            _waivers()
            .WaiverRequestBuilder()
            .request(
                evaluation=_read_mapping(evaluation_path),
                rule_id=rule_id,
                reason=reason,
                expires_at=expires_at,
                requested_by=requested_by,
            )
        )

    def waiver_approve(
        self,
        *,
        request_path: str,
        actor: str,
        approval_path: str,
        authority_check_path: str | None = None,
        approval_quorum_path: str | None = None,
        approved_at: str | None = None,
    ) -> dict[str, Any]:
        return (
            _waivers()
            .WaiverApprover()
            .approve(
                request=_read_mapping(request_path),
                actor=actor,
                approval=_read_mapping(approval_path),
                authority_check=_read_optional(authority_check_path),
                approval_quorum=_read_optional(approval_quorum_path),
                approved_at=approved_at,
            )
        )

    def gate(
        self,
        *,
        evaluation_path: str,
        waiver_paths: tuple[str, ...],
        authority_gate_path: str | None = None,
        profile: str,
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        return (
            _policy()
            .DataProductPolicyGate()
            .evaluate(
                evaluation=_read_mapping(evaluation_path),
                waivers=tuple(_read_mapping(path) for path in waiver_paths),
                authority_gate=_read_optional(authority_gate_path),
                profile=profile,
                observed_at=observed_at,
            )
        )

    def report(self, *, gate_path: str) -> dict[str, Any]:
        return _policy().DataProductPolicyGate().report(gate=_read_mapping(gate_path))


def _evidence(
    *,
    bundle_path: str | None,
    bundle: Mapping[str, Any] | None,
    evidence_dir: str | None,
) -> dict[str, dict[str, Any]]:
    evidence: dict[str, dict[str, Any]] = {}
    if evidence_dir:
        for path in sorted(Path(evidence_dir).rglob("*")):
            if path.suffix.lower() in {".json", ".yaml", ".yml"} and path.is_file():
                _add_evidence(evidence, path)
    if bundle and bundle_path:
        for artifact in bundle.get("artifacts", []):
            if isinstance(artifact, Mapping) and artifact.get("path") and artifact.get("kind"):
                path = _artifact_path(str(artifact["path"]), bundle_path)
                if path.exists():
                    evidence[str(artifact["kind"])] = _read_mapping(str(path))
    return evidence


def _add_evidence(evidence: dict[str, dict[str, Any]], path: Path) -> None:
    try:
        payload = _read_mapping(str(path))
    except (OSError, ValueError, json.JSONDecodeError, yaml.YAMLError):
        return
    kind = _kind_from_schema(payload) or path.stem.replace("-", "_")
    evidence[kind] = payload


def _kind_from_schema(payload: Mapping[str, Any]) -> str | None:
    schema = str(payload.get("schema_version") or "")
    if not schema.startswith("dpone.") or not schema.endswith(".v1"):
        return None
    return schema.removeprefix("dpone.").removesuffix(".v1")


def _artifact_path(path: str, bundle_path: str) -> Path:
    raw = Path(path)
    return raw if raw.is_absolute() or raw.exists() else Path(bundle_path).parent / raw


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _policy() -> Any:
    return import_module("dpone.readiness.data_product_policy")


def _waivers() -> Any:
    return import_module("dpone.readiness.data_product_policy_waivers")


__all__ = ["DataProductPolicyFacade"]
