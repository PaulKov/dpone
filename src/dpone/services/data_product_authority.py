"""File-IO facade for data product authority commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductAuthorityFacade:
    """Thin CLI facade; authority decisions live in readiness modules."""

    def registry_build(self, *, manifest_path: str) -> dict[str, Any]:
        return _authority().AuthorityRegistryBuilder().build(manifest=_read_mapping(manifest_path))

    def check(self, *, registry_path: str, actor: str, action: str, subject_path: str) -> dict[str, Any]:
        return (
            _authority()
            .AuthorityCheckEvaluator()
            .check(
                registry=_read_mapping(registry_path),
                actor=actor,
                action=action,
                subject=_read_mapping(subject_path),
            )
        )

    def quorum_verify(
        self,
        *,
        registry_path: str,
        request_path: str,
        approval_paths: tuple[str, ...],
    ) -> dict[str, Any]:
        return (
            _authority()
            .ApprovalQuorumVerifier()
            .verify(
                registry=_read_mapping(registry_path),
                request=_read_mapping(request_path),
                approvals=tuple(_read_mapping(path) for path in approval_paths),
            )
        )

    def signature_sign(self, *, registry_path: str, artifact_path: str, actor: str) -> dict[str, Any]:
        return (
            _signing()
            .EvidenceSigner()
            .sign(
                registry=_read_mapping(registry_path),
                artifact=_read_mapping(artifact_path),
                actor=actor,
            )
        )

    def signature_verify(self, *, registry_path: str, artifact_path: str, signature_path: str) -> dict[str, Any]:
        return (
            _signing()
            .EvidenceSignatureVerifier()
            .verify(
                registry=_read_mapping(registry_path),
                artifact=_read_mapping(artifact_path),
                signature=_read_mapping(signature_path),
            )
        )

    def gate(
        self,
        *,
        authority_check_path: str | None,
        approval_quorum_path: str | None,
        signature_paths: tuple[str, ...],
        profile: str,
    ) -> dict[str, Any]:
        return (
            _authority()
            .AuthorityGate()
            .evaluate(
                authority_check=_read_optional(authority_check_path),
                approval_quorum=_read_optional(approval_quorum_path),
                signatures=tuple(_read_mapping(path) for path in signature_paths),
                profile=profile,
            )
        )

    def report(self, *, gate_path: str) -> dict[str, Any]:
        return _authority().AuthorityGate().report(gate=_read_mapping(gate_path))


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


def _authority() -> Any:
    return import_module("dpone.readiness.data_product_authority")


def _signing() -> Any:
    return import_module("dpone.readiness.data_product_authority_signing")


__all__ = ["DataProductAuthorityFacade"]
