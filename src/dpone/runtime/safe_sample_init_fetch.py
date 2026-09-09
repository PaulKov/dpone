"""Runtime init-fetch adapter for safe sample execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.artifact_delivery import InitFetchAttestationVerifier


from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.runtime.artifact_delivery import (
    DEFAULT_MAX_ARTIFACT_COUNT,
    DEFAULT_MAX_TOTAL_BYTES,
    InitFetchExecutor,
    LocalArtifactRegistry,
    build_init_fetch_plan,
)


class SafeSampleInitFetchArtifactFetcher:
    """Fetch pinned workload packs through the runtime init-fetch contract."""

    def __init__(
        self,
        *,
        registry_root: str | Path,
        destination_root: str | Path,
        max_artifact_count: int = DEFAULT_MAX_ARTIFACT_COUNT,
        max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
        attestation_verifier: InitFetchAttestationVerifier | None = None,
    ) -> None:
        self._registry_root = registry_root
        self._destination_root = destination_root
        self._max_artifact_count = max_artifact_count
        self._max_total_bytes = max_total_bytes
        self._attestation_verifier = attestation_verifier

    def fetch(self, plan: Any) -> Mapping[str, Any]:
        context = getattr(plan, "deployment_context", None)
        if context is None:
            raise ValueError("deployment context is required for init_fetch")
        delivery = _mapping(getattr(context, "runtime_artifact_delivery", None))
        if delivery.get("mode") != "init_fetch":
            raise ValueError("runtime_artifact_delivery.mode must be init_fetch")
        init_fetch_plan = build_init_fetch_plan(
            release_id=str(getattr(context, "release_id", "") or ""),
            deployment_id=str(getattr(context, "deployment_id", "") or ""),
            artifact_registry_ref=str(delivery.get("artifact_registry_ref") or ""),
            workload_packs=tuple(_mappings(getattr(context, "workload_packs", ()))),
            service_account=_service_account(delivery),
            checksums=_checksums(delivery),
            attestations=_attestations(delivery),
        )
        return (
            InitFetchExecutor(
                registry=LocalArtifactRegistry(self._registry_root),
                destination_root=self._destination_root,
                max_artifact_count=self._max_artifact_count,
                max_total_bytes=self._max_total_bytes,
                attestation_verifier=self._attestation_verifier,
            )
            .execute(init_fetch_plan)
            .to_dict()
        )

    def read_pinned_bytes(self, artifact_ref: str) -> bytes:
        """Read one local registry artifact for pre-fetch source-pin verification."""

        return LocalArtifactRegistry(self._registry_root).read_bytes(artifact_ref)


def _service_account(delivery: Mapping[str, Any]) -> str:
    identity = delivery.get("identity")
    if not isinstance(identity, Mapping):
        return "dpone-runtime"
    return str(identity.get("service_account") or "dpone-runtime")


def _attestations(delivery: Mapping[str, Any]) -> str:
    verify = delivery.get("verify")
    if not isinstance(verify, Mapping):
        return "required_for_prod"
    return str(verify.get("attestations") or "required_for_prod")


def _checksums(delivery: Mapping[str, Any]) -> str:
    verify = delivery.get("verify")
    if not isinstance(verify, Mapping):
        return "required"
    return str(verify.get("checksums") or "required")


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _mappings(value: object) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, tuple | list):
        return ()
    return tuple(dict(item) for item in value if isinstance(item, Mapping))


__all__ = ["SafeSampleInitFetchArtifactFetcher"]
