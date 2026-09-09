"""Pure promotion identity, authorization and pointer provenance policy."""

from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

_UTC = timezone.utc  # noqa: UP017 - mypy and packaging still target Python 3.10.


class DeploymentCachePromotionPolicy:
    """Validate promotion actors and construct canonical pointer identities."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
        activation_id_factory: Callable[[], str] | None = None,
        allowed_promoters: tuple[str, ...] | None = None,
        error_factory: Callable[[str, str], Exception],
    ) -> None:
        self._clock = clock or (lambda: datetime.now(_UTC))
        self._activation_id_factory = activation_id_factory or (lambda: str(uuid4()))
        self._allowed_promoters = frozenset(promoter for promoter in allowed_promoters or () if promoter)
        self._error_factory = error_factory

    def authorize(self, promoted_by: str) -> None:
        if self._allowed_promoters and promoted_by not in self._allowed_promoters:
            raise self._error_factory(
                "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED",
                "current pointer promotion requires an allowed CI/service identity",
            )

    def activation_id(self, requested: str | None) -> str:
        value = self._activation_id_factory() if requested is None else requested
        try:
            parsed = UUID(value)
        except (AttributeError, TypeError, ValueError) as exc:
            raise self._error_factory(
                "DPONE_CURRENT_POINTER_ACTIVATION_ID_INVALID",
                "activation_id must be a canonical UUIDv4",
            ) from exc
        canonical = str(parsed)
        if parsed.version != 4 or value != canonical:
            raise self._error_factory(
                "DPONE_CURRENT_POINTER_ACTIVATION_ID_INVALID",
                "activation_id must be a canonical UUIDv4",
            )
        return canonical

    def pointer(
        self,
        *,
        deployment: dict[str, Any],
        environment: str,
        promoted_by: str,
        previous_deployment_id: str | None,
        source_commit: str | None,
        attestation_ref: str | None,
        activation_id: str,
        workspace_authority_connection_ref: str | None = None,
    ) -> dict[str, Any]:
        if (
            workspace_authority_connection_ref is not None
            and re.fullmatch(r"[a-z][a-z0-9_]{0,127}", workspace_authority_connection_ref) is None
        ):
            raise self._error_factory(
                "DPONE_CURRENT_POINTER_INVALID",
                "workspace authority connection_ref is invalid",
            )
        result = {
            "schema": "dpone.current-pointer.v1",
            "activation_id": activation_id,
            "environment": environment,
            "deployment_id": deployment["deployment_id"],
            "release_id": deployment["release_id"],
            "current_path": "current",
            "promoted_by": promoted_by,
            "promoted_at": self._clock().astimezone(_UTC).isoformat(),
            "previous_deployment_id": previous_deployment_id,
        }
        if source_commit:
            result["source_commit"] = source_commit
        if attestation_ref:
            result["attestation_ref"] = attestation_ref
        if workspace_authority_connection_ref:
            result["workspace_authority_connection_ref"] = workspace_authority_connection_ref
        return result

    def timestamp(self) -> str:
        """Return one canonical UTC recovery timestamp from the injected clock."""

        return self._clock().astimezone(_UTC).isoformat()


__all__ = ["DeploymentCachePromotionPolicy"]
