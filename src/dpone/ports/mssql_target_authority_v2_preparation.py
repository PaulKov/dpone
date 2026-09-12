"""Internal capability port for guarded MSSQL R1 preparation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from dpone.contracts.mssql_target_authority_v2_identity import MssqlArtifactAuthorityV2
    from dpone.contracts.mssql_target_authority_v2_models import MssqlEffectRequestV2
    from dpone.contracts.mssql_target_authority_v2_preparation import (
        MssqlArtifactSealRequestV2,
        MssqlIntentSealRequestV2,
        MssqlOpenStagingArtifactV2,
    )


@runtime_checkable
class MssqlTargetAuthorityPreparationPort(Protocol):
    """Persist OPEN, SEALED-artifact, and first-sealed-intent transitions."""

    def register_open(self, handle: object, artifact: MssqlOpenStagingArtifactV2) -> None:
        """Register an exact OPEN artifact before any payload load."""

    def seal_artifact(self, handle: object, request: MssqlArtifactSealRequestV2) -> MssqlArtifactAuthorityV2:
        """Remove mutation rights and return the persisted seal proof."""

    def seal_intent(self, handle: object, request: MssqlIntentSealRequestV2) -> MssqlEffectRequestV2:
        """Atomically freeze the complete artifact set and effect request."""

    def load_sealed_request(self, handle: object, effect_key: bytes) -> MssqlEffectRequestV2:
        """Rehydrate one exact target-local effect without source I/O."""


__all__ = ["MssqlTargetAuthorityPreparationPort"]
