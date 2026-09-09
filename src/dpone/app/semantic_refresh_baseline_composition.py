"""Composition root for protected semantic-refresh baseline issuance."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from dpone.adapters.semantic_refresh_mssql_baseline import (
    MssqlSemanticRefreshBaselineReceiptStore,
)
from dpone.contracts.dbt_semantic_refresh_baseline_issuance import (
    SemanticRefreshBaselineIssuanceService,
)

if TYPE_CHECKING:
    from dpone.contracts.dbt_semantic_refresh_activation import (
        SemanticRefreshProtectedAssuranceVerifierPort,
    )
    from dpone.contracts.dbt_semantic_refresh_baseline_issuance import (
        SemanticRefreshBaselineAuthorityVerifierPort,
        SemanticRefreshBaselineEvidenceProviderPort,
    )
    from dpone.contracts.dbt_semantic_refresh_baseline_types import (
        SemanticRefreshBaselineIssuancePlan,
        SemanticRefreshBaselineIssuanceSubject,
    )
    from dpone.contracts.semantic_refresh_baseline_receipt import (
        SemanticRefreshBaselineAdoptionReceipt,
    )


@dataclass(frozen=True, slots=True)
class SemanticRefreshBaselineIssuanceRuntime:
    """Supported platform-operator boundary for baseline plan/apply."""

    service: SemanticRefreshBaselineIssuanceService

    def plan(
        self,
        subject: SemanticRefreshBaselineIssuanceSubject,
    ) -> SemanticRefreshBaselineIssuancePlan:
        """Create a non-mutating plan from exact protected deployment authority."""

        return self.service.plan(subject)

    def apply(
        self,
        plan: SemanticRefreshBaselineIssuancePlan,
    ) -> SemanticRefreshBaselineAdoptionReceipt:
        """Observe all engines and create/replay the canonical complete receipt."""

        return self.service.apply(plan)


def build_semantic_refresh_baseline_issuance_runtime(
    *,
    mssql_connection_factory: Callable[[], Any],
    evidence_provider: SemanticRefreshBaselineEvidenceProviderPort,
    authority_verifier: SemanticRefreshBaselineAuthorityVerifierPort,
    assurance_verifier: SemanticRefreshProtectedAssuranceVerifierPort,
    clock: Callable[[], datetime],
    control_schema: str = "dpone_control",
) -> SemanticRefreshBaselineIssuanceRuntime:
    """Wire protected observation and verification to create-only MSSQL storage."""

    return SemanticRefreshBaselineIssuanceRuntime(
        service=SemanticRefreshBaselineIssuanceService(
            evidence_provider=evidence_provider,
            authority_verifier=authority_verifier,
            assurance_verifier=assurance_verifier,
            receipt_store=MssqlSemanticRefreshBaselineReceiptStore(
                mssql_connection_factory,
                control_schema=control_schema,
            ),
            clock=clock,
        )
    )


__all__ = [
    "SemanticRefreshBaselineIssuanceRuntime",
    "build_semantic_refresh_baseline_issuance_runtime",
]
