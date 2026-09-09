"""Pure status policy for route certification matrix rows."""

from __future__ import annotations

from collections.abc import Sequence

from dpone.ops.route_certification_matrix_models import (
    RouteCertificationDimensions,
    RouteCertificationMatrixRow,
    RouteCertificationProof,
)


class RouteCertificationMatrixPolicy:
    """Derive one public status without reading files or invoking integrations."""

    def evaluate(
        self,
        *,
        route_id: str,
        dimensions: RouteCertificationDimensions,
        sampling_mode: str,
        catalog_status: str,
        capability_status: str,
        docs_link: str,
        proofs: Sequence[RouteCertificationProof],
    ) -> RouteCertificationMatrixRow:
        ordered = tuple(sorted(proofs, key=lambda proof: (proof.evidence_set, proof.deployment_id or "")))
        failed = tuple(proof for proof in ordered if proof.evidence_status == "FAIL")
        route_proofs = tuple(proof for proof in ordered if proof.route_certified)
        production_proofs = tuple(proof for proof in ordered if proof.production_certified)
        blockers = [blocker for proof in ordered for blocker in proof.blockers]
        if failed:
            status = "experimental"
            contract_status = "FAIL"
            live_status = "FAIL"
        elif route_proofs:
            status = "route-certified"
            contract_status = "PASS"
            live_status = "PASS"
        else:
            status = "experimental"
            contract_status = "UNVERIFIED"
            live_status = "UNVERIFIED"
            blockers.append("route_matrix.live_evidence_unverified")

        production_status = _production_status(ordered, production_proofs)
        if not failed and production_proofs:
            status = "production-certified"
            if _independent(production_proofs):
                status = "enterprise-certified"
            elif len(production_proofs) > 1:
                blockers.append("route_matrix.enterprise_independence_missing")
        elif production_status == "UNVERIFIED":
            blockers.append("route_matrix.production_evidence_unverified")

        if capability_status == "not_supported":
            status = "experimental"
            contract_status = "FAIL"
            blockers.append("route_matrix.capability_not_supported")
        return RouteCertificationMatrixRow(
            route_id=route_id,
            dimensions=dimensions,
            sampling_mode=sampling_mode,
            status=status,
            catalog_status=catalog_status,
            capability_status=capability_status,
            contract_status=contract_status,
            live_status=live_status,
            production_status=production_status,
            docs_link=docs_link,
            blockers=tuple(dict.fromkeys(blockers)),
            proofs=ordered,
        )


def _production_status(
    proofs: tuple[RouteCertificationProof, ...],
    production_proofs: tuple[RouteCertificationProof, ...],
) -> str:
    if any(proof.evidence_status == "FAIL" and proof.production_attempted for proof in proofs):
        return "FAIL"
    return "PASS" if production_proofs else "UNVERIFIED"


def _independent(proofs: tuple[RouteCertificationProof, ...]) -> bool:
    deployments = {proof.deployment_id for proof in proofs if proof.deployment_id}
    signers = {proof.signer_identity for proof in proofs if proof.signer_identity}
    return len(deployments) >= 2 and len(signers) >= 2


__all__ = ["RouteCertificationMatrixPolicy"]
