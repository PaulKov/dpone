"""Semantic validation for one route certification matrix bundle."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from dpone.contracts.capability_discovery import RouteCertificationCandidateProtocol
from dpone.contracts.route_attestation import parse_aware_datetime
from dpone.ops.certification_artifacts import certification_trust
from dpone.ops.route_certification_matrix_stages import validate_bundle_stages


def validate_certification_bundle(
    payload: Mapping[str, Any],
    *,
    bundle_directory: Path,
    candidate: RouteCertificationCandidateProtocol,
    release_identity: str | None,
    expected_commit: str,
    evaluated_at: datetime,
    max_age_hours: int,
) -> tuple[str, ...]:
    """Validate route, release claim, freshness and stage integrity together."""

    blockers: list[str] = []
    route = payload.get("route")
    expected_route = {"source": candidate.source, "sink": candidate.sink, "strategy": candidate.strategy}
    if (
        payload.get("schema_version") != "dpone.route_certification_bundle.v1"
        or not certification_trust(payload).passed
        or payload.get("level") != "certified"
        or payload.get("profile") != "vendor_live"
        or not isinstance(route, Mapping)
        or any(route.get(key) != value for key, value in expected_route.items())
    ):
        blockers.append("route_matrix.certification_bundle_not_vendor_live")
    required = payload.get("required_evidence")
    if not isinstance(required, list) or "route_live_evidence_bundle" not in required:
        blockers.append("route_matrix.live_evidence_not_required")
    claim = payload.get("matrix_claim")
    certified_at = parse_aware_datetime(claim.get("certified_at")) if isinstance(claim, Mapping) else None
    if (
        not isinstance(claim, Mapping)
        or claim.get("schema") != "dpone.route-matrix-claim.v1"
        or claim.get("release_id") != release_identity
        or claim.get("source_commit") != expected_commit
        or claim.get("route") != _attested_route(candidate)
        or certified_at is None
    ):
        blockers.append("route_matrix.bundle_claim_invalid")
        return tuple(blockers)
    age = evaluated_at - certified_at
    if age > timedelta(hours=max_age_hours):
        blockers.append("route_matrix.certification_bundle_stale")
    if age < -timedelta(minutes=5):
        blockers.append("route_matrix.certification_bundle_from_future")
    blockers.extend(validate_bundle_stages(payload, bundle_directory=bundle_directory))
    return tuple(dict.fromkeys(blockers))


def _attested_route(candidate: RouteCertificationCandidateProtocol) -> dict[str, str]:
    return {
        "route_id": candidate.certification_id,
        "source": candidate.source,
        "sink": candidate.sink,
        "strategy": candidate.strategy,
        "transport": candidate.transport,
        "schema_evolution": candidate.schema_evolution,
        "airflow_runtime_mode": candidate.airflow_runtime_mode,
        "sampling_mode": candidate.sampling_mode,
    }


__all__ = ["validate_certification_bundle"]
