"""Build content-bound claims consumed by the public route matrix."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest, release_id
from dpone.ops.route_certification_matrix_models import RouteCertificationDimensions, RouteCertificationMatrixError
from dpone.readiness.route_attestation_files import RouteAttestationFileError, read_strict_json_mapping
from dpone.services.sample_route_certifications import CertifiedSamplingRoute, certified_sampling_route_catalog

_MAX_RELEASE_BYTES = 4 * 1024 * 1024
_COMMIT_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")


class RouteCertificationMatrixClaimBuilder:
    """Bind one vendor-live bundle to immutable release and route identities."""

    def __init__(self, *, candidates: Sequence[CertifiedSamplingRoute] | None = None) -> None:
        self._candidates = tuple(candidates if candidates is not None else certified_sampling_route_catalog())

    def build(
        self,
        *,
        release_set_path: str | Path,
        source: str,
        sink: str,
        strategy: str,
        certified_at: datetime,
    ) -> dict[str, Any]:
        release = _read_release_set(Path(release_set_path))
        release_identity, source_commit = _release_identity(release)
        candidate = _candidate(
            self._candidates,
            source=source,
            sink=sink,
            strategy=strategy,
        )
        if certified_at.tzinfo is None or certified_at.utcoffset() is None:
            raise RouteCertificationMatrixError(
                "DPONE_ROUTE_MATRIX_CLAIM_CLOCK_INVALID",
                "Route certification claim clock must return an offset-aware timestamp.",
            )
        dimensions = RouteCertificationDimensions(
            source=candidate.source,
            sink=candidate.sink,
            strategy=candidate.strategy,
            transport=candidate.transport,
            schema_evolution=candidate.schema_evolution,
            airflow_runtime_mode=candidate.airflow_runtime_mode,
        )
        return {
            "schema": "dpone.route-matrix-claim.v1",
            "release_id": release_identity,
            "source_commit": source_commit,
            "certified_at": _utc_text(certified_at),
            "route": {
                "route_id": candidate.certification_id,
                **dimensions.to_dict(),
                "sampling_mode": candidate.sampling_mode,
            },
        }


def _read_release_set(path: Path) -> dict[str, Any]:
    try:
        payload, _ = read_strict_json_mapping(path, max_bytes=_MAX_RELEASE_BYTES, label="release-set")
    except RouteAttestationFileError as exc:
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_CLAIM_RELEASE_UNSAFE",
            "Release-set must be a bounded regular JSON file.",
        ) from exc
    return payload


def _release_identity(payload: dict[str, Any]) -> tuple[str, str]:
    identity = payload.get("release_id")
    provenance = payload.get("provenance")
    source_commit = provenance.get("source_commit") if isinstance(provenance, dict) else None
    if (
        payload.get("schema") != "dpone.release-set.v1"
        or not is_canonical_sha256_digest(identity)
        or release_id(payload) != identity
        or not isinstance(source_commit, str)
        or _COMMIT_PATTERN.fullmatch(source_commit) is None
    ):
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_CLAIM_RELEASE_INVALID",
            "Release-set must have a valid content identity and complete source commit.",
        )
    return identity, source_commit


def _candidate(
    candidates: tuple[CertifiedSamplingRoute, ...],
    *,
    source: str,
    sink: str,
    strategy: str,
) -> CertifiedSamplingRoute:
    key = tuple(_normalized(item) for item in (source, sink, strategy))
    matches = [candidate for candidate in candidates if candidate.key == key]
    if len(matches) != 1:
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_CLAIM_ROUTE_UNKNOWN",
            "Route must map to exactly one registered six-dimensional certification candidate.",
        )
    return matches[0]


def _normalized(value: object) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


__all__ = ["RouteCertificationMatrixClaimBuilder"]
