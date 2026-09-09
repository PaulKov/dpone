"""Publish the evidence-backed six-dimensional route certification matrix."""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path

from dpone.contracts.route_attestation import parse_aware_datetime
from dpone.ops.route_certification_matrix_evidence import RouteCertificationEvidenceReader
from dpone.ops.route_certification_matrix_models import (
    RouteCertificationDimensions,
    RouteCertificationMatrixError,
    RouteCertificationMatrixReport,
    RouteCertificationMatrixRow,
    RouteCertificationProof,
    route_status_counts,
)
from dpone.ops.route_certification_matrix_policy import RouteCertificationMatrixPolicy
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import RouteKey
from dpone.readiness.route_attestation_files import (
    RouteAttestationFileError,
    read_bounded_file,
    read_strict_json_mapping,
)
from dpone.services.sample_route_certifications import (
    CertifiedSamplingRoute,
    certified_sampling_route_catalog,
)

_COMMIT_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")


@dataclass(frozen=True, slots=True)
class RouteCertificationMatrixRequest:
    """Explicit publication inputs; evidence discovery is intentionally absent."""

    expected_commit: str
    evidence_dirs: tuple[Path, ...]
    output_dir: Path
    max_age_hours: int = 168


class RouteCertificationMatrixService:
    """Compose existing catalogs and immutable proof sets into one projection."""

    def __init__(
        self,
        *,
        candidates: Sequence[CertifiedSamplingRoute] | None = None,
        profiles: RouteProfileCatalog | None = None,
        reader: RouteCertificationEvidenceReader | None = None,
        policy: RouteCertificationMatrixPolicy | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._candidates = tuple(candidates if candidates is not None else certified_sampling_route_catalog())
        self._profiles = profiles or RouteProfileCatalog.default()
        self._reader = reader or RouteCertificationEvidenceReader()
        self._policy = policy or RouteCertificationMatrixPolicy()
        self._clock = clock or (lambda: datetime.now(UTC))

    def publish(self, request: RouteCertificationMatrixRequest) -> RouteCertificationMatrixReport:
        evaluated_at = _validated_inputs(request, now=self._clock())
        candidates = _validated_candidates(self._candidates)
        proofs: dict[str, list[RouteCertificationProof]] = {candidate.certification_id: [] for candidate in candidates}
        errors = []
        for evidence_dir in _unique_directories(request.evidence_dirs):
            result = self._reader.read(
                evidence_dir,
                candidates=candidates,
                expected_commit=request.expected_commit,
                evaluated_at=evaluated_at,
                max_age_hours=request.max_age_hours,
            )
            if result.issue is not None:
                errors.append(result.issue)
            elif result.route_id is not None and result.proof is not None:
                proofs[result.route_id].append(result.proof)
        rows = tuple(
            self._row(candidate, tuple(proofs[candidate.certification_id]))
            for candidate in sorted(candidates, key=lambda item: item.certification_id)
        )
        report = RouteCertificationMatrixReport(
            expected_commit=request.expected_commit,
            evaluated_at=_utc_text(evaluated_at),
            has_input_failures=bool(errors)
            or any(proof.evidence_status == "FAIL" for row_proofs in proofs.values() for proof in row_proofs),
            counts=route_status_counts(rows),
            rows=rows,
            errors=tuple(errors),
            output_dir=str(request.output_dir),
        )
        return _publish(report)

    def _row(
        self,
        candidate: CertifiedSamplingRoute,
        proofs: tuple[RouteCertificationProof, ...],
    ) -> RouteCertificationMatrixRow:
        key = RouteKey.of(candidate.source, candidate.sink, candidate.strategy)
        profile = self._profiles.get(key)
        capability_status = profile.certification_status if profile is not None else "not_supported"
        docs_link = profile.docs_link if profile is not None else "docs/source-sink-matrix.md"
        dimensions = RouteCertificationDimensions(
            source=candidate.source,
            sink=candidate.sink,
            strategy=candidate.strategy,
            transport=candidate.transport,
            schema_evolution=candidate.schema_evolution,
            airflow_runtime_mode=candidate.airflow_runtime_mode,
        )
        return self._policy.evaluate(
            route_id=candidate.certification_id,
            dimensions=dimensions,
            sampling_mode=candidate.sampling_mode,
            catalog_status=candidate.status,
            capability_status=capability_status,
            docs_link=docs_link,
            proofs=proofs,
        )


def _validated_inputs(request: RouteCertificationMatrixRequest, *, now: datetime) -> datetime:
    commit = str(request.expected_commit).strip()
    if _COMMIT_PATTERN.fullmatch(commit) is None:
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_COMMIT_INVALID",
            "Expected commit must be one complete lowercase 40- or 64-character Git SHA.",
        )
    if not 1 <= request.max_age_hours <= 8760:
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_MAX_AGE_INVALID",
            "max_age_hours must be between 1 and 8760.",
        )
    if now.tzinfo is None or now.utcoffset() is None:
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_CLOCK_INVALID",
            "Route matrix clock must return an offset-aware timestamp.",
        )
    output = request.output_dir
    if (
        output.is_symlink()
        or _path_has_symlink_component(output)
        or not output.name
        or any(part in {"", ".", ".."} for part in output.parts)
    ):
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_OUTPUT_UNSAFE",
            "Output directory must be a stable local path and cannot be a symlink.",
        )
    return now.astimezone(UTC)


def _validated_candidates(
    candidates: tuple[CertifiedSamplingRoute, ...],
) -> tuple[CertifiedSamplingRoute, ...]:
    if not candidates:
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_CATALOG_EMPTY",
            "Route certification candidate catalog is empty.",
        )
    ids = [candidate.certification_id for candidate in candidates]
    dimensions = [
        (
            candidate.source,
            candidate.sink,
            candidate.strategy,
            candidate.transport,
            candidate.schema_evolution,
            candidate.airflow_runtime_mode,
        )
        for candidate in candidates
    ]
    if len(ids) != len(set(ids)) or len(dimensions) != len(set(dimensions)):
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_CATALOG_DUPLICATE",
            "Route certification candidate identities and dimensions must be unique.",
        )
    return candidates


def _unique_directories(values: tuple[Path, ...]) -> tuple[Path, ...]:
    result: list[Path] = []
    seen: set[str] = set()
    for value in values:
        key = str(value.absolute())
        if key not in seen:
            seen.add(key)
            result.append(value)
    return tuple(result)


def _publish(report: RouteCertificationMatrixReport) -> RouteCertificationMatrixReport:
    destination = Path(report.output_dir)
    json_bytes = report.to_json().encode("utf-8")
    markdown_bytes = report.to_markdown().encode("utf-8")
    if destination.exists():
        if destination.is_symlink() or not destination.is_dir():
            raise RouteCertificationMatrixError(
                "DPONE_ROUTE_MATRIX_OUTPUT_UNSAFE",
                "Route matrix destination must be a regular directory.",
            )
        existing = _existing_equivalent_report(destination, report)
        if existing is not None:
            return existing
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_OUTPUT_CONFLICT",
            "Route matrix destination already contains different bytes; use a new immutable output directory.",
        )
    if _path_has_symlink_component(destination.parent):
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_OUTPUT_UNSAFE",
            "Route matrix output cannot traverse a symlinked directory.",
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    if _path_has_symlink_component(destination.parent):
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_OUTPUT_UNSAFE",
            "Route matrix output parent changed while it was prepared.",
        )
    staging = Path(tempfile.mkdtemp(prefix=".dpone-route-matrix-", dir=destination.parent))
    try:
        _write_private(staging / "route-certification-matrix.json", json_bytes)
        _write_private(staging / "route-certification-matrix.md", markdown_bytes)
        os.rename(staging, destination)
    except FileExistsError as exc:
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_OUTPUT_CONFLICT",
            "Route matrix destination was created concurrently.",
        ) from exc
    except OSError as exc:
        raise RouteCertificationMatrixError(
            "DPONE_ROUTE_MATRIX_OUTPUT_UNSAFE",
            "Route matrix output could not be published atomically.",
        ) from exc
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return report


def _existing_equivalent_report(
    destination: Path,
    report: RouteCertificationMatrixReport,
) -> RouteCertificationMatrixReport | None:
    try:
        json_path = destination / "route-certification-matrix.json"
        markdown_path = destination / "route-certification-matrix.md"
        payload, json_bytes = read_strict_json_mapping(
            json_path,
            max_bytes=4 * 1024 * 1024,
            label="route certification matrix",
        )
        evaluated_at = payload.get("evaluated_at")
        if not isinstance(evaluated_at, str) or parse_aware_datetime(evaluated_at) is None:
            return None
        candidate = replace(report, evaluated_at=evaluated_at)
        markdown_bytes = read_bounded_file(
            markdown_path,
            max_bytes=4 * 1024 * 1024,
            label="route certification matrix markdown",
        )
        if json_bytes != candidate.to_json().encode("utf-8"):
            return None
        if markdown_bytes != candidate.to_markdown().encode("utf-8"):
            return None
        return candidate
    except RouteAttestationFileError:
        return None


def _path_has_symlink_component(path: Path) -> bool:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        current /= part
        if current.is_symlink():
            return True
    return False


def _write_private(path: Path, content: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(content)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o644)
    finally:
        os.close(descriptor)


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "RouteCertificationMatrixError",
    "RouteCertificationMatrixRequest",
    "RouteCertificationMatrixService",
]
