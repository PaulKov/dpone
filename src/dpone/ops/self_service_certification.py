"""Publish evidence-backed Airflow self-service certification reports."""

from __future__ import annotations

import os
import re
import shutil
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path

from dpone.ops.self_service_certification_files import (
    SelfServiceEvidenceFileError,
    read_bytes,
    read_json,
)
from dpone.ops.self_service_certification_models import (
    SelfServiceCertificationError,
    SelfServiceCertificationReport,
)
from dpone.ops.self_service_certification_policy import overall_status
from dpone.ops.self_service_certification_reference import ReferenceDeploymentEvidenceReader
from dpone.ops.self_service_certification_usability import SelfServiceUsabilityReader

_COMMIT_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_MAX_REPORT_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class SelfServiceCertificationRequest:
    """Explicit local inputs; evidence discovery is intentionally absent."""

    expected_commit: str
    output_dir: Path
    usability_study: Path | None = None
    reference_deployments: tuple[Path, ...] = ()
    max_age_hours: int = 168


class SelfServiceCertificationService:
    """Compose human and production evidence into one immutable projection."""

    def __init__(
        self,
        *,
        usability_reader: SelfServiceUsabilityReader | None = None,
        reference_reader: ReferenceDeploymentEvidenceReader | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._usability_reader = usability_reader or SelfServiceUsabilityReader()
        self._reference_reader = reference_reader or ReferenceDeploymentEvidenceReader()
        self._clock = clock or (lambda: datetime.now(UTC))

    def publish(self, request: SelfServiceCertificationRequest) -> SelfServiceCertificationReport:
        evaluated_at = _validated_inputs(request, now=self._clock())
        usability = (
            self._usability_reader.missing()
            if request.usability_study is None
            else self._usability_reader.read(request.usability_study, expected_commit=request.expected_commit)
        )
        references = self._reference_reader.read_many(
            request.reference_deployments,
            expected_commit=request.expected_commit,
            evaluated_at=evaluated_at,
            max_age_hours=request.max_age_hours,
        )
        report = SelfServiceCertificationReport(
            expected_commit=request.expected_commit,
            evaluated_at=_utc_text(evaluated_at),
            overall_status=overall_status(usability, references),
            has_input_failures=usability.status == "FAIL" or references.status == "FAIL",
            usability=usability,
            reference_deployments=references,
            output_dir=str(request.output_dir),
        )
        return _publish(report)


def _validated_inputs(request: SelfServiceCertificationRequest, *, now: datetime) -> datetime:
    if _COMMIT_PATTERN.fullmatch(str(request.expected_commit).strip()) is None:
        raise SelfServiceCertificationError(
            "DPONE_SELF_SERVICE_COMMIT_INVALID",
            "Expected commit must be one complete lowercase 40- or 64-character Git SHA.",
        )
    if not 1 <= request.max_age_hours <= 8760:
        raise SelfServiceCertificationError(
            "DPONE_SELF_SERVICE_MAX_AGE_INVALID",
            "max_age_hours must be between 1 and 8760.",
        )
    if now.tzinfo is None or now.utcoffset() is None:
        raise SelfServiceCertificationError(
            "DPONE_SELF_SERVICE_CLOCK_INVALID",
            "Certification clock must return an offset-aware timestamp.",
        )
    output = request.output_dir
    if (
        output.is_symlink()
        or _path_has_symlink_component(output)
        or not output.name
        or any(part in {"", ".", ".."} for part in output.parts)
    ):
        raise SelfServiceCertificationError(
            "DPONE_SELF_SERVICE_OUTPUT_UNSAFE",
            "Output directory must be a stable local path and cannot be a symlink.",
        )
    return now.astimezone(UTC)


def _publish(report: SelfServiceCertificationReport) -> SelfServiceCertificationReport:
    destination = Path(report.output_dir)
    json_bytes = report.to_json().encode("utf-8")
    markdown_bytes = report.to_markdown().encode("utf-8")
    if destination.exists():
        if destination.is_symlink() or not destination.is_dir():
            raise _output_error("Self-service certification destination must be a regular directory.")
        existing = _existing_equivalent_report(destination, report)
        if existing is not None:
            return existing
        raise SelfServiceCertificationError(
            "DPONE_SELF_SERVICE_OUTPUT_CONFLICT",
            "Destination already contains different bytes; use a new immutable output directory.",
        )
    if _path_has_symlink_component(destination.parent):
        raise _output_error("Self-service certification output cannot traverse a symlinked directory.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if _path_has_symlink_component(destination.parent):
        raise _output_error("Self-service certification output parent changed while it was prepared.")
    staging = Path(tempfile.mkdtemp(prefix=".dpone-self-service-", dir=destination.parent))
    try:
        _write_private(staging / "self-service-certification.json", json_bytes)
        _write_private(staging / "self-service-certification.md", markdown_bytes)
        os.rename(staging, destination)
    except FileExistsError as exc:
        raise SelfServiceCertificationError(
            "DPONE_SELF_SERVICE_OUTPUT_CONFLICT",
            "Self-service certification destination was created concurrently.",
        ) from exc
    except OSError as exc:
        raise _output_error("Self-service certification could not be published atomically.") from exc
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return report


def _existing_equivalent_report(
    destination: Path,
    report: SelfServiceCertificationReport,
) -> SelfServiceCertificationReport | None:
    try:
        payload, _ = read_json(
            destination / "self-service-certification.json",
            max_bytes=_MAX_REPORT_BYTES,
            label="self-service certification",
        )
        evaluated_at = payload.get("evaluated_at")
        if not isinstance(evaluated_at, str) or _parse_aware_datetime(evaluated_at) is None:
            return None
        candidate = replace(report, evaluated_at=evaluated_at)
        markdown = read_bytes(
            destination / "self-service-certification.md",
            max_bytes=_MAX_REPORT_BYTES,
            label="self-service certification markdown",
        )
    except SelfServiceEvidenceFileError:
        return None
    if payload == candidate.to_dict() and markdown == candidate.to_markdown().encode("utf-8"):
        return candidate
    return None


def _write_private(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(data)
        written = 0
        while written < len(view):
            count = os.write(descriptor, view[written:])
            if count <= 0:
                raise OSError("short write")
            written += count
        os.fsync(descriptor)
        os.fchmod(descriptor, 0o644)
    finally:
        os.close(descriptor)


def _path_has_symlink_component(path: Path) -> bool:
    current = Path(path.anchor) if path.is_absolute() else Path()
    parts = path.parts[1:] if path.is_absolute() else path.parts
    for part in parts:
        current /= part
        if current.is_symlink():
            return True
        if not current.exists():
            return False
    return False


def _output_error(message: str) -> SelfServiceCertificationError:
    return SelfServiceCertificationError("DPONE_SELF_SERVICE_OUTPUT_UNSAFE", message)


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_aware_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


__all__ = [
    "SelfServiceCertificationError",
    "SelfServiceCertificationRequest",
    "SelfServiceCertificationService",
]
