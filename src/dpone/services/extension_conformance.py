"""Closed, evidence-based extension conformance evaluation."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Callable, Mapping
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from typing import Any

from dpone.services.catalog_supply_chain_support import (
    PROFILE_CHECKS,
    REQUEST_SCHEMA,
    ExtensionCheckResult,
    ExtensionConformanceError,
    ExtensionConformanceReport,
    RecipeBundleSupportError,
    canonical_json_bytes,
    conformance_id,
    parse_bounded_mapping,
    read_project_file,
    safe_project_relative,
    sha256_bytes,
    validate_registered_schema,
)

_MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
_UTC = UTC


class ExtensionConformanceService:
    """Aggregate only the fixed evidence checklist for one built-in profile."""

    def __init__(self, *, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(_UTC))

    def evaluate(
        self,
        request: Mapping[str, object],
        *,
        project_root: Path,
    ) -> ExtensionConformanceReport:
        if not validate_registered_schema(request, REQUEST_SCHEMA):
            raise ExtensionConformanceError(
                "DPONE_EXTENSION_CONFORMANCE_REQUEST_INVALID",
                "Extension conformance request schema is invalid.",
            )
        profile = str(request["profile"])
        required = PROFILE_CHECKS[profile]
        subject = _subject(request["subject"])
        evidence = _evidence(request["evidence"])
        named: dict[str, Mapping[str, Any]] = {}
        for item in evidence:
            check = str(item["check"])
            if check not in required or check in named:
                raise ExtensionConformanceError(
                    "DPONE_EXTENSION_CONFORMANCE_REQUEST_INVALID",
                    "Evidence checks must be unique members of the selected profile.",
                )
            named[check] = item
        root = project_root.resolve(strict=True)
        results = tuple(
            _evaluate_check(root=root, check=check, item=named.get(check), subject_digest=subject["digest"])
            for check in required
        )
        status = (
            "FAIL"
            if any(item.status == "FAIL" for item in results)
            else ("UNVERIFIED" if any(item.status != "PASS" for item in results) else "PASS")
        )
        now = self._clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("extension conformance clock must be timezone-aware")
        return ExtensionConformanceReport(
            conformance_id=conformance_id(profile=profile, subject=subject, evidence=evidence),
            profile=profile,
            subject=subject,
            status=status,
            checks=results,
            evaluated_at=_utc_text(now),
        )

    def evaluate_file(self, request_path: Path, *, project_root: Path) -> ExtensionConformanceReport:
        root = project_root.resolve(strict=True)
        request = _parse_mapping(_read(root, request_path))
        return self.evaluate(request, project_root=root)

    def write(self, report: ExtensionConformanceReport, *, output_dir: Path, project_root: Path) -> None:
        root = project_root.resolve(strict=True)
        destination = _confined_output(root, output_dir)
        json_bytes = canonical_json_bytes(report.to_dict())
        markdown_bytes = report.to_markdown().encode("utf-8")
        if destination.exists():
            try:
                exact = (
                    not destination.is_symlink()
                    and (destination / "extension-conformance.json").read_bytes() == json_bytes
                    and (destination / "extension-conformance.md").read_bytes() == markdown_bytes
                )
            except OSError:
                exact = False
            if exact:
                return
            raise ExtensionConformanceError(
                "DPONE_EXTENSION_CONFORMANCE_OUTPUT_CONFLICT",
                "Extension conformance output already differs.",
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".dpone-conformance-", dir=destination.parent))
        try:
            _write_private(staging / "extension-conformance.json", json_bytes)
            _write_private(staging / "extension-conformance.md", markdown_bytes)
            os.rename(staging, destination)
        finally:
            if staging.exists():
                for child in staging.iterdir():
                    child.unlink(missing_ok=True)
                staging.rmdir()


def _evaluate_check(
    *, root: Path, check: str, item: Mapping[str, Any] | None, subject_digest: str
) -> ExtensionCheckResult:
    if item is None:
        return ExtensionCheckResult(check, "UNVERIFIED", "DPONE_EXTENSION_EVIDENCE_MISSING", None, None)
    artifact_ref = str(item["artifact_ref"])
    expected_digest = str(item["sha256"])
    try:
        content = read_project_file(root, artifact_ref, max_bytes=_MAX_EVIDENCE_BYTES)
    except RecipeBundleSupportError:
        return ExtensionCheckResult(
            check, "UNVERIFIED", "DPONE_EXTENSION_EVIDENCE_UNAVAILABLE", artifact_ref, expected_digest
        )
    if sha256_bytes(content) != expected_digest:
        return ExtensionCheckResult(
            check, "FAIL", "DPONE_EXTENSION_EVIDENCE_DIGEST_MISMATCH", artifact_ref, expected_digest
        )
    try:
        payload = _parse_mapping(content)
    except ExtensionConformanceError:
        return ExtensionCheckResult(check, "FAIL", "DPONE_EXTENSION_EVIDENCE_INVALID", artifact_ref, expected_digest)
    expected_schema = str(item["expected_schema"])
    if payload.get("schema") != expected_schema or not validate_registered_schema(payload, expected_schema):
        return ExtensionCheckResult(check, "FAIL", "DPONE_EXTENSION_EVIDENCE_INVALID", artifact_ref, expected_digest)
    if expected_schema == "dpone.catalog-bundle-verification.v1":
        status, code = _catalog_status(payload, subject_digest)
    else:
        status, code = _generic_status(payload, check, subject_digest)
    return ExtensionCheckResult(check, status, code, artifact_ref, expected_digest)


def _catalog_status(payload: Mapping[str, Any], subject_digest: str) -> tuple[str, str]:
    if payload.get("bundle_id") != subject_digest:
        return "FAIL", "DPONE_EXTENSION_EVIDENCE_SUBJECT_MISMATCH"
    decision = payload.get("decision")
    if decision == "verified":
        return "PASS", "DPONE_EXTENSION_CHECK_PASSED"
    if decision == "unverified":
        return "UNVERIFIED", "DPONE_EXTENSION_CHECK_UNVERIFIED"
    return "FAIL", "DPONE_EXTENSION_CHECK_FAILED"


def _generic_status(payload: Mapping[str, Any], check: str, subject_digest: str) -> tuple[str, str]:
    if payload.get("check") != check or payload.get("subject_digest") != subject_digest:
        return "FAIL", "DPONE_EXTENSION_EVIDENCE_SUBJECT_MISMATCH"
    status = payload.get("status")
    if status == "PASS":
        return "PASS", "DPONE_EXTENSION_CHECK_PASSED"
    if status == "FAIL":
        return "FAIL", "DPONE_EXTENSION_CHECK_FAILED"
    return "UNVERIFIED", "DPONE_EXTENSION_CHECK_UNVERIFIED"


def _subject(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ExtensionConformanceError("DPONE_EXTENSION_CONFORMANCE_REQUEST_INVALID", "Subject is invalid.")
    return {key: str(value[key]) for key in ("id", "version", "digest")}


def _evidence(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(item, Mapping) for item in value):
        raise ExtensionConformanceError("DPONE_EXTENSION_CONFORMANCE_REQUEST_INVALID", "Evidence is invalid.")
    return list(value)


def _read(root: Path, path: Path) -> bytes:
    try:
        relative = safe_project_relative(root, path)
        return read_project_file(root, relative, max_bytes=_MAX_EVIDENCE_BYTES)
    except (RecipeBundleSupportError, ValueError):
        raise ExtensionConformanceError(
            "DPONE_EXTENSION_CONFORMANCE_REQUEST_INVALID", "Conformance request path is unsafe."
        ) from None


def _parse_mapping(content: bytes) -> dict[str, Any]:
    try:
        return parse_bounded_mapping(content, max_bytes=_MAX_EVIDENCE_BYTES)
    except RecipeBundleSupportError as exc:
        raise ExtensionConformanceError("DPONE_EXTENSION_EVIDENCE_INVALID", "Evidence is invalid.") from exc


def _confined_output(root: Path, value: Path) -> Path:
    destination = value.resolve(strict=False) if value.is_absolute() else (root / value).resolve(strict=False)
    try:
        destination.relative_to(root)
    except ValueError as exc:
        raise ExtensionConformanceError(
            "DPONE_EXTENSION_CONFORMANCE_OUTPUT_UNSAFE", "Conformance output must stay inside project root."
        ) from exc
    return destination


def _write_private(path: Path, payload: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        pending = memoryview(payload)
        while pending:
            written = os.write(descriptor, pending)
            if written <= 0:
                raise OSError("failed to write extension conformance artifact")
            pending = pending[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _utc_text(value: datetime) -> str:
    return value.astimezone(_UTC).isoformat().replace("+00:00", "Z")


__all__ = ["ExtensionConformanceService"]
