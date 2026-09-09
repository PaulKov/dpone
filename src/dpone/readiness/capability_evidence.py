"""Bounded certification-matrix projection for capability discovery."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from dpone.contracts.capability_discovery import (
    CapabilityIssue,
    RouteCertificationCandidate,
    RouteCertificationVariant,
    parse_aware_datetime,
)
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.manifest.confined_files import (
    ConfinedFileError,
    project_relative_path,
    read_confined_file,
)
from dpone.readiness.capability_discovery_service import normalize_route_ref
from dpone.readiness.capability_evidence_trust import (
    has_independent_production_proofs,
    load_trusted_proofs,
)

_MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
_MAX_EVIDENCE_ROWS = 10_000
_DIGEST_PATTERN = re.compile(r"sha256:[0-9a-f]{64}")
_COMMIT_PATTERN = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")


def read_certification_matrix(
    root: Path,
    path: Path,
    *,
    expected_commit: str | None,
    evidence_dirs: Sequence[Path],
    max_age_hours: int,
    now: datetime,
) -> tuple[tuple[RouteCertificationVariant, ...], tuple[CapabilityIssue, ...]]:
    """Read one matrix and promote only proofs reproduced from canonical evidence."""

    try:
        relative = project_relative_path(root, path)
        content = read_confined_file(root, relative, max_bytes=_MAX_EVIDENCE_BYTES)
        payload = _strict_json_object(content)
    except (ConfinedFileError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return (), (
            _evidence_issue(
                "DPONE_CAPABILITY_EVIDENCE_INVALID",
                "Route certification evidence could not be read as bounded strict JSON.",
            ),
        )
    preflight_issue = _validate_matrix_header(
        payload,
        expected_commit=expected_commit,
        max_age_hours=max_age_hours,
        now=now,
    )
    if preflight_issue is not None:
        return (), (preflight_issue,)
    assert expected_commit is not None
    evaluated_at = parse_aware_datetime(payload.get("evaluated_at"))
    assert evaluated_at is not None
    rows = payload.get("rows")
    if not isinstance(rows, list) or len(rows) > _MAX_EVIDENCE_ROWS:
        return (), (
            _evidence_issue(
                "DPONE_CAPABILITY_EVIDENCE_ROWS_INVALID",
                "Route certification evidence rows are missing or exceed the supported bound.",
            ),
        )
    try:
        candidates = tuple(_certification_candidate(row) for row in rows)
    except (KeyError, TypeError, ValueError):
        return (), (
            _evidence_issue(
                "DPONE_CAPABILITY_EVIDENCE_ROW_INVALID",
                "A route certification row is malformed and cannot define a proof candidate.",
            ),
        )
    trusted_proofs, trust_issues = load_trusted_proofs(
        root,
        evidence_dirs,
        candidates=candidates,
        expected_commit=expected_commit,
        evaluated_at=now,
        max_age_hours=max_age_hours,
    )
    if trust_issues:
        return (), trust_issues
    variants: list[RouteCertificationVariant] = []
    issues: list[CapabilityIssue] = []
    for index, row in enumerate(rows):
        try:
            variants.append(_certification_variant(row, trusted_proofs=trusted_proofs))
        except (KeyError, TypeError, ValueError):
            issues.append(
                CapabilityIssue(
                    code="DPONE_CAPABILITY_EVIDENCE_ROW_INVALID",
                    entity_kind="route_certification_row",
                    entity_id=str(index),
                    message="A route certification row is malformed and was not used.",
                )
            )
    return ((), tuple(issues)) if issues else (tuple(variants), ())


def _certification_candidate(raw: object) -> RouteCertificationCandidate:
    if not isinstance(raw, Mapping):
        raise TypeError
    dimensions = raw["dimensions"]
    if not isinstance(dimensions, Mapping):
        raise TypeError
    return RouteCertificationCandidate(
        certification_id=_required_text(raw, "route_id"),
        source=_required_text(dimensions, "source"),
        sink=_required_text(dimensions, "sink"),
        strategy=_required_text(dimensions, "strategy"),
        transport=_required_text(dimensions, "transport"),
        schema_evolution=_required_text(dimensions, "schema_evolution"),
        airflow_runtime_mode=_required_text(dimensions, "airflow_runtime_mode"),
        sampling_mode=_required_text(raw, "sampling_mode"),
    )


def _validate_matrix_header(
    payload: Mapping[str, Any],
    *,
    expected_commit: str | None,
    max_age_hours: int,
    now: datetime,
) -> CapabilityIssue | None:
    if GitOpsSchemaValidator().validate(
        payload,
        expected_kind="dpone.route-certification-matrix.v1",
    ):
        return _evidence_issue(
            "DPONE_CAPABILITY_EVIDENCE_SCHEMA_UNSUPPORTED",
            "Route certification evidence does not satisfy its canonical schema.",
        )
    if expected_commit is None:
        return _evidence_issue(
            "DPONE_CAPABILITY_EVIDENCE_COMMIT_REQUIRED",
            "Expected source commit is required before certification evidence can promote a route.",
        )
    if _COMMIT_PATTERN.fullmatch(expected_commit) is None:
        return _evidence_issue(
            "DPONE_CAPABILITY_EVIDENCE_COMMIT_INVALID",
            "Expected source commit must be one complete lowercase Git SHA.",
        )
    if payload.get("expected_commit") != expected_commit:
        return _evidence_issue(
            "DPONE_CAPABILITY_EVIDENCE_FOREIGN_COMMIT",
            "Route certification evidence belongs to a different source commit.",
        )
    if payload.get("has_input_failures") is not False or payload.get("errors") not in ([], ()):
        return _evidence_issue(
            "DPONE_CAPABILITY_EVIDENCE_INPUT_FAILURES",
            "Route certification matrix contains unresolved input failures.",
        )
    evaluated_at = parse_aware_datetime(payload.get("evaluated_at"))
    if evaluated_at is None or not 1 <= max_age_hours <= 8760:
        return _evidence_issue(
            "DPONE_CAPABILITY_EVIDENCE_TIME_INVALID",
            "Route certification evidence needs a valid evaluation time and freshness policy.",
        )
    if evaluated_at > now + timedelta(minutes=5):
        return _evidence_issue(
            "DPONE_CAPABILITY_EVIDENCE_TIME_INVALID",
            "Route certification evidence evaluation time is ahead of the trusted clock.",
        )
    if now - evaluated_at > timedelta(hours=max_age_hours):
        return _evidence_issue(
            "DPONE_CAPABILITY_EVIDENCE_STALE",
            "Route certification evidence is older than the configured freshness policy.",
        )
    return None


def _certification_variant(
    raw: object,
    *,
    trusted_proofs: Mapping[tuple[str, str], Mapping[str, Any]],
) -> RouteCertificationVariant:
    if not isinstance(raw, Mapping):
        raise TypeError
    dimensions = raw["dimensions"]
    if not isinstance(dimensions, Mapping):
        raise TypeError
    route_id = normalize_route_ref(":".join(_required_text(dimensions, key) for key in ("source", "sink", "strategy")))
    variant_id = _required_text(raw, "route_id")
    raw_level = _required_text(raw, "status")
    if raw_level not in {
        "experimental",
        "route-certified",
        "production-certified",
        "enterprise-certified",
    }:
        raise ValueError
    proofs = raw.get("proofs")
    if not isinstance(proofs, list):
        raise TypeError
    proof_statuses = _proof_statuses(proofs)
    blockers = _string_items(raw.get("blockers"))
    blocking_reasons = _blocking_reasons(blockers, requested_level=raw_level)
    qualifying = tuple(
        proof
        for proof in proofs
        if isinstance(proof, Mapping)
        and _proof_qualifies(proof, requested_level=raw_level)
        and _proof_is_trusted(
            proof,
            variant_id=variant_id,
            trusted_proofs=trusted_proofs,
        )
    )
    row_statuses_pass = (
        raw.get("contract_status") == "PASS"
        and raw.get("live_status") == "PASS"
        and (raw_level == "route-certified" or raw.get("production_status") == "PASS")
    )
    evidence_refs = tuple(
        sorted(
            {
                value
                for proof in qualifying
                for key, value in proof.items()
                if key.endswith("_sha256") and isinstance(value, str) and _DIGEST_PATTERN.fullmatch(value)
            }
        )
    )
    promoted = raw_level != "experimental"
    passed = (
        promoted
        and row_statuses_pass
        and not blocking_reasons
        and "FAIL" not in proof_statuses
        and bool(qualifying)
        and bool(evidence_refs)
        and (raw_level != "enterprise-certified" or has_independent_production_proofs(qualifying))
    )
    evidence_status = (
        "PASS"
        if passed
        else "FAIL"
        if "FAIL" in proof_statuses or blocking_reasons
        else "SKIP"
        if "SKIP" in proof_statuses
        else "UNVERIFIED"
    )
    level = raw_level if promoted and passed else "experimental"
    reason_codes = tuple(
        dict.fromkeys(
            (
                *blockers,
                *(("route_certification_evidence_incomplete",) if promoted and not passed else ()),
            )
        )
    )
    return RouteCertificationVariant(
        id=variant_id,
        route_id=route_id,
        transport=_required_text(dimensions, "transport"),
        schema_evolution=_required_text(dimensions, "schema_evolution"),
        airflow_runtime_mode=_required_text(dimensions, "airflow_runtime_mode"),
        level=level,
        evidence_status=evidence_status,
        evidence_refs=evidence_refs,
        reason_codes=reason_codes,
    )


def _blocking_reasons(
    blockers: tuple[str, ...],
    *,
    requested_level: str,
) -> tuple[str, ...]:
    informational = {
        "route-certified": {
            "route_matrix.production_evidence_unverified",
            "route_matrix.enterprise_independence_missing",
        },
        "production-certified": {"route_matrix.enterprise_independence_missing"},
    }
    ignored = informational.get(requested_level, set())
    return tuple(item for item in blockers if item not in ignored)


def _proof_statuses(proofs: list[object]) -> set[str]:
    return {str(item.get("evidence_status")) for item in proofs if isinstance(item, Mapping)}


def _proof_qualifies(
    proof: Mapping[str, Any],
    *,
    requested_level: str,
) -> bool:
    if proof.get("evidence_status") != "PASS" or _string_items(proof.get("blockers")):
        return False
    accepted_levels = {
        "route-certified": {"route-certified", "production-certified"},
        "production-certified": {"production-certified"},
        "enterprise-certified": {"production-certified"},
    }
    if proof.get("certification_level") not in accepted_levels.get(requested_level, set()):
        return False
    if any(not _is_digest(proof.get(key)) for key in ("release_set_sha256", "certification_bundle_sha256")):
        return False
    if requested_level in {"production-certified", "enterprise-certified"}:
        if (
            proof.get("production_attempted") is not True
            or not _is_digest(proof.get("deployment_id"))
            or not _is_digest(proof.get("attestation_sha256"))
            or not _is_digest(proof.get("verification_sha256"))
            or not isinstance(proof.get("signer_identity"), str)
            or not str(proof.get("signer_identity")).strip()
        ):
            return False
    return True


def _proof_is_trusted(
    proof: Mapping[str, Any],
    *,
    variant_id: str,
    trusted_proofs: Mapping[tuple[str, str], Mapping[str, Any]],
) -> bool:
    evidence_set = proof.get("evidence_set")
    if not isinstance(evidence_set, str):
        return False
    trusted = trusted_proofs.get((variant_id, evidence_set))
    return trusted is not None and dict(proof) == dict(trusted)


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and _DIGEST_PATTERN.fullmatch(value) is not None


def _strict_json_object(content: bytes) -> Mapping[str, Any]:
    def pairs(items: Sequence[tuple[str, Any]]) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for key, value in items:
            if key in payload:
                raise ValueError("duplicate JSON key")
            payload[key] = value
        return payload

    value = json.loads(content.decode("utf-8"), object_pairs_hook=pairs)
    if not isinstance(value, Mapping):
        raise ValueError("JSON root must be an object")
    return value


def _required_text(payload: Mapping[str, Any], key: str) -> str:
    value = payload[key]
    if not isinstance(value, str) or not value.strip():
        raise ValueError
    return value.strip()


def _string_items(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def _evidence_issue(code: str, message: str) -> CapabilityIssue:
    return CapabilityIssue(
        code=code,
        entity_kind="route_certification_matrix",
        entity_id="configured",
        message=message,
    )


__all__ = ["read_certification_matrix"]
