"""Authenticated development delivery receipt and exact execution subjects.

The receipt is documentary output from an injected original-byte verifier. It
contains no credential and is not itself a signer or a runtime permit. Protected
entrypoints must reopen current policy, revocation and deployment state before
issuing credentials or writers.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest

DEVELOPMENT_RELEASE_SCHEMA = "dpone.dbt-release-set.development.v1"
DEVELOPMENT_COMPOSITION_PROFILE = "development_workspace_delivery_v1"
DEVELOPMENT_AUTHORITY_SCHEMA = "dpone.development-authority.v1"
_TOKEN = re.compile(r"[a-z0-9][a-z0-9_.-]{0,249}\Z")
_ENVIRONMENT = re.compile(r"[a-z0-9][a-z0-9_-]{0,62}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z\Z")
_KINDS = frozenset({"runtime", "pre_hook"})
_RELEASE_FIELDS = {
    "schema",
    "policy_sha256",
    "grant_sha256",
    "signature_subject_sha256",
    "environment",
    "source_repository_sha256",
    "source_commit",
    "revocation_epoch",
    "max_workloads",
    "max_source_bytes",
    "execution_subjects",
}


class DevelopmentAuthorityError(ValueError):
    """Stable fail-closed development authority error without private values."""

    code = "DPONE_DEVELOPMENT_AUTHORITY_INVALID"

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"{self.code}: {reason}")


@dataclass(frozen=True, order=True, slots=True)
class DevelopmentExecutionSubject:
    """One workload command that an independently verified grant may admit."""

    workload_id: str
    kind: str
    hook_id: str | None = None

    def __post_init__(self) -> None:
        if _TOKEN.fullmatch(self.workload_id) is None or self.kind not in _KINDS:
            raise DevelopmentAuthorityError("execution_subject")
        if self.kind == "runtime" and self.hook_id is not None:
            raise DevelopmentAuthorityError("execution_subject")
        if self.kind == "pre_hook" and (self.hook_id is None or _TOKEN.fullmatch(self.hook_id) is None):
            raise DevelopmentAuthorityError("execution_subject")

    def to_dict(self) -> dict[str, str]:
        result = {"workload_id": self.workload_id, "kind": self.kind}
        if self.hook_id is not None:
            result["hook_id"] = self.hook_id
        return result

    @classmethod
    def from_dict(cls, value: object) -> DevelopmentExecutionSubject:
        if not isinstance(value, dict):
            raise DevelopmentAuthorityError("execution_subject")
        expected = {"workload_id", "kind"} | ({"hook_id"} if "hook_id" in value else set())
        if set(value) != expected:
            raise DevelopmentAuthorityError("execution_subject")
        return cls(
            workload_id=str(value["workload_id"]),
            kind=str(value["kind"]),
            hook_id=str(value["hook_id"]) if "hook_id" in value else None,
        )


@dataclass(frozen=True, slots=True)
class DevelopmentAuthorityReceipt:
    """Verified delivery scope with optional exact workload execution subjects."""

    policy_sha256: str
    grant_sha256: str
    signature_subject_sha256: str
    environment: str
    source_repository_sha256: str
    source_commit: str
    not_before: str
    expires_at: str
    revocation_epoch: int
    max_workloads: int
    max_source_bytes: int
    execution_subjects: tuple[DevelopmentExecutionSubject, ...] = ()
    schema: str = DEVELOPMENT_AUTHORITY_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != DEVELOPMENT_AUTHORITY_SCHEMA or _ENVIRONMENT.fullmatch(self.environment) is None:
            raise DevelopmentAuthorityError("family")
        for value in (
            self.policy_sha256,
            self.grant_sha256,
            self.signature_subject_sha256,
            self.source_repository_sha256,
        ):
            if not is_canonical_sha256_digest(value):
                raise DevelopmentAuthorityError("digest")
        if _COMMIT.fullmatch(self.source_commit) is None:
            raise DevelopmentAuthorityError("source_commit")
        start, end = _utc(self.not_before), _utc(self.expires_at)
        if not start < end or (end - start).total_seconds() > 7 * 24 * 60 * 60:
            raise DevelopmentAuthorityError("validity")
        if type(self.revocation_epoch) is not int or self.revocation_epoch < 0:
            raise DevelopmentAuthorityError("revocation_epoch")
        if type(self.max_workloads) is not int or not 1 <= self.max_workloads <= 10_000:
            raise DevelopmentAuthorityError("max_workloads")
        if type(self.max_source_bytes) is not int or not 1 <= self.max_source_bytes <= 9_223_372_036_854_775_807:
            raise DevelopmentAuthorityError("max_source_bytes")
        if (
            not isinstance(self.execution_subjects, tuple)
            or any(type(item) is not DevelopmentExecutionSubject for item in self.execution_subjects)
            or tuple(sorted(self.execution_subjects)) != self.execution_subjects
            or len(set(self.execution_subjects)) != len(self.execution_subjects)
        ):
            raise DevelopmentAuthorityError("execution_subjects")

    def require_delivery(
        self,
        *,
        source_repository_sha256: str,
        source_commit: str,
        workload_ids: tuple[str, ...],
        source_bytes: int,
        now: datetime,
        current_revocation_epoch: int,
    ) -> None:
        """Compare independently reconstructed source and current trust inputs."""
        self.__post_init__()
        if (
            source_repository_sha256 != self.source_repository_sha256
            or source_commit != self.source_commit
            or not isinstance(workload_ids, tuple)
            or not workload_ids
            or len(workload_ids) != len(set(workload_ids))
            or tuple(sorted(workload_ids)) != workload_ids
            or any(_TOKEN.fullmatch(value) is None for value in workload_ids)
        ):
            raise DevelopmentAuthorityError("delivery_subject")
        if (
            len(workload_ids) > self.max_workloads
            or type(source_bytes) is not int
            or not 0 <= source_bytes <= self.max_source_bytes
        ):
            raise DevelopmentAuthorityError("budget")
        self.require_current(now=now, current_revocation_epoch=current_revocation_epoch)

    def require_current(self, *, now: datetime, current_revocation_epoch: int) -> None:
        """Recheck time and revocation at each protected entrypoint."""
        self.__post_init__()
        if type(current_revocation_epoch) is not int or current_revocation_epoch != self.revocation_epoch:
            raise DevelopmentAuthorityError("revoked")
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise DevelopmentAuthorityError("clock")
        if not _utc(self.not_before) <= now.astimezone(UTC) < _utc(self.expires_at):
            raise DevelopmentAuthorityError("expired_or_not_yet_valid")

    def allows_execution(self, subject: DevelopmentExecutionSubject) -> bool:
        """Return exact membership only; delivery never implies execution."""
        if type(subject) is not DevelopmentExecutionSubject:
            raise DevelopmentAuthorityError("execution_subject")
        subject.__post_init__()
        return subject in self.execution_subjects

    def require_release_budget(self, *, workload_ids: tuple[str, ...], source_bytes: int) -> None:
        """Bind bounded assembled membership without repeating trust verification."""
        if (
            not isinstance(workload_ids, tuple)
            or not workload_ids
            or tuple(sorted(workload_ids)) != workload_ids
            or len(set(workload_ids)) != len(workload_ids)
            or any(_TOKEN.fullmatch(value) is None for value in workload_ids)
        ):
            raise DevelopmentAuthorityError("delivery_subject")
        if (
            len(workload_ids) > self.max_workloads
            or type(source_bytes) is not int
            or not 0 <= source_bytes <= self.max_source_bytes
        ):
            raise DevelopmentAuthorityError("budget")

    def release_projection(self) -> dict[str, Any]:
        """Return identity-bearing nonsecret metadata for a development release."""
        self.__post_init__()
        return {
            "schema": self.schema,
            "policy_sha256": self.policy_sha256,
            "grant_sha256": self.grant_sha256,
            "signature_subject_sha256": self.signature_subject_sha256,
            "environment": self.environment,
            "source_repository_sha256": self.source_repository_sha256,
            "source_commit": self.source_commit,
            "revocation_epoch": self.revocation_epoch,
            "max_workloads": self.max_workloads,
            "max_source_bytes": self.max_source_bytes,
            "execution_subjects": [item.to_dict() for item in self.execution_subjects],
        }


def _utc(value: str) -> datetime:
    if not isinstance(value, str) or _TIMESTAMP.fullmatch(value) is None:
        raise DevelopmentAuthorityError("timestamp")
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise DevelopmentAuthorityError("timestamp") from None


def validate_development_release_projection(value: object) -> tuple[DevelopmentExecutionSubject, ...]:
    """Validate the nonsecret authority projection embedded in release identity."""
    if not isinstance(value, dict) or set(value) != _RELEASE_FIELDS:
        raise DevelopmentAuthorityError("release_projection")
    if value.get("schema") != DEVELOPMENT_AUTHORITY_SCHEMA:
        raise DevelopmentAuthorityError("release_projection")
    for field in ("policy_sha256", "grant_sha256", "signature_subject_sha256", "source_repository_sha256"):
        if not is_canonical_sha256_digest(value.get(field)):
            raise DevelopmentAuthorityError("release_projection")
    if (
        _ENVIRONMENT.fullmatch(str(value.get("environment") or "")) is None
        or _COMMIT.fullmatch(str(value.get("source_commit") or "")) is None
    ):
        raise DevelopmentAuthorityError("release_projection")
    if type(value.get("revocation_epoch")) is not int or value["revocation_epoch"] < 0:
        raise DevelopmentAuthorityError("release_projection")
    if type(value.get("max_workloads")) is not int or not 1 <= value["max_workloads"] <= 10_000:
        raise DevelopmentAuthorityError("release_projection")
    if (
        type(value.get("max_source_bytes")) is not int
        or not 1 <= value["max_source_bytes"] <= 9_223_372_036_854_775_807
    ):
        raise DevelopmentAuthorityError("release_projection")
    raw_subjects = value.get("execution_subjects")
    if not isinstance(raw_subjects, list):
        raise DevelopmentAuthorityError("release_projection")
    subjects = tuple(DevelopmentExecutionSubject.from_dict(item) for item in raw_subjects)
    if subjects != tuple(sorted(subjects)) or len(subjects) != len(set(subjects)):
        raise DevelopmentAuthorityError("release_projection")
    return subjects


def require_development_runtime_authority(
    projection: object,
    *,
    authority: object,
    workload_id: str,
    now: object,
    current_revocation_epoch: object,
) -> None:
    """Bind embedded delivery scope to current external execution authority."""
    validate_development_release_projection(projection)
    if (
        type(authority) is not DevelopmentAuthorityReceipt
        or not isinstance(now, datetime)
        or type(current_revocation_epoch) is not int
        or authority.release_projection() != projection
    ):
        raise DevelopmentAuthorityError("current_external_authority")
    authority.require_current(now=now, current_revocation_epoch=current_revocation_epoch)
    if not authority.allows_execution(DevelopmentExecutionSubject(workload_id, "runtime")):
        raise DevelopmentAuthorityError("workload_execution")


def dbt_development_release_authority_violation(release: Mapping[str, object]) -> str | None:
    """Validate delivery-only development authority without production claims."""
    from dpone.contracts.dbt_release import dbt_release_producer_violation, dbt_selection_fingerprint
    from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2

    if release.get("schema") != DEVELOPMENT_RELEASE_SCHEMA:
        return "development dbt release schema is invalid"
    if release.get("selection_authority") != "dbt_cli":
        return "development dbt release requires dbt-authoritative selection"
    violation = dbt_release_producer_violation(release, expected_wire_contract=DBT_RUNTIME_WIRE_V2)
    if violation is not None:
        return violation
    try:
        validate_development_release_projection(release.get("development_authority"))
    except ValueError:
        return "development dbt release authority projection is invalid"
    provenance = release.get("provenance")
    if not isinstance(provenance, Mapping):
        return "development dbt release requires selection provenance"
    source_snapshot = provenance.get("source_snapshot_sha256")
    selections = provenance.get("selection_fingerprints")
    certifications = provenance.get("route_certifications")
    if (
        not is_canonical_sha256_digest(source_snapshot)
        or not isinstance(selections, list)
        or not selections
        or selections != sorted(set(selections))
        or any(not is_canonical_sha256_digest(item) for item in selections)
        or certifications != []
    ):
        return "development dbt release provenance is invalid"
    expected = dbt_selection_fingerprint(
        source_snapshot_sha256=str(source_snapshot),
        selection_fingerprints=tuple(str(item) for item in selections),
        route_certifications=(),
    )
    if release.get("selection_fingerprint") != expected:
        return "development dbt release selection fingerprint differs from source authority"
    return None


__all__ = [
    "DEVELOPMENT_AUTHORITY_SCHEMA",
    "DEVELOPMENT_COMPOSITION_PROFILE",
    "DEVELOPMENT_RELEASE_SCHEMA",
    "DevelopmentAuthorityError",
    "DevelopmentAuthorityReceipt",
    "DevelopmentExecutionSubject",
    "dbt_development_release_authority_violation",
    "require_development_runtime_authority",
    "validate_development_release_projection",
]
