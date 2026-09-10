"""Shared public release-set validation for build and cache activation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.dbt_release import (
    DBT_RELEASE_WIRE_CONTRACT,
    dbt_release_authority_violation,
    dbt_release_runtime_wire_contract,
    is_workspace_dbt_wire,
)
from dpone.contracts.release_composition import COMPOSITION_ADMISSION, COMPOSITION_SCHEMA
from dpone.contracts.release_composition_policy import validate_composition_metadata
from dpone.gitops.schema_validation import GitOpsSchemaValidator


@dataclass(frozen=True, slots=True)
class ReleaseSetValidationFailure:
    """Stable failure shared by adapters with different public error types."""

    code: str
    message: str


@dataclass(frozen=True, slots=True)
class ReleaseSetValidation:
    """Shared schema/authority result, not a signature or activation grant."""

    failure: ReleaseSetValidationFailure | None = None
    dbt_runtime_wire_contract: str | None = None


def release_set_validation_failure(release: Mapping[str, Any]) -> ReleaseSetValidationFailure | None:
    """Preserve the original first-failure-only API."""

    return validate_release_set(release).failure


def validate_release_set(release: Mapping[str, Any]) -> ReleaseSetValidation:
    """Preserve verified producer identity with the schema/authority decision."""

    kind = release.get("schema")
    if kind not in {"dpone.release-set.v1", "dpone.release-set.v2", COMPOSITION_SCHEMA}:
        return ReleaseSetValidation(
            ReleaseSetValidationFailure("DPONE_RELEASE_SCHEMA_INVALID", "release-set schema is invalid")
        )
    if GitOpsSchemaValidator().validate(release, expected_kind=kind):
        if _has_invalid_artifact_digest(release):
            return ReleaseSetValidation(
                ReleaseSetValidationFailure("DPONE_DEPLOYMENT_DIGEST_INVALID", "release artifact sha256 is invalid")
            )
        return ReleaseSetValidation(
            ReleaseSetValidationFailure("DPONE_RELEASE_SET_INVALID", "release-set violates its public schema")
        )
    if kind == COMPOSITION_SCHEMA:
        try:
            validate_composition_metadata(release)
        except (ValueError, TypeError, KeyError):
            return ReleaseSetValidation(
                ReleaseSetValidationFailure(
                    "DPONE_COMPOSITION_INVALID", "composition ownership or constituent authority is invalid"
                )
            )
        return ReleaseSetValidation(dbt_runtime_wire_contract=COMPOSITION_ADMISSION)
    if kind == "dpone.release-set.v1" and "producer" not in release:
        return ReleaseSetValidation()
    try:
        wire = dbt_release_runtime_wire_contract(release)
        if kind == "dpone.release-set.v1":
            if wire == DBT_RELEASE_WIRE_CONTRACT:
                return ReleaseSetValidation(dbt_runtime_wire_contract=wire)
        elif dbt_release_authority_violation(release, expected_wire_contract=wire) is None:
            return ReleaseSetValidation(dbt_runtime_wire_contract=wire)
    except ValueError:
        pass
    return ReleaseSetValidation(
        ReleaseSetValidationFailure(
            "DPONE_RELEASE_SET_INVALID", "dbt release-set violates its selection or certification authority"
        )
    )


def release_activation_failure(dbt_wire: str | None) -> ReleaseSetValidationFailure | None:
    """Keep workspace reader support separate from physical activation authority."""

    if dbt_wire == COMPOSITION_ADMISSION:
        return ReleaseSetValidationFailure(
            "DPONE_COMPOSITION_ADMISSION_UNAVAILABLE",
            "composition activation requires physical-target admission for all constituents; current is unchanged",
        )
    if is_workspace_dbt_wire(dbt_wire):
        return ReleaseSetValidationFailure(
            "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE",
            "workspace activation requires certified physical-target admission; the existing current is unchanged",
        )
    return None


def _has_invalid_artifact_digest(release: Mapping[str, Any]) -> bool:
    artifacts = release.get("artifacts")
    if not isinstance(artifacts, Mapping):
        return False
    return any(
        isinstance(item, Mapping) and not is_canonical_sha256_digest(item.get("sha256"))
        for entries in artifacts.values()
        if isinstance(entries, list)
        for item in entries
    )


__all__ = [
    "ReleaseSetValidation",
    "ReleaseSetValidationFailure",
    "release_set_validation_failure",
    "release_activation_failure",
    "validate_release_set",
]
