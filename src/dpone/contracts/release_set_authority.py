"""Pure release authority decisions around mandatory adapter-owned schema checks."""

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


def release_schema_kind_failure(kind: object) -> ReleaseSetValidationFailure | None:
    """Reject unknown envelopes before an adapter chooses a registered schema."""
    if kind not in {"dpone.release-set.v1", "dpone.release-set.v2", COMPOSITION_SCHEMA}:
        return ReleaseSetValidationFailure("DPONE_RELEASE_SCHEMA_INVALID", "release-set schema is invalid")
    return None


def release_schema_rejection(release: Mapping[str, Any]) -> ReleaseSetValidation:
    """Preserve digest diagnostic priority when mandatory schema validation fails."""
    if _has_invalid_artifact_digest(release):
        return ReleaseSetValidation(
            ReleaseSetValidationFailure("DPONE_DEPLOYMENT_DIGEST_INVALID", "release artifact sha256 is invalid")
        )
    return ReleaseSetValidation(
        ReleaseSetValidationFailure("DPONE_RELEASE_SET_INVALID", "release-set violates its public schema")
    )


def admit_release_authority(release: Mapping[str, Any]) -> ReleaseSetValidation:
    """Check constituent or native authority only AFTER registered schema admission.

    This pure phase grants neither signature trust nor physical activation. The
    schema-validating adapter must reject unknown envelopes and schema failures
    before invoking it.
    """
    kind = release.get("schema")
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
