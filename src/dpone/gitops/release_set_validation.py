"""Shared schema orchestration for build and cache release admission."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.release_set_authority import ReleaseSetValidation as ReleaseSetValidation
from dpone.contracts.release_set_authority import ReleaseSetValidationFailure as ReleaseSetValidationFailure
from dpone.contracts.release_set_authority import (
    admit_release_authority,
    release_schema_kind_failure,
    release_schema_rejection,
)
from dpone.contracts.release_set_authority import release_activation_failure as release_activation_failure
from dpone.gitops.schema_validation import GitOpsSchemaValidator


def release_set_validation_failure(release: Mapping[str, Any]) -> ReleaseSetValidationFailure | None:
    """Preserve the original first-failure-only API."""

    return validate_release_set(release).failure


def validate_release_set(release: Mapping[str, Any]) -> ReleaseSetValidation:
    """Run registered schema checks before the pure producer-authority decision."""
    kind = release.get("schema")
    failure = release_schema_kind_failure(kind)
    if failure is not None:
        return ReleaseSetValidation(failure)
    if GitOpsSchemaValidator().validate(release, expected_kind=kind):
        return release_schema_rejection(release)
    return admit_release_authority(release)


__all__ = [
    "ReleaseSetValidation",
    "ReleaseSetValidationFailure",
    "release_set_validation_failure",
    "release_activation_failure",
    "validate_release_set",
]
