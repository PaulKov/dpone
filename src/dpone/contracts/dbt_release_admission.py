"""Pure native installation proof before and after adapter-owned schema checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.dbt_release import (
    dbt_release_authority_violation,
    dbt_release_producer_violation,
    dbt_release_runtime_wire_contract,
    is_workspace_dbt_wire,
)
from dpone.contracts.strict_json import StrictJsonError, strict_json_object


class NativeReleaseAdmissionError(ValueError):
    """A native producer installation proof failed before publication."""


@dataclass(frozen=True, slots=True)
class NativeReleaseAdmission:
    """Detached parsed metadata; registered schema admission remains mandatory.

    The adapter validates the registered schema before requiring identity and
    producer compatibility. This record does not authorize source capture,
    signatures, activation or execution by itself.
    """

    release: dict[str, Any]
    wire_contract: str

    @property
    def is_workspace(self) -> bool:
        return is_workspace_dbt_wire(self.wire_contract)

    def require_identity(self, expected_release_id: str | None) -> str:
        """Check claimed, content-addressed and requested identity in that order."""
        claimed = self.release.get("release_id")
        if not isinstance(claimed, str):
            raise NativeReleaseAdmissionError("compiled dbt release id is invalid")
        if claimed != compute_release_id(self.release):
            raise NativeReleaseAdmissionError("compiled dbt release identity does not match its content")
        if expected_release_id is not None and claimed != expected_release_id:
            raise NativeReleaseAdmissionError("compiled dbt release differs from the expected release")
        return claimed

    def require_producer(self, producer_version: str) -> None:
        """Check the explicitly supplied local producer version after identity."""
        violation = dbt_release_producer_violation(
            self.release, expected_dpone_version=producer_version, expected_wire_contract=self.wire_contract
        )
        if violation is not None:
            raise NativeReleaseAdmissionError(violation)


def parse_native_release_admission(payload: bytes) -> NativeReleaseAdmission:
    """Decode bounded bytes and require native wire authority before schema checks.

    Acquisition bounds belong to the caller. Errors preserve the native adapter's
    JSON, discriminator, producer and authority diagnostic priority.
    """
    try:
        release = strict_json_object(payload)
    except StrictJsonError as exc:
        raise NativeReleaseAdmissionError("compiled dbt release-set is invalid JSON") from exc
    if release.get("schema") != "dpone.release-set.v2":
        raise NativeReleaseAdmissionError("compiled dbt release must use dpone.release-set.v2")
    try:
        wire = dbt_release_runtime_wire_contract(release)
    except ValueError as exc:
        raise NativeReleaseAdmissionError("compiled dbt release producer identity is invalid") from exc
    violation = dbt_release_authority_violation(release, expected_wire_contract=wire)
    if violation is not None:
        raise NativeReleaseAdmissionError(violation)
    return NativeReleaseAdmission(release, wire)
