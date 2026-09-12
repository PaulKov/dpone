"""Wire authority for supervised composition execution admission.

One cohesive facade over the three authenticated byte sources that decide whether
a workload may execute as immutable composition:

* release-set bytes admit composition and produce the admission marker;
* deployment-set bytes pin the exact non-secret supervisor capability;
* one attempt's runtime evidence proves the worker actually produced a result.

Everything here is pure value and wire semantics. No filesystem access, no
process environment, no policy, and no activation authority. Failures raise
``ValueError`` with one fixed token from ``COMPOSITION_AUTHORITY_REASONS``, so a
runtime caller can report its own public code and never echo release, deployment,
environment or credential text.
"""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping
from typing import Any

from dpone.contracts.composition_supervisor import CompositionSupervisorProjection
from dpone.contracts.release_composition import COMPOSITION_ADMISSION, COMPOSITION_SCHEMA
from dpone.contracts.release_set_authority import admit_release_authority
from dpone.contracts.strict_json import strict_json_object

RUNTIME_RELEASE_ADMISSION_ENV = "DPONE_RUNTIME_RELEASE_ADMISSION"
COMPOSITION_SUPERVISOR_B64_ENV = "DPONE_COMPOSITION_SUPERVISOR_B64"
COMPOSITION_DEPLOYMENT_SCHEMA = "dpone.deployment-set.v3"
MAX_SUPERVISOR_TRANSPORT_CHARS = 4096

RELEASE_AUTHORITY_INVALID = "composition_release_authority"
SUPERVISOR_FORBIDDEN = "composition_supervisor_forbidden"
SUPERVISOR_AUTHORITY_MISSING = "composition_supervisor_authority_missing"
SUPERVISOR_AUTHORITY_INVALID = "composition_supervisor_authority_invalid"
SUPERVISOR_AUTHORITY_NONCANONICAL = "composition_supervisor_authority_noncanonical"
SUPERVISOR_AUTHORITY_OVERSIZE = "composition_supervisor_authority_oversize"
EVIDENCE_INVALID = "composition_evidence_invalid"
COMPOSITION_AUTHORITY_REASONS = frozenset(
    {
        RELEASE_AUTHORITY_INVALID,
        SUPERVISOR_FORBIDDEN,
        SUPERVISOR_AUTHORITY_MISSING,
        SUPERVISOR_AUTHORITY_INVALID,
        SUPERVISOR_AUTHORITY_NONCANONICAL,
        SUPERVISOR_AUTHORITY_OVERSIZE,
        EVIDENCE_INVALID,
    }
)


def is_composition_admission(value: object) -> bool:
    """Return whether ``value`` is exactly the composition admission marker."""

    return value == COMPOSITION_ADMISSION


def release_composition_admission(payload: bytes) -> str | None:
    """Project the composition admission marker from authenticated release bytes.

    Only a registered ``dpone.release-set.v3`` envelope whose constituent
    authority holds produces a marker. Legacy v1/v2 releases return ``None`` and
    keep their existing selection and certification diagnostics unchanged.
    """

    try:
        release = strict_json_object(payload)
        if release.get("schema") != COMPOSITION_SCHEMA:
            return None
        authority = admit_release_authority(release)
    except Exception as exc:
        raise ValueError(RELEASE_AUTHORITY_INVALID) from exc
    if authority.failure is not None:
        raise ValueError(RELEASE_AUTHORITY_INVALID)
    return authority.dbt_runtime_wire_contract


def deployment_supervisor_transport(payload: bytes, *, admission: str | None) -> str | None:
    """Pin the supervisor transport from authenticated deployment-set bytes.

    A composition release (``admission`` is not ``None``) requires the exact
    sealed capability on the v3 deployment wire. A legacy release requires its
    absence, even though legacy releases with MSSQL asset outlets share the same
    v3 deployment wire; deployment wire alone is never composition authority.
    """

    try:
        deployment = strict_json_object(payload)
    except ValueError as exc:
        raise ValueError(SUPERVISOR_AUTHORITY_INVALID) from exc
    value = deployment.get("composition_supervisor")
    if admission is None:
        if value is not None:
            raise ValueError(SUPERVISOR_FORBIDDEN)
        return None
    if value is None:
        raise ValueError(SUPERVISOR_AUTHORITY_MISSING)
    if deployment.get("schema") != COMPOSITION_DEPLOYMENT_SCHEMA or not isinstance(value, Mapping):
        raise ValueError(SUPERVISOR_AUTHORITY_INVALID)
    try:
        projection = CompositionSupervisorProjection.from_mapping(value)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(SUPERVISOR_AUTHORITY_INVALID) from exc
    if dict(value) != projection.to_dict():
        raise ValueError(SUPERVISOR_AUTHORITY_NONCANONICAL)
    return supervisor_transport(projection)


def supervisor_transport(projection: CompositionSupervisorProjection) -> str:
    """Encode one supervisor capability as the canonical non-secret transport."""

    return base64.b64encode(_canonical_supervisor_bytes(projection)).decode("ascii")


def supervisor_from_transport(value: object) -> CompositionSupervisorProjection:
    """Parse exactly one canonical supervisor transport value.

    Padding, alphabet, byte canonicality, JSON object shape, closed field set and
    identity-range policy must all hold. Anything else is rejected: a shape-valid
    but non-canonical value could otherwise smuggle unpinned capability bytes.
    """

    if value is None or value == "":
        raise ValueError(SUPERVISOR_AUTHORITY_MISSING)
    if not isinstance(value, str):
        raise ValueError(SUPERVISOR_AUTHORITY_INVALID)
    if len(value) > MAX_SUPERVISOR_TRANSPORT_CHARS:
        raise ValueError(SUPERVISOR_AUTHORITY_OVERSIZE)
    try:
        payload = base64.b64decode(value.encode("ascii"), validate=True)
    except (binascii.Error, UnicodeEncodeError, ValueError) as exc:
        raise ValueError(SUPERVISOR_AUTHORITY_INVALID) from exc
    if base64.b64encode(payload).decode("ascii") != value:
        raise ValueError(SUPERVISOR_AUTHORITY_NONCANONICAL)
    try:
        mapping = strict_json_object(payload)
    except ValueError as exc:
        raise ValueError(SUPERVISOR_AUTHORITY_INVALID) from exc
    try:
        projection = CompositionSupervisorProjection.from_mapping(mapping)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(SUPERVISOR_AUTHORITY_INVALID) from exc
    if _canonical_supervisor_bytes(projection) != payload:
        raise ValueError(SUPERVISOR_AUTHORITY_NONCANONICAL)
    return projection


def composition_evidence_object(payload: bytes) -> Mapping[str, Any]:
    """Return one duplicate-free evidence object, or reject the attempt bytes."""

    try:
        return strict_json_object(payload)
    except ValueError as exc:
        raise ValueError(EVIDENCE_INVALID) from exc


def _canonical_supervisor_bytes(projection: CompositionSupervisorProjection) -> bytes:
    return json.dumps(
        projection.to_dict(),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")


__all__ = [
    "COMPOSITION_AUTHORITY_REASONS",
    "COMPOSITION_DEPLOYMENT_SCHEMA",
    "COMPOSITION_SUPERVISOR_B64_ENV",
    "EVIDENCE_INVALID",
    "MAX_SUPERVISOR_TRANSPORT_CHARS",
    "RELEASE_AUTHORITY_INVALID",
    "RUNTIME_RELEASE_ADMISSION_ENV",
    "SUPERVISOR_AUTHORITY_INVALID",
    "SUPERVISOR_AUTHORITY_MISSING",
    "SUPERVISOR_AUTHORITY_NONCANONICAL",
    "SUPERVISOR_AUTHORITY_OVERSIZE",
    "SUPERVISOR_FORBIDDEN",
    "CompositionSupervisorProjection",
    "composition_evidence_object",
    "deployment_supervisor_transport",
    "is_composition_admission",
    "release_composition_admission",
    "supervisor_from_transport",
    "supervisor_transport",
]
