"""Finite immutable physical registration vocabulary, without SQL authority.

Principal mappings and permission digests are representation claims only. Actual
provisioning must authenticate them and verify effective permissions separately.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from dpone.contracts.dbt_mssql_physical_validation import require_sql_positive_integer
from dpone.contracts.native_delivery_json import MAX_NATIVE_JSON_BYTES, NativeJsonValue, decode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativePlatformOriginalSubject, encode_native_original_subject


class PhysicalRegistrationError(ValueError):
    """Invalid registration representation; diagnostics never echo input values."""


def reference_payload(value: OriginalRef) -> dict[str, NativeJsonValue]:
    """Copy an exact validated reference, without resolving its authority."""
    if type(value) is not OriginalRef:
        raise PhysicalRegistrationError("registration requires an exact original reference")
    value.__post_init__()
    return {"locator": value.locator, "sha256": value.sha256}


def platform_subject_payload(value: NativePlatformOriginalSubject) -> dict[str, NativeJsonValue]:
    """Reuse the existing subject codec while rejecting other subject variants."""
    if type(value) is not NativePlatformOriginalSubject:
        raise PhysicalRegistrationError("registration requires an exact platform subject")
    return decode_native_delivery_json(encode_native_original_subject(value))


def require_registration_digest(value: str) -> None:
    """Use the canonical existing digest grammar; this is not authentication."""
    OriginalRef("registration-digest", value)


@dataclass(frozen=True, slots=True)
class PlatformSelection:
    """Resolved upstream subject and reference, with no hidden subject fallback."""

    reference: OriginalRef
    subject: NativePlatformOriginalSubject

    def __post_init__(self) -> None:
        reference_payload(self.reference)
        platform_subject_payload(self.subject)

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {"reference": reference_payload(self.reference), "subject": platform_subject_payload(self.subject)}


@dataclass(frozen=True, slots=True)
class ProgramAuthority:
    """Pinned program/package/macro hashes; the actual bytes remain unverified."""

    control_program_sha256: str
    package_bundle_sha256: str
    macro_authority_sha256: str

    def __post_init__(self) -> None:
        for digest in (self.control_program_sha256, self.package_bundle_sha256, self.macro_authority_sha256):
            require_registration_digest(digest)

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {
            "control_program_id": "dpone.mssql-physical-control.v1",
            "control_program_sha256": self.control_program_sha256,
            "package_bundle_sha256": self.package_bundle_sha256,
            "macro_authority_sha256": self.macro_authority_sha256,
            "physical_policy": "sqlserver-table-physical-v1",
        }


@dataclass(frozen=True, slots=True)
class RegisteredLimits:
    """Explicit ceilings; definition bytes use UTF-16LE, independent of JSON."""

    max_metadata_bytes: int
    max_generation_bytes: int
    max_catalog_rows: int
    max_definition_utf16_bytes: int
    max_dependency_rows: int
    max_columns: int

    def __post_init__(self) -> None:
        for name in (
            "max_metadata_bytes",
            "max_catalog_rows",
            "max_definition_utf16_bytes",
            "max_dependency_rows",
            "max_columns",
        ):
            require_sql_positive_integer(getattr(self, name), name)
        require_sql_positive_integer(self.max_generation_bytes, "max_generation_bytes", bigint=True)
        if self.max_metadata_bytes > MAX_NATIVE_JSON_BYTES:
            raise PhysicalRegistrationError("metadata ceiling exceeds the canonical JSON bound")
        if self.max_dependency_rows > self.max_catalog_rows:
            raise PhysicalRegistrationError("dependency ceiling exceeds the catalog row ceiling")
        if self.max_columns != 256:
            raise PhysicalRegistrationError("physical admission column ceiling must equal 256")

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {
            "max_metadata_bytes": self.max_metadata_bytes,
            "max_generation_bytes": self.max_generation_bytes,
            "max_catalog_rows": self.max_catalog_rows,
            "max_definition_utf16_bytes": self.max_definition_utf16_bytes,
            "max_dependency_rows": self.max_dependency_rows,
            "max_columns": self.max_columns,
        }


@dataclass(frozen=True, slots=True)
class DatabasePrincipal:
    """Database-local principal ID and exact SID bytes encoded as lowercase hex."""

    principal_id: int
    sid_hex: str

    def __post_init__(self) -> None:
        require_sql_positive_integer(self.principal_id, "principal_id")
        if self.principal_id < 5:
            raise PhysicalRegistrationError("runtime principal ID must be at least 5")
        if type(self.sid_hex) is not str or re.fullmatch(r"(?:[0-9a-f]{2}){1,85}", self.sid_hex) is None:
            raise PhysicalRegistrationError("principal SID requires 1 to 85 lowercase hex-encoded bytes")

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {"principal_id": self.principal_id, "sid_hex": self.sid_hex}


@dataclass(frozen=True, slots=True)
class DatabaseRoleMapping:
    """One role's independent identities in control and model namespaces."""

    control: DatabasePrincipal
    model: DatabasePrincipal

    def __post_init__(self) -> None:
        for principal in (self.control, self.model):
            if type(principal) is not DatabasePrincipal:
                raise PhysicalRegistrationError("role mapping requires exact database principals")
            principal.__post_init__()

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {"control": self.control.to_dict(), "model": self.model.to_dict()}


@dataclass(frozen=True, slots=True)
class DedicatedObserver:
    """Dedicated observation mapping, not an assertion of effective read-only grants."""

    mapping: DatabaseRoleMapping

    def __post_init__(self) -> None:
        if type(self.mapping) is not DatabaseRoleMapping:
            raise PhysicalRegistrationError("dedicated observer requires an exact role mapping")
        self.mapping.__post_init__()

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {"mode": "DEDICATED", "mapping": self.mapping.to_dict()}


@dataclass(frozen=True, slots=True)
class SharedObserver:
    """Explicit sharing representation; a hash alone grants no permission."""

    mode: str
    permission_contract_sha256: str

    def __post_init__(self) -> None:
        if type(self.mode) is not str or self.mode not in {"SHARE_METADATA", "SHARE_BUILD"}:
            raise PhysicalRegistrationError("shared observer requires an exact sharing mode")
        require_registration_digest(self.permission_contract_sha256)

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {"mode": self.mode, "permission_contract_sha256": self.permission_contract_sha256}


@dataclass(frozen=True, slots=True)
class RegisteredPrincipals:
    """Finite role separation evaluated within each database namespace only."""

    metadata: DatabaseRoleMapping
    build: DatabaseRoleMapping
    observer: DedicatedObserver | SharedObserver

    def __post_init__(self) -> None:
        for mapping in (self.metadata, self.build):
            if type(mapping) is not DatabaseRoleMapping:
                raise PhysicalRegistrationError("registered roles require exact database mappings")
            mapping.__post_init__()
        if type(self.observer) not in {DedicatedObserver, SharedObserver}:
            raise PhysicalRegistrationError("observer requires an exact closed role variant")
        self.observer.__post_init__()
        for namespace in ("control", "model"):
            principals = [getattr(self.metadata, namespace), getattr(self.build, namespace)]
            if type(self.observer) is DedicatedObserver:
                principals.append(getattr(self.observer.mapping, namespace))
            if len({principal.principal_id for principal in principals}) != len(principals) or len(
                {principal.sid_hex for principal in principals}
            ) != len(principals):
                raise PhysicalRegistrationError("separate roles require distinct IDs and SIDs in each database")

    def to_dict(self) -> dict[str, NativeJsonValue]:
        return {"metadata": self.metadata.to_dict(), "build": self.build.to_dict(), "observer": self.observer.to_dict()}
