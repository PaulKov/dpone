"""Immutable catalog projection representation; construction grants no authority.

The application authenticates actual archive/policy members before producing this
record. The SQL store independently binds it to protected registration and module
observations. Neither a digest nor this DTO substitutes for either operation.
"""

from dataclasses import dataclass, fields
from hashlib import sha256
from typing import Any, cast

from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
from dpone.contracts.dbt_mssql_physical_registration_values import (
    PlatformSelection,
    platform_subject_payload,
    reference_payload,
    require_registration_digest,
)
from dpone.contracts.dbt_mssql_physical_validation import (
    require_physical_identifier,
    require_physical_text,
    require_physical_uuid,
    require_sql_positive_integer,
)
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativePlatformOriginalSubject, decode_native_original_subject
from dpone.contracts.native_project_documents import NATIVE_POLICY_MEMBER

BINDING_SCHEMA = "dpone.mssql-physical-catalog-binding.v1"


@dataclass(frozen=True, slots=True)
class CatalogRegistrationBinding:
    """Canonical selected policy and observed deployment, without copied limits."""

    registration_id: str
    registration_sha256: str
    platform_subject: NativePlatformOriginalSubject
    trusted_profile: PlatformSelection
    profile_name: str
    workflow_id: str
    policy_member: OriginalRef
    project_archive_sha256: str
    model_database_name: str
    model_schema: str
    resource_bounds: OriginalRef
    model_schema_id: int
    model_schema_owner_id: int
    catalog_module_sha256: str

    def __post_init__(self) -> None:
        require_physical_uuid(self.registration_id, "registration_id")
        for name in ("registration_sha256", "project_archive_sha256", "catalog_module_sha256"):
            require_registration_digest(getattr(self, name))
        platform_subject_payload(self.platform_subject)
        if type(self.trusted_profile) is not PlatformSelection:
            raise ValueError("catalog binding requires an exact profile selection")
        self.trusted_profile.__post_init__()
        for name in ("profile_name", "workflow_id"):
            value = require_physical_text(getattr(self, name), name)
            if not value.strip() or len(value.encode("utf-8")) > 4096:
                raise ValueError("catalog binding requires bounded nonblank selection labels")
        reference_payload(self.policy_member)
        reference_payload(self.resource_bounds)
        if self.policy_member != OriginalRef(NATIVE_POLICY_MEMBER, self.platform_subject.platform_policy_sha256):
            raise ValueError("catalog binding policy member differs from its PLATFORM subject")
        if self.resource_bounds != self.trusted_profile.reference:
            raise ValueError("catalog binding bounds must identify its selected profile projection")
        require_physical_identifier(self.model_database_name, "model_database_name")
        native_control_schema(self.model_schema)
        if self.model_schema.casefold() in {"dbo", "sys", "information_schema"}:
            raise ValueError("catalog binding requires a dedicated model schema")
        require_sql_positive_integer(self.model_schema_id, "model_schema_id")
        if type(self.model_schema_owner_id) is not int or self.model_schema_owner_id != 1:
            raise ValueError("catalog binding requires dbo model schema ownership")

    def to_dict(self) -> dict[str, NativeJsonValue]:
        """Return a detached closed representation, never a verification receipt."""
        result: dict[str, NativeJsonValue] = {
            "schema": BINDING_SCHEMA,
            "platform_subject": platform_subject_payload(self.platform_subject),
            "trusted_profile": self.trusted_profile.to_dict(),
            "policy_member": reference_payload(self.policy_member),
            "resource_bounds": reference_payload(self.resource_bounds),
        }
        for field in fields(self):
            if field.name not in result:
                result[field.name] = getattr(self, field.name)
        return result


def encode_catalog_binding(value: CatalogRegistrationBinding) -> bytes:
    """Encode a validated exact record using the existing bounded native codec."""
    if type(value) is not CatalogRegistrationBinding:
        raise ValueError("catalog binding requires an exact record")
    value.__post_init__()
    return encode_native_delivery_json(value.to_dict())


def catalog_binding_digest(value: CatalogRegistrationBinding) -> str:
    """Hash full canonical bytes; this is integrity, not authentication."""
    return "sha256:" + sha256(encode_catalog_binding(value)).hexdigest()


def decode_catalog_binding(payload: bytes) -> CatalogRegistrationBinding:
    """Reject noncanonical, unknown, incomplete or inconsistent binding bytes."""
    raw = cast(dict[str, Any], decode_native_delivery_json(payload))
    if set(raw) != {"schema", *(field.name for field in fields(CatalogRegistrationBinding))}:
        raise ValueError("catalog binding has unknown or missing fields")
    if raw["schema"] != BINDING_SCHEMA or encode_native_delivery_json(raw) != payload:
        raise ValueError("catalog binding requires its canonical closed schema")
    values = dict(raw)
    del values["schema"]
    try:
        values["platform_subject"] = decode_native_original_subject(
            encode_native_delivery_json(raw["platform_subject"])
        )
        selected = raw["trusted_profile"]
        if type(selected) is not dict or set(selected) != {"reference", "subject"}:
            raise ValueError("catalog binding requires a closed profile selection")
        subject = decode_native_original_subject(encode_native_delivery_json(selected["subject"]))
        if type(subject) is not NativePlatformOriginalSubject:
            raise ValueError("catalog binding requires a PLATFORM profile subject")
        values["trusted_profile"] = PlatformSelection(OriginalRef(**selected["reference"]), subject)
        for name in ("policy_member", "resource_bounds"):
            values[name] = OriginalRef(**raw[name])
        return CatalogRegistrationBinding(**values)
    except (TypeError, KeyError) as exc:
        raise ValueError("catalog binding contains malformed nested fields") from exc


def require_catalog_binding_registration(
    value: CatalogRegistrationBinding, registration: MssqlPhysicalRuntimeRegistration
) -> None:
    """Compare exact claimed identities; the store still verifies retained SQL."""
    encode_catalog_binding(value)
    digest = physical_runtime_registration_digest(registration)
    if (
        value.registration_id != registration.registration_id
        or value.registration_sha256 != digest
        or value.platform_subject != registration.platform_subject
        or value.trusted_profile != registration.trusted_profile
        or value.model_database_name != registration.model_database.database_name
        or value.model_schema.casefold()
        in {registration.local_schema.casefold(), registration.control_schema.casefold()}
        or len(encode_catalog_binding(value)) > registration.limits.max_metadata_bytes
    ):
        raise ValueError("catalog binding differs from its complete registration or metadata budget")
