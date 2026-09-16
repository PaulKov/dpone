"""Immutable physical runtime registration and its closed canonical encoding.

Construction checks representation and internal consistency only. Neither
construction nor decoding authenticates upstream originals, verifies permissions,
or authorizes SQL execution. Vocabulary remains with its existing value owner.
"""

from dataclasses import dataclass
from hashlib import sha256
from typing import cast
from uuid import UUID

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_mssql_physical_registration_values import (
    DatabasePrincipal,
    DatabaseRoleMapping,
    DedicatedObserver,
    PhysicalRegistrationError,
    PlatformSelection,
    ProgramAuthority,
    RegisteredLimits,
    RegisteredPrincipals,
    SharedObserver,
    platform_subject_payload,
    reference_payload,
    require_registration_digest,
)
from dpone.contracts.dbt_mssql_physical_validation import (
    require_physical_identifier,
    require_physical_text,
    require_physical_timestamp,
    require_physical_uuid,
    require_sql_positive_integer,
)
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativePlatformOriginalSubject, decode_native_original_subject


def database_pin_payload(pin: MssqlDatabaseAuthorityPin) -> dict[str, NativeJsonValue]:
    """Retain exact physical identity without the legacy parser's coercion."""
    if type(pin) is not MssqlDatabaseAuthorityPin:
        raise PhysicalRegistrationError("registration requires an exact database pin")
    require_physical_identifier(pin.database_name, "database_name")
    require_sql_positive_integer(pin.database_id, "database_id")
    require_physical_timestamp(pin.create_token, "create_token")
    if type(pin.database_guid) is not UUID:
        raise PhysicalRegistrationError("database_guid requires an exact UUID")
    return {
        "database_name": pin.database_name,
        "database_id": pin.database_id,
        "create_token": pin.create_token,
        "database_guid": str(pin.database_guid),
    }


@dataclass(frozen=True, slots=True)
class MssqlPhysicalRuntimeRegistration:
    """Complete registration data; UUID is independent and digest stays external.

    Same-database consistency is conservative within the asserted service: an
    equal ID or GUID requires equal full pins and equal per-role mappings. This
    is not a global GUID uniqueness claim or a live same-service verification.
    Capacity authority is capacity-only; no model resource admission is inferred.
    """

    registration_id: str
    platform_subject: NativePlatformOriginalSubject
    control_authority: OriginalRef
    trusted_profile: PlatformSelection
    trusted_toolchain: PlatformSelection
    qualification_policy_id: str
    control_connection_ref: str
    model_connection_ref: str
    service_authority_sha256: str
    control_database: MssqlDatabaseAuthorityPin
    model_database: MssqlDatabaseAuthorityPin
    control_schema: str
    local_schema: str
    program: ProgramAuthority
    capacity_authority: OriginalRef
    limits: RegisteredLimits
    principals: RegisteredPrincipals

    def __post_init__(self) -> None:
        require_physical_uuid(self.registration_id, "registration_id")
        platform_subject_payload(self.platform_subject)
        reference_payload(self.control_authority)
        reference_payload(self.capacity_authority)
        for value, expected in (
            (self.trusted_profile, PlatformSelection),
            (self.trusted_toolchain, PlatformSelection),
            (self.program, ProgramAuthority),
            (self.limits, RegisteredLimits),
            (self.principals, RegisteredPrincipals),
        ):
            if type(value) is not expected:
                raise PhysicalRegistrationError("registration requires exact nested records")
            value.__post_init__()
        for name in ("qualification_policy_id", "control_connection_ref", "model_connection_ref"):
            label = require_physical_text(getattr(self, name), name)
            if len(label) > 256 or not label.strip():
                raise PhysicalRegistrationError("registration policy labels require nonblank bounded tokens")
        require_registration_digest(self.service_authority_sha256)
        database_pin_payload(self.control_database)
        database_pin_payload(self.model_database)
        native_control_schema(self.control_schema)
        native_control_schema(self.local_schema)
        control, model = self.control_database, self.model_database
        if control.database_id == model.database_id or control.database_guid == model.database_guid:
            if control != model:
                raise PhysicalRegistrationError("matching database identity components require equal complete pins")
            mappings = [self.principals.metadata, self.principals.build]
            if type(self.principals.observer) is DedicatedObserver:
                mappings.append(self.principals.observer.mapping)
            if any(mapping.control != mapping.model for mapping in mappings):
                raise PhysicalRegistrationError("same-database role mappings must be equal")

    def to_dict(self) -> dict[str, NativeJsonValue]:
        """Return a detached complete projection, not authenticated evidence."""
        return {
            "schema": "dpone.mssql-physical-runtime-registration.v1",
            "registration_id": self.registration_id,
            "platform_subject": platform_subject_payload(self.platform_subject),
            "control_authority": reference_payload(self.control_authority),
            "trusted_profile": self.trusted_profile.to_dict(),
            "trusted_toolchain": self.trusted_toolchain.to_dict(),
            "qualification_policy_id": self.qualification_policy_id,
            "control_connection_ref": self.control_connection_ref,
            "model_connection_ref": self.model_connection_ref,
            "service_authority_sha256": self.service_authority_sha256,
            "control_database": database_pin_payload(self.control_database),
            "model_database": database_pin_payload(self.model_database),
            "control_schema": self.control_schema,
            "local_schema": self.local_schema,
            "program": self.program.to_dict(),
            "capacity_authority": reference_payload(self.capacity_authority),
            "limits": self.limits.to_dict(),
            "principals": self.principals.to_dict(),
        }


def _object(value: object, keys: str) -> dict[str, NativeJsonValue]:
    if type(value) is not dict or set(value) != set(keys.split()):
        raise PhysicalRegistrationError("registration record requires exact closed fields")
    return cast(dict[str, NativeJsonValue], value)


def _reference(value: object) -> OriginalRef:
    obj = _object(value, "locator sha256")
    return OriginalRef(require_physical_text(obj["locator"], "locator"), require_physical_text(obj["sha256"], "sha256"))


def _subject(value: NativeJsonValue) -> NativePlatformOriginalSubject:
    if type(value) is not dict:
        raise PhysicalRegistrationError("registration subject requires an object")
    result = decode_native_original_subject(encode_native_delivery_json(value))
    if type(result) is not NativePlatformOriginalSubject:
        raise PhysicalRegistrationError("registration requires a platform subject")
    return result


def _selection(value: object) -> PlatformSelection:
    obj = _object(value, "reference subject")
    return PlatformSelection(_reference(obj["reference"]), _subject(obj["subject"]))


def _pin(value: object) -> MssqlDatabaseAuthorityPin:
    obj = _object(value, "database_name database_id create_token database_guid")
    return MssqlDatabaseAuthorityPin(
        require_physical_text(obj["database_name"], "database_name"),
        require_sql_positive_integer(obj["database_id"], "database_id"),
        require_physical_text(obj["create_token"], "create_token"),
        UUID(require_physical_uuid(obj["database_guid"], "database_guid")),
    )


def _program(value: object) -> ProgramAuthority:
    obj = _object(
        value, "control_program_id control_program_sha256 package_bundle_sha256 macro_authority_sha256 physical_policy"
    )
    if (
        obj["control_program_id"] != "dpone.mssql-physical-control.v1"
        or obj["physical_policy"] != "sqlserver-table-physical-v1"
    ):
        raise PhysicalRegistrationError("registration program tags are unsupported")
    return ProgramAuthority(
        *(
            require_physical_text(obj[key], key)
            for key in ("control_program_sha256", "package_bundle_sha256", "macro_authority_sha256")
        )
    )


def _limits(value: object) -> RegisteredLimits:
    keys = "max_metadata_bytes max_generation_bytes max_catalog_rows max_definition_utf16_bytes max_dependency_rows max_columns"
    obj = _object(value, keys)
    return RegisteredLimits(
        *(require_sql_positive_integer(obj[key], key, bigint=key == "max_generation_bytes") for key in keys.split())
    )


def _principal(value: object) -> DatabasePrincipal:
    obj = _object(value, "principal_id sid_hex")
    return DatabasePrincipal(
        require_sql_positive_integer(obj["principal_id"], "principal_id"),
        require_physical_text(obj["sid_hex"], "sid_hex"),
    )


def _mapping(value: object) -> DatabaseRoleMapping:
    obj = _object(value, "control model")
    return DatabaseRoleMapping(_principal(obj["control"]), _principal(obj["model"]))


def _observer(value: object) -> DedicatedObserver | SharedObserver:
    if type(value) is not dict:
        raise PhysicalRegistrationError("observer requires a closed object")
    if value.get("mode") == "DEDICATED":
        obj = _object(value, "mode mapping")
        return DedicatedObserver(_mapping(obj["mapping"]))
    obj = _object(value, "mode permission_contract_sha256")
    return SharedObserver(
        require_physical_text(obj["mode"], "mode"),
        require_physical_text(obj["permission_contract_sha256"], "permission_contract_sha256"),
    )


def _principals(value: object) -> RegisteredPrincipals:
    obj = _object(value, "metadata build observer")
    return RegisteredPrincipals(_mapping(obj["metadata"]), _mapping(obj["build"]), _observer(obj["observer"]))


def _registration(value: object) -> MssqlPhysicalRuntimeRegistration:
    obj = _object(
        value,
        "schema registration_id platform_subject control_authority trusted_profile trusted_toolchain "
        "qualification_policy_id control_connection_ref model_connection_ref service_authority_sha256 "
        "control_database model_database control_schema local_schema program capacity_authority limits principals",
    )
    if obj["schema"] != "dpone.mssql-physical-runtime-registration.v1":
        raise PhysicalRegistrationError("registration schema is unsupported")
    return MssqlPhysicalRuntimeRegistration(
        registration_id=require_physical_uuid(obj["registration_id"], "registration_id"),
        platform_subject=_subject(obj["platform_subject"]),
        control_authority=_reference(obj["control_authority"]),
        trusted_profile=_selection(obj["trusted_profile"]),
        trusted_toolchain=_selection(obj["trusted_toolchain"]),
        qualification_policy_id=require_physical_text(obj["qualification_policy_id"], "qualification_policy_id"),
        control_connection_ref=require_physical_text(obj["control_connection_ref"], "control_connection_ref"),
        model_connection_ref=require_physical_text(obj["model_connection_ref"], "model_connection_ref"),
        service_authority_sha256=require_physical_text(obj["service_authority_sha256"], "service_authority_sha256"),
        control_database=_pin(obj["control_database"]),
        model_database=_pin(obj["model_database"]),
        control_schema=require_physical_text(obj["control_schema"], "control_schema"),
        local_schema=require_physical_text(obj["local_schema"], "local_schema"),
        program=_program(obj["program"]),
        capacity_authority=_reference(obj["capacity_authority"]),
        limits=_limits(obj["limits"]),
        principals=_principals(obj["principals"]),
    )


def encode_physical_runtime_registration(value: MssqlPhysicalRuntimeRegistration) -> bytes:
    """Encode the complete bounded document; no self-digest field is omitted."""
    try:
        if type(value) is not MssqlPhysicalRuntimeRegistration:
            raise PhysicalRegistrationError("encoding requires an exact registration")
        value.__post_init__()
        return encode_native_delivery_json(value.to_dict())
    except (ValueError, TypeError, DbtPublishingError):
        raise PhysicalRegistrationError("invalid physical registration representation") from None


def decode_physical_runtime_registration(payload: bytes) -> MssqlPhysicalRuntimeRegistration:
    """Reject alternate bytes and malformed records without external lookups."""
    try:
        result = _registration(decode_native_delivery_json(payload))
        if encode_physical_runtime_registration(result) != payload:
            raise PhysicalRegistrationError("registration bytes are not canonical")
        return result
    except (ValueError, TypeError, DbtPublishingError):
        raise PhysicalRegistrationError("invalid canonical physical registration") from None


def physical_runtime_registration_digest(value: MssqlPhysicalRuntimeRegistration) -> str:
    """Hash all canonical bytes; this digest alone is not authentication."""
    return "sha256:" + sha256(encode_physical_runtime_registration(value)).hexdigest()


# Preserve historical global lookup coordinates for existing and new pickles.
encode_physical_runtime_registration.__module__ = "dpone.contracts.dbt_mssql_physical_registration_codec"
decode_physical_runtime_registration.__module__ = "dpone.contracts.dbt_mssql_physical_registration_codec"
physical_runtime_registration_digest.__module__ = "dpone.contracts.dbt_mssql_physical_registration_codec"
