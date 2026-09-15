"""Immutable physical runtime registration claims, without provisioning I/O.

Construction checks representation and internal consistency only. It does not
authenticate upstream originals, verify permissions or authorize SQL execution.
"""

from dataclasses import dataclass
from uuid import UUID

from dpone.contracts.dbt_mssql_physical_registration_values import (
    DedicatedObserver,
    PhysicalRegistrationError,
    PlatformSelection,
    ProgramAuthority,
    RegisteredLimits,
    RegisteredPrincipals,
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
from dpone.contracts.native_delivery_json import NativeJsonValue
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativePlatformOriginalSubject


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
