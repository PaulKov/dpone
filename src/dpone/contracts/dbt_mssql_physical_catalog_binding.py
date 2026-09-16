"""Immutable catalog projection representation; construction grants no authority.

The application authenticates actual archive/policy members before producing this
record. The SQL store independently binds it to protected registration and module
observations. Neither a digest nor this DTO substitutes for either operation.
"""

from dataclasses import dataclass, fields
from hashlib import sha256
from typing import TYPE_CHECKING, Any, cast

from dpone.contracts.dbt_execution_pack import SUPPORTED_DBT_ADAPTER
from dpone.contracts.dbt_mssql_physical_registration import (
    MssqlPhysicalRuntimeRegistration,
    physical_runtime_registration_digest,
)
from dpone.contracts.dbt_mssql_physical_registration_values import (
    PlatformSelection,
    RegisteredLimits,
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

if TYPE_CHECKING:
    from dpone.contracts.native_delivery import ResolvedNativeOriginals

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


@dataclass(frozen=True, slots=True)
class CatalogPolicyProjection:
    """Detached policy selection, never an independently authenticating token.

    ``resource_bounds`` identifies the selected opaque profile projection. It is
    not capacity authority and no profile payload is decoded. Bounds are explicit
    platform-authorized policy choices, not an empirical qualification result.
    """

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


def project_catalog_policy(
    *,
    original: "ResolvedNativeOriginals",
    policy_bytes: bytes,
    intent: dict[str, Any],
    registration: MssqlPhysicalRuntimeRegistration,
) -> CatalogPolicyProjection:
    """Compare selected policy with registration; acquire no authentication authority.

    The policy reader must first validate the exact registration, authenticate
    originals, read the actual archive members and compare full policy bytes.
    This pure projection consumes those values without I/O. Direct invocation
    or construction of its result cannot substitute for the reader or the
    independent protected SQL registration and deployment observations.
    """
    policy = cast(dict[str, Any], decode_native_delivery_json(policy_bytes))
    # The concrete member reader has already validated the full v4 schema
    # and exact selected intent. Decode solely to project authenticated fields.
    profile = policy["profiles"][intent["profile"]]
    native = profile["native_execution"]
    catalog = native.get("physical_catalog_limits")
    if catalog is None:
        raise ValueError("catalog provisioning requires native_execution.physical_catalog_limits")
    target = profile.get("authoring_template", {}).get("invocation_target")
    if target is None:
        raise ValueError("catalog provisioning requires authoring_template.invocation_target")
    subject = NativePlatformOriginalSubject(original.authority, original.policy_sha256)
    selected = _selection(native["trusted_execution"]["profile"], subject)
    limits = RegisteredLimits(
        max_metadata_bytes=native["limits"]["max_metadata_bytes"],
        max_generation_bytes=native["generation"]["max_generation_bytes"],
        **catalog,
    )
    if registration.platform_subject != subject:
        raise ValueError("catalog registration PLATFORM subject differs from selected policy")
    if registration.trusted_profile != selected:
        raise ValueError("catalog registration profile reference or subject differs from selected policy")
    if registration.limits != limits:
        raise ValueError("catalog registration limits differ from selected policy")
    control = native["control"]
    if (
        registration.control_authority != OriginalRef(**control["authority"])
        or registration.control_connection_ref != control["connection_ref"]
        or registration.control_schema != control["schema"]
    ):
        raise ValueError("catalog registration control selection differs from selected policy")
    if registration.capacity_authority != OriginalRef(**native["generation"]["capacity_authority"]):
        raise ValueError("catalog registration capacity authority differs from selected policy")
    if registration.trusted_toolchain != _selection(native["trusted_execution"]["toolchain"], subject):
        raise ValueError("catalog registration toolchain selection differs from selected policy")
    if registration.qualification_policy_id != native["trusted_execution"]["qualification_policy_id"]:
        raise ValueError("catalog registration qualification policy differs from selected policy")
    database = require_physical_identifier(target["database"], "model_database")
    schema = native_control_schema(target["schema"])
    if schema.lower() in {
        "dbo",
        "sys",
        "information_schema",
        registration.local_schema.lower(),
        registration.control_schema.lower(),
    }:
        raise ValueError("catalog model schema must be dedicated and distinct from control schemas")
    if registration.model_database.database_name != database:
        raise ValueError("catalog registration model database differs from selected invocation target")
    owners = tuple(
        item
        for item in original.sources.workflows
        if item.project.project_bundle_sha256 == original.project_bundle.archive_sha256
        and item.source.workflow_id == intent["workflow"]
    )
    if len(owners) != 1:
        raise ValueError("catalog policy requires one authenticated execution owner")
    effective = owners[0].execution.invocation_profile()
    if effective.adapter_type != SUPPORTED_DBT_ADAPTER:
        raise ValueError("catalog execution requires the supported SQL Server adapter")
    if registration.model_connection_ref != effective.connection_ref:
        raise ValueError("catalog registration model connection differs from authenticated execution profile")
    if (effective.database, effective.schema) != (database, schema):
        raise ValueError("catalog policy differs from the effective execution target")
    return CatalogPolicyProjection(
        registration.registration_id,
        physical_runtime_registration_digest(registration),
        subject,
        registration.trusted_profile,
        intent["profile"],
        intent["workflow"],
        OriginalRef(NATIVE_POLICY_MEMBER, original.policy_sha256),
        original.project_bundle.archive_sha256,
        database,
        schema,
        selected.reference,
    )


def _selection(value: dict[str, Any], subject: NativePlatformOriginalSubject) -> PlatformSelection:
    retained = value["subject"]
    selected_subject = (
        subject if retained is None else decode_native_original_subject(encode_native_delivery_json(retained))
    )
    if type(selected_subject) is not NativePlatformOriginalSubject:
        raise ValueError("catalog selection requires a PLATFORM subject")
    return PlatformSelection(OriginalRef(**value["reference"]), selected_subject)
