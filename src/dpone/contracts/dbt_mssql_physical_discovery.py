"""Closed temporal discovery claims, not plan membership or execution authority."""

from dataclasses import asdict, dataclass
from hashlib import sha256
from typing import Any, cast
from uuid import UUID

from dpone.contracts.dbt_mssql_physical_catalog_binding import (
    CatalogRegistrationBinding,
    require_catalog_binding_registration,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
from dpone.contracts.dbt_mssql_physical_registration_values import (
    DatabasePrincipal,
    platform_subject_payload,
    require_registration_digest,
)
from dpone.contracts.dbt_mssql_physical_validation import (
    require_physical_identifier,
    require_physical_text,
    require_physical_uuid,
    require_sql_positive_integer,
)
from dpone.contracts.dbt_workspace_activation import DbtWorkspaceGuardEpoch
from dpone.contracts.dbt_workspace_attempt import DbtWorkspaceAttemptRequest
from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_originals import NativePlatformOriginalSubject, decode_native_original_subject

REQUEST_SCHEMA = "dpone.mssql-physical-discovery-request.v1"
_ROLES = ("TARGET", "CANDIDATE", "HELPER")


@dataclass(frozen=True, slots=True)
class DiscoveryObject:
    """One exact proposed schema-object name; collation checks belong to SQL."""

    model_unique_id: str
    role: str
    name: str

    def __post_init__(self) -> None:
        require_physical_text(self.model_unique_id, "model_unique_id")
        require_physical_identifier(self.name, "name")
        if type(self.role) is not str or self.role not in _ROLES:
            raise ValueError("discovery requires TARGET, CANDIDATE or HELPER")


@dataclass(frozen=True, slots=True)
class PhysicalDiscoveryRequest:
    """Complete P identity and proposed names; UUIDs only correlate preparation."""

    subject: NativePlatformOriginalSubject
    workspace_attempt: DbtWorkspaceAttemptRequest
    guard: DbtWorkspaceGuardEpoch
    generation_id: str
    invocation_id: str
    filegroup_name: str
    objects: tuple[DiscoveryObject, ...]

    def __post_init__(self) -> None:
        platform_subject_payload(self.subject)
        if (
            type(self.workspace_attempt) is not DbtWorkspaceAttemptRequest
            or type(self.guard) is not DbtWorkspaceGuardEpoch
        ):
            raise ValueError("discovery requires exact attempt and guard records")
        self.workspace_attempt.__post_init__()
        self.guard.__post_init__()
        require_physical_text(self.guard.guard_id, "guard_id")
        if len(self.guard.guard_id.encode("utf-16-le")) > 1024:
            raise ValueError("discovery guard exceeds SQL owner width")
        require_physical_uuid(self.workspace_attempt.activation_id, "activation_id")
        require_sql_positive_integer(self.guard.fencing_epoch, "fencing_epoch", bigint=True)
        for name in ("generation_id", "invocation_id"):
            require_physical_uuid(getattr(self, name), name)
        require_physical_identifier(self.filegroup_name, "filegroup_name")
        if type(self.objects) is not tuple or not self.objects or len(self.objects) % 3:
            raise ValueError("discovery requires nonempty complete role triples")
        previous = b""
        for index in range(0, len(self.objects), 3):
            group = self.objects[index : index + 3]
            if any(type(value) is not DiscoveryObject for value in group):
                raise ValueError("discovery requires exact object records")
            for value in group:
                value.__post_init__()
            identity = group[0].model_unique_id.encode("utf-8")
            if (
                identity <= previous
                or tuple(value.role for value in group) != _ROLES
                or any(value.model_unique_id != group[0].model_unique_id for value in group)
            ):
                raise ValueError("discovery requires unique UTF-8 ordered model triples")
            previous = identity
        if len({value.name for value in self.objects}) != len(self.objects):
            raise ValueError("discovery object names must be distinct before SQL collation checks")

    def to_dict(self) -> dict[str, NativeJsonValue]:
        """Detach the exact request; no database/schema/budget overrides exist."""
        return dict(
            schema=REQUEST_SCHEMA,
            subject=platform_subject_payload(self.subject),
            workspace_attempt=cast(
                dict[str, NativeJsonValue],
                {**asdict(self.workspace_attempt), "write_subjects": list(self.workspace_attempt.write_subjects)},
            ),
            guard=cast(dict[str, NativeJsonValue], self.guard.to_dict()),
            generation_id=self.generation_id,
            invocation_id=self.invocation_id,
            filegroup_name=self.filegroup_name,
            objects=[cast(dict[str, NativeJsonValue], asdict(value)) for value in self.objects],
        )


def encode_discovery_request(value: PhysicalDiscoveryRequest, *, max_bytes: int, max_objects: int) -> bytes:
    """Enforce explicit registration-derived limits and the native JSON ceiling."""
    require_sql_positive_integer(max_bytes, "max_bytes")
    require_sql_positive_integer(max_objects, "max_objects")
    if type(value) is not PhysicalDiscoveryRequest:
        raise ValueError("discovery requires an exact request")
    value.__post_init__()
    if len(value.objects) > max_objects:
        raise ValueError("discovery object bound exceeded")
    payload = encode_native_delivery_json(value.to_dict())
    if len(payload) > min(max_bytes, 1048576):
        raise ValueError("discovery metadata bound exceeded")
    return payload


def decode_discovery_request(payload: bytes, *, max_bytes: int, max_objects: int) -> PhysicalDiscoveryRequest:
    """Reject incomplete, noncanonical or coerced nested requests before SQL."""
    require_sql_positive_integer(max_bytes, "max_bytes")
    require_sql_positive_integer(max_objects, "max_objects")
    if type(payload) is not bytes or not 0 < len(payload) <= min(max_bytes, 1048576):
        raise ValueError("discovery metadata bound exceeded")
    raw = cast(dict[str, Any], decode_native_delivery_json(payload))
    if (
        set(raw)
        != {
            "schema",
            "subject",
            "workspace_attempt",
            "guard",
            "generation_id",
            "invocation_id",
            "filegroup_name",
            "objects",
        }
        or raw["schema"] != REQUEST_SCHEMA
    ):
        raise ValueError("discovery requires its closed request schema")
    if type(raw["objects"]) is not list or len(raw["objects"]) > max_objects:
        raise ValueError("discovery object bound exceeded")
    try:
        attempt = dict(raw["workspace_attempt"])
        if type(attempt.get("write_subjects")) is not list:
            raise ValueError("discovery write subjects require an ordered array")
        attempt["write_subjects"] = tuple(attempt["write_subjects"])
        subject = decode_native_original_subject(encode_native_delivery_json(raw["subject"]))
        if type(subject) is not NativePlatformOriginalSubject:
            raise ValueError("discovery requires a PLATFORM subject")
        result = PhysicalDiscoveryRequest(
            subject,
            DbtWorkspaceAttemptRequest(**attempt),
            DbtWorkspaceGuardEpoch(**raw["guard"]),
            raw["generation_id"],
            raw["invocation_id"],
            raw["filegroup_name"],
            tuple(DiscoveryObject(**item) for item in raw["objects"]),
        )
    except (TypeError, KeyError) as error:
        raise ValueError("discovery has malformed nested fields") from error
    if encode_discovery_request(result, max_bytes=max_bytes, max_objects=max_objects) != payload:
        raise ValueError("discovery requires canonical request bytes")
    return result


def discovery_request_digest(payload: bytes) -> str:
    """Hash complete encoded request bytes, without granting selection authority."""
    return "sha256:" + sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class PhysicalDiscoveryObservation:
    """Acknowledged temporal observation, never a durable absence reservation."""

    registration_id: str
    registration_digest: str
    request_digest: str
    database_id: int
    database_guid: UUID
    schema_id: int
    schema_name: str
    schema_owner_id: int
    fencing_epoch: int
    object_count: int
    filegroup_id: int
    filegroup_name: str
    filegroup_type: str
    model_principal: DatabasePrincipal
    control_principal: DatabasePrincipal

    def __post_init__(self) -> None:
        require_physical_uuid(self.registration_id, "registration_id")
        require_registration_digest(self.registration_digest)
        require_registration_digest(self.request_digest)
        for name in ("database_id", "schema_id", "schema_owner_id", "object_count", "filegroup_id"):
            require_sql_positive_integer(getattr(self, name), name)
        require_sql_positive_integer(self.fencing_epoch, "fencing_epoch", bigint=True)
        for name in ("schema_name", "filegroup_name"):
            require_physical_identifier(getattr(self, name), name)
        if (
            type(self.database_guid) is not UUID
            or self.schema_owner_id != 1
            or type(self.filegroup_type) is not str
            or self.filegroup_type != "FG"
        ):
            raise ValueError("discovery database/schema/filegroup facts are unsupported")
        for principal in (self.model_principal, self.control_principal):
            if type(principal) is not DatabasePrincipal:
                raise ValueError("discovery requires exact principal facts")
            principal.__post_init__()


def require_discovery_observation(
    value: PhysicalDiscoveryObservation,
    registration: MssqlPhysicalRuntimeRegistration,
    binding: CatalogRegistrationBinding,
    request: PhysicalDiscoveryRequest,
    payload: bytes,
) -> None:
    """Compare all observed identities with complete expected request and cohort."""
    value.__post_init__()
    require_catalog_binding_registration(binding, registration)
    if (
        request.subject != registration.platform_subject
        or value.registration_id != registration.registration_id
        or value.registration_digest != physical_runtime_registration_digest(registration)
        or value.request_digest != discovery_request_digest(payload)
        or (value.database_id, value.database_guid)
        != (registration.model_database.database_id, registration.model_database.database_guid)
        or (value.schema_id, value.schema_name, value.schema_owner_id)
        != (binding.model_schema_id, binding.model_schema, binding.model_schema_owner_id)
        or value.fencing_epoch != request.guard.fencing_epoch
        or value.object_count != len(request.objects)
        or value.filegroup_name != request.filegroup_name
        or value.model_principal != registration.principals.metadata.model
        or value.control_principal != registration.principals.metadata.control
    ):
        raise ValueError("discovery observed facts differ from request or registration")


def decode_discovery_observation(row: tuple[object, ...]) -> PhysicalDiscoveryObservation:
    """Decode one driver-normalized 18-field tuple, retaining exact primitives."""
    if type(row) is not tuple or len(row) != 18 or type(row[0]) is not int or row[0] != 1:
        raise ValueError("discovery requires its exact version-one scalar row")
    values = cast(tuple[Any, ...], row)
    return PhysicalDiscoveryObservation(
        values[1],
        values[2],
        values[3],
        values[4],
        values[5],
        values[6],
        values[7],
        values[8],
        values[9],
        values[10],
        values[11],
        values[12],
        values[13],
        DatabasePrincipal(values[14], values[15]),
        DatabasePrincipal(values[16], values[17]),
    )
