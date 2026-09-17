"""Closed ephemeral transport packet representation, not admission authority.

Only the qualified launcher's inherited descriptor authenticates delivery. A
constructed or decoded value, including every digest, is merely a claim.
"""

from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any, cast
from uuid import UUID

from dpone.contracts.dbt_mssql_physical_registration import database_pin_payload
from dpone.contracts.dbt_mssql_physical_registration_values import (
    PlatformSelection,
    RegisteredLimits,
    platform_subject_payload,
    require_registration_digest,
)
from dpone.contracts.dbt_mssql_physical_validation import (
    require_physical_identifier,
    require_physical_text,
    require_physical_uuid,
)
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativePlatformOriginalSubject, decode_native_original_subject

DELIVERY_ENVIRONMENT_KEY = "DPONE_PHYSICAL_TRANSPORT_FD"
DELIVERY_SCHEMA = "dpone.dbt-physical-transport-delivery.v1"
_FIELDS = frozenset(
    "schema launch_id command_index parent_pid admitted_monotonic_ns deadline_monotonic_ns "
    "generation_id executor_invocation_id guard_epoch command_plan toolchain qualification "
    "registration_id registration_sha256 platform_subject trusted_profile limits plan_set "
    "model_database model_schema adapter_type profile_name target_name argv_sha256 "
    "profile_file_device profile_file_inode profile_file_sha256 local_schema".split()
)


def physical_transport_argv_digest(args: tuple[str, ...]) -> str:
    """Hash the original exact dbt argv as a canonical native JSON array."""
    if type(args) is not tuple or not args or args[0] != "dbt" or any(type(arg) is not str for arg in args):
        raise ValueError("physical delivery requires the original dbt argv")
    return "sha256:" + sha256(encode_native_delivery_json(list(args))).hexdigest()


def _validate(raw: dict[str, Any], payload: bytes) -> None:
    if set(raw) != _FIELDS or raw["schema"] != DELIVERY_SCHEMA:
        raise ValueError("physical delivery requires its exact closed schema")
    if encode_native_delivery_json(raw) != payload:
        raise ValueError("physical delivery requires canonical bytes")
    if raw["adapter_type"] != "dpone_sqlserver" or raw["local_schema"] != "dpone_physical":
        raise ValueError("physical delivery requires its distinct adapter and local schema")
    for name in ("launch_id", "generation_id", "executor_invocation_id", "registration_id"):
        require_physical_uuid(raw[name], name)
    for name in (
        "command_index",
        "parent_pid",
        "admitted_monotonic_ns",
        "deadline_monotonic_ns",
        "guard_epoch",
        "profile_file_device",
        "profile_file_inode",
    ):
        minimum = 0 if name in {"command_index", "profile_file_device"} else 1
        if type(raw[name]) is not int or not minimum <= raw[name] <= 9223372036854775807:
            raise ValueError("physical delivery integer is outside its exact range")
    if raw["deadline_monotonic_ns"] <= raw["admitted_monotonic_ns"]:
        raise ValueError("physical delivery deadline must follow admission")
    for name in ("registration_sha256", "argv_sha256", "profile_file_sha256"):
        require_registration_digest(raw[name])
    for name in ("command_plan", "toolchain", "qualification", "plan_set"):
        OriginalRef(**raw[name])
    for name in ("profile_name", "target_name"):
        require_physical_text(raw[name], name)
    require_physical_identifier(raw["model_schema"], "model_schema")
    subject = decode_native_original_subject(encode_native_delivery_json(raw["platform_subject"]))
    if type(subject) is not NativePlatformOriginalSubject:
        raise ValueError("physical delivery requires a PLATFORM subject")
    platform_subject_payload(subject)
    selected = raw["trusted_profile"]
    if type(selected) is not dict or set(selected) != {"reference", "subject"}:
        raise ValueError("physical delivery requires a closed profile selection")
    profile_subject = decode_native_original_subject(encode_native_delivery_json(selected["subject"]))
    if type(profile_subject) is not NativePlatformOriginalSubject:
        raise ValueError("physical delivery profile requires a PLATFORM subject")
    PlatformSelection(OriginalRef(**selected["reference"]), profile_subject)
    pin = raw["model_database"]
    if type(pin) is not dict or set(pin) != {"database_name", "database_id", "create_token", "database_guid"}:
        raise ValueError("physical delivery requires an exact database pin")
    require_physical_uuid(pin["database_guid"], "database_guid")
    database_pin_payload(MssqlDatabaseAuthorityPin(**{**pin, "database_guid": UUID(pin["database_guid"])}))
    limits = RegisteredLimits(**raw["limits"])
    if len(payload) > limits.max_metadata_bytes:
        raise ValueError("physical delivery exceeds registered metadata capacity")


@dataclass(frozen=True, slots=True)
class PhysicalTransportDelivery:
    """Immutable canonical private bytes; repr deliberately omits profile hash.

    Accessors return detached representations. No method authenticates policy,
    retained registration, original membership or permission to execute SQL.
    """

    payload: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if type(self.payload) is not bytes:
            raise ValueError("physical delivery requires exact immutable bytes")
        raw = cast(dict[str, Any], decode_native_delivery_json(self.payload))
        try:
            _validate(raw, self.payload)
        except (TypeError, KeyError, AttributeError) as exc:
            raise ValueError("physical delivery contains malformed nested fields") from exc

    def to_dict(self) -> dict[str, Any]:
        """Return a detached private representation; never log this mapping."""
        return cast(dict[str, Any], decode_native_delivery_json(self.payload))

    @property
    def deadline_monotonic_ns(self) -> int:
        return cast(int, self.to_dict()["deadline_monotonic_ns"])

    @property
    def limits(self) -> RegisteredLimits:
        return RegisteredLimits(**self.to_dict()["limits"])
