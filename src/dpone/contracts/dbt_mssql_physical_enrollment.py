"""Complete immutable enrollment bytes; representation never supplies authority."""

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, cast

from dpone.contracts.dbt_mssql_physical import AbsentPredecessor, PhysicalPlanSet
from dpone.contracts.dbt_mssql_physical_invocation import PhysicalManagedInvocation, require_managed_command
from dpone.contracts.dbt_mssql_physical_registration_values import reference_payload, require_registration_digest
from dpone.contracts.dbt_mssql_physical_validation import require_physical_uuid, require_sql_positive_integer
from dpone.contracts.dbt_mssql_physical_wire import decode_physical_plan_set
from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import (
    NativeGenerationOriginalSubject,
    decode_native_original_subject,
    encode_native_original_subject,
)
from dpone.contracts.native_source_custody import (
    SourceExecutorBinding,
    decode_source_executor_binding,
    encode_source_executor_binding,
)

SCHEMA = "dpone.mssql-physical-enrollment.v1"


def enrollment_digest(payload: bytes) -> str:
    """Digest the exact canonical external envelope bytes."""
    return "sha256:" + sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class PhysicalPlanEnrollment:
    """Detached complete original components; SQL and original readers authenticate."""

    subject: NativeGenerationOriginalSubject
    registration_id: str
    registration_sha256: str
    catalog_binding_sha256: str
    plan_reference: OriginalRef
    plan_payload: bytes
    executor: SourceExecutorBinding
    command_payload: bytes

    def __post_init__(self) -> None:
        if type(self.subject) is not NativeGenerationOriginalSubject:
            raise ValueError("enrollment requires an exact generation subject")
        encode_native_original_subject(self.subject)
        require_physical_uuid(self.registration_id, "registration_id")
        require_registration_digest(self.registration_sha256)
        require_registration_digest(self.catalog_binding_sha256)
        if type(self.plan_reference) is not OriginalRef or type(self.executor) is not SourceExecutorBinding:
            raise ValueError("enrollment requires exact reference and executor records")
        self.plan_reference.__post_init__()
        encode_source_executor_binding(self.executor)
        plan = self.plan
        if (
            self.subject.generation_id != self.executor.generation_id
            or plan.generation_id != str(self.subject.generation_id)
            or plan.runtime_registration_id != self.registration_id
            or plan.guard.fencing_epoch != self.executor.guard_epoch
            or plan.profile != self.executor.profile
            or enrollment_digest(self.plan_payload) != self.plan_reference.sha256
        ):
            raise ValueError("enrollment plan/executor identity mismatch")
        if any(type(model.predecessor) is not AbsentPredecessor for model in plan.models):
            raise ValueError("enrollment first cell requires every target absent")
        require_managed_command(
            self.command_payload,
            expected=PhysicalManagedInvocation(
                plan.generation_id, str(self.executor.invocation_id), self.plan_reference, self.registration_id
            ),
        )
        if enrollment_digest(self.command_payload) != self.executor.command.sha256:
            raise ValueError("enrollment command bytes differ from executor original")

    @property
    def plan(self) -> PhysicalPlanSet:
        """Decode existing canonical plan rules without repairing component bytes."""
        return decode_physical_plan_set(self.plan_payload)

    def to_dict(self) -> dict[str, NativeJsonValue]:
        """Retain one embedded plan and command, with exactly eight root keys."""
        return dict(
            schema=SCHEMA,
            subject=decode_native_delivery_json(encode_native_original_subject(self.subject)),
            registration=dict(id=self.registration_id, sha256=self.registration_sha256),
            catalog_binding_sha256=self.catalog_binding_sha256,
            plan_set=dict(
                reference=reference_payload(self.plan_reference), payload=decode_native_delivery_json(self.plan_payload)
            ),
            reservation=reference_payload(self.executor.reservation),
            executor=decode_native_delivery_json(encode_source_executor_binding(self.executor)),
            command=dict(
                reference=reference_payload(self.executor.command),
                payload=decode_native_delivery_json(self.command_payload),
            ),
        )


def encode_enrollment(value: PhysicalPlanEnrollment, *, max_bytes: int) -> bytes:
    """Enforce the aggregate registered bound and all native primitive ceilings."""
    require_sql_positive_integer(max_bytes, "max_bytes")
    if type(value) is not PhysicalPlanEnrollment:
        raise ValueError("enrollment requires an exact complete record")
    value.__post_init__()
    payload = encode_native_delivery_json(value.to_dict())
    if len(payload) > min(max_bytes, 1048576):
        raise ValueError("enrollment aggregate byte bound exceeded")
    return payload


def _object(value: object, fields: set[str]) -> dict[str, Any]:
    if type(value) is not dict or set(value) != fields:
        raise ValueError("enrollment requires exact nested object fields")
    return cast(dict[str, Any], value)


def _reference(value: object) -> OriginalRef:
    return OriginalRef(**_object(value, {"locator", "sha256"}))


def decode_enrollment(payload: bytes, *, max_bytes: int) -> PhysicalPlanEnrollment:
    """Reject missing/extra/substituted components and noncanonical whole bytes."""
    require_sql_positive_integer(max_bytes, "max_bytes")
    if type(payload) is not bytes or not 0 < len(payload) <= min(max_bytes, 1048576):
        raise ValueError("enrollment aggregate byte bound exceeded")
    raw = _object(
        decode_native_delivery_json(payload),
        {
            "schema",
            "subject",
            "registration",
            "catalog_binding_sha256",
            "plan_set",
            "reservation",
            "executor",
            "command",
        },
    )
    if raw["schema"] != SCHEMA:
        raise ValueError("enrollment schema is unsupported")
    registration = _object(raw["registration"], {"id", "sha256"})
    plan = _object(raw["plan_set"], {"reference", "payload"})
    command = _object(raw["command"], {"reference", "payload"})
    subject = decode_native_original_subject(encode_native_delivery_json(raw["subject"]))
    if type(subject) is not NativeGenerationOriginalSubject:
        raise ValueError("enrollment requires a generation subject")
    executor = decode_source_executor_binding(encode_native_delivery_json(raw["executor"]))
    if executor.reservation != _reference(raw["reservation"]) or executor.command != _reference(command["reference"]):
        raise ValueError("enrollment duplicate original projections differ")
    value = PhysicalPlanEnrollment(
        subject,
        registration["id"],
        registration["sha256"],
        raw["catalog_binding_sha256"],
        _reference(plan["reference"]),
        encode_native_delivery_json(plan["payload"]),
        executor,
        encode_native_delivery_json(command["payload"]),
    )
    if encode_enrollment(value, max_bytes=max_bytes) != payload:
        raise ValueError("enrollment bytes must be canonical")
    return value


def build_enrollment(
    *,
    subject: NativeGenerationOriginalSubject,
    registration_sha256: str,
    catalog_binding_sha256: str,
    plan_reference: OriginalRef,
    plan_payload: bytes,
    executor: SourceExecutorBinding,
    command_payload: bytes,
    max_bytes: int,
) -> PhysicalPlanEnrollment:
    """Assemble already acquired components; this pure function performs no I/O."""
    value = PhysicalPlanEnrollment(
        subject,
        decode_physical_plan_set(plan_payload).runtime_registration_id,
        registration_sha256,
        catalog_binding_sha256,
        plan_reference,
        plan_payload,
        executor,
        command_payload,
    )
    encode_enrollment(value, max_bytes=max_bytes)
    return value


@dataclass(frozen=True, slots=True)
class PhysicalEnrollmentObservation:
    """Complete read response; construction is not original or SQL authority."""

    enrollment: PhysicalPlanEnrollment
    payload: bytes
    payload_sha256: str


def require_enrollment_row(
    row: tuple[object, ...], *, expected: PhysicalPlanEnrollment, max_bytes: int
) -> PhysicalEnrollmentObservation:
    """Compare normalized enrollment8 and complete bytes with locally acquired originals.

    Driver UUID/binary normalization belongs to the adapter. The public carrier
    represents a response, never permission to launch or a durable P lease.
    """
    expected_payload = encode_enrollment(expected, max_bytes=max_bytes)
    if type(row) is not tuple or len(row) != 8 or type(row[0]) is not int or row[0] != 1:
        raise ValueError("enrollment response requires the exact version-one eight-field row")
    if type(row[5]) is not int or row[5] != expected.executor.guard_epoch:
        raise ValueError("enrollment response differs from the current guard")
    expected_row = (
        1,
        expected.registration_id,
        expected.registration_sha256,
        str(expected.subject.generation_id),
        str(expected.executor.invocation_id),
        expected.executor.guard_epoch,
        enrollment_digest(expected_payload),
        expected_payload,
    )
    if any(
        type(actual) is not type(wanted) or actual != wanted for actual, wanted in zip(row, expected_row, strict=True)
    ):
        raise ValueError("enrollment response differs from complete expected identity or bytes")
    observed = decode_enrollment(expected_payload, max_bytes=max_bytes)
    return PhysicalEnrollmentObservation(observed, expected_payload, enrollment_digest(expected_payload))


def require_enrollment_reservation(value: PhysicalPlanEnrollment, reservation_payload: bytes) -> None:
    """Compare the complete actual G request, including activation and guard.

    The adapter must read these bytes through the real original binding reader
    before calling this representation check or any membership/SQL consumer.
    No VerifiedGenerationRequest object supplies authentication here.
    """
    from dpone.contracts.native_generation_admission import generation_admission_request_bytes

    raw = decode_native_delivery_json(reservation_payload)
    expected = generation_admission_request_bytes(
        subject=value.subject,
        workspace_attempt=value.plan.workspace_attempt,
        guard=value.plan.guard,
        profile=value.executor.profile,
        command=value.executor.command,
        requested_bytes=cast(int, raw.get("requested_bytes")),
    )
    if reservation_payload != expected or enrollment_digest(reservation_payload) != value.executor.reservation.sha256:
        raise ValueError("enrollment differs from the complete retained generation request")
