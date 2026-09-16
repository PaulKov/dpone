"""Point-in-time source facts; never a receipt, qualification or mutation grant."""

from dataclasses import dataclass
from typing import cast
from uuid import UUID

from dpone.contracts.dbt_mssql_physical_registration import (
    MssqlPhysicalRuntimeRegistration,
    physical_runtime_registration_digest,
)
from dpone.contracts.dbt_mssql_physical_registration_values import DatabasePrincipal
from dpone.contracts.dbt_mssql_physical_validation import require_physical_uuid, require_sql_positive_integer
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody_codec import decode_source_executor_binding


@dataclass(frozen=True, slots=True)
class PhysicalSourceIdentity:
    """One closed observation from the signed BUILDING/ACTIVE source read.

    The caller retains its transaction while consuming the observation. These
    facts cannot authorize replay, source release or a later model transaction.
    """

    wire_version: int
    registration_id: str
    registration_digest: str
    generation_id: str
    executor_invocation_id: str
    guard_epoch: int
    source_revision: int
    reservation: OriginalRef
    executor_payload: bytes
    observed_model_principal: DatabasePrincipal
    observed_control_principal: DatabasePrincipal

    def __post_init__(self) -> None:
        if type(self.wire_version) is not int or self.wire_version != 1:
            raise ValueError("unsupported source identity wire version")
        for field in ("registration_id", "generation_id", "executor_invocation_id"):
            require_physical_uuid(getattr(self, field), field)
        OriginalRef("registration", self.registration_digest)
        for field in ("guard_epoch", "source_revision"):
            require_sql_positive_integer(getattr(self, field), field, bigint=True)
        if type(self.reservation) is not OriginalRef:
            raise ValueError("source identity requires an exact reservation")
        self.reservation.__post_init__()
        for principal in (self.observed_model_principal, self.observed_control_principal):
            if type(principal) is not DatabasePrincipal:
                raise ValueError("source identity requires exact observed principals")
            principal.__post_init__()
        executor = decode_source_executor_binding(self.executor_payload)
        if (str(executor.generation_id), str(executor.invocation_id), executor.guard_epoch, executor.reservation) != (
            self.generation_id,
            self.executor_invocation_id,
            self.guard_epoch,
            self.reservation,
        ):
            raise ValueError("source facts disagree with retained executor")


def require_source_identity(
    facts: PhysicalSourceIdentity,
    registration: MssqlPhysicalRuntimeRegistration,
    generation_id: str,
    executor_invocation_id: str,
) -> PhysicalSourceIdentity:
    """Compare exact request, registration and corresponding M/C role mappings.

    This pure check authenticates no SQL connection, module signature or original.
    SQL provisioning and the protected source procedure establish those premises.
    """
    if type(facts) is not PhysicalSourceIdentity or type(registration) is not MssqlPhysicalRuntimeRegistration:
        raise ValueError("exact source facts and registration are required")
    facts.__post_init__()
    registration.__post_init__()
    if (facts.registration_id, facts.registration_digest, facts.generation_id, facts.executor_invocation_id) != (
        registration.registration_id,
        physical_runtime_registration_digest(registration),
        generation_id,
        executor_invocation_id,
    ):
        raise ValueError("source identity differs from the expected request")
    if decode_source_executor_binding(facts.executor_payload).profile != registration.trusted_profile.reference:
        raise ValueError("source executor profile differs from registration")
    if not any(
        (facts.observed_model_principal, facts.observed_control_principal) == (role.model, role.control)
        for role in (registration.principals.metadata, registration.principals.build)
    ):
        raise ValueError("source caller mappings do not identify the same admitted role")
    return facts


class PhysicalSourceReadError(RuntimeError):
    """No accepted source observation; no retry, mutation or release is implied."""


def _bytes(value: object, maximum: int) -> bytes:
    if type(value) not in {bytes, bytearray, memoryview}:
        raise PhysicalSourceReadError("source fact binary field has an invalid representation")
    result = bytes(cast(bytes | bytearray | memoryview, value))
    if not 0 < len(result) <= maximum:
        raise PhysicalSourceReadError("source fact binary field exceeds its bound")
    return result


def _uuid(value: object) -> str:
    # SQL uniqueidentifier drivers may use uppercase hex. Normalize this driver
    # representation only; public request strings remain strictly canonical.
    normalized = str(value) if type(value) is UUID else value.lower() if type(value) is str else value
    return require_physical_uuid(normalized, "source UUID")


def decode_physical_source_row(row: tuple[object, ...] | None) -> PhysicalSourceIdentity:
    """Decode the closed driver row without acquiring or authenticating SQL facts.

    Preserve driver UUID normalization and exact binary-field bounds. Both
    readers must still enforce rowset cardinality, request/registration matching
    and successful transaction settlement before returning an observation.
    """
    if row is None or len(row) != 14:
        raise PhysicalSourceReadError("source read requires exactly one closed fact row")
    return PhysicalSourceIdentity(
        wire_version=cast(int, row[0]),
        registration_id=_uuid(row[1]),
        registration_digest=_bytes(row[2], 71).decode("ascii"),
        generation_id=_uuid(row[3]),
        executor_invocation_id=_uuid(row[4]),
        guard_epoch=cast(int, row[5]),
        source_revision=cast(int, row[6]),
        reservation=OriginalRef(_bytes(row[7], 4096).decode("utf-8"), _bytes(row[8], 71).decode("ascii")),
        executor_payload=_bytes(row[9], 1048576),
        observed_model_principal=DatabasePrincipal(cast(int, row[10]), _bytes(row[11], 85).hex()),
        observed_control_principal=DatabasePrincipal(cast(int, row[12]), _bytes(row[13], 85).hex()),
    )
