"""Physical enrollment original acquisition using actual retained binding ports.

This step authenticates exact external bytes and does not enroll, grant managed
membership, infer current P/G, or authorize a launch. SQL settlement is separate.
"""

from collections.abc import Callable

from dpone.adapters.dbt_mssql_physical_catalog_fetch import CatalogReadBudget
from dpone.adapters.dbt_mssql_physical_plan_membership import MssqlPhysicalPlanMembershipReader
from dpone.adapters.native_generation_invocation_auth import InvocationOriginalReader
from dpone.contracts.dbt_mssql_physical_catalog_binding import CatalogRegistrationBinding
from dpone.contracts.dbt_mssql_physical_enrollment import (
    PhysicalEnrollmentObservation,
    PhysicalPlanEnrollment,
    build_enrollment,
    require_enrollment_reservation,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_validation import require_sql_positive_integer
from dpone.contracts.native_delivery import NativeOriginalsRefV1
from dpone.contracts.native_delivery_json import MAX_NATIVE_JSON_BYTES
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativeGenerationOriginalSubject
from dpone.contracts.native_source_custody import SourceExecutorBinding
from dpone.ports.native_originals import NativeOriginalBindingPort, NativeOriginalReaderPort
from dpone.ports.physical_catalog_connection import PhysicalCatalogConnection


def acquire_enrollment_originals(
    *,
    originals: NativeOriginalReaderPort,
    bindings: NativeOriginalBindingPort,
    subject: NativeGenerationOriginalSubject,
    plan_reference: OriginalRef,
    executor: SourceExecutorBinding,
    registration_sha256: str,
    catalog_binding_sha256: str,
    max_bytes: int,
    budget: CatalogReadBudget,
) -> PhysicalPlanEnrollment:
    """Read exact bound plan and command versions within the existing deadline.

    Providers own finite I/O timeouts; deadline checks bracket each call and never
    renew the operation. The existing reader authenticates full original binding,
    subject, kind, immutable object/version, retention representation and SHA.
    Registration/binding claims are compared by the later protected SQL consumer.
    """
    budget.seconds()
    require_sql_positive_integer(max_bytes, "max_bytes")
    max_bytes = min(max_bytes, MAX_NATIVE_JSON_BYTES)
    reader = InvocationOriginalReader(originals=originals, bindings=bindings, subject=subject, max_bytes=max_bytes)
    reader.require_generation(executor)
    plan = reader.read(plan_reference, "mssql_physical_plan_set_v1")
    budget.seconds()
    command = reader.read(executor.command, "trusted_dbt_command_plan_v1")
    budget.seconds()
    result = build_enrollment(
        subject=subject,
        registration_sha256=registration_sha256,
        catalog_binding_sha256=catalog_binding_sha256,
        plan_reference=plan_reference,
        plan_payload=plan,
        executor=executor,
        command_payload=command,
        max_bytes=max_bytes,
    )
    budget.seconds()
    reservation = reader.read(executor.reservation, "generation_stored_file_v1")
    budget.seconds()
    require_enrollment_reservation(result, reservation)
    budget.seconds()
    return result


class PhysicalEnrollmentError(RuntimeError):
    """No independently confirmed enrollment; retain G and originals for readback."""


def _observe_enrollment(
    *,
    connection_factory: Callable[[int], PhysicalCatalogConnection],
    expected: PhysicalPlanEnrollment,
    budget: CatalogReadBudget,
    max_bytes: int,
    enroll: bool,
) -> PhysicalEnrollmentObservation:
    from dpone.adapters import dbapi_lifecycle
    from dpone.adapters.dbt_mssql_physical_catalog_fetch import require_final_rowset
    from dpone.adapters.dbt_mssql_physical_enrollment_queries import ENROLL, READ
    from dpone.adapters.dbt_mssql_physical_source import _uuid
    from dpone.contracts.dbt_mssql_physical_enrollment import (
        encode_enrollment,
        enrollment_digest,
        require_enrollment_row,
    )

    payload = encode_enrollment(expected, max_bytes=max_bytes)
    digest = enrollment_digest(payload).encode("ascii")
    parameters = (expected.registration_id, str(expected.subject.generation_id), str(expected.executor.invocation_id))
    suffix = ", @payload=?, @payload_digest=?" if enroll else ", @expected_payload_digest=?"
    connection = cursor = None
    try:
        connection = connection_factory(budget.seconds())
        connection.autocommit = True
        connection.timeout = budget.seconds()
        cursor = connection.cursor()
        cursor.execute(
            f"EXEC [dpone_physical].[{ENROLL if enroll else READ}] @registration_id=?, @generation=?, @expected_invocation=?"
            + suffix,
            *parameters,
            *((payload, digest) if enroll else (digest,)),
        )
        budget.seconds()
        raw = cursor.fetchone()
        if raw is None or len(raw) != 8:
            raise ValueError("enrollment requires one complete eight-field row")
        row = list(raw)
        for index in (1, 3, 4):
            row[index] = _uuid(row[index])
        for index in (2, 6, 7):
            value = row[index]
            maximum = max_bytes if index == 7 else 71
            if type(value) not in (bytes, bytearray, memoryview):
                raise ValueError("enrollment has an unsupported binary representation")
            size = value.nbytes if isinstance(value, memoryview) else len(value)
            if not 0 < size <= maximum:
                raise ValueError("enrollment returned oversized binary data")
            row[index] = bytes(value) if index == 7 else bytes(value).decode("ascii")
        result = require_enrollment_row(tuple(row), expected=expected, max_bytes=max_bytes)
        if cursor.fetchone() is not None:
            raise ValueError("enrollment returned extra facts")
        require_final_rowset(cursor)
        budget.seconds()
        return result
    finally:
        dbapi_lifecycle.close(cursor)
        dbapi_lifecycle.close(connection)


def _settle_enrollment(
    *,
    connection_factory: Callable[[int], PhysicalCatalogConnection],
    expected: PhysicalPlanEnrollment,
    budget: CatalogReadBudget,
    max_bytes: int,
) -> PhysicalEnrollmentObservation:
    """Mutation acknowledgement is followed by an independent fresh read only."""
    _observe_enrollment(
        connection_factory=connection_factory, expected=expected, budget=budget, max_bytes=max_bytes, enroll=True
    )
    return _observe_enrollment(
        connection_factory=connection_factory, expected=expected, budget=budget, max_bytes=max_bytes, enroll=False
    )


class MssqlPhysicalPlanEnroller:
    """Authenticate actual originals and membership before immutable SQL enrollment.

    The connection factory receives remaining seconds and must supply a fresh
    ordinary METADATA connection with that finite login timeout. The caller passes
    the preparation operation's existing deadline budget, never a renewed lease.
    These methods never reserve, launch, release capacity, retry a mutation or
    authorize transaction binding. SQL owns each short transaction and returns
    only after commit; Python accepts enrollment only after independent readback.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[int], PhysicalCatalogConnection],
        registration: MssqlPhysicalRuntimeRegistration,
        binding: CatalogRegistrationBinding,
        originals: NativeOriginalReaderPort,
        bindings: NativeOriginalBindingPort,
        membership_reader: MssqlPhysicalPlanMembershipReader,
        source_refs: NativeOriginalsRefV1,
    ) -> None:
        from dpone.adapters.dbt_mssql_physical_plan_membership import MssqlPhysicalPlanMembershipReader
        from dpone.contracts.dbt_mssql_physical_catalog_binding import require_catalog_binding_registration
        from dpone.contracts.dbt_mssql_physical_registration_values import DedicatedObserver
        from dpone.contracts.native_delivery import NativeOriginalsRefV1

        require_catalog_binding_registration(binding, registration)
        if (
            registration.local_schema != "dpone_physical"
            or type(registration.principals.observer) is not DedicatedObserver
        ):
            raise ValueError("managed enrollment requires the fixed local schema and dedicated observer")
        if (
            type(membership_reader) is not MssqlPhysicalPlanMembershipReader
            or type(source_refs) is not NativeOriginalsRefV1
        ):
            raise ValueError("enrollment requires concrete membership acquisition and exact original locators")
        source_refs.__post_init__()
        self._connect, self._registration, self._binding = connection_factory, registration, binding
        self._originals, self._bindings = originals, bindings
        self._membership, self._refs = membership_reader, source_refs

    def _acquire(
        self, plan_reference: OriginalRef, executor: SourceExecutorBinding, budget: CatalogReadBudget
    ) -> PhysicalPlanEnrollment:
        from dpone.contracts.dbt_mssql_physical_catalog_binding import catalog_binding_digest
        from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest

        registration = self._registration
        value = acquire_enrollment_originals(
            originals=self._originals,
            bindings=self._bindings,
            subject=NativeGenerationOriginalSubject(registration.platform_subject.authority, executor.generation_id),
            plan_reference=plan_reference,
            executor=executor,
            registration_sha256=physical_runtime_registration_digest(registration),
            catalog_binding_sha256=catalog_binding_digest(self._binding),
            max_bytes=registration.limits.max_metadata_bytes,
            budget=budget,
        )
        if value.registration_id != registration.registration_id:
            raise ValueError("retained enrollment names another registration")
        return value

    def enroll(
        self, *, plan_reference: OriginalRef, executor: SourceExecutorBinding, budget: CatalogReadBudget
    ) -> PhysicalEnrollmentObservation:
        """Return complete observation after real membership and fresh readback."""
        try:
            value = self._acquire(plan_reference, executor, budget)
            budget.seconds()
            self._membership.require_plan_membership(self._refs, registration=self._registration, plan_set=value.plan)
            budget.seconds()
            return _settle_enrollment(
                connection_factory=self._connect,
                expected=value,
                budget=budget,
                max_bytes=self._registration.limits.max_metadata_bytes,
            )
        except Exception as error:
            raise PhysicalEnrollmentError("enrollment was not independently observed") from error

    def read(
        self, *, plan_reference: OriginalRef, executor: SourceExecutorBinding, budget: CatalogReadBudget
    ) -> PhysicalEnrollmentObservation:
        """Reconcile exact retained originals through current-P SQL; never mutate."""
        try:
            value = self._acquire(plan_reference, executor, budget)
            return _observe_enrollment(
                connection_factory=self._connect,
                expected=value,
                budget=budget,
                max_bytes=self._registration.limits.max_metadata_bytes,
                enroll=False,
            )
        except Exception as error:
            raise PhysicalEnrollmentError("enrollment readback was unavailable") from error
