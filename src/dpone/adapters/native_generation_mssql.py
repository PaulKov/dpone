"""Once-only generation admission through administrator-installed procedures.

Connections are fresh, dedicated and bounded by the injected factory. A retained
BUILDING snapshot is evidence of an admission, never a second dispatch grant.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from hashlib import sha256
from typing import TypeVar
from uuid import UUID

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.native_generation_mssql_freeze import freeze_parameters
from dpone.adapters.native_generation_mssql_queries import generation_procedure_name
from dpone.adapters.native_generation_mssql_rows import decode_admission_row, decode_custody_row
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_delivery import FrozenGeneration, GenerationReservation
from dpone.contracts.native_generation_admission import VerifiedGenerationRequest
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import (
    SourceAdmissionClosure,
    SourceCustodySnapshot,
    SourceExecutorBinding,
    SourceTrustedBuildCompletion,
    require_trusted_build_completion,
)
from dpone.contracts.native_source_custody_codec import (
    decode_source_admission_closure,
    decode_source_executor_binding,
    decode_source_trusted_build_completion,
    encode_source_admission_closure,
    encode_source_executor_binding,
    encode_source_trusted_build_completion,
)
from dpone.ports.native_source_custody import SourceBuildCompletionVerifier
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor

_Result = TypeVar("_Result")


class NativeGenerationAdmissionError(RuntimeError):
    """Admission was not proved; no launch or capacity release is authorized."""


class NativeWriterAdmissionUncertain(NativeGenerationAdmissionError):
    """A mutation failed or replayed; independent readback cannot grant dispatch."""

    def __init__(self, observed: SourceCustodySnapshot | None) -> None:
        super().__init__("writer admission is uncertain; retained custody does not authorize redispatch")
        self.observed = observed


class MssqlNativeGenerationControl:
    """Reserve under existing P, bind once, and independently observe custody.

    Installation and trusted profile enrollment are administrator operations.
    This runtime adapter has no schema creation or enrollment path. Successful
    reserve is idempotent metadata only; bind success is returned only for a
    fresh CAS whose execute and commit acknowledgements were both observed.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], SqlControlConnection],
        control_schema: str,
        control_authority: OriginalRef,
        completion_verifier: SourceBuildCompletionVerifier | None = None,
    ) -> None:
        if type(control_authority) is not OriginalRef:
            raise TypeError("control_authority must be an exact OriginalRef")
        self._authority = OriginalRef(control_authority.locator, control_authority.sha256)
        self._schema = native_control_schema(control_schema)
        self._connect = connection_factory
        self._completion_verifier = completion_verifier

    def reserve(self, request: VerifiedGenerationRequest) -> GenerationReservation:
        """Charge capacity once, retaining it after all ambiguous outcomes."""
        if type(request) is not VerifiedGenerationRequest:
            raise TypeError("reserve requires a complete verified generation request")
        request.__post_init__()
        body = request.request_bytes()
        reference = OriginalRef(request.reservation.locator, request.reservation.sha256)
        generation_id = request.subject.generation_id
        epoch = request.guard.fencing_epoch
        failure: Exception | None = None
        try:
            self._execute("reserve", (body, reference.locator.encode("utf-8"), reference.sha256.encode("ascii")))
        except Exception as exc:
            failure = exc
        try:
            observed = self.read_custody(generation_id)
            if (observed.guard_epoch, observed.reservation) != (epoch, reference):
                raise NativeGenerationAdmissionError("reservation readback differs from requested identity")
        except Exception as exc:
            raise NativeGenerationAdmissionError("generation reservation could not be independently proved") from (
                failure or exc
            )
        if failure is not None:
            raise NativeGenerationAdmissionError(
                "reservation acknowledgement failed; retained metadata does not prove current physical admission"
            ) from failure
        return GenerationReservation(generation_id, epoch, observed.revision, reference)

    def bind_writer_invocation(
        self,
        reservation: GenerationReservation,
        binding: SourceExecutorBinding,
        *,
        expected_revision: int,
    ) -> SourceCustodySnapshot:
        """One CAS attempt only; an exception never becomes a replay permission."""
        if type(expected_revision) is not int or not 1 <= expected_revision < 9223372036854775807:
            raise ValueError("expected_revision must permit a positive SQL bigint successor")
        if type(reservation) is not GenerationReservation:
            raise TypeError("writer admission requires the exact generation reservation")
        reservation.__post_init__()
        payload = encode_source_executor_binding(binding)
        frozen = decode_source_executor_binding(payload)
        if (frozen.generation_id, frozen.guard_epoch, frozen.reservation) != (
            reservation.generation_id,
            reservation.guard_epoch,
            reservation.reservation,
        ):
            raise ValueError("writer binding differs from the supplied reservation")
        try:
            observed = self._execute("bind", (str(frozen.generation_id), expected_revision, payload))
            if (
                observed.executor != frozen
                or observed.revision != expected_revision + 1
                or observed.state != "BUILDING"
            ):
                raise NativeGenerationAdmissionError("writer CAS returned another admission")
            independently_read = self.read_custody(frozen.generation_id)
            if independently_read != observed:
                raise NativeGenerationAdmissionError("writer admission changed during independent readback")
            return observed
        except Exception as exc:
            retained = None
            try:
                retained = self.read_custody(frozen.generation_id)
            except Exception:
                pass
            raise NativeWriterAdmissionUncertain(retained) from exc

    def read_custody(self, generation_id: UUID) -> SourceCustodySnapshot:
        """Read retained metadata without acquiring a writer or releasing quota."""
        if type(generation_id) is not UUID:
            raise TypeError("generation_id must be an exact UUID")
        snapshot = self._execute("read", (str(generation_id),))
        if snapshot.generation_id != generation_id:
            raise NativeGenerationAdmissionError("custody read returned another generation")
        return snapshot

    def close_writer_admission(
        self, reservation: GenerationReservation, *, expected_revision: int
    ) -> SourceAdmissionClosure:
        """Close metadata once; lost acknowledgements permit independent readback only."""
        if type(reservation) is not GenerationReservation:
            raise TypeError("closing admission requires the exact generation reservation")
        reservation.__post_init__()
        if type(expected_revision) is not int or not 2 <= expected_revision < 9223372036854775807:
            raise ValueError("expected_revision must permit a bound generation's SQL bigint successor")
        before = self.read_custody(reservation.generation_id)
        if (
            (before.guard_epoch, before.reservation) != (reservation.guard_epoch, reservation.reservation)
            or before.executor is None
            or before.state != "BUILDING"
            or before.outcome not in {"ACTIVE", "UNKNOWN"}
            or before.revision != expected_revision + (before.writer_admission == "CLOSED")
        ):
            raise NativeGenerationAdmissionError("admission closure differs from the current reservation or revision")
        observed: SourceAdmissionClosure | None = None
        failure: Exception | None = None
        if before.writer_admission == "OPEN":
            try:
                observed = self._query(
                    "close",
                    (
                        str(reservation.generation_id),
                        expected_revision,
                        reservation.reservation.locator.encode("utf-8"),
                        reservation.reservation.sha256.encode("ascii"),
                    ),
                    lambda row: decode_admission_row(row, reservation.generation_id),
                )
            except Exception as exc:
                failure = exc
        try:
            retained = self.read_admission_closure(reservation.generation_id)
            if (
                retained.executor != before.executor
                or retained.revision != expected_revision + 1
                or (observed is not None and observed != retained)
            ):
                raise NativeGenerationAdmissionError("independent admission closure differs from the exact request")
            return retained
        except Exception as exc:
            raise NativeGenerationAdmissionError("admission closure could not be independently proved") from (
                failure or exc
            )

    def record_trusted_build_completion(
        self,
        reservation: GenerationReservation,
        closure: SourceAdmissionClosure,
        completion: SourceTrustedBuildCompletion,
        completion_ref: OriginalRef,
        *,
        expected_revision: int,
    ) -> SourceCustodySnapshot:
        """One positive CAS followed by a fresh exact-current-owner read.

        Re-entry may reconcile the same retained request, never dispatch a build.
        The read procedure revalidates current physical ownership independently;
        a historical custody row alone cannot prove safe acknowledgement.
        """
        if self._completion_verifier is None:
            raise NativeGenerationAdmissionError("positive completion authentication is not configured")
        if type(reservation) is not GenerationReservation or type(completion_ref) is not OriginalRef:
            raise ValueError("positive completion requires exact reservation and descriptor")
        reservation.__post_init__()
        if type(expected_revision) is not int or not 3 <= expected_revision < 9223372036854775807:
            raise ValueError("expected_revision must permit a closed generation's SQL bigint successor")
        payload = encode_source_trusted_build_completion(completion)
        accepted = decode_source_trusted_build_completion(payload)
        reference = OriginalRef(completion_ref.locator, completion_ref.sha256)
        if reference.sha256 != "sha256:" + sha256(payload).hexdigest():
            raise ValueError("completion descriptor differs from the supplied canonical payload")
        admission_payload = encode_source_admission_closure(closure)
        admission = decode_source_admission_closure(admission_payload, receipt=closure.receipt)
        before = self.read_custody(reservation.generation_id)
        require_trusted_build_completion(before, admission, accepted)
        if (before.guard_epoch, before.reservation) != (reservation.guard_epoch, reservation.reservation):
            raise ValueError("positive completion differs from the supplied reservation")
        if self.read_admission_closure(reservation.generation_id) != admission:
            raise ValueError("positive completion admission differs from retained closure")
        already_recorded = before.closure is not None
        if before.revision != expected_revision + already_recorded or (
            already_recorded and (before.closure != reference or before.outcome != "ACTIVE")
        ):
            raise ValueError("positive completion differs from the exact expected revision or original")
        self._completion_verifier.authenticate(accepted, reference)
        parameters = (
            str(reservation.generation_id),
            expected_revision,
            admission_payload,
            payload,
            reference.locator.encode("utf-8"),
            reference.sha256.encode("ascii"),
        )
        expected = replace(before, revision=expected_revision + 1, closure=reference, outcome="ACTIVE")
        observed = None
        failure: Exception | None = None
        if not already_recorded:
            try:
                observed = self._execute("complete", parameters)
            except Exception as exc:
                failure = exc
        try:
            retained = self._execute("completion_read", parameters)
            if retained != expected or (observed is not None and observed != retained):
                raise NativeGenerationAdmissionError("positive completion current readback differs from request")
            return retained
        except Exception as exc:
            raise NativeGenerationAdmissionError("positive completion could not be independently proved") from (
                failure or exc
            )

    def inspect_freeze(
        self,
        reservation: GenerationReservation,
        admission: SourceAdmissionClosure,
        completion: SourceTrustedBuildCompletion,
        frozen: FrozenGeneration,
        *,
        expected_revision: int,
    ) -> SourceCustodySnapshot:
        """Read exact metadata preconditions under current ownership, without mutation."""
        return self._execute(
            "freeze_inspect",
            freeze_parameters(
                reservation,
                admission,
                completion,
                frozen,
                expected_revision=expected_revision,
            ),
        )

    def record_freeze(
        self,
        reservation: GenerationReservation,
        admission: SourceAdmissionClosure,
        completion: SourceTrustedBuildCompletion,
        frozen: FrozenGeneration,
        *,
        expected_revision: int,
    ) -> SourceCustodySnapshot:
        """Attempt the exact freeze CAS; the runtime must independently read back."""
        return self._execute(
            "freeze",
            freeze_parameters(
                reservation,
                admission,
                completion,
                frozen,
                expected_revision=expected_revision,
            ),
        )

    def read_freeze(
        self,
        reservation: GenerationReservation,
        admission: SourceAdmissionClosure,
        completion: SourceTrustedBuildCompletion,
        frozen: FrozenGeneration,
        *,
        expected_revision: int,
    ) -> SourceCustodySnapshot:
        """Independently prove the full freeze request and current physical owner."""
        return self._execute(
            "freeze_read",
            freeze_parameters(
                reservation,
                admission,
                completion,
                frozen,
                expected_revision=expected_revision,
            ),
        )

    def read_admission_closure(self, generation_id: UUID) -> SourceAdmissionClosure:
        """Resolve the immutable admission descriptor separately from positive completion."""
        if type(generation_id) is not UUID:
            raise TypeError("generation_id must be an exact UUID")
        return self._query("closure_read", (str(generation_id),), lambda row: decode_admission_row(row, generation_id))

    def _execute(self, operation: str, parameters: tuple[object, ...]) -> SourceCustodySnapshot:
        return self._query(operation, parameters, decode_custody_row)

    def _query(
        self, operation: str, parameters: tuple[object, ...], decoder: Callable[[tuple[object, ...]], _Result]
    ) -> _Result:
        connection: SqlControlConnection | None = None
        cursor: SqlControlCursor | None = None
        try:
            connection = self._connect()
            connection.autocommit = False
            cursor = connection.cursor()
            arguments = (self._authority.locator.encode("utf-8"), self._authority.sha256.encode("ascii"), *parameters)
            placeholders = ", ".join("?" for _ in arguments)
            cursor.execute(f"EXEC [{self._schema}].[{generation_procedure_name(operation)}] {placeholders}", *arguments)
            row = dbapi_lifecycle.row(cursor)
            if row is None or dbapi_lifecycle.row(cursor) is not None:
                raise NativeGenerationAdmissionError("admission requires exactly one ledger row")
            try:
                decoded = decoder(tuple(row))
            except (ValueError, DbtPublishingError) as exc:
                raise NativeGenerationAdmissionError(str(exc)) from exc
            connection.commit()
            return decoded
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
