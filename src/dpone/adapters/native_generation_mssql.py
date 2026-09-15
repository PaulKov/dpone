"""Once-only generation admission through administrator-installed procedures.

Connections are fresh, dedicated and bounded by the injected factory. A retained
BUILDING snapshot is evidence of an admission, never a second dispatch grant.
"""

from __future__ import annotations

from collections.abc import Callable
from uuid import UUID

from dpone.adapters import dbapi_lifecycle
from dpone.adapters.native_generation_mssql_queries import generation_procedure_name
from dpone.contracts.mssql_object_name import native_control_schema
from dpone.contracts.native_delivery import GenerationReservation
from dpone.contracts.native_generation_admission import VerifiedGenerationRequest
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import SourceCustodySnapshot, SourceExecutorBinding
from dpone.contracts.native_source_custody_codec import decode_source_executor_binding, encode_source_executor_binding
from dpone.ports.sql_connection import SqlControlConnection, SqlControlCursor


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
    ) -> None:
        if type(control_authority) is not OriginalRef:
            raise TypeError("control_authority must be an exact OriginalRef")
        self._authority = OriginalRef(control_authority.locator, control_authority.sha256)
        self._schema = native_control_schema(control_schema)
        self._connect = connection_factory

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

    def _execute(self, operation: str, parameters: tuple[object, ...]) -> SourceCustodySnapshot:
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
            if row is None or len(row) != 6 or dbapi_lifecycle.row(cursor) is not None:
                raise NativeGenerationAdmissionError("admission requires exactly one six-column custody row")
            generation, epoch, revision, locator, digest, binding = row
            if type(generation) is not str or type(locator) is not bytes or type(digest) is not bytes:
                raise NativeGenerationAdmissionError("invalid custody identity column types")
            if binding is not None and type(binding) is not bytes:
                raise NativeGenerationAdmissionError("invalid custody executor column type")
            executor = None if binding is None else decode_source_executor_binding(binding)
            snapshot = SourceCustodySnapshot(
                generation_id=UUID(generation),
                guard_epoch=epoch,
                revision=revision,
                executor=executor,
                reservation=OriginalRef(locator.decode("utf-8"), digest.decode("ascii")),
                closure=None,
                frozen=None,
                quality=None,
                export_plan=None,
                active_reads=(),
                state="RESERVED" if executor is None else "BUILDING",
                writer_admission="OPEN",
                outcome="ACTIVE",
            )
            connection.commit()
            return snapshot
        except BaseException:
            dbapi_lifecycle.rollback(connection)
            raise
        finally:
            dbapi_lifecycle.close(cursor)
            dbapi_lifecycle.close(connection)
