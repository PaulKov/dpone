"""Authenticated generation freeze without dispatch or capacity release.

The composition root fixes original authorities and physical ledger capability.
Successful immutable publication alone is never proof of a committed transition.
"""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from threading import Lock

from dpone.adapters.native_generation_invocation_auth import InvocationOriginalReader
from dpone.contracts.native_delivery import FrozenGeneration, GenerationReservation
from dpone.contracts.native_delivery_codec import (
    decode_frozen_generation,
    decode_source_closure_receipt,
    encode_source_closure_receipt,
    prepare_frozen_generation,
    prepare_source_closure_receipt,
)
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_original_kinds import NativeOriginalKind
from dpone.contracts.native_source_custody import NativeSourceCustodyError, SourceExecutorBinding
from dpone.contracts.native_source_custody_codec import (
    decode_source_executor_binding,
    encode_source_executor_binding,
    encode_source_trusted_build_completion,
)
from dpone.ports.native_generation_control import SourceGenerationFreezeLedger
from dpone.ports.native_originals import BoundNativeOriginalPublisher
from dpone.ports.native_source_custody import SourceBuildCompletionReader


class NativeGenerationFreeze:
    """Reauthenticate originals and inspect before each explicit metadata attempt.

    Each call attempts at most one CAS, followed by independent accepted-result
    readback. An explicit retry starts with fresh exact current-owner inspection;
    no failure or absent response grants permission. Freeze never dispatches dbt
    or BCP, and its metadata retry must not be reused for dispatching operations.
    """

    def __init__(
        self,
        *,
        executor: SourceExecutorBinding,
        ledger: SourceGenerationFreezeLedger,
        completion_reader: SourceBuildCompletionReader,
        original_reader: InvocationOriginalReader,
        publish_original: BoundNativeOriginalPublisher,
    ) -> None:
        self._executor = decode_source_executor_binding(encode_source_executor_binding(executor))
        original_reader.require_generation(self._executor)
        self._ledger, self._completion = ledger, completion_reader
        self._originals, self._publish = original_reader, publish_original
        self._publications: dict[str, tuple[NativeOriginalKind, bytes, OriginalRef]] = {}
        self._lock = Lock()

    def freeze(self, reservation: GenerationReservation, *, expected_revision: int) -> FrozenGeneration:
        """Freeze the exact retained positive completion at the next revision."""
        with self._lock:
            return self._freeze(reservation, expected_revision=expected_revision)

    def _freeze(self, reservation: GenerationReservation, *, expected_revision: int) -> FrozenGeneration:
        if type(reservation) is not GenerationReservation:
            raise NativeSourceCustodyError("freeze requires the exact generation reservation")
        reservation.__post_init__()
        if type(expected_revision) is not int or not 4 <= expected_revision < 9223372036854775807:
            raise NativeSourceCustodyError("freeze revision must permit a positive SQL bigint successor")
        before = self._ledger.read_custody(reservation.generation_id)
        before.__post_init__()
        retained = before.state == "FROZEN"
        if (
            before.executor != self._executor
            or (before.generation_id, before.guard_epoch, before.reservation)
            != (reservation.generation_id, reservation.guard_epoch, reservation.reservation)
            or before.state not in {"BUILDING", "FROZEN"}
            or before.writer_admission != "CLOSED"
            or before.outcome not in ({"ACTIVE"} if retained else {"ACTIVE", "UNKNOWN"})
            or before.closure is None
            or before.revision != expected_revision + int(retained)
            or before.quality is not None
            or before.export_plan is not None
            or before.active_reads
        ):
            raise NativeSourceCustodyError("freeze differs from current closed generation custody")
        admission = self._ledger.read_admission_closure(reservation.generation_id)
        admission.__post_init__()
        positive = self._completion.read_completion(before.closure)
        if (
            positive.executor != self._executor
            or admission.executor != self._executor
            or admission.revision > expected_revision
            or "sha256:" + sha256(encode_source_trusted_build_completion(positive)).hexdigest() != before.closure.sha256
        ):
            raise NativeSourceCustodyError("freeze completion differs from the exact admitted original")
        if retained:
            assert before.frozen is not None
            frozen = self._read_frozen(before.frozen)
        else:
            closure_payload = prepare_source_closure_receipt(
                generation_id=reservation.generation_id,
                guard_epoch=reservation.guard_epoch,
                revision=expected_revision,
                reservation=reservation.reservation,
                completion=before.closure,
            )
            reference = self._emit("source_closure_receipt_v1", "closure.json", closure_payload, expected_revision)
            closed = decode_source_closure_receipt(closure_payload, receipt=reference)
            payload = prepare_frozen_generation(closed, revision=expected_revision + 1)
            reference = self._emit("frozen_generation_v1", "frozen.json", payload, expected_revision)
            frozen = self._read_frozen(reference)
        if (frozen.generation_id, frozen.guard_epoch, frozen.reservation, frozen.revision) != (
            reservation.generation_id,
            reservation.guard_epoch,
            reservation.reservation,
            expected_revision + 1,
        ) or frozen.closure.completion != before.closure:
            raise NativeSourceCustodyError("frozen original differs from the complete requested generation")
        expected = replace(
            before, state="FROZEN", outcome="ACTIVE", revision=expected_revision + 1, frozen=frozen.frozen
        )
        observed = None
        failure: Exception | None = None
        inspected = self._ledger.inspect_freeze(
            reservation,
            admission,
            positive,
            frozen,
            expected_revision=expected_revision,
        )
        if inspected != before and inspected != expected:
            raise NativeSourceCustodyError("freeze inspection differs from the complete current request")
        if inspected.state == "BUILDING":
            try:
                observed = self._ledger.record_freeze(
                    reservation,
                    admission,
                    positive,
                    frozen,
                    expected_revision=expected_revision,
                )
            except Exception as error:
                failure = error
        try:
            current = self._ledger.read_freeze(
                reservation,
                admission,
                positive,
                frozen,
                expected_revision=expected_revision,
            )
            if current != expected or (observed is not None and observed != current):
                raise NativeSourceCustodyError("freeze current read differs from the complete request")
        except Exception as error:
            raise NativeSourceCustodyError("freeze could not be independently proved") from (failure or error)
        return frozen

    def _read_frozen(self, reference: OriginalRef) -> FrozenGeneration:
        frozen = decode_frozen_generation(self._originals.read(reference, "frozen_generation_v1"), frozen=reference)
        raw = self._originals.read(frozen.closure.receipt, "source_closure_receipt_v1")
        if raw != encode_source_closure_receipt(frozen.closure):
            raise NativeSourceCustodyError("frozen closure differs from independently read original")
        return frozen

    def _emit(self, kind: NativeOriginalKind, filename: str, payload: bytes, revision: int) -> OriginalRef:
        if len(payload) > self._originals.max_bytes:
            raise NativeSourceCustodyError("freeze metadata exceeds its configured byte bound")
        locator = f"generations/{self._executor.generation_id}/freeze/{revision}/{filename}"
        reference = OriginalRef(locator, "sha256:" + sha256(payload).hexdigest())
        previous = self._publications.get(locator)
        if previous is not None and previous != (kind, payload, reference):
            raise NativeSourceCustodyError("freeze original changed after publication was attempted")
        if previous is None:
            self._publications[locator] = (kind, payload, reference)
            try:
                returned = self._publish(
                    kind=kind, locator=locator, payload=payload, max_bytes=self._originals.max_bytes
                )
                if type(returned) is not OriginalRef or returned != reference:
                    raise NativeSourceCustodyError("publication returned another freeze original")
            except Exception:
                # Resolve the original independently; do not repeat publication.
                pass
        if self._originals.read(reference, kind) != payload:
            raise NativeSourceCustodyError("freeze original independent readback differs")
        return reference
