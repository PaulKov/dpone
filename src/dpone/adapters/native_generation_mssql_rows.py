"""Exact detached ledger rows; decoding does not establish current ownership."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from typing import Literal, cast
from uuid import UUID

from dpone.contracts.native_delivery_codec import decode_frozen_generation
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import (
    SourceAdmissionClosure,
    SourceCustodySnapshot,
    decode_source_admission_closure,
    decode_source_executor_binding,
    decode_source_trusted_build_completion,
)


def decode_admission_row(row: tuple[object, ...], generation_id: UUID) -> SourceAdmissionClosure:
    """Decode the historical immutable admission descriptor independently."""
    original = _original_triple(row)
    if original is None:
        raise ValueError("closure requires exact payload, locator and digest bytes")
    payload, reference = original
    if reference.locator != f"generations/{generation_id}/writer-admission-closure.json":
        raise ValueError("admission closure locator differs from the requested generation")
    value = decode_source_admission_closure(payload, receipt=reference)
    if value.executor.generation_id != generation_id:
        raise ValueError("admission closure belongs to another generation")
    return value


def decode_custody_row(row: tuple[object, ...]) -> SourceCustodySnapshot:
    """Read historical admission or complete positive layout, never partial rows."""
    if len(row) not in {10, 17}:
        raise ValueError("custody requires an exact ten- or seventeen-column ledger response")
    generation, epoch, revision, locator, digest, binding, admission, outcome, sequence, closure = row[:10]
    if type(generation) is not str or type(locator) is not bytes or type(digest) is not bytes:
        raise ValueError("invalid custody identity column types")
    if binding is not None and type(binding) is not bytes:
        raise ValueError("invalid custody executor column type")
    if type(admission) is not str or type(outcome) is not str or type(sequence) is not int:
        raise ValueError("invalid durable admission column types")
    if type(epoch) is not int or type(revision) is not int:
        raise ValueError("custody epoch and revision must be exact integers")
    if admission not in {"OPEN", "CLOSED"} or outcome not in {"ACTIVE", "FAILED", "UNKNOWN"}:
        raise ValueError("custody has an unsupported admission or outcome")
    if str(UUID(generation)) != generation:
        raise ValueError("custody generation UUID must be canonical")
    executor = None if binding is None else decode_source_executor_binding(binding)
    if sequence != (0 if executor is None else 1) or (executor is None and revision != 1):
        raise ValueError("custody sequence differs from the once-only executor")
    if executor is not None and revision < 2:
        raise ValueError("bound custody requires its admitted revision")
    if admission == "CLOSED":
        if type(closure) is not bytes:
            raise ValueError("closed admission requires its retained payload")
        reference = OriginalRef(
            f"generations/{generation}/writer-admission-closure.json", "sha256:" + sha256(closure).hexdigest()
        )
        retained = decode_source_admission_closure(closure, receipt=reference)
        if (
            retained.executor != executor
            or retained.admission_sequence != sequence
            or retained.revision > revision
            or (len(row) == 10 and retained.revision != revision)
        ):
            raise ValueError("closed admission differs from retained custody")
    elif closure is not None:
        raise ValueError("open admission cannot contain a closure payload")
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
        writer_admission=cast(Literal["OPEN", "CLOSED"], admission),
        outcome=cast(Literal["ACTIVE", "FAILED", "UNKNOWN"], outcome),
    )
    return snapshot if len(row) == 10 else _positive_snapshot(snapshot, row[10:])


def _positive_snapshot(before: SourceCustodySnapshot, columns: tuple[object, ...]) -> SourceCustodySnapshot:
    phase = columns[0]
    if type(phase) is not str or phase not in {"RESERVED", "BUILDING", "FROZEN"}:
        raise ValueError("positive custody phase is invalid")
    completion, frozen = _original_triple(columns[1:4]), _original_triple(columns[4:7])
    if completion is not None:
        value = decode_source_trusted_build_completion(completion[0])
        if value.executor != before.executor or before.revision < 4:
            raise ValueError("positive completion differs from custody executor or revision")
    if frozen is not None:
        value_frozen = decode_frozen_generation(frozen[0], frozen=frozen[1])
        if (
            completion is None
            or (
                value_frozen.generation_id,
                value_frozen.guard_epoch,
                value_frozen.reservation,
                value_frozen.closure.completion,
            )
            != (before.generation_id, before.guard_epoch, before.reservation, completion[1])
            or value_frozen.revision > before.revision
        ):
            raise ValueError("frozen original differs from custody or positive completion")
    return replace(
        before,
        state=cast(Literal["RESERVED", "BUILDING", "FROZEN"], phase),
        closure=None if completion is None else completion[1],
        frozen=None if frozen is None else frozen[1],
    )


def _original_triple(columns: tuple[object, ...]) -> tuple[bytes, OriginalRef] | None:
    if len(columns) != 3:
        raise ValueError("original row requires exactly three columns")
    if all(value is None for value in columns):
        return None
    if any(type(value) is not bytes for value in columns):
        raise ValueError("original row cannot contain partial or inexact values")
    payload, locator, digest = columns
    assert isinstance(payload, bytes) and isinstance(locator, bytes) and isinstance(digest, bytes)
    if not 0 < len(payload) <= 1048576 or not 0 < len(locator) <= 4096 or len(digest) != 71:
        raise ValueError("original row exceeds canonical byte bounds")
    reference = OriginalRef(locator.decode("utf-8"), digest.decode("ascii"))
    if reference.sha256 != "sha256:" + sha256(payload).hexdigest():
        raise ValueError("original row digest differs from its complete payload")
    return payload, reference
