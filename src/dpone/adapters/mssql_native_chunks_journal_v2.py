"""Fenced v2 native journal with an append-only, hash-linked writer chain.

Each event is persisted at its own immutable v2 key before the CAS projection
points at it. An identical orphan can be adopted under root CAS; changed bytes
retain custody and never become a verified receipt.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime
from functools import wraps
from threading import RLock
from typing import Any, Concatenate, ParamSpec, TypeVar

from dpone.adapters.mssql_native_chunks_journal_v2_events import (
    adopt_orphan_event,
    event_key,
    initial_projection,
    validate_projection,
)
from dpone.adapters.mssql_native_publication_journal import NativePublicationJournal, validate_publication_state
from dpone.contracts.bounded_window import WindowContractError, WindowLease
from dpone.contracts.mssql_native_chunks import (
    EncodedNativeFile,
    NativeChunkReceipt,
    NativeStageComplete,
)
from dpone.contracts.mssql_native_verification import (
    ARTIFACT_FIELDS,
    NEXT_EVENTS,
    NativeVerificationIdentityV2,
    canonical_sha256,
    is_nonnegative_int,
    is_sha256_digest,
    matches_native_receipt,
    native_completion_digests,
    opaque_native_stage_id,
    ordered_native_receipts,
    validate_native_writer_event,
    validated_native_receipt,
)
from dpone.contracts.strict_json import StrictJsonError, canonical_json_bytes, strict_json_object
from dpone.ports.bounded_window import WindowStore

_P = ParamSpec("_P")
_R = TypeVar("_R")


def _serialized(method: Callable[Concatenate[Any, _P], _R]) -> Callable[Concatenate[Any, _P], _R]:
    """Hold one instance lock from snapshot through every durable CAS write."""

    @wraps(method)
    def call(self: Any, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        with self._lock:
            return method(self, *args, **kwargs)

    return call


class _PublicationView:
    """Serialize each existing publication transition as one journal mutation."""

    _MUTATORS = frozenset(
        {
            "prepared",
            "preparation_started",
            "publication_started",
            "publication_confirmed",
            "publication_rolled_back",
            "evidence_complete",
            "succeeded",
        }
    )

    def __init__(self, inner: NativePublicationJournal, lock: Any) -> None:
        self._inner, self._lock = inner, lock

    def __getattr__(self, name: str) -> Any:
        member = getattr(self._inner, name)
        if name not in self._MUTATORS:
            return member

        @wraps(member)
        def call(*args: Any, **kwargs: Any) -> Any:
            with self._lock:
                return member(*args, **kwargs)

        return call


class NativeChunkJournalV2:
    """Own v2 identity, immutable event keys, and the existing publication view."""

    def __init__(self, store: WindowStore, lease: WindowLease, identity: NativeVerificationIdentityV2) -> None:
        if not isinstance(identity, NativeVerificationIdentityV2) or identity.plan.target_id != lease.target_id:
            raise WindowContractError("mssql_native.invalid_plan_identity")
        self.store, self.lease, self.identity, self.plan = store, lease, identity, identity.plan
        self.key = "mssql-native-chunks-v2/" + identity.invocation_key
        self._lock = RLock()
        self._recovery_only = False
        self.revision: int | None = None
        self._data: dict[str, Any] | None = None
        self._load()
        self.publication = _PublicationView(
            NativePublicationJournal(snapshot=lambda: self.data, save=self._save, completed=self.completed), self._lock
        )

    @property
    @_serialized
    def data(self) -> dict[str, Any] | None:
        """Detached audit projection; mutation cannot change CAS authority."""
        return None if self._data is None else json.loads(json.dumps(self._data))

    def _event_key(self, event: dict[str, Any]) -> str:
        return event_key(self.key, event["ordinal"], event["attempt_id"], event["sequence"])

    def opaque_stage_id(self, qualified_stage_id: str) -> str:
        """Return the coordinate-free binding used in v2 writer events."""
        return opaque_native_stage_id(self.identity, qualified_stage_id)

    def _load(self) -> None:
        self.store.assert_lease(self.lease)
        record = self.store.load(self.key)
        if record is None:
            return
        try:
            value = strict_json_object(record.payload)
            if value.get("identity") != self.identity.document():
                raise WindowContractError("mssql_native.journal_identity_changed")
            for event in validate_projection(self.identity, value):
                stored = self.store.load(self._event_key(event))
                if stored is None or stored.payload.encode() != canonical_json_bytes(event):
                    raise ValueError("event authority")
            validate_publication_state(value)
            self.revision, self._data = record.revision, value
            self._recovery_only = value["phase"] == "staging"
            if value["phase"] == "stage_complete":
                self.completed()
        except (KeyError, TypeError, ValueError, StrictJsonError) as error:
            raise WindowContractError("mssql_native.invalid_journal_event") from error

    def _save(self, value: dict[str, Any], event: dict[str, Any] | None = None) -> None:
        if event is not None:
            key, payload = self._event_key(event), canonical_json_bytes(event).decode()
            existing = self.store.load(key)
            if existing is None:
                try:
                    self.store.save(key, None, payload, self.lease)
                except WindowContractError:
                    existing = self.store.load(key)
                    if existing is None or existing.payload != payload:
                        raise
            elif existing.payload != payload:
                raise WindowContractError("mssql_native.orphan_event_changed")
        payload = canonical_json_bytes(value).decode()
        record = self.store.save(self.key, self.revision, payload, self.lease)
        self.revision, self._data = record.revision, strict_json_object(payload)

    @_serialized
    def begin(self) -> None:
        """Start only a new invocation; a partial pre-EOF stream is not replayable."""
        if self._data is not None:
            raise WindowContractError("mssql_native.reextract_required")
        self._save(initial_projection(self.identity))

    @_serialized
    def attempt_id(self, ordinal: int, attempt: int) -> str:
        """Return only an already persisted random-nonce attempt identity."""
        if self._data is None or not is_nonnegative_int(ordinal) or not is_nonnegative_int(attempt):
            raise WindowContractError("mssql_native.missing_attempt")
        chunk = self._data["chunks"].get(str(ordinal))
        if chunk is None or chunk["attempt"] != attempt:
            raise WindowContractError("mssql_native.missing_attempt")
        return str(chunk["attempt_id"])

    @_serialized
    def attempt(self, ordinal: int, attempt: int, file: EncodedNativeFile | None = None) -> None:
        """Persist a sealed-file INTENT and fresh nonce before target mutation."""
        data = self._staging()
        if (
            not is_nonnegative_int(ordinal)
            or not is_nonnegative_int(attempt)
            or attempt > 2
            or file is None
            or file.ordinal != ordinal
        ):
            raise WindowContractError("mssql_native.invalid_attempt")
        artifact = {name: getattr(file, name) for name in ARTIFACT_FIELDS}
        if not all(is_nonnegative_int(artifact[name]) for name in ("ordinal", "rows", "encoded_bytes")) or not all(
            is_sha256_digest(artifact[name]) for name in ("file_sha256", "typed_digest")
        ):
            raise WindowContractError("mssql_native.invalid_sealed_file")
        prior = data["chunks"].get(str(ordinal))
        if (prior is None and attempt != 0) or (
            prior is not None
            and (attempt != prior["attempt"] + 1 or data["events"][prior["attempt_id"]][-1]["event"] != "RETIRED")
        ):
            raise WindowContractError("mssql_native.invalid_retry_order")
        nonce = secrets.token_hex(32)
        attempt_id = canonical_sha256([self.identity.invocation_key, ordinal, nonce])
        data["nonces"][attempt_id] = nonce
        data["chunks"][str(ordinal)] = dict(
            attempt=attempt, attempt_id=attempt_id, phase="staging", file=artifact, receipt=None
        )
        self._append(data, ordinal, attempt_id, "INTENT", artifact, None, None, None, None)

    @_serialized
    def append_event(
        self,
        ordinal: int,
        attempt_id: str,
        event: str,
        *,
        stage_binding: dict[str, Any] | None = None,
        writer_binding: dict[str, Any] | None = None,
        observation: dict[str, Any] | None = None,
        created_at: str | None = None,
    ) -> dict[str, Any]:
        """Append one closed event under CAS; omitted bindings inherit prior proof."""
        data = self._staging(allow_recovery=True)
        chunk = data["chunks"].get(str(ordinal))
        if chunk is None or chunk["attempt_id"] != attempt_id or event not in NEXT_EVENTS:
            raise WindowContractError("mssql_native.invalid_journal_event")
        prior = data["events"][attempt_id][-1]
        stage = prior["stage_binding"] if stage_binding is None else stage_binding
        writer = prior["writer_binding"] if writer_binding is None else writer_binding
        observed = prior["observation"] if observation is None else observation
        if (
            event == prior["event"]
            and (stage, writer, observed) == (prior["stage_binding"], prior["writer_binding"], prior["observation"])
            and (created_at is None or created_at == prior["created_at"])
        ):
            if self._recovery_only:
                raise WindowContractError("mssql_native.pre_eof_reextract_required")
            return json.loads(json.dumps(prior))
        return self._append(data, ordinal, attempt_id, event, chunk["file"], stage, writer, observed, created_at)

    def _append(
        self,
        data: dict[str, Any],
        ordinal: int,
        attempt_id: str,
        event: str,
        artifact: dict[str, Any],
        stage: dict[str, Any] | None,
        writer: dict[str, Any] | None,
        observation: dict[str, Any] | None,
        created_at: str | None = None,
    ) -> dict[str, Any]:
        events = data["events"].setdefault(attempt_id, [])
        previous = events[-1] if events else None
        entry = dict(
            schema_version=2,
            kind="dpone.mssql-native-writer-state.v2",
            invocation_key=self.identity.invocation_key,
            ordinal=ordinal,
            attempt_id=attempt_id,
            sequence=len(events),
            previous_sha256=None if previous is None else canonical_sha256(previous),
            event=event,
            stage_binding=stage,
            artifact_binding=artifact,
            writer_binding=writer,
            observation=observation,
            created_at=created_at or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        )
        orphan = self.store.load(self._event_key(entry))
        if self._recovery_only and orphan is None:
            raise WindowContractError("mssql_native.pre_eof_reextract_required")
        adopt_orphan_event(entry, None if orphan is None else orphan.payload, explicit_timestamp=created_at is not None)
        try:
            validate_native_writer_event(self.identity, entry, previous, len(events), attempt_id)
        except (KeyError, TypeError, ValueError) as error:
            raise WindowContractError("mssql_native.invalid_journal_event_transition") from error
        events.append(entry)
        self._save(data, entry)
        return json.loads(json.dumps(entry))

    @_serialized
    def verified(self, receipt: NativeChunkReceipt) -> None:
        """Commit the existing receipt only after a real terminal VERIFIED event."""
        data = self._staging()
        validated_native_receipt(asdict(receipt))
        chunk = data["chunks"].get(str(receipt.ordinal))
        if chunk is None or chunk["attempt_id"] != receipt.attempt_id or chunk["phase"] != "staging":
            raise WindowContractError("mssql_native.receipt_attempt_mismatch")
        event = data["events"][receipt.attempt_id][-1]
        if event["event"] != "VERIFIED" or not matches_native_receipt(self.identity, receipt, event):
            raise WindowContractError("mssql_native.receipt_event_mismatch")
        chunk.update(phase="verified", receipt=asdict(receipt))
        self._save(data)

    def _staging(self, *, allow_recovery: bool = False) -> dict[str, Any]:
        if self._data is None or self._data["phase"] != "staging":
            raise WindowContractError("mssql_native.not_staging")
        if self._recovery_only and not allow_recovery:
            raise WindowContractError("mssql_native.pre_eof_reextract_required")
        return json.loads(json.dumps(self._data))

    @_serialized
    def complete(
        self,
        *,
        source_eof: bool,
        observations: tuple[dict[str, Any], ...] = (),
        completion_metadata: dict[str, Any] | None = None,
    ) -> NativeStageComplete:
        """Bind EOF, contiguous verified receipts, and completion metadata once."""
        data = self._staging()
        if source_eof is not True:
            raise WindowContractError("mssql_native.source_EOF_required")
        receipts = ordered_native_receipts(data["chunks"])
        metadata = {} if completion_metadata is None else completion_metadata
        complete = native_completion_digests(receipts, metadata)
        data.update(
            phase="stage_complete", complete=complete, observations=list(observations), completion_metadata=metadata
        )
        self._save(data)
        return NativeStageComplete(
            receipts,
            complete["rows"],
            complete["receipt_digest"],
            observations=tuple(json.loads(json.dumps(observations))),
            metadata_digest=complete["metadata_digest"],
        )

    @_serialized
    def completed(self) -> NativeStageComplete | None:
        """Reconstruct source-free authority only from a sealed EOF record."""
        if self._data is None or self._data["phase"] != "stage_complete":
            return None
        receipts = ordered_native_receipts(self._data["chunks"])
        expected = native_completion_digests(receipts, self._data["completion_metadata"])
        if self._data["complete"] != expected:
            raise WindowContractError("mssql_native.stage_complete_changed")
        return NativeStageComplete(
            receipts,
            expected["rows"],
            expected["receipt_digest"],
            observations=tuple(json.loads(json.dumps(self._data["observations"]))),
            metadata_digest=expected["metadata_digest"],
        )

    @_serialized
    def attempts(self) -> tuple[str, ...]:
        """Return every persisted attempt, including abandoned retries."""
        return () if self._data is None else tuple(self._data["events"])

    @_serialized
    def record_observations(self, observations: tuple[dict[str, Any], ...]) -> None:
        """Persist bounded worker outcomes even when extraction stops pre-EOF."""
        data = self._staging()
        data["observations"] = json.loads(json.dumps(observations))
        self._save(data)

    @_serialized
    def reextract_required(self) -> None:
        """Retain partial extraction custody; never reopen its source stream."""
        data = self._staging(allow_recovery=True)
        data["phase"] = "reextract_required"
        self._save(data)

    @_serialized
    def completed_metadata(self) -> dict[str, Any]:
        """Return detached metadata captured in the EOF CAS."""
        if self.completed() is None or self._data is None:
            raise WindowContractError("mssql_native.missing_completion_metadata")
        return json.loads(json.dumps(self._data["completion_metadata"]))

    @_serialized
    def bind_limits(self, limits: dict[str, Any]) -> None:
        """Freeze resource policy before I/O and reject changed recovery limits."""
        if self._data is None:
            return
        if self._data["limits"] is not None:
            if self._data["limits"] != limits:
                raise WindowContractError("mssql_native.resource_limits_changed")
            return
        if self._recovery_only:
            raise WindowContractError("mssql_native.pre_eof_reextract_required")
        if self._data["phase"] != "staging":
            raise WindowContractError("mssql_native.resource_limits_missing")
        self._save(dict(self._data, limits=limits))
