"""Single-writer CAS journal for one non-reopenable native source stream.

The parent holds the revision it actually read. A concurrent write cannot be
silently overwritten by reloading its revision immediately before save.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from typing import Any

from dpone.adapters.mssql_native_publication_journal import NativePublicationJournal, validate_publication_state
from dpone.contracts.bounded_window import WindowContractError, WindowLease
from dpone.contracts.mssql_native_chunks import (
    EncodedNativeFile,
    NativeChunkPlan,
    NativeChunkReceipt,
    NativeStageComplete,
)
from dpone.ports.bounded_window import WindowStore


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _receipt(value: Any) -> NativeChunkReceipt:
    try:
        receipt = NativeChunkReceipt(**json.loads(json.dumps(value)))
        if any(
            type(getattr(receipt, name)) is not int or getattr(receipt, name) < 0
            for name in ("ordinal", "rows", "encoded_bytes")
        ):
            raise ValueError("invalid counters")
        if any(
            not isinstance(getattr(receipt, name), str) or not getattr(receipt, name)
            for name in ("attempt_id", "stage_id", "file_sha256", "typed_digest")
        ):
            raise ValueError("invalid identity")
        for name in ("file_sha256", "typed_digest"):
            digest = getattr(receipt, name)
            if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
                raise ValueError("invalid digest")
        if not isinstance(receipt.consumed_part_evidence, dict):
            raise ValueError("invalid evidence")
        return receipt
    except (TypeError, ValueError) as error:
        raise WindowContractError("mssql_native.invalid_receipt") from error


class NativeChunkJournal:
    """Persist immutable plan bindings and ordered attempt progress under fencing."""

    def __init__(self, store: WindowStore, lease: WindowLease, plan: NativeChunkPlan) -> None:
        if plan.target_id != lease.target_id or any(not value for value in asdict(plan).values()):
            raise WindowContractError("mssql_native.invalid_plan_identity")
        self.store, self.lease, self.plan = store, lease, plan
        self.key = "mssql-native-chunks-v1/" + _digest([plan.target_id, plan.run_id])
        self.revision: int | None = None
        self._data: dict[str, Any] | None = None
        self._load()
        self.publication = NativePublicationJournal(
            snapshot=lambda: self.data, save=self._save, completed=self.completed
        )

    @property
    def data(self) -> dict[str, Any] | None:
        """Detached audit snapshot; callers cannot mutate the current CAS projection."""
        return None if self._data is None else json.loads(json.dumps(self._data))

    def _load(self) -> None:
        self.store.assert_lease(self.lease)
        record = self.store.load(self.key)
        if record is None:
            return
        try:
            value = json.loads(record.payload)
            if not isinstance(value, dict) or set(value) != {
                "version",
                "identity",
                "phase",
                "chunks",
                "complete",
                "observations",
                "publication",
                "completion_metadata",
                "rollback_history",
                "limits",
            }:
                raise ValueError("invalid record")
            if type(value["version"]) is not int or value["version"] != 1:
                raise ValueError("invalid version")
            if value["identity"] != asdict(self.plan):
                raise WindowContractError("mssql_native.journal_identity_changed")
            if value["phase"] not in ("staging", "stage_complete", "reextract_required"):
                raise ValueError("invalid phase")
            if not isinstance(value["chunks"], dict):
                raise ValueError("invalid chunks")
            for key, chunk in value["chunks"].items():
                if str(int(key)) != key or int(key) < 0 or not isinstance(chunk, dict):
                    raise ValueError("invalid ordinal")
                if type(chunk["attempt"]) is not int or not 0 <= chunk["attempt"] <= 2:
                    raise ValueError("invalid attempt")
                if chunk["phase"] not in ("staging", "verified"):
                    raise ValueError("invalid chunk phase")
                if chunk["phase"] == "verified":
                    receipt = _receipt(chunk["receipt"])
                    if receipt.ordinal != int(key) or receipt.attempt_id != self.attempt_id(int(key), chunk["attempt"]):
                        raise ValueError("invalid attempt binding")
                    if chunk.get("file") is not None:
                        for name in ("ordinal", "rows", "encoded_bytes", "file_sha256", "typed_digest"):
                            if getattr(receipt, name) != chunk["file"][name]:
                                raise ValueError("changed sealed file binding")
            if not isinstance(value["completion_metadata"], dict):
                raise ValueError("invalid completion metadata")
            validate_publication_state(value)
            self.revision, self._data = record.revision, value
            if value["phase"] == "stage_complete":
                self.completed()
        except (KeyError, TypeError, ValueError) as error:
            raise WindowContractError("mssql_native.invalid_journal") from error

    def _save(self, value: dict[str, Any]) -> None:
        # Copy through serialization: caller-owned evidence cannot mutate authority.
        payload = json.dumps(value, sort_keys=True, separators=(",", ":"))
        record = self.store.save(self.key, self.revision, payload, self.lease)
        self.revision, self._data = record.revision, json.loads(payload)

    def begin(self) -> None:
        """A restart cannot append to a partial extraction or reuse its row offsets."""
        if self._data is not None:
            raise WindowContractError("mssql_native.reextract_required")
        self._save(
            dict(
                version=1,
                identity=asdict(self.plan),
                phase="staging",
                chunks={},
                complete=None,
                observations=[],
                publication=None,
                completion_metadata={},
                rollback_history=[],
                limits=None,
            )
        )

    def attempt_id(self, ordinal: int, attempt: int) -> str:
        """Derive an invocation-owned physical attempt independently of scheduling."""
        return f"{self.plan.run_id}-{ordinal}-{attempt}"

    def attempt(self, ordinal: int, attempt: int, file: EncodedNativeFile | None = None) -> None:
        """Persist import intent before any target mutation and bound retry budget."""
        data = self._staging()
        if type(ordinal) is not int or ordinal < 0 or type(attempt) is not int or not 0 <= attempt <= 2:
            raise WindowContractError("mssql_native.invalid_attempt")
        prior = data["chunks"].get(str(ordinal))
        if (prior is None and attempt != 0) or (prior is not None and attempt != prior["attempt"] + 1):
            raise WindowContractError("mssql_native.invalid_retry_order")
        if prior and prior["phase"] == "verified":
            raise WindowContractError("mssql_native.verified_attempt_immutable")
        sealed = None if file is None else dict(asdict(file), path=str(file.path))
        if prior and prior.get("file") != sealed:
            raise WindowContractError("mssql_native.retry_file_changed")
        data["chunks"][str(ordinal)] = dict(attempt=attempt, phase="staging", file=sealed)
        self._save(data)

    def record_observations(self, observations: tuple[dict[str, Any], ...]) -> None:
        """Persist worker outcomes even when a stop threshold prevents EOF."""
        data = self._staging()
        data["observations"] = json.loads(json.dumps(observations))
        self._save(data)

    def verified(self, receipt: NativeChunkReceipt) -> None:
        """Commit verification before releasing the sealed local file."""
        data = self._staging()
        _receipt(asdict(receipt))
        prior = data["chunks"].get(str(receipt.ordinal))
        if prior is None or receipt.attempt_id != self.attempt_id(receipt.ordinal, prior["attempt"]):
            raise WindowContractError("mssql_native.receipt_attempt_mismatch")
        if prior.get("file") is not None:
            for name in ("ordinal", "rows", "encoded_bytes", "file_sha256", "typed_digest"):
                if getattr(receipt, name) != prior["file"][name]:
                    raise WindowContractError("mssql_native.receipt_file_mismatch")
        prior.update(phase="verified", receipt=asdict(receipt))
        self._save(data)

    def _staging(self) -> dict[str, Any]:
        if self._data is None or self._data["phase"] != "staging":
            raise WindowContractError("mssql_native.not_staging")
        return json.loads(json.dumps(self._data))

    def _ordered(self) -> tuple[NativeChunkReceipt, ...]:
        if self._data is None:
            raise WindowContractError("mssql_native.missing_journal")
        chunks = self._data["chunks"]
        if not chunks or set(chunks) != {str(i) for i in range(len(chunks))}:
            raise WindowContractError("mssql_native.noncontiguous_receipts")
        if any(chunk["phase"] != "verified" for chunk in chunks.values()):
            raise WindowContractError("mssql_native.unverified_attempts")
        receipts = tuple(_receipt(chunks[str(i)]["receipt"]) for i in range(len(chunks)))
        if len({receipt.stage_id for receipt in receipts}) != len(receipts):
            raise WindowContractError("mssql_native.shared_stage_identity")
        return receipts

    def complete(
        self,
        *,
        source_eof: bool,
        observations: tuple[dict[str, Any], ...] = (),
        completion_metadata: dict[str, Any] | None = None,
    ) -> NativeStageComplete:
        """One CAS binds EOF, all verified chunks, row authority and ordered evidence."""
        data = self._staging()
        if source_eof is not True:
            raise WindowContractError("mssql_native.source_EOF_required")
        receipts = self._ordered()
        metadata = {} if completion_metadata is None else completion_metadata
        complete: dict[str, Any] = dict(
            rows=sum(r.rows for r in receipts),
            receipt_digest=_digest([asdict(r) for r in receipts]),
            metadata_digest=_digest(metadata),
        )
        data.update(
            phase="stage_complete", complete=complete, observations=list(observations), completion_metadata=metadata
        )
        self._save(data)
        return NativeStageComplete(receipts, **complete, observations=tuple(json.loads(json.dumps(observations))))

    def completed(self) -> NativeStageComplete | None:
        """Load complete authority without opening or validating a source."""
        if self._data is None or self._data["phase"] != "stage_complete":
            return None
        receipts = self._ordered()
        expected: dict[str, Any] = dict(
            rows=sum(r.rows for r in receipts),
            receipt_digest=_digest([asdict(r) for r in receipts]),
            metadata_digest=_digest(self._data["completion_metadata"]),
        )
        if self._data["complete"] != expected:
            raise WindowContractError("mssql_native.stage_complete_changed")
        return NativeStageComplete(
            receipts, **expected, observations=tuple(json.loads(json.dumps(self._data["observations"])))
        )

    def attempts(self) -> tuple[str, ...]:
        """All owned attempts, including abandoned retries, for fenced settlement."""
        if self._data is None:
            return ()
        return tuple(
            self.attempt_id(int(ordinal), attempt)
            for ordinal, chunk in self._data["chunks"].items()
            for attempt in range(chunk["attempt"] + 1)
        )

    def reextract_required(self) -> None:
        """Mark settled partial extraction without deleting its durable audit trail."""
        data = self._staging()
        data["phase"] = "reextract_required"
        self._save(data)

    def completed_metadata(self) -> dict[str, Any]:
        """Metadata captured at EOF in the same CAS as complete receipt authority."""
        if self.completed() is None or self._data is None:
            raise WindowContractError("mssql_native.missing_completion_metadata")
        return json.loads(json.dumps(self._data["completion_metadata"]))

    def bind_limits(self, limits: dict[str, Any]) -> None:
        """Freeze resource policy before row I/O and reject changed recovery limits."""
        if self._data is None:
            return
        if self._data["limits"] is not None:
            if self._data["limits"] != limits:
                raise WindowContractError("mssql_native.resource_limits_changed")
            return
        if self._data["phase"] != "staging":
            raise WindowContractError("mssql_native.resource_limits_missing")
        self._save(dict(self._data, limits=limits))
