"""V2 event projection, orphan adoption, and scoped recovery operations."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_chunks import NativeChunkReceipt
from dpone.contracts.mssql_native_verification import (
    NativeVerificationIdentityV2,
    canonical_sha256,
    is_nonnegative_int,
    is_sha256_digest,
    matches_native_receipt,
    validate_native_writer_event,
    validated_native_receipt,
)
from dpone.contracts.strict_json import StrictJsonError, canonical_json_bytes, strict_json_object

ROOT_FIELDS = frozenset(
    {
        "version",
        "identity",
        "phase",
        "chunks",
        "events",
        "nonces",
        "complete",
        "observations",
        "publication",
        "completion_metadata",
        "rollback_history",
        "limits",
    }
)


def requires_dedicated_recovery(import_backend: str, prior: str, event: str) -> bool:
    """Keep authority-bearing BCP recovery outside generic append calls."""
    return import_backend == "bcp" and (prior, event) in {
        ("UNKNOWN", "QUIESCENT"),
        ("VERIFIED", "FAILED_RETIRABLE"),
        ("FAILED_RETIRABLE", "RETIRED"),
    }


def initial_projection(identity: NativeVerificationIdentityV2) -> dict[str, Any]:
    """Create the closed v2 root before a source stream is consumed."""
    return dict(
        version=2,
        identity=identity.document(),
        phase="staging",
        chunks={},
        events={},
        nonces={},
        complete=None,
        observations=[],
        publication=None,
        completion_metadata={},
        rollback_history=[],
        limits=None,
    )


def event_key(root: str, ordinal: int, attempt_id: str, sequence: int) -> str:
    """Place one immutable event below its invocation and attempt keyspace."""
    return f"{root}/{ordinal:020d}/{attempt_id}/{sequence:020d}"


def validate_projection(identity: NativeVerificationIdentityV2, value: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    """Validate the complete in-memory v2 authority before any replay decision."""
    if set(value) != ROOT_FIELDS or type(value["version"]) is not int or value["version"] != 2:
        raise ValueError("root shape")
    if value["phase"] not in {"staging", "stage_complete", "reextract_required"}:
        raise ValueError("phase")
    if not all(isinstance(value[name], dict) for name in ("chunks", "events", "nonces")):
        raise ValueError("chunks/events")
    if set(value["events"]) != set(value["nonces"]):
        raise ValueError("attempt nonces")
    if not isinstance(value["observations"], list) or not isinstance(value["completion_metadata"], dict):
        raise ValueError("completion")
    nonpublication = [
        item
        for item in value["rollback_history"]
        if isinstance(item, dict) and item.get("kind") == "pre_eof_nonpublication"
    ]
    if any(
        set(item) != {"kind", "proof_sha256"} or not is_sha256_digest(item["proof_sha256"]) for item in nonpublication
    ) or (nonpublication and (len(nonpublication) != 1 or value["publication"] is not None)):
        raise ValueError("nonpublication proof")
    all_events: list[dict[str, Any]] = []
    for attempt_id, events in value["events"].items():
        if not is_sha256_digest(attempt_id) or not isinstance(events, list) or not events:
            raise ValueError("event chain")
        nonce = value["nonces"][attempt_id]
        if (
            not is_sha256_digest(nonce)
            or canonical_sha256([identity.invocation_key, events[0]["ordinal"], nonce]) != attempt_id
        ):
            raise ValueError("attempt nonce")
        previous = None
        for sequence, event in enumerate(events):
            validate_native_writer_event(identity, event, previous, sequence, attempt_id)
            all_events.append(event)
            previous = event
        if identity.import_backend == "bcp":
            for index, event in enumerate(events):
                if event["event"] == "QUIESCENT" and index and events[index - 1]["event"] == "UNKNOWN":
                    if events[index - 1]["observation"]["writer_outcome"] != "success" or not any(
                        prior["event"] == "WRITER_TERMINAL" and prior["observation"]["writer_outcome"] == "success"
                        for prior in events[:index]
                    ):
                        raise ValueError("bcp terminal proof missing")
        if any(
            event["event"] == "FAILED_RETIRABLE" and events[index - 1]["event"] == "VERIFIED"
            for index, event in enumerate(events)
            if index
        ):
            if not nonpublication:
                raise ValueError("retirement proof missing")
    for ordinal, chunk in value["chunks"].items():
        if (
            str(int(ordinal)) != ordinal
            or int(ordinal) < 0
            or set(chunk) != {"attempt", "attempt_id", "phase", "file", "receipt"}
        ):
            raise ValueError("chunk shape")
        if (
            not is_nonnegative_int(chunk["attempt"])
            or chunk["attempt"] > 2
            or chunk["phase"] not in {"staging", "verified"}
        ):
            raise ValueError("chunk phase")
        events = value["events"].get(chunk["attempt_id"])
        if not events or events[0]["ordinal"] != int(ordinal):
            raise ValueError("missing attempt")
        if chunk["file"] != events[0]["artifact_binding"]:
            raise ValueError("file drift")
        if chunk["phase"] == "verified":
            receipt = validated_native_receipt(chunk["receipt"])
            verified = next((event for event in events if event["event"] == "VERIFIED"), None)
            if (
                verified is None
                or events[-1]["event"] not in {"VERIFIED", "FAILED_RETIRABLE", "RETIRED"}
                or not matches_native_receipt(identity, receipt, verified)
            ):
                raise ValueError("receipt binding")
        elif chunk["receipt"] is not None:
            raise ValueError("unverified receipt")
    if value["phase"] == "stage_complete" and any(
        events[-1]["event"] != "VERIFIED" for events in value["events"].values()
    ):
        raise ValueError("retired stage cannot complete")
    return tuple(all_events)


def adopt_orphan_event(
    entry: dict[str, Any], orphan_payload: str | None, *, explicit_timestamp: bool
) -> dict[str, Any]:
    """Adopt only identical canonical bytes, keeping an orphan's timestamp."""
    if orphan_payload is None:
        return entry
    try:
        stored = strict_json_object(orphan_payload)
        if not explicit_timestamp:
            entry["created_at"] = stored["created_at"]
        if canonical_json_bytes(entry) != orphan_payload.encode():
            raise ValueError("changed orphan")
    except (KeyError, ValueError, StrictJsonError) as error:
        raise WindowContractError("mssql_native.orphan_event_changed") from error
    return entry


def commit_verified_receipt(journal: Any, receipt: NativeChunkReceipt, *, allow_recovery: bool) -> None:
    data = journal._staging(allow_recovery=allow_recovery)
    validated_native_receipt(asdict(receipt))
    chunk = data["chunks"].get(str(receipt.ordinal))
    if allow_recovery and chunk is not None and chunk["phase"] == "verified" and chunk["receipt"] == asdict(receipt):
        return
    if chunk is None or chunk["attempt_id"] != receipt.attempt_id or chunk["phase"] != "staging":
        raise WindowContractError("mssql_native.receipt_attempt_mismatch")
    event = data["events"][receipt.attempt_id][-1]
    if event["event"] != "VERIFIED" or not matches_native_receipt(journal.identity, receipt, event):
        raise WindowContractError("mssql_native.receipt_event_mismatch")
    chunk.update(phase="verified", receipt=asdict(receipt))
    journal._save(data)
