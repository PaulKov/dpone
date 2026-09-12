"""Canonical bounded originals for protected dbt registration and event journals."""

from __future__ import annotations

import base64
from dataclasses import asdict, fields
from hashlib import sha256
from typing import Any

from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_dbt_outcome import (
    ARTIFACT_ROLES,
    MAX_ARTIFACT_BYTES,
    DbtArtifactOriginal,
    DbtCaptureError,
    DbtCaptureRecord,
    DbtChildExit,
    DbtDispatchIntent,
    DbtExitRecord,
    DbtOutcomeExpectation,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

MAX_CAPTURE_DOCUMENT_BYTES = 128 * 1024 * 1024
EVENT_PHASES = frozenset({"DISPATCH", "EXIT", "CAPTURE", "UNDISPATCHED"})


def document_sha256(raw: bytes) -> str:
    return "sha256:" + sha256(raw).hexdigest()


def _object(value: Any, names: set[str]) -> dict[str, Any]:
    if type(value) is not dict or set(value) != names:
        raise DbtCaptureError("capture_codec_fields")
    return value


def _fields(cls: type) -> set[str]:
    return {field.name for field in fields(cls)}


def _intent(raw: Any) -> DbtDispatchIntent:
    """Require every protected launch field, including cwd and timeout; no defaults."""
    body = dict(_object(raw, _fields(DbtDispatchIntent)))
    attempt = dict(_object(body["attempt"], _fields(CompositionAttemptIdentity)))
    attempt["guard_epochs"] = tuple(tuple(pair) for pair in attempt["guard_epochs"])
    body["attempt"] = CompositionAttemptIdentity(**attempt)
    body["argv"] = tuple(body["argv"])
    body["artifact_paths"] = tuple(tuple(pair) for pair in body["artifact_paths"])
    return DbtDispatchIntent(**body)


def _expectation(raw: Any) -> DbtOutcomeExpectation:
    body = dict(_object(raw, _fields(DbtOutcomeExpectation)))
    for name in ("selected_graph_unique_ids", "expected_run_result_unique_ids"):
        body[name] = tuple(body[name])
    for name in ("evidence_subject", "materializations"):
        body[name] = tuple(tuple(pair) for pair in body[name])
    return DbtOutcomeExpectation(**body)


def _encode(raw: bytes) -> str:
    if type(raw) is not bytes or not 0 < len(raw) <= MAX_ARTIFACT_BYTES:
        raise DbtCaptureError("capture_codec_bytes")
    return base64.b64encode(raw).decode("ascii")


def _decode(value: Any) -> bytes:
    if type(value) is not str or len(value) > 4 * ((MAX_ARTIFACT_BYTES + 2) // 3):
        raise DbtCaptureError("capture_codec_bytes")
    raw = base64.b64decode(value, validate=True)
    if _encode(raw) != value:
        raise DbtCaptureError("capture_codec_bytes")
    return raw


def _original_document(value: DbtArtifactOriginal) -> dict[str, Any]:
    if type(value) is not DbtArtifactOriginal:
        raise DbtCaptureError("capture_codec_artifact")
    value.__post_init__()
    return {
        "role": value.role,
        "relative_path": value.relative_path,
        "content": _encode(value.content),
        "sha256": value.sha256,
    }


def _original(raw: Any) -> DbtArtifactOriginal:
    body = _object(raw, {"role", "relative_path", "content", "sha256"})
    value = DbtArtifactOriginal(body["role"], body["relative_path"], _decode(body["content"]))
    if value.sha256 != body["sha256"]:
        raise DbtCaptureError("capture_codec_artifact")
    return value


def _exit_document(value: DbtExitRecord) -> dict[str, Any]:
    if type(value) is not DbtExitRecord:
        raise DbtCaptureError("capture_codec_exit")
    value.intent.__post_init__()
    value.child.__post_init__()
    if (value.preflight_original.role, value.preflight_original.relative_path) != value.intent.artifact_paths[
        0
    ] or value.preflight_original.sha256 != value.intent.preflight_manifest_sha256:
        raise DbtCaptureError("capture_codec_preflight")
    return {
        "intent": asdict(value.intent),
        "child": asdict(value.child),
        "quiescence_original": _encode(value.quiescence_original),
        "preflight_original": _original_document(value.preflight_original),
    }


def _exit(raw: Any) -> DbtExitRecord:
    body = _object(raw, _fields(DbtExitRecord))
    value = DbtExitRecord(
        _intent(body["intent"]),
        DbtChildExit(**_object(body["child"], _fields(DbtChildExit))),
        _decode(body["quiescence_original"]),
        _original(body["preflight_original"]),
    )
    _exit_document(value)
    return value


def _capture_document(value: DbtCaptureRecord, phase: str) -> dict[str, Any]:
    if type(value) is not DbtCaptureRecord or value.phase != ("CAPTURED" if phase == "CAPTURE" else "UNDISPATCHED"):
        raise DbtCaptureError("capture_codec_phase")
    value.intent.__post_init__()
    if phase == "CAPTURE":
        if (
            value.exit_record is None
            or value.exit_record.intent != value.intent
            or value.undispatched_closure_original is not None
            or type(value.originals) is not tuple
            or tuple(row.role for row in value.originals) != ARTIFACT_ROLES
            or tuple((row.role, row.relative_path) for row in value.originals) != value.intent.artifact_paths
            or value.originals[0] != value.exit_record.preflight_original
        ):
            raise DbtCaptureError("capture_codec_capture")
    elif value.exit_record is not None or value.originals or value.undispatched_closure_original is None:
        raise DbtCaptureError("capture_codec_undispatched")
    return {
        "intent": asdict(value.intent),
        "phase": value.phase,
        "exit_record": _exit_document(value.exit_record) if value.exit_record is not None else None,
        "originals": [_original_document(row) for row in value.originals],
        "undispatched_closure_original": _encode(value.undispatched_closure_original)
        if value.undispatched_closure_original is not None
        else None,
    }


def _capture(raw: Any, phase: str) -> DbtCaptureRecord:
    body = _object(raw, _fields(DbtCaptureRecord))
    value = DbtCaptureRecord(
        _intent(body["intent"]),
        body["phase"],
        _exit(body["exit_record"]) if body["exit_record"] is not None else None,
        tuple(_original(row) for row in body["originals"]),
        _decode(body["undispatched_closure_original"]) if body["undispatched_closure_original"] is not None else None,
    )
    _capture_document(value, phase)
    return value


def _document(body: dict[str, Any]) -> bytes:
    raw = canonical_json_bytes(body)
    if len(raw) > MAX_CAPTURE_DOCUMENT_BYTES:
        raise DbtCaptureError("capture_codec_size")
    return raw


def _read(raw: bytes, expected_sha256: str, schema: str) -> dict[str, Any]:
    if (
        type(raw) is not bytes
        or not 0 < len(raw) <= MAX_CAPTURE_DOCUMENT_BYTES
        or document_sha256(raw) != expected_sha256
    ):
        raise DbtCaptureError("capture_codec_original")
    body = strict_json_object(raw)
    if body.get("schema") != schema or canonical_json_bytes(body) != raw:
        raise DbtCaptureError("capture_codec_canonical")
    return body


def encode_registration(intent: DbtDispatchIntent, expectation: DbtOutcomeExpectation) -> bytes:
    """Detach and revalidate typed registration before protected persistence."""
    if type(intent) is not DbtDispatchIntent or type(expectation) is not DbtOutcomeExpectation:
        raise DbtCaptureError("capture_registration_invalid")
    intent.__post_init__()
    expectation.__post_init__()
    return _document(
        {
            "schema": "dpone.dbt-capture-registration.v1",
            "intent": asdict(_intent(asdict(intent))),
            "expectation": asdict(_expectation(asdict(expectation))),
        }
    )


def decode_registration(raw: bytes, expected_sha256: str) -> tuple[DbtDispatchIntent, DbtOutcomeExpectation]:
    try:
        body = _object(
            _read(raw, expected_sha256, "dpone.dbt-capture-registration.v1"), {"schema", "intent", "expectation"}
        )
        value = _intent(body["intent"]), _expectation(body["expectation"])
        if encode_registration(*value) != raw:
            raise DbtCaptureError("capture_codec_canonical")
        return value
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise DbtCaptureError("capture_registration_invalid") from None


def encode_event(phase: str, value: Any) -> bytes:
    if phase not in EVENT_PHASES:
        raise DbtCaptureError("capture_codec_phase")
    body = (
        _original_document(value)
        if phase == "DISPATCH"
        else _exit_document(value)
        if phase == "EXIT"
        else _capture_document(value, phase)
    )
    return _document({"schema": "dpone.dbt-capture-event.v1", "phase": phase, "value": body})


def decode_event(raw: bytes, expected_sha256: str, phase: str) -> Any:
    try:
        body = _object(_read(raw, expected_sha256, "dpone.dbt-capture-event.v1"), {"schema", "phase", "value"})
        if phase not in EVENT_PHASES or body["phase"] != phase:
            raise DbtCaptureError("capture_codec_phase")
        value = (
            _original(body["value"])
            if phase == "DISPATCH"
            else _exit(body["value"])
            if phase == "EXIT"
            else _capture(body["value"], phase)
        )
        if encode_event(phase, value) != raw:
            raise DbtCaptureError("capture_codec_canonical")
        return value
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError, OverflowError):
        raise DbtCaptureError("capture_event_invalid") from None
