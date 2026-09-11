"""Canonical protected capture originals; hashes are not admission grants."""

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.contracts.composition_dbt_capture_codec import (
    decode_event,
    decode_registration,
    encode_event,
    encode_registration,
)
from dpone.contracts.composition_dbt_outcome import (
    EVIDENCE_SUBJECT_FIELDS,
    DbtArtifactOriginal,
    DbtCaptureError,
    DbtCaptureRecord,
    DbtChildExit,
    DbtDispatchIntent,
    DbtExitRecord,
    DbtOutcomeExpectation,
)
from tests.composition_mssql_gate_helpers import attempt
from tests.test_composition_activation_contract import digest


def subject():
    original = DbtArtifactOriginal("preflight_manifest", "preflight/manifest.json", b"original")
    intent = DbtDispatchIntent(
        attempt(),
        ("/opt/dbt/bin/dbt", "build"),
        digest("tool"),
        "mssql-sid:" + "61" * 16,
        "/var/lib/dpone/attempt",
        0,
        1001,
        1001,
        (
            ("preflight_manifest", "preflight/manifest.json"),
            ("build_manifest", "target/manifest.json"),
            ("run_results", "target/run_results.json"),
            ("execution_evidence", "evidence.json"),
        ),
        original.sha256,
    )
    expected = DbtOutcomeExpectation(
        digest("graph"),
        ("model.p.a",),
        ("model.p.a",),
        "1.12.3",
        "v12",
        "v6",
        tuple(
            (key, intent.toolchain_sha256 if key == "toolchain_sha256" else digest(key))
            for key in sorted(EVIDENCE_SUBJECT_FIELDS)
        ),
        (("model.p.a", "table", digest("schema")),),
    )
    return intent, expected, original


def test_registration_roundtrip_canonical_and_original_identity():
    intent, expected, _ = subject()
    raw = encode_registration(intent, expected)
    assert decode_registration(raw, "sha256:" + sha256(raw).hexdigest()) == (intent, expected)
    with pytest.raises(DbtCaptureError):
        decode_registration(raw + b" ", "sha256:" + sha256(raw + b" ").hexdigest())


def test_event_roundtrip_and_exact_raw_artifact_bytes():
    intent, _, original = subject()
    exited = DbtExitRecord(intent, DbtChildExit(123, 100, 0), b'{"closed":true}', original)
    captured = DbtCaptureRecord(
        intent,
        "CAPTURED",
        exited,
        tuple(
            original if role == "preflight_manifest" else DbtArtifactOriginal(role, path, b"\x00\xff original")
            for role, path in intent.artifact_paths
        ),
    )
    for phase, value in (("DISPATCH", original), ("EXIT", exited), ("CAPTURE", captured)):
        raw = encode_event(phase, value)
        assert decode_event(raw, "sha256:" + sha256(raw).hexdigest(), phase) == value


def test_changed_preflight_is_not_a_valid_capture():
    intent, _, original = subject()
    exited = DbtExitRecord(intent, DbtChildExit(123, 100, 0), b"{}", original)
    captured = DbtCaptureRecord(intent, "CAPTURED", exited, (replace(original, content=b"other"),))
    with pytest.raises(DbtCaptureError):
        encode_event("CAPTURE", captured)


@pytest.mark.parametrize("damage", ["hash", "base64", "unknown_field", "duplicate", "phase"])
def test_event_original_tampering_fails_closed(damage):
    import json

    from dpone.contracts.strict_json import canonical_json_bytes

    _, _, original = subject()
    raw = encode_event("DISPATCH", original)
    body = json.loads(raw)
    if damage == "hash":
        body["value"]["sha256"] = digest("other")
    elif damage == "base64":
        body["value"]["content"] = "!invalid!"
    elif damage == "unknown_field":
        body["caller_asserted"] = True
    elif damage == "phase":
        body["phase"] = "EXIT"
    raw = canonical_json_bytes(body)
    if damage == "duplicate":
        raw = raw.replace(b'"phase":"DISPATCH"', b'"phase":"DISPATCH","phase":"DISPATCH"')
    with pytest.raises(DbtCaptureError):
        decode_event(raw, "sha256:" + sha256(raw).hexdigest(), "DISPATCH")
