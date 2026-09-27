"""Generated writer-state schema stays aligned with the runtime v2 contract."""

from __future__ import annotations

import json
from pathlib import Path

import jsonschema

from dpone.contracts.mssql_native_writer_state_schema import mssql_native_writer_state_v2_schema

SCHEMA_PATH = Path("src/dpone/schema/dpone.mssql-native-writer-state.v2.schema.json")
SHA = "a" * 64


def _event(name: str) -> dict[str, object]:
    late = name not in {"INTENT", "STAGE_OWNED", "GRANTED", "WRITING"}
    return {
        "schema_version": 2,
        "kind": "dpone.mssql-native-writer-state.v2",
        "invocation_key": SHA,
        "ordinal": 0,
        "attempt_id": SHA,
        "sequence": 0 if name == "INTENT" else 1,
        "previous_sha256": None if name == "INTENT" else SHA,
        "event": name,
        "stage_binding": None
        if name == "INTENT"
        else {"stage_id": SHA, "owner_binding_sha256": SHA, "object_id": 1, "schema_sha256": SHA},
        "artifact_binding": {
            "ordinal": 0,
            "rows": 1,
            "encoded_bytes": 8,
            "file_sha256": SHA,
            "typed_digest": SHA,
        },
        "writer_binding": None
        if name in {"INTENT", "STAGE_OWNED"}
        else {
            "import_backend": "bcp",
            "writer_proof_capability": "bcp-supervised-stage-barrier-v1",
            "protocol_sha256": SHA,
            "package_sha256": SHA,
            "capability_sha256": SHA,
            "grant_token_sha256": SHA,
            "timeout_policy_sha256": SHA,
        },
        "observation": {
            "writer_outcome": "cleanup_failed",
            "input_rows_consumed": None,
            "row_count": None,
            "count_overflow": None,
            "limbs": None,
            "quiescence": "failed",
            "diagnostic_code": "mssql_native.cleanup_failed",
        }
        if late
        else None,
        "created_at": "2026-09-27T12:00:00Z",
    }


def test_checked_in_writer_state_schema_equals_canonical_producer() -> None:
    assert json.loads(SCHEMA_PATH.read_text()) == mssql_native_writer_state_v2_schema()


def test_writer_state_schema_closes_phased_bindings_and_cleanup_outcome() -> None:
    validator = jsonschema.Draft7Validator(
        mssql_native_writer_state_v2_schema(), format_checker=jsonschema.FormatChecker()
    )
    for name in ("INTENT", "STAGE_OWNED", "GRANTED", "WRITING", "UNKNOWN"):
        assert not list(validator.iter_errors(_event(name)))

    invalid = _event("UNKNOWN")
    invalid["unexpected"] = True
    assert list(validator.iter_errors(invalid))

    invalid = _event("GRANTED")
    invalid["observation"] = _event("UNKNOWN")["observation"]
    assert list(validator.iter_errors(invalid))

    invalid = _event("GRANTED")
    invalid["stage_binding"] = None
    assert list(validator.iter_errors(invalid))

    invalid = _event("GRANTED")
    invalid["writer_binding"]["writer_proof_capability"] = "sqlclient-session-applock-v1"  # type: ignore[index]
    assert list(validator.iter_errors(invalid))

    invalid = _event("INTENT")
    invalid["sequence"] = 9
    assert list(validator.iter_errors(invalid))

    for timestamp in (
        "2026-99-99T99:99:99Z",
        "2026-02-30T12:00:00Z",
        "2025-02-29T12:00:00Z",
        "0000-01-01T00:00:00Z",
    ):
        invalid = _event("UNKNOWN")
        invalid["created_at"] = timestamp
        assert list(validator.iter_errors(invalid))

    valid = _event("UNKNOWN")
    valid["created_at"] = "2024-02-29T23:59:59.123Z"
    assert not list(validator.iter_errors(valid))
