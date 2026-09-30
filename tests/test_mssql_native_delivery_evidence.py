"""Closed, content-addressed evidence for optimized MSSQL native delivery."""

from __future__ import annotations

import json
from pathlib import Path

import jsonschema
import pytest

from dpone.adapters.mssql_native_delivery_evidence import write_mssql_native_delivery_evidence
from dpone.contracts.mssql_native_delivery_evidence import (
    mssql_native_delivery_evidence_v2_schema,
    validate_mssql_native_delivery_evidence,
)
from dpone.contracts.mssql_native_delivery_evidence_builder import (
    DeliveryCorrectness,
    DeliveryRecovery,
    DeliveryTimings,
    build_sqlclient_delivery_evidence,
)

SHA = "a" * 64
SCHEMA_PATH = Path("src/dpone/schema/dpone.mssql-native-delivery-evidence.v2.schema.json")


def _payload() -> dict[str, object]:
    return {
        "schema_version": 2,
        "kind": "dpone.mssql-native-delivery-evidence.v2",
        "subject": {
            "invocation_id": SHA,
            "source_commit_sha": "b" * 40,
            "dirty": False,
            "runner_image_sha256": "c" * 64,
            "environment_receipt_sha256": "d" * 64,
        },
        "backend": {
            "import_backend": "mssql_sqlclient",
            "backend_identity_sha256": "d" * 64,
            "wire_identity_sha256": "e" * 64,
            "package_version": "0.88.0",
        },
        "timings": {
            "source_read_seconds": 1.0,
            "bulk_write_seconds": 2.0,
            "target_digest_seconds": 3.0,
            "preparation_seconds": 4.0,
            "publication_seconds": 5.0,
            "confirmed_visibility_seconds": 15.0,
        },
        "resources": {"business_rows_read_back": 0, "bcp_process_count": 0},
        "correctness": {
            "source_rows": 10,
            "published_rows": 10,
            "receipt_count": 1,
            "target_digest_verified": True,
            "publication_verified": True,
        },
        "recovery": {
            "retry_count": 0,
            "unknown_outcome_count": 0,
            "classification": "not_required",
        },
        "privacy": {"synthetic_only": True, "scan_status": "PASS"},
        "status": "PASS",
    }


def test_checked_in_schema_equals_canonical_producer_and_accepts_success() -> None:
    schema = mssql_native_delivery_evidence_v2_schema()
    assert json.loads(SCHEMA_PATH.read_text(encoding="utf-8")) == schema
    assert not list(jsonschema.Draft7Validator(schema).iter_errors(_payload()))
    validate_mssql_native_delivery_evidence(_payload())


@pytest.mark.parametrize(
    ("mutate", "diagnostic"),
    [
        (lambda value: value.update(extra=True), "invalid_fields"),
        (lambda value: value["resources"].update(business_rows_read_back=1), "business_rows_read_back"),
        (lambda value: value["resources"].update(bcp_process_count=1), "bcp_process_count"),
        (lambda value: value["correctness"].update(published_rows=9), "row_count_mismatch"),
    ],
)
def test_sqlclient_pass_evidence_fails_closed(mutate, diagnostic) -> None:
    payload = _payload()
    mutate(payload)

    with pytest.raises(ValueError, match=diagnostic):
        validate_mssql_native_delivery_evidence(payload)


@pytest.mark.parametrize(
    ("mutate", "diagnostic"),
    [
        (lambda value: value["subject"].update(dirty=True), "dirty_source"),
        (lambda value: value["recovery"].update(unknown_outcome_count=1), "recovery_inconsistent"),
        (lambda value: value["recovery"].update(retry_count=1), "recovery_inconsistent"),
        (lambda value: value["correctness"].update(receipt_count=0), "receipt_count"),
    ],
)
def test_pass_evidence_rejects_unproved_source_recovery_or_receipts(mutate, diagnostic) -> None:
    payload = _payload()
    mutate(payload)

    with pytest.raises(ValueError, match=diagnostic):
        validate_mssql_native_delivery_evidence(payload)


def test_typed_sqlclient_builder_binds_backend_and_wire_to_verified_identity() -> None:
    from hashlib import sha256
    from types import SimpleNamespace

    from dpone.contracts.mssql_native_chunks import NativeChunkPlan
    from dpone.contracts.mssql_native_verification_identity import build_sqlclient_target_local_verification_identity

    companion = SimpleNamespace(
        package_version="0.88.0",
        artifact_sha256="d" * 64,
        writer_identity_sha256="e" * 64,
        runtime_identity_sha256="f" * 64,
        runtime_major=10,
        protocol="dpone.mssql-sqlclient.ipc.v1",
    )
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "a" * 64)
    identity = build_sqlclient_target_local_verification_identity(plan, timeout_seconds=60, companion=companion)

    payload = build_sqlclient_delivery_evidence(
        identity=identity,
        companion=companion,
        source_commit_sha="b" * 40,
        dirty=False,
        runner_image_sha256="c" * 64,
        environment_receipt_sha256="d" * 64,
        timings=DeliveryTimings(1, 2, 3, 4, 5, 6),
        correctness=DeliveryCorrectness(10, 10, 1, True, True),
        recovery=DeliveryRecovery(0, 0, "not_required"),
        synthetic_only=True,
        privacy_scan_status="PASS",
    )

    assert payload["subject"]["invocation_id"] == identity.invocation_key
    assert payload["backend"] == {
        "import_backend": "mssql_sqlclient",
        "backend_identity_sha256": identity.capability_layout_sha256,
        "wire_identity_sha256": sha256(plan.wire_fingerprint.encode("utf-8")).hexdigest(),
        "package_version": "0.88.0",
    }
    assert payload["privacy"]["synthetic_only"] is True


def test_typed_sqlclient_builder_preserves_non_synthetic_classification() -> None:
    from types import SimpleNamespace

    from dpone.contracts.mssql_native_chunks import NativeChunkPlan
    from dpone.contracts.mssql_native_verification_identity import build_sqlclient_target_local_verification_identity

    companion = SimpleNamespace(
        package_version="0.88.0",
        artifact_sha256="d" * 64,
        writer_identity_sha256="e" * 64,
        runtime_identity_sha256="f" * 64,
        runtime_major=10,
        protocol="dpone.mssql-sqlclient.ipc.v1",
    )
    identity = build_sqlclient_target_local_verification_identity(
        NativeChunkPlan("run", "target", "query", "window", "schema", "a" * 64),
        timeout_seconds=60,
        companion=companion,
    )

    payload = build_sqlclient_delivery_evidence(
        identity=identity,
        companion=companion,
        source_commit_sha="b" * 40,
        dirty=False,
        runner_image_sha256="c" * 64,
        environment_receipt_sha256="d" * 64,
        timings=DeliveryTimings(1, 2, 3, 4, 5, 6),
        correctness=DeliveryCorrectness(10, 10, 1, True, True),
        recovery=DeliveryRecovery(0, 0, "not_required"),
        synthetic_only=False,
        privacy_scan_status="PASS",
    )

    assert payload["privacy"]["synthetic_only"] is False


def test_writer_creates_immutable_revision_and_atomic_pointer(tmp_path: Path) -> None:
    first = write_mssql_native_delivery_evidence(tmp_path, _payload())
    second = write_mssql_native_delivery_evidence(tmp_path, _payload())

    assert first == second
    assert first.name == f"{first.stem}.json"
    assert json.loads(first.read_text(encoding="utf-8")) == _payload()
    pointer = json.loads((tmp_path / "current.json").read_text(encoding="utf-8"))
    assert pointer == {"schema_version": 1, "sha256": first.stem}
    assert not list(tmp_path.glob("*.tmp"))


def test_changed_evidence_creates_new_revision_without_overwrite(tmp_path: Path) -> None:
    first = write_mssql_native_delivery_evidence(tmp_path, _payload())
    changed = _payload()
    changed["status"] = "UNVERIFIED"
    changed["timings"]["confirmed_visibility_seconds"] = None

    second = write_mssql_native_delivery_evidence(tmp_path, changed)

    assert first != second
    assert first.exists() and second.exists()
