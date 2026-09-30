from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime

import pytest

from dpone.contracts.mssql_native_recovery_authority import (
    MssqlNativeRecoveryBindings,
    build_mssql_native_recovery_authority,
    restore_mssql_native_recovery_admission,
)
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlOperationRequest,
    MssqlTransactionAdmission,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.sinks.mssql_native_completed_payload import authenticate_recovery_admission, completion_metadata


def _digest(character: str) -> str:
    return character * 64


def _admission() -> MssqlTransactionAdmission:
    attempt = MssqlTransactionAttempt(
        MssqlAttemptRequest(
            InvocationIdentity("run", "process", "partition"),
            bytes.fromhex(_digest("1")),
            bytes.fromhex(_digest("2")),
            "load",
            "warehouse",
            "staging",
            "target",
            "full_refresh",
        ),
        3,
    )
    request = MssqlOperationRequest(
        bytes.fromhex(_digest("3")),
        bytes.fromhex(_digest("4")),
        datetime(2030, 1, 2, 3, 4, tzinfo=UTC),
    )
    return MssqlTransactionAdmission(
        operation=MssqlTransactionOperation(
            attempt,
            request.operation_key(attempt),
            request.scope_hash,
            request.owner_digest,
            7,
            request.lease_expires_at_utc,
        )
    )


def _bindings() -> MssqlNativeRecoveryBindings:
    return MssqlNativeRecoveryBindings(
        authored_route_sha256=_digest("5"),
        verification_identity_sha256=_digest("6"),
        target_connection_sha256=_digest("7"),
        state_store_sha256=_digest("8"),
        work_root_sha256=_digest("9"),
    )


def test_recovery_authority_round_trips_one_exact_operation_without_runtime_objects() -> None:
    authority = build_mssql_native_recovery_authority(_admission(), _bindings())

    encoded = json.dumps(authority, allow_nan=False, sort_keys=True)
    restored = restore_mssql_native_recovery_admission(json.loads(encoded), expected_bindings=_bindings())

    assert restored == _admission()
    assert set(authority) == {"version", "kind", "bindings", "operation"}
    assert "credential" not in encoded and "password" not in encoded and "/tmp" not in encoded


@pytest.mark.parametrize(
    "field",
    [
        "authored_route_sha256",
        "verification_identity_sha256",
        "target_connection_sha256",
        "state_store_sha256",
        "work_root_sha256",
    ],
)
def test_recovery_authority_rejects_any_runtime_binding_drift(field: str) -> None:
    authority = build_mssql_native_recovery_authority(_admission(), _bindings())
    values = {**asdict(_bindings()), field: _digest("a")}

    with pytest.raises(ValueError, match="mssql_native.recovery_binding_changed"):
        restore_mssql_native_recovery_admission(
            authority,
            expected_bindings=MssqlNativeRecoveryBindings(**values),
        )


def test_recovery_authority_rejects_unknown_fields_and_operation_key_forgery() -> None:
    authority = build_mssql_native_recovery_authority(_admission(), _bindings())
    authority["unexpected"] = True
    with pytest.raises(ValueError, match="mssql_native.recovery_authority_invalid"):
        restore_mssql_native_recovery_admission(authority, expected_bindings=_bindings())

    authority = build_mssql_native_recovery_authority(_admission(), _bindings())
    authority["operation"]["operation_key"] = _digest("f")
    with pytest.raises(ValueError, match="mssql_native.recovery_operation_changed"):
        restore_mssql_native_recovery_admission(authority, expected_bindings=_bindings())


def test_recovery_authority_rejects_replay_receipt_admission() -> None:
    with pytest.raises(ValueError, match="mssql_native.recovery_operation_required"):
        build_mssql_native_recovery_authority(
            MssqlTransactionAdmission(operation=None, replay_receipt=object()),  # type: ignore[arg-type]
            _bindings(),
        )


def test_completed_source_metadata_persists_authority_in_same_eof_document() -> None:
    now = datetime(2030, 1, 1, tzinfo=UTC)
    payload = type(
        "Payload",
        (),
        {
            "schema": (("id", "bigint"),),
            "relation_schema": None,
            "relation_dialect": None,
            "mssql_target_mutation_plan": None,
            "mssql_transaction_admission": _admission(),
            "require_completed_extraction": lambda self: ExtractionLifecycleReceipt(
                now,
                "test-authority",
                extraction_completed_at=now,
            ),
        },
    )()

    metadata = completion_metadata(payload, recovery_bindings=_bindings())

    assert (
        restore_mssql_native_recovery_admission(
            metadata["recovery_authority_v1"],
            expected_bindings=_bindings(),
        )
        == _admission()
    )


def test_completed_v2_recovery_authenticates_fresh_equivalent_admission() -> None:
    metadata = {"recovery_authority_v1": build_mssql_native_recovery_authority(_admission(), _bindings())}
    journal = type(
        "Journal",
        (),
        {
            "completed": lambda self: object(),
            "completed_metadata": lambda self: metadata,
        },
    )()
    context = type(
        "Context",
        (),
        {
            "journal_factory": lambda self: journal,
            "verification_identity": object(),
            "recovery_bindings": lambda self, admission: _bindings(),
        },
    )()

    assert authenticate_recovery_admission(context, _admission()) == _admission()

    changed = _admission()
    changed = MssqlTransactionAdmission(
        operation=changed.operation.__class__(
            changed.operation.attempt,
            changed.operation.operation_key,
            bytes.fromhex(_digest("a")),
            changed.operation.owner_digest,
            changed.operation.epoch,
            changed.operation.lease_expires_at_utc,
        )
    )
    with pytest.raises(ValueError, match="recovery_operation_changed"):
        authenticate_recovery_admission(context, changed)


def test_completed_v2_legacy_record_fails_closed() -> None:
    journal = type(
        "Journal",
        (),
        {
            "completed": lambda self: object(),
            "completed_metadata": lambda self: {},
        },
    )()
    context = type(
        "Context",
        (),
        {
            "journal_factory": lambda self: journal,
            "verification_identity": object(),
            "recovery_bindings": lambda self, admission: _bindings(),
        },
    )()

    with pytest.raises(ValueError, match="recovery_authority_required"):
        authenticate_recovery_admission(context, _admission())
