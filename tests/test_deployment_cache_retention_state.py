from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import jsonschema
import pytest

from dpone.contracts.airflow_loader_ack import AirflowLoaderAck
from dpone.contracts.deployment_cache_retention_state import (
    RETENTION_RECOVERY_SCHEMA_V1,
    DeploymentCacheRetentionStateError,
    build_activation_history,
    build_retention_recovery,
    canonical_digest,
    parse_activation_history,
    parse_retention_recovery,
)
from dpone.contracts.deployment_cache_retention_transaction import bind_retention_transaction
from dpone.gitops.schema_deployment_cache_state_contracts import (
    deployment_cache_activation_history_contract,
    deployment_cache_retention_recovery_contract,
    deployment_cache_retention_recovery_v2_contract,
)
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.runtime.deployment_cache_activation_history import DeploymentCacheActivationHistory
from dpone.runtime.deployment_cache_retention_state_codec import migrate_retention_recovery

_DEPLOYMENT_ID = "sha256:" + "1" * 64
_ACTIVATION_ID = str(UUID("12345678-1234-4234-9234-123456789abc"))


def _activation_history() -> dict[str, object]:
    return build_activation_history(
        {
            _ACTIVATION_ID: {
                "activation_id": _ACTIVATION_ID,
                "release_id": "sha256:" + "2" * 64,
                "deployment_id": _DEPLOYMENT_ID,
                "airflow_index_sha256": "sha256:" + "3" * 64,
                "loaded_dag_ids": ["DAG__platform__cache__smoke"],
                "verified_at": "2026-08-03T00:00:00+00:00",
            }
        },
        [],
    )


def _transaction(*, phase: str) -> dict[str, object]:
    return {
        "deployment_id": _DEPLOYMENT_ID,
        "environment": "dev",
        "phase": phase,
        "original_path": "/cache/deployments/dev/sha256-1",
        "detached_path": "/cache/.retention-trash/sha256-1.nonce",
        "activation_path": "/cache/activations/dev/sha256-1",
        "expected_device": 1,
        "expected_inode": 2,
    }


def _transaction_v2(*, phase: str) -> dict[str, object]:
    return bind_retention_transaction({**_transaction(phase=phase), "operation_id": None})


def test_activation_history_runtime_and_json_schema_reject_unknown_fields() -> None:
    payload = _activation_history()
    invalid = deepcopy(payload)
    invalid["unexpected"] = True

    with pytest.raises(DeploymentCacheRetentionStateError):
        parse_activation_history(invalid)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid, deployment_cache_activation_history_contract().schema)


def test_activation_history_rejects_duplicate_dag_inventory() -> None:
    payload = _activation_history()
    entry = next(iter(payload["entries"].values()))
    entry["loaded_dag_ids"] = ["duplicate", "duplicate"]

    with pytest.raises(DeploymentCacheRetentionStateError):
        parse_activation_history(payload)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, deployment_cache_activation_history_contract().schema)


def test_recovery_runtime_and_json_schema_require_transactions() -> None:
    payload = build_retention_recovery(
        status="recovering",
        restored_deployment_ids=[],
        pending_deployment_ids=[_DEPLOYMENT_ID],
        transactions={_DEPLOYMENT_ID: _transaction(phase="prepared")},
    )
    invalid = deepcopy(payload)
    invalid.pop("transactions")

    with pytest.raises(DeploymentCacheRetentionStateError):
        parse_retention_recovery(invalid)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(invalid, deployment_cache_retention_recovery_v2_contract().schema)


def test_recovery_rejects_status_and_transaction_phase_disagreement() -> None:
    with pytest.raises(DeploymentCacheRetentionStateError):
        build_retention_recovery(
            status="recovered",
            restored_deployment_ids=[],
            pending_deployment_ids=[_DEPLOYMENT_ID],
            transactions={_DEPLOYMENT_ID: _transaction(phase="prepared")},
        )


def test_recovery_rejects_blocked_status_without_blocked_transaction() -> None:
    with pytest.raises(DeploymentCacheRetentionStateError):
        build_retention_recovery(
            status="blocked",
            restored_deployment_ids=[],
            pending_deployment_ids=[_DEPLOYMENT_ID],
            transactions={_DEPLOYMENT_ID: _transaction(phase="prepared")},
        )


def test_recovery_rejects_quarantine_projection_drift() -> None:
    with pytest.raises(DeploymentCacheRetentionStateError):
        build_retention_recovery(
            status="blocked",
            restored_deployment_ids=[],
            pending_deployment_ids=[_DEPLOYMENT_ID],
            transactions={_DEPLOYMENT_ID: _transaction(phase="blocked")},
            quarantined_paths=["/cache/.retention-trash/unrelated"],
        )


def test_v1_recovery_migrates_without_guessing_operation_identity() -> None:
    body = {
        "schema": RETENTION_RECOVERY_SCHEMA_V1,
        "status": "recovered",
        "restored_deployment_ids": [],
        "pending_deployment_ids": [],
        "transactions": {_DEPLOYMENT_ID: _transaction(phase="committed")},
    }
    legacy = {**body, "revision": canonical_digest(body)}

    migrated = migrate_retention_recovery(legacy)

    assert migrated["schema"] == "dpone.deployment-cache-retention-recovery.v2"
    transaction = next(iter(migrated["transactions"].values()))
    assert transaction["deployment_id"] == _DEPLOYMENT_ID
    assert transaction["operation_id"] is None
    assert transaction["transaction_id"] in migrated["transactions"]
    assert parse_retention_recovery(migrated) == migrated


def test_v2_recovery_wraps_malformed_occurrence_identity_as_state_error() -> None:
    transaction = _transaction_v2(phase="prepared")
    transaction["transaction_id"] = "not-a-digest"
    body = {
        "schema": "dpone.deployment-cache-retention-recovery.v2",
        "status": "recovering",
        "restored_deployment_ids": [],
        "pending_deployment_ids": [_DEPLOYMENT_ID],
        "transactions": {"not-a-digest": transaction},
        "quarantined_paths": [],
    }

    with pytest.raises(DeploymentCacheRetentionStateError):
        parse_retention_recovery({**body, "revision": canonical_digest(body)})


def test_prospective_activation_history_revision_is_clock_independent_and_durable(tmp_path: Path) -> None:
    ack = AirflowLoaderAck(
        release_id="sha256:" + "2" * 64,
        deployment_id=_DEPLOYMENT_ID,
        airflow_index_sha256="sha256:" + "3" * 64,
        activation_id=_ACTIVATION_ID,
        loaded_dag_ids=("DAG__platform__cache__smoke",),
        skipped_dag_ids=(),
        error_codes=(),
        fatal=False,
        acknowledged_at="2026-08-03T00:00:00+00:00",
    )
    first_root = tmp_path / "first"
    second_root = tmp_path / "second"
    first = DeploymentCacheActivationHistory(
        first_root,
        clock=lambda: datetime(2030, 1, 1, tzinfo=UTC),
    )
    second = DeploymentCacheActivationHistory(
        second_root,
        clock=lambda: datetime(2040, 1, 1, tzinfo=UTC),
    )

    prospective = first.prospective_revision(ack)

    assert prospective == second.prospective_revision(ack)
    assert not (first_root / ".retention-activation-history.v2.json").exists()
    assert first.upsert_expected(ack, expected_revision=prospective) == prospective
    assert first.revision_for(ack) == prospective


@pytest.mark.parametrize(
    ("payload", "kind"),
    (
        pytest.param(
            {
                "schema": RETENTION_RECOVERY_SCHEMA_V1,
                "status": "recovering",
                "restored_deployment_ids": [],
                "pending_deployment_ids": ["sha256:" + "4" * 64],
                "transactions": {_DEPLOYMENT_ID: _transaction(phase="prepared")},
            },
            RETENTION_RECOVERY_SCHEMA_V1,
            id="v1",
        ),
        pytest.param(
            {
                "schema": "dpone.deployment-cache-retention-recovery.v2",
                "status": "recovering",
                "restored_deployment_ids": [],
                "pending_deployment_ids": ["sha256:" + "4" * 64],
                "transactions": {
                    str(_transaction_v2(phase="prepared")["transaction_id"]): _transaction_v2(phase="prepared")
                },
            },
            "dpone.deployment-cache-retention-recovery.v2",
            id="v2",
        ),
    ),
)
def test_recovery_semantic_dispatch_rejects_projection_drift(
    payload: dict[str, object],
    kind: str,
) -> None:
    candidate = {**payload, "revision": canonical_digest(payload)}

    issues = GitOpsSchemaValidator().validate(candidate, expected_kind=kind)

    assert [(issue.code, issue.path) for issue in issues] == [("schema_state_semantics_invalid", "$")]


def test_recovery_v1_semantic_dispatch_remains_compatible() -> None:
    body = {
        "schema": RETENTION_RECOVERY_SCHEMA_V1,
        "status": "recovered",
        "restored_deployment_ids": [],
        "pending_deployment_ids": [],
        "transactions": {_DEPLOYMENT_ID: _transaction(phase="committed")},
    }
    legacy = {**body, "revision": canonical_digest(body)}

    assert (
        GitOpsSchemaValidator().validate(
            legacy,
            expected_kind=deployment_cache_retention_recovery_contract().kind,
        )
        == ()
    )
