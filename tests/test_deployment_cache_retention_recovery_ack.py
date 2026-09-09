from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from dpone.adapters.deployment_cache_retention_receipt_store import DeploymentCacheRetentionReceiptStore
from dpone.app.airflow_cache_retention_composition import build_deployment_cache_retention_applier
from dpone.contracts.deployment_cache_retention_receipt import retention_operation_id
from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError
from dpone.runtime.deployment_cache_retention_receipts import DeploymentCacheRetentionReceipts
from dpone.runtime.deployment_cache_retention_state_codec import (
    DeploymentCacheRetentionRecoveryAckError,
    build_retention_recovery_ack,
    parse_retention_recovery_ack,
)
from tests.test_airflow_cache_retention_receipt_hardening import _prepared_retention


def test_recovery_ack_is_closed_and_revision_bound() -> None:
    payload = build_retention_recovery_ack(
        environment="dev",
        acknowledged_transaction_ids=["sha256:" + "b" * 64],
    )

    assert parse_retention_recovery_ack(payload) == payload

    tampered = deepcopy(payload)
    tampered["acknowledged_transaction_ids"] = ["sha256:" + "c" * 64]
    with pytest.raises(DeploymentCacheRetentionRecoveryAckError):
        parse_retention_recovery_ack(tampered)


def test_recovery_ack_requires_sorted_unique_transaction_ids() -> None:
    transaction_id = "sha256:" + "b" * 64

    with pytest.raises(DeploymentCacheRetentionRecoveryAckError):
        build_retention_recovery_ack(
            environment="dev",
            acknowledged_transaction_ids=[transaction_id, transaction_id],
        )


def test_oversized_recovery_ack_is_rejected_before_json_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_common import DeploymentCacheError
    from dpone.runtime.deployment_cache_retention_journal import (
        MAX_RETENTION_RECOVERY_ACK_BYTES,
        DeploymentCacheRetentionJournal,
    )

    cache_root = tmp_path / ".dpone-cache"
    cache_root.mkdir()
    ack_path = cache_root / ".retention-recovery-ack.json"
    with ack_path.open("wb") as handle:
        handle.truncate(MAX_RETENTION_RECOVERY_ACK_BYTES + 1)

    def forbidden_parse(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("oversized acknowledgement reached json parsing")

    monkeypatch.setattr("json.loads", forbidden_parse)
    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheRetentionJournal(cache_root)._read_ack()

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_INVALID"


class _FatalCrash(BaseException):
    pass


def _restart_retention(cache_root: Path):
    receipts = DeploymentCacheRetentionReceipts(DeploymentCacheRetentionReceiptStore(cache_root))
    applier = build_deployment_cache_retention_applier(
        cache_root,
        allowed_promoters=("ci://retention",),
        receipts=receipts,
    )
    return receipts, applier


def _interrupt_after_detach(
    monkeypatch: pytest.MonkeyPatch,
    *,
    applier: object,
    authorization: dict[str, object],
) -> None:
    from dpone.runtime import deployment_cache_retention_transaction as transaction

    original_remove = transaction.remove_path

    def crash(_path: Path) -> None:
        raise _FatalCrash("simulated process death after detach")

    monkeypatch.setattr(transaction, "remove_path", crash)
    with pytest.raises(_FatalCrash):
        applier.apply(  # type: ignore[attr-defined]
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    monkeypatch.setattr(transaction, "remove_path", original_remove)


def test_restart_after_receipt_abort_before_recovery_ack_requires_fresh_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root, stale, current, authorization, _, applier = _prepared_retention(tmp_path)
    old_plan = str(authorization["expected_plan_sha256"])
    old_operation = retention_operation_id(
        environment="dev",
        reviewed_plan_sha256=old_plan,
        review_id=str(authorization["review_id"]),
    )
    _interrupt_after_detach(monkeypatch, applier=applier, authorization=authorization)

    original_ack = applier._transactions.acknowledge_recovery

    def crash_before_ack(**_kwargs: object) -> None:
        raise _FatalCrash("simulated process death before recovery acknowledgement")

    monkeypatch.setattr(applier._transactions, "acknowledge_recovery", crash_before_ack)
    with pytest.raises(_FatalCrash):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    monkeypatch.setattr(applier._transactions, "acknowledge_recovery", original_ack)

    receipts, restarted = _restart_retention(cache_root)
    with pytest.raises(DeploymentCacheRetentionApplyError) as recovered:
        restarted.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert recovered.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"
    receipt = receipts.load(old_operation)
    assert receipt is not None and receipt["status"] == "aborted"
    assert stale.exists() and current.exists()
    fresh_authorization = _prepared_authorization(cache_root)
    assert fresh_authorization["expected_plan_sha256"] != old_plan

    with pytest.raises(DeploymentCacheRetentionApplyError) as old_replay:
        restarted.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    assert old_replay.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"


def test_restart_after_recovery_ack_before_error_observation_blocks_old_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root, stale, current, authorization, _, applier = _prepared_retention(tmp_path)
    old_plan = str(authorization["expected_plan_sha256"])
    old_operation = retention_operation_id(
        environment="dev",
        reviewed_plan_sha256=old_plan,
        review_id=str(authorization["review_id"]),
    )
    _interrupt_after_detach(monkeypatch, applier=applier, authorization=authorization)

    original_ack = applier._transactions.acknowledge_recovery

    def crash_after_ack(**kwargs: object) -> None:
        original_ack(**kwargs)
        raise _FatalCrash("simulated process death after recovery acknowledgement")

    monkeypatch.setattr(applier._transactions, "acknowledge_recovery", crash_after_ack)
    with pytest.raises(_FatalCrash):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    receipts, restarted = _restart_retention(cache_root)
    receipt = receipts.load(old_operation)
    assert receipt is not None and receipt["status"] == "aborted"
    with pytest.raises(DeploymentCacheRetentionApplyError) as old_replay:
        restarted.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert old_replay.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"
    assert stale.exists() and current.exists()
    fresh_authorization = _prepared_authorization(cache_root)
    assert fresh_authorization["expected_plan_sha256"] != old_plan


def _prepared_authorization(cache_root: Path) -> dict[str, object]:
    from tests.test_airflow_cache_materializer import _retention_authorization

    return _retention_authorization(cache_root)
