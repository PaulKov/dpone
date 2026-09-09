from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.contracts.deployment_cache_retention_state import canonical_digest
from dpone.contracts.deployment_cache_retention_transaction import bind_retention_transaction
from dpone.runtime.deployment_cache_retention_contracts import (
    DeploymentCacheRetentionApplyError,
    RetentionTransactionOccurrence,
)
from dpone.runtime.deployment_cache_retention_journal import DeploymentCacheRetentionJournal
from tests.test_airflow_cache_materializer import _deployment_id, _retention_authorization, _write_deployment
from tests.test_airflow_cache_retention_receipt_hardening import (
    _authorized_operation_id,
    _prepared_retention,
)

_SECOND_REVIEW_ID = "00000000-0000-4000-8000-000000000002"


class _AbruptDeath(BaseException):
    pass


@pytest.mark.parametrize(
    ("phase", "restored"),
    (
        ("prepared", True),
        ("detached", True),
        ("deletion_started", True),
        ("deployment_deleted", False),
        ("activation_deleted", False),
        ("committed", False),
    ),
)
def test_every_wal_boundary_has_a_restart_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    restored: bool,
) -> None:
    cache_root, stale, current, authorization, _, applier = _prepared_retention(tmp_path)
    journal = applier._transactions._journal
    original_commit = journal.commit
    original_advance = journal.advance

    if phase == "prepared":

        def crash_after_prepare(transaction: dict[str, object]) -> None:
            original_commit(transaction)
            if transaction["phase"] == "prepared":
                raise _AbruptDeath(phase)

        monkeypatch.setattr(journal, "commit", crash_after_prepare)
    else:

        def crash_after_advance(transaction: dict[str, object], next_phase: str) -> None:
            original_advance(transaction, next_phase)
            if next_phase == phase:
                raise _AbruptDeath(phase)

        monkeypatch.setattr(journal, "advance", crash_after_advance)

    with pytest.raises(_AbruptDeath):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    monkeypatch.setattr(journal, "commit", original_commit)
    monkeypatch.setattr(journal, "advance", original_advance)

    if restored:
        with pytest.raises(DeploymentCacheRetentionApplyError) as recovery:
            applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
        assert recovery.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"
        assert stale.exists() and current.exists()
    else:
        report = applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
        assert report.transaction_status == "committed"
        assert not stale.exists() and current.exists()


def test_same_operation_does_not_claim_a_rematerialized_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    stale_id = _deployment_id(stale)
    original_mark = receipts.mark_deleted

    def crash_before_receipt_progress(_receipt: dict[str, object], _deployment_id: str) -> None:
        raise SystemExit("simulated death after WAL commit")

    monkeypatch.setattr(receipts, "mark_deleted", crash_before_receipt_progress)
    with pytest.raises(SystemExit):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    replacement = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    assert _deployment_id(replacement) == stale_id
    monkeypatch.setattr(receipts, "mark_deleted", original_mark)

    with pytest.raises(DeploymentCacheRetentionApplyError) as replay:
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)

    assert replay.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert replay.value.details["replacement_deployment_ids"] == [stale_id]
    assert replay.value.details["transaction_status"] == "aborted"
    assert replacement.exists() and current.exists()
    forensic = tuple(applier._transactions._journal.read_transactions().values())
    assert any(item["deployment_id"] == stale_id and item["phase"] == "committed" for item in forensic)

    fresh = _retention_authorization(cache_root)
    fresh["review_id"] = _SECOND_REVIEW_ID
    report = applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **fresh)
    assert report.transaction_status == "committed"
    assert report.deleted_deployment_ids == (stale_id,)
    assert not replacement.exists()


def test_replay_rejects_rematerialization_after_receipt_progress_was_durable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root, first, current, _, receipts, applier = _prepared_retention(tmp_path)
    second = _write_deployment(cache_root, "sha256:" + "c" * 64, complete=True)
    authorization = _retention_authorization(cache_root)
    first_id = _deployment_id(first)
    original_mark = receipts.mark_deleted
    marked = 0

    def crash_after_first_progress(receipt: dict[str, object], deployment_id: str):
        nonlocal marked
        result = original_mark(receipt, deployment_id)
        marked += 1
        if marked == 1:
            raise _AbruptDeath("receipt progress is durable")
        return result

    monkeypatch.setattr(receipts, "mark_deleted", crash_after_first_progress)
    with pytest.raises(_AbruptDeath):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    monkeypatch.setattr(receipts, "mark_deleted", original_mark)
    replacement = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)

    with pytest.raises(DeploymentCacheRetentionApplyError) as replay:
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)

    assert replay.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert replay.value.details["replacement_deployment_ids"] == [first_id]
    assert replay.value.details["transaction_status"] == "aborted"
    assert replacement.exists() and second.exists() and current.exists()


def test_replay_treats_a_broken_symlink_as_a_reappeared_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    stale_id = _deployment_id(stale)
    original_mark = receipts.mark_deleted

    def crash_before_receipt_progress(_receipt: dict[str, object], _deployment_id: str) -> None:
        raise _AbruptDeath("filesystem commit is durable")

    monkeypatch.setattr(receipts, "mark_deleted", crash_before_receipt_progress)
    with pytest.raises(_AbruptDeath):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    monkeypatch.setattr(receipts, "mark_deleted", original_mark)
    stale.symlink_to(cache_root / "missing-replacement-target")
    assert stale.is_symlink() and not stale.exists()

    with pytest.raises(DeploymentCacheRetentionApplyError) as replay:
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)

    assert replay.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert replay.value.details["replacement_deployment_ids"] == [stale_id]
    assert stale.is_symlink() and current.exists()


def test_replay_rejects_wal_path_mismatch_and_preserves_terminal_wal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    original_mark = receipts.mark_deleted

    def crash_before_receipt_progress(_receipt: dict[str, object], _deployment_id: str) -> None:
        raise _AbruptDeath("filesystem commit is durable")

    monkeypatch.setattr(receipts, "mark_deleted", crash_before_receipt_progress)
    with pytest.raises(_AbruptDeath):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    monkeypatch.setattr(receipts, "mark_deleted", original_mark)
    occurrences = applier._transactions.committed_occurrences(
        environment="dev",
        operation_id=str(_authorized_operation_id(authorization)),
    )
    assert len(occurrences) == 1
    observed = occurrences[0]
    altered = RetentionTransactionOccurrence(
        transaction_id=observed.transaction_id,
        deployment_id=observed.deployment_id,
        operation_id=observed.operation_id,
        original_path=f"{observed.original_path}.alias",
    )
    monkeypatch.setattr(
        applier._transactions,
        "committed_occurrences",
        lambda **_kwargs: (altered,),
    )

    with pytest.raises(DeploymentCacheRetentionApplyError) as drift:
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)

    assert drift.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert drift.value.details["occurrence_path_mismatch_count"] == 1
    assert drift.value.details["transaction_status"] == "aborted"
    assert not stale.exists() and current.exists()
    assert applier._transactions._journal.read_transactions()


def test_replay_rejects_duplicate_terminal_wal_occurrences(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)

    def crash_before_receipt_progress(_receipt: dict[str, object], _deployment_id: str) -> None:
        raise _AbruptDeath("filesystem commit is durable")

    monkeypatch.setattr(receipts, "mark_deleted", crash_before_receipt_progress)
    with pytest.raises(_AbruptDeath):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    occurrences = applier._transactions.committed_occurrences(
        environment="dev",
        operation_id=str(_authorized_operation_id(authorization)),
    )
    observed = occurrences[0]
    duplicate = RetentionTransactionOccurrence(
        transaction_id="sha256:" + "f" * 64,
        deployment_id=observed.deployment_id,
        operation_id=observed.operation_id,
        original_path=observed.original_path,
    )
    monkeypatch.setattr(
        applier._transactions,
        "committed_occurrences",
        lambda **_kwargs: (observed, duplicate),
    )

    with pytest.raises(DeploymentCacheRetentionApplyError) as drift:
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)

    assert drift.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert drift.value.details["occurrence_path_mismatch_count"] == 1
    assert drift.value.details["transaction_status"] == "aborted"
    assert not stale.exists() and current.exists()


def test_applying_deleted_receipt_without_terminal_wal_aborts_instead_of_committing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    operation_id = _authorized_operation_id(authorization)
    original_commit = receipts.commit

    def crash_before_receipt_commit(_receipt: dict[str, object]) -> None:
        raise _AbruptDeath("receipt item is durable but receipt commit is not")

    monkeypatch.setattr(receipts, "commit", crash_before_receipt_commit)
    with pytest.raises(_AbruptDeath):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    receipt = receipts.load(operation_id)
    assert receipt is not None and receipt["status"] == "applying"
    assert "deleted" in [item["action"] for item in receipt["items"]]
    applier._transactions._journal._path.unlink()
    monkeypatch.setattr(receipts, "commit", original_commit)

    with pytest.raises(DeploymentCacheRetentionApplyError) as replay:
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)

    assert replay.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert replay.value.details["occurrence_path_mismatch_count"] == 1
    assert replay.value.details["transaction_status"] == "aborted"
    persisted = receipts.load(operation_id)
    assert persisted is not None and persisted["status"] == "aborted"
    assert not stale.exists() and current.exists()


def test_occurrence_abort_write_failure_never_reports_false_aborted_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    operation_id = _authorized_operation_id(authorization)

    def crash_before_receipt_progress(_receipt: dict[str, object], _deployment_id: str) -> None:
        raise _AbruptDeath("filesystem commit is durable")

    monkeypatch.setattr(receipts, "mark_deleted", crash_before_receipt_progress)
    with pytest.raises(_AbruptDeath):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    replacement = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    store = receipts._store

    def fail_write(_payload: dict[str, object]) -> None:
        raise store._error("simulated durable abort failure")

    monkeypatch.setattr(store, "_write", fail_write)
    with pytest.raises(DeploymentCacheRetentionApplyError) as failed:
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)

    assert failed.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID"
    persisted = receipts.load(operation_id)
    assert persisted is not None and persisted["status"] == "applying"
    assert replacement.exists() and current.exists()
    assert applier._transactions._journal.read_transactions()


def test_new_review_id_releases_an_aborted_plan_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root, stale, current, authorization, _, applier = _prepared_retention(tmp_path)
    stale_id = _deployment_id(stale)

    def crash_before_transaction(*_args: object, **_kwargs: object) -> None:
        raise SystemExit("simulated death before WAL prepare")

    monkeypatch.setattr(applier._transactions, "delete", crash_before_transaction)
    with pytest.raises(SystemExit):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    monkeypatch.undo()

    with pytest.raises(DeploymentCacheRetentionApplyError) as aborted:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            protected_deployment_ids=(stale_id,),
            **authorization,
        )
    assert aborted.value.details["transaction_status"] == "aborted"

    fresh = _retention_authorization(cache_root)
    assert fresh["expected_plan_sha256"] == authorization["expected_plan_sha256"]
    fresh["review_id"] = _SECOND_REVIEW_ID
    report = applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **fresh)
    assert report.transaction_status == "committed"
    assert not stale.exists() and current.exists()


def test_recovery_ack_survives_an_unrelated_journal_revision(tmp_path: Path) -> None:
    cache_root = tmp_path / ".dpone-cache"
    cache_root.mkdir()
    journal = DeploymentCacheRetentionJournal(cache_root)
    restored = _transaction(cache_root, suffix="a", phase="restored")
    journal.commit(restored)
    occurrence = journal.unacknowledged_restored(environment="dev")
    assert tuple(item.transaction_id for item in occurrence) == (restored["transaction_id"],)
    journal.acknowledge_restored(environment="dev", transaction_ids=(str(restored["transaction_id"]),))
    assert journal.unacknowledged_restored(environment="dev") == ()

    unrelated = _transaction(cache_root, suffix="b", phase="committed")
    journal.commit(unrelated)

    assert journal.unacknowledged_restored(environment="dev") == ()
    acknowledgement = json.loads((cache_root / ".retention-recovery-ack.json").read_text(encoding="utf-8"))
    assert acknowledgement["schema"] == "dpone.deployment-cache-retention-recovery-ack.v2"
    assert acknowledgement["acknowledged_transaction_ids"] == [restored["transaction_id"]]


def test_legacy_revision_ack_is_upgraded_before_an_unrelated_write(tmp_path: Path) -> None:
    cache_root = tmp_path / ".dpone-cache"
    cache_root.mkdir()
    journal = DeploymentCacheRetentionJournal(cache_root)
    restored = _transaction(cache_root, suffix="a", phase="restored")
    journal.commit(restored)
    state = json.loads((cache_root / ".retention-recovery.json").read_text(encoding="utf-8"))
    body = {
        "schema": "dpone.deployment-cache-retention-recovery-ack.v1",
        "environment": "dev",
        "journal_revision": state["revision"],
        "restored_deployment_ids": state["restored_deployment_ids"],
    }
    (cache_root / ".retention-recovery-ack.json").write_text(
        json.dumps({**body, "revision": canonical_digest(body)}),
        encoding="utf-8",
    )

    journal.commit(_transaction(cache_root, suffix="b", phase="committed"))

    assert journal.unacknowledged_restored(environment="dev") == ()
    acknowledgement = json.loads((cache_root / ".retention-recovery-ack.json").read_text(encoding="utf-8"))
    assert acknowledgement["schema"] == "dpone.deployment-cache-retention-recovery-ack.v2"
    assert acknowledgement["acknowledged_transaction_ids"] == [restored["transaction_id"]]


def test_true_v1_wal_and_matching_v1_ack_upgrade_without_reopening_recovery(tmp_path: Path) -> None:
    cache_root = tmp_path / ".dpone-cache"
    cache_root.mkdir()
    journal = DeploymentCacheRetentionJournal(cache_root)
    restored = _transaction(cache_root, suffix="a", phase="restored")
    deployment_id = str(restored["deployment_id"])
    legacy_transaction = {
        key: value for key, value in restored.items() if key not in {"operation_id", "transaction_id"}
    }
    state_body = {
        "schema": "dpone.deployment-cache-retention-recovery.v1",
        "status": "recovered",
        "restored_deployment_ids": [deployment_id],
        "pending_deployment_ids": [],
        "transactions": {deployment_id: legacy_transaction},
    }
    state = {**state_body, "revision": canonical_digest(state_body)}
    (cache_root / ".retention-recovery.json").write_text(json.dumps(state), encoding="utf-8")
    ack_body = {
        "schema": "dpone.deployment-cache-retention-recovery-ack.v1",
        "environment": "dev",
        "journal_revision": state["revision"],
        "restored_deployment_ids": [deployment_id],
    }
    (cache_root / ".retention-recovery-ack.json").write_text(
        json.dumps({**ack_body, "revision": canonical_digest(ack_body)}),
        encoding="utf-8",
    )

    assert journal.unacknowledged_restored(environment="dev") == ()
    acknowledgement = json.loads((cache_root / ".retention-recovery-ack.json").read_text(encoding="utf-8"))
    migrated = next(iter(journal.read_transactions().values()))
    assert acknowledgement["schema"] == "dpone.deployment-cache-retention-recovery-ack.v2"
    assert acknowledgement["acknowledged_transaction_ids"] == [migrated["transaction_id"]]


def _transaction(cache_root: Path, *, suffix: str, phase: str) -> dict[str, object]:
    deployment_id = "sha256:" + suffix * 64
    return bind_retention_transaction(
        {
            "deployment_id": deployment_id,
            "environment": "dev",
            "phase": phase,
            "original_path": (cache_root / "deployments" / "dev" / deployment_id).as_posix(),
            "detached_path": (cache_root / ".retention-trash" / f"{deployment_id}.{suffix}").as_posix(),
            "activation_path": (cache_root / ".retention-activations" / "dev" / deployment_id).as_posix(),
            "expected_device": 1,
            "expected_inode": 1,
            "operation_id": "sha256:" + "f" * 64,
        }
    )
