from __future__ import annotations

import inspect
import json
import os
import shutil
import stat
from pathlib import Path

import pytest

from dpone.adapters.deployment_cache_retention_receipt_store import DeploymentCacheRetentionReceiptStore
from dpone.app.airflow_cache_retention_composition import build_deployment_cache_retention_applier
from dpone.contracts.deployment_cache_retention_receipt import (
    DeploymentCacheRetentionReceiptError,
    build_retention_apply_receipt,
    parse_retention_apply_receipt,
    retention_operation_id,
)
from dpone.contracts.posix_permissions import has_posix_access_mode
from dpone.runtime.deployment_cache_retention_receipts import DeploymentCacheRetentionReceipts
from tests.test_airflow_cache_materializer import (
    _deployment_id,
    _retention_authorization,
    _write_deployment,
)


def _prepared_retention(tmp_path: Path):
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    receipts = DeploymentCacheRetentionReceipts(DeploymentCacheRetentionReceiptStore(cache_root))
    applier = build_deployment_cache_retention_applier(
        cache_root,
        allowed_promoters=("ci://retention",),
        receipts=receipts,
    )
    return cache_root, stale, current, authorization, receipts, applier


def _authorized_operation_id(authorization: dict[str, object]) -> str:
    return retention_operation_id(
        environment="dev",
        reviewed_plan_sha256=str(authorization["expected_plan_sha256"]),
        review_id=str(authorization["review_id"]),
    )


def test_legacy_direct_retention_constructor_remains_backward_compatible(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import (
        DeploymentCacheRetentionApplier,
    )
    from dpone.runtime.deployment_cache import (
        build_deployment_cache_retention_applier as build_legacy,
    )

    cache_root = tmp_path / ".dpone-cache"
    applier = DeploymentCacheRetentionApplier(
        cache_root,
        allowed_promoters=("ci://retention",),
    )

    assert applier._cache_root == cache_root.resolve(strict=False)
    assert build_legacy(cache_root)._cache_root == cache_root.resolve(strict=False)


def test_legacy_retention_facade_preserves_explicit_apply_signature() -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheRetentionApplier

    signature = inspect.signature(DeploymentCacheRetentionApplier.apply)

    assert tuple(signature.parameters) == (
        "self",
        "environment",
        "confirm_delete",
        "promoted_by",
        "expected_plan_sha256",
        "review_id",
        "loader_ack_reader",
        "checkpoint_reader",
        "protected_deployment_ids",
    )
    assert all(parameter.kind is not inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values())
    assert "DeploymentRetentionApplyReport" in str(signature.return_annotation)
    assert "__getattr__" not in DeploymentCacheRetentionApplier.__dict__
    assert DeploymentCacheRetentionApplier.__module__ == "dpone.runtime.deployment_cache"


def test_public_retention_facades_freeze_optional_authorization_defaults() -> None:
    from dpone.readiness.airflow_cache_retention import AirflowCacheRetentionService
    from dpone.readiness.airflow_self_service_application import AirflowSelfServiceService
    from dpone.readiness.airflow_self_service_cache import cache_retention_apply_result

    for public_callable in (
        AirflowCacheRetentionService.apply,
        cache_retention_apply_result,
        AirflowSelfServiceService.cache_retention_apply,
    ):
        parameters = inspect.signature(public_callable).parameters
        assert parameters["expected_plan_sha256"].default is None
        assert parameters["loader_ack_file"].default is None


def test_canonical_retention_module_does_not_import_compatibility_namespace() -> None:
    source = Path("src/dpone/runtime/deployment_cache.py").read_text(encoding="utf-8")

    assert "dpone.compat" not in source
    assert ".__module__" not in source


def test_cache_retention_default_wiring_has_one_owner() -> None:
    composition = Path("src/dpone/app/airflow_cache_retention_composition.py").read_text(encoding="utf-8")
    barrel = Path("src/dpone/runtime/deployment_cache.py").read_text(encoding="utf-8")
    factory = Path("src/dpone/runtime/deployment_cache_retention_factory.py").read_text(encoding="utf-8")
    compatibility = Path("src/dpone/runtime/deployment_cache_retention_compatibility.py").read_text(encoding="utf-8")

    assert "deployment_cache_retention_factory" in composition
    assert "DeploymentCacheRetentionReceiptStore" not in barrel
    assert "DeploymentCacheRetentionReceiptStore" not in factory
    assert composition.count("DeploymentCacheRetentionReceiptStore") == 2
    assert compatibility.count("DeploymentCacheRetentionReceiptStore") == 2
    assert "dpone.app" not in factory
    assert "dpone.adapters" not in factory
    assert "import_module" not in factory
    assert "from dpone.runtime.deployment_cache import" not in factory
    assert "assemble_deployment_cache_retention_applier(" in composition
    assert "build_legacy_deployment_cache_retention_applier" in barrel
    assert "build_deployment_cache_retention_applier" in barrel


def test_authority_drift_aborts_receipt_and_allows_fresh_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    stale_id = _deployment_id(stale)
    old_plan = str(authorization["expected_plan_sha256"])
    old_operation = _authorized_operation_id(authorization)
    original = receipts.mark_deleted

    def interrupt_after_filesystem_commit(_receipt: dict[str, object], _deployment_id: str) -> None:
        raise SystemExit("simulated process death before receipt progress")

    monkeypatch.setattr(receipts, "mark_deleted", interrupt_after_filesystem_commit)
    with pytest.raises(SystemExit):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    monkeypatch.setattr(receipts, "mark_deleted", original)
    assert not stale.exists()

    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    current_authority = _retention_authorization(cache_root)
    with pytest.raises(DeploymentCacheRetentionApplyError) as drift:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            expected_plan_sha256=old_plan,
            review_id=str(authorization["review_id"]),
            loader_ack_reader=current_authority["loader_ack_reader"],
            checkpoint_reader=current_authority["checkpoint_reader"],
        )

    assert drift.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert drift.value.details["state_may_have_changed"] is True
    assert drift.value.details["transaction_status"] == "aborted"
    receipt = receipts.load(old_operation)
    assert receipt is not None and receipt["status"] == "aborted"
    assert next(item for item in receipt["items"] if item["deployment_id"] == stale_id)["action"] == "deleted"

    replacement = _write_deployment(cache_root, "sha256:" + "c" * 64, complete=True)
    fresh = _retention_authorization(cache_root)
    report = applier.apply(
        environment="dev",
        confirm_delete=True,
        promoted_by="ci://retention",
        **fresh,
    )
    assert report.transaction_status == "committed"
    assert not replacement.exists()


def test_reviewed_plan_that_becomes_empty_fails_before_noop_success(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    _cache_root, stale, current, authorization, _receipts, applier = _prepared_retention(tmp_path)
    shutil.rmtree(stale)

    with pytest.raises(DeploymentCacheRetentionApplyError) as drift:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert drift.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert set(drift.value.details) == {"expected_plan_sha256", "actual_plan_sha256"}
    assert drift.value.details["expected_plan_sha256"] == authorization["expected_plan_sha256"]
    assert drift.value.details["actual_plan_sha256"] != authorization["expected_plan_sha256"]
    assert current.exists()


def test_terminal_wal_is_retained_until_the_operation_receipt_commits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root, first, _current, _, receipts, applier = _prepared_retention(tmp_path)
    second = _write_deployment(cache_root, "sha256:" + "c" * 64, complete=True)
    first_id = _deployment_id(first)
    second_id = _deployment_id(second)
    authorization = _retention_authorization(cache_root)
    journal = applier._transactions._journal
    original = journal.commit
    prepared_count = 0

    class FatalCrash(BaseException):
        pass

    def interrupt_second_prepare(transaction: dict[str, object]) -> None:
        nonlocal prepared_count
        original(transaction)
        if transaction["phase"] == "prepared":
            prepared_count += 1
            if prepared_count == 2:
                raise FatalCrash("simulated process death after second WAL prepare")

    monkeypatch.setattr(journal, "commit", interrupt_second_prepare)
    with pytest.raises(FatalCrash):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    transactions = journal.read_transactions().values()
    phases = {str(item["deployment_id"]): str(item["phase"]) for item in transactions}
    assert phases[first_id] == "committed"
    assert phases[second_id] == "prepared"
    operation = _authorized_operation_id(authorization)
    receipt = receipts.load(operation)
    assert receipt is not None and receipt["status"] == "applying"


def test_foreign_incomplete_operation_blocks_new_plan_without_pruning_replay_wal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError
    from dpone.runtime.deployment_cache_retention_planner import DeploymentCacheRetentionPlanner

    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    original = receipts.mark_deleted

    def interrupt_after_filesystem_commit(_receipt: dict[str, object], _deployment_id: str) -> None:
        raise SystemExit("simulated process death before receipt progress")

    monkeypatch.setattr(receipts, "mark_deleted", interrupt_after_filesystem_commit)
    with pytest.raises(SystemExit):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    assert not stale.exists()

    unexpected = _write_deployment(cache_root, "sha256:" + "c" * 64, complete=True)
    second_plan = DeploymentCacheRetentionPlanner(cache_root).plan(environment="dev")
    with pytest.raises(DeploymentCacheRetentionApplyError) as exc_info:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            expected_plan_sha256=second_plan.plan_sha256,
            loader_ack_reader=authorization["loader_ack_reader"],
            checkpoint_reader=authorization["checkpoint_reader"],
        )

    assert exc_info.value.code == "DPONE_DEPLOYMENT_CACHE_GC_INCOMPLETE_OPERATION"
    assert exc_info.value.details["incomplete_operation_ids"]

    monkeypatch.setattr(receipts, "mark_deleted", original)
    with pytest.raises(DeploymentCacheRetentionApplyError) as drift:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    assert drift.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert drift.value.details["unexpected_candidate_ids"] == [_deployment_id(unexpected)]
    assert drift.value.details["transaction_status"] == "aborted"
    fresh = _retention_authorization(cache_root)
    replay = applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **fresh)
    assert replay.transaction_status == "committed"
    assert not unexpected.exists()
    assert current.exists()


def test_retention_operation_identity_is_stable_and_receipt_revision_is_closed() -> None:
    plan_sha256 = "sha256:" + "a" * 64
    operation_id = retention_operation_id(environment="dev", reviewed_plan_sha256=plan_sha256)
    assert operation_id == retention_operation_id(environment="dev", reviewed_plan_sha256=plan_sha256)
    assert operation_id != retention_operation_id(environment="prod", reviewed_plan_sha256=plan_sha256)
    receipt = build_retention_apply_receipt(
        operation_id=operation_id,
        status="committed",
        environment="dev",
        promoted_by="ci://retention",
        current_deployment_id="sha256:" + "b" * 64,
        reviewed_plan_sha256=plan_sha256,
        activation_history_revision="sha256:" + "c" * 64,
        items=[
            {
                "deployment_id": "sha256:" + "d" * 64,
                "action": "deleted",
                "reason": "unreferenced",
                "path": ".dpone-cache/deployments/dev/sha256-d",
            }
        ],
    )
    receipt["promoted_by"] = "ci://tampered"

    with pytest.raises(DeploymentCacheRetentionReceiptError):
        parse_retention_apply_receipt(receipt)


def test_retention_receipt_rejects_duplicate_destructive_deployment_ids() -> None:
    plan_sha256 = "sha256:" + "a" * 64
    operation_id = retention_operation_id(environment="dev", reviewed_plan_sha256=plan_sha256)
    item = {
        "deployment_id": "sha256:" + "d" * 64,
        "action": "deleted",
        "reason": "unreferenced",
        "path": "/cache/deployments/dev/sha256-d",
    }

    with pytest.raises(DeploymentCacheRetentionReceiptError):
        build_retention_apply_receipt(
            operation_id=operation_id,
            status="committed",
            environment="dev",
            promoted_by="ci://retention",
            current_deployment_id="sha256:" + "b" * 64,
            reviewed_plan_sha256=plan_sha256,
            activation_history_revision="sha256:" + "c" * 64,
            items=[item, item],
        )


def test_invalid_review_id_is_a_stable_apply_error(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    cache_root, stale, _, authorization, _, applier = _prepared_retention(tmp_path)

    with pytest.raises(DeploymentCacheRetentionApplyError) as invalid:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            expected_plan_sha256=str(authorization["expected_plan_sha256"]),
            review_id="not-a-uuid",
            loader_ack_reader=authorization["loader_ack_reader"],
        )

    assert invalid.value.code == "DPONE_DEPLOYMENT_CACHE_GC_REVIEW_ID_INVALID"
    assert invalid.value.details == {"state_may_have_changed": False}
    assert stale.exists()


def test_missing_review_id_blocks_before_mutation(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    _, stale, _, authorization, _, applier = _prepared_retention(tmp_path)

    with pytest.raises(DeploymentCacheRetentionApplyError) as missing:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            expected_plan_sha256=str(authorization["expected_plan_sha256"]),
            loader_ack_reader=authorization["loader_ack_reader"],
            checkpoint_reader=authorization["checkpoint_reader"],
        )

    assert missing.value.code == "DPONE_DEPLOYMENT_CACHE_GC_REVIEW_ID_REQUIRED"
    assert missing.value.details == {"state_may_have_changed": False}
    assert stale.exists()


def test_recovery_aborts_old_receipt_and_requires_new_reviewed_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_retention_transaction as transaction
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    old_plan_sha256 = str(authorization["expected_plan_sha256"])
    old_operation_id = _authorized_operation_id(authorization)

    class FatalCrash(BaseException):
        pass

    original_remove = transaction.remove_path

    def crash_after_detach(_path: Path) -> None:
        raise FatalCrash("simulated process death after detach")

    monkeypatch.setattr(transaction, "remove_path", crash_after_detach)
    with pytest.raises(FatalCrash):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    monkeypatch.setattr(transaction, "remove_path", original_remove)

    with pytest.raises(DeploymentCacheRetentionApplyError) as recovered:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert recovered.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"
    assert recovered.value.details["aborted_operation_ids"] == [old_operation_id]
    assert stale.exists()
    old_receipt = receipts.load(old_operation_id)
    assert old_receipt is not None
    assert old_receipt["status"] == "aborted"
    assert not any(item["action"] == "pending" for item in old_receipt["items"])

    fresh_authorization = _retention_authorization(cache_root)
    assert fresh_authorization["expected_plan_sha256"] != old_plan_sha256
    report = applier.apply(
        environment="dev",
        confirm_delete=True,
        promoted_by="ci://retention",
        **fresh_authorization,
    )

    assert report.transaction_status == "committed"
    assert not stale.exists()
    assert current.exists()


def test_inline_oserror_recovery_aborts_receipt_before_old_plan_can_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_retention_transaction as transaction
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    old_plan_sha256 = str(authorization["expected_plan_sha256"])
    old_operation_id = _authorized_operation_id(authorization)
    original_remove = transaction.remove_path

    def fail_after_detach(_path: Path) -> None:
        raise OSError("simulated recursive delete failure")

    monkeypatch.setattr(transaction, "remove_path", fail_after_detach)
    with pytest.raises(DeploymentCacheRetentionApplyError) as failed:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    assert failed.value.code == "DPONE_DEPLOYMENT_CACHE_GC_DELETE_FAILED"
    assert failed.value.details["restored"] is True
    assert stale.exists()
    monkeypatch.setattr(transaction, "remove_path", original_remove)

    with pytest.raises(DeploymentCacheRetentionApplyError) as recovered:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert recovered.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"
    assert recovered.value.details["aborted_operation_ids"] == [old_operation_id]
    receipt = receipts.load(old_operation_id)
    assert receipt is not None and receipt["status"] == "aborted"
    assert stale.exists()

    with pytest.raises(DeploymentCacheRetentionApplyError) as old_replay:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    assert old_replay.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"

    fresh_authorization = _retention_authorization(cache_root)
    assert fresh_authorization["expected_plan_sha256"] != old_plan_sha256
    report = applier.apply(
        environment="dev",
        confirm_delete=True,
        promoted_by="ci://retention",
        **fresh_authorization,
    )

    assert report.transaction_status == "committed"
    assert not stale.exists()
    assert current.exists()


def test_retention_replays_commit_when_process_dies_before_receipt_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    stale_id = _deployment_id(stale)
    original = receipts.mark_deleted
    attempts = 0

    def interrupt_once(receipt: dict[str, object], deployment_id: str):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise SystemExit("simulated process death after filesystem commit")
        return original(receipt, deployment_id)

    monkeypatch.setattr(receipts, "mark_deleted", interrupt_once)

    with pytest.raises(SystemExit):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert not stale.exists()
    report = applier.apply(
        environment="dev",
        confirm_delete=True,
        promoted_by="ci://retention",
        **authorization,
    )

    expected_operation_id = _authorized_operation_id(authorization)
    assert report.operation_id == expected_operation_id
    assert report.deleted_deployment_ids == (stale_id,)
    assert report.transaction_status == "committed"
    assert current.exists()
    assert not tuple((cache_root / ".retention-trash").iterdir())


def test_retention_replay_aborts_when_cache_root_occurrence_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    old_root, stale, _, authorization, receipts, applier = _prepared_retention(tmp_path / "old")
    stale_id = _deployment_id(stale)
    operation_id = _authorized_operation_id(authorization)

    def interrupt_after_filesystem_commit(_receipt: dict[str, object], _deployment_id: str) -> None:
        raise SystemExit("simulated process death before receipt progress")

    monkeypatch.setattr(receipts, "mark_deleted", interrupt_after_filesystem_commit)
    with pytest.raises(SystemExit):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    assert not stale.exists()

    new_root = tmp_path / "new" / ".dpone-cache"
    shutil.copytree(old_root, new_root, symlinks=True)
    replacement = _write_deployment(new_root, stale_id, complete=True)
    new_authority = _retention_authorization(new_root)
    restarted = build_deployment_cache_retention_applier(
        new_root,
        allowed_promoters=("ci://retention",),
    )

    with pytest.raises(DeploymentCacheRetentionApplyError) as drift:
        restarted.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            expected_plan_sha256=str(authorization["expected_plan_sha256"]),
            review_id=str(authorization["review_id"]),
            loader_ack_reader=new_authority["loader_ack_reader"],
            checkpoint_reader=new_authority["checkpoint_reader"],
        )

    assert drift.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert drift.value.details["transaction_status"] == "aborted"
    assert drift.value.details["occurrence_path_mismatch"] is True
    assert replacement.exists()
    copied_receipts = DeploymentCacheRetentionReceipts(DeploymentCacheRetentionReceiptStore(new_root))
    copied = copied_receipts.load(operation_id)
    assert copied is not None and copied["status"] == "aborted"


def test_committed_receipt_and_wal_abort_after_cache_root_relocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    old_root, stale, _, authorization, _, applier = _prepared_retention(tmp_path / "old")
    operation_id = _authorized_operation_id(authorization)

    def interrupt_before_wal_prune(*, operation_id: str, deployment_ids: tuple[str, ...]) -> None:
        raise SystemExit(f"simulated death before WAL prune: {operation_id} {deployment_ids!r}")

    monkeypatch.setattr(
        applier._transactions,
        "acknowledge_committed_receipt",
        interrupt_before_wal_prune,
    )
    with pytest.raises(SystemExit):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    assert not stale.exists()

    new_root = tmp_path / "new" / ".dpone-cache"
    shutil.copytree(old_root, new_root, symlinks=True)
    new_authority = _retention_authorization(new_root)
    restarted = build_deployment_cache_retention_applier(
        new_root,
        allowed_promoters=("ci://retention",),
    )

    with pytest.raises(DeploymentCacheRetentionApplyError) as drift:
        restarted.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            expected_plan_sha256=str(authorization["expected_plan_sha256"]),
            review_id=str(authorization["review_id"]),
            loader_ack_reader=new_authority["loader_ack_reader"],
            checkpoint_reader=new_authority["checkpoint_reader"],
        )

    assert drift.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert drift.value.details["transaction_status"] == "aborted"
    assert drift.value.details["occurrence_path_mismatch"] is True
    assert not (new_root / "deployments" / "dev" / stale.name).exists()
    copied_receipts = DeploymentCacheRetentionReceipts(DeploymentCacheRetentionReceiptStore(new_root))
    copied = copied_receipts.load(operation_id)
    assert copied is not None and copied["status"] == "aborted"
    assert any(item["phase"] == "committed" for item in restarted._transactions._journal.read_transactions().values())


def test_legacy_applying_receipt_cannot_authorize_pending_deletion(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    plan_sha256 = str(authorization["expected_plan_sha256"])
    operation_id = retention_operation_id(environment="dev", reviewed_plan_sha256=plan_sha256)
    legacy = build_retention_apply_receipt(
        operation_id=operation_id,
        status="applying",
        environment="dev",
        promoted_by="ci://retention",
        current_deployment_id=_deployment_id(current),
        reviewed_plan_sha256=plan_sha256,
        activation_history_revision="sha256:" + "c" * 64,
        items=[
            {
                "deployment_id": _deployment_id(stale),
                "action": "pending",
                "reason": "unreferenced",
                "path": stale.as_posix(),
            }
        ],
    )
    receipt_root = cache_root / ".retention-apply-receipts"
    receipt_root.mkdir(mode=0o700)
    receipt_path = receipt_root / f"{operation_id.replace(':', '-', 1)}.json"
    receipt_path.write_text(json.dumps(legacy), encoding="utf-8")
    receipt_path.chmod(0o600)

    with pytest.raises(DeploymentCacheRetentionApplyError) as blocked:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            expected_plan_sha256=plan_sha256,
            loader_ack_reader=authorization["loader_ack_reader"],
            checkpoint_reader=authorization["checkpoint_reader"],
        )

    assert blocked.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"
    assert blocked.value.details["legacy_receipt_requires_fresh_review"] is True
    assert stale.exists() and current.exists()
    aborted = receipts.load(operation_id)
    assert aborted is not None and aborted["status"] == "aborted"
    assert aborted["items"][0]["action"] == "skipped"


def test_legacy_receipt_reconciles_only_exact_terminal_wal_then_requires_fresh_review(
    tmp_path: Path,
) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError
    from dpone.runtime.deployment_cache_retention_deletion import directory_identity

    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    stale_id = _deployment_id(stale)
    plan_sha256 = str(authorization["expected_plan_sha256"])
    operation_id = retention_operation_id(environment="dev", reviewed_plan_sha256=plan_sha256)
    legacy = build_retention_apply_receipt(
        operation_id=operation_id,
        status="applying",
        environment="dev",
        promoted_by="ci://retention",
        current_deployment_id=_deployment_id(current),
        reviewed_plan_sha256=plan_sha256,
        activation_history_revision="sha256:" + "c" * 64,
        items=[
            {
                "deployment_id": stale_id,
                "action": "pending",
                "reason": "unreferenced",
                "path": stale.as_posix(),
            }
        ],
    )
    receipt_root = cache_root / ".retention-apply-receipts"
    receipt_root.mkdir(mode=0o700)
    receipt_path = receipt_root / f"{operation_id.replace(':', '-', 1)}.json"
    receipt_path.write_text(json.dumps(legacy), encoding="utf-8")
    receipt_path.chmod(0o600)
    applier._transactions.delete(
        stale,
        expected_identity=directory_identity(stale),
        deployment_id=stale_id,
        environment="dev",
        operation_id=operation_id,
    )

    with pytest.raises(DeploymentCacheRetentionApplyError) as closed:
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)

    assert closed.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"
    aborted = receipts.load(operation_id)
    assert aborted is not None and aborted["status"] == "aborted"
    assert aborted["items"][0]["action"] == "deleted"
    assert not stale.exists() and current.exists()
    assert applier._transactions._journal.read_transactions()

    replacement = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    fresh = _retention_authorization(cache_root)
    report = applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **fresh)

    assert report.transaction_status == "committed"
    assert report.deleted_deployment_ids == (stale_id,)
    assert not replacement.exists() and current.exists()


def test_v3_projection_keeps_valid_noop_compatibility() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.runtime.deployment_cache_retention_contracts import (
        DeploymentRetentionApplyReport,
    )

    report = DeploymentRetentionApplyReport(
        environment="dev",
        promoted_by="ci://retention",
        current_deployment_id="sha256:" + "b" * 64,
        items=(),
    )

    payload = report.to_v3_dict()
    schema = json.loads(
        Path("docs/schemas/gitops/deployment-cache-retention-apply-v3.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(payload, schema)
    assert payload["schema"] == "dpone.deployment-cache-retention-apply.v3"
    assert payload["deleted_deployment_ids"] == []
    assert "operation_id" not in payload


def test_v3_projection_rejects_destructive_report_without_reviewed_receipt() -> None:
    from dpone.runtime.deployment_cache_retention_contracts import (
        DeploymentCacheRetentionApplyError,
        DeploymentRetentionApplyItem,
        DeploymentRetentionApplyReport,
    )

    deployment_id = "sha256:" + "a" * 64
    report = DeploymentRetentionApplyReport(
        environment="dev",
        promoted_by="ci://retention",
        current_deployment_id="sha256:" + "b" * 64,
        items=(
            DeploymentRetentionApplyItem(
                deployment_id=deployment_id,
                action="deleted",
                reason="unreferenced",
                path=f"/cache/deployments/dev/{deployment_id}",
            ),
        ),
    )

    with pytest.raises(DeploymentCacheRetentionApplyError) as unavailable:
        report.to_v3_dict()

    assert unavailable.value.code == "DPONE_DEPLOYMENT_CACHE_GC_V3_EVIDENCE_UNAVAILABLE"
    assert unavailable.value.details["missing_fields"] == [
        "reviewed_plan_sha256",
        "activation_history_revision",
        "operation_id",
        "review_id",
        "receipt_revision",
    ]


def test_legacy_committed_receipt_keeps_v1_v2_history_but_cannot_project_v3(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    plan_sha256 = "sha256:" + "a" * 64
    operation_id = retention_operation_id(environment="dev", reviewed_plan_sha256=plan_sha256)
    legacy = build_retention_apply_receipt(
        operation_id=operation_id,
        status="committed",
        environment="dev",
        promoted_by="ci://retention",
        current_deployment_id="sha256:" + "b" * 64,
        reviewed_plan_sha256=plan_sha256,
        activation_history_revision="sha256:" + "c" * 64,
        items=[
            {
                "deployment_id": "sha256:" + "d" * 64,
                "action": "deleted",
                "reason": "unreferenced",
                "path": "/cache/deployments/dev/sha256-d",
            }
        ],
    )
    receipts = DeploymentCacheRetentionReceipts(DeploymentCacheRetentionReceiptStore(tmp_path))
    report = receipts.report(legacy)

    assert report.to_dict()["schema"] == "dpone.deployment-cache-retention-apply.v1"
    assert report.to_v2_dict()["schema"] == "dpone.deployment-cache-retention-apply.v2"
    with pytest.raises(DeploymentCacheRetentionApplyError) as unavailable:
        report.to_v3_dict()
    assert unavailable.value.code == "DPONE_DEPLOYMENT_CACHE_GC_V3_EVIDENCE_UNAVAILABLE"


def test_receipt_begin_failure_leaves_activation_history_and_candidates_untouched(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)

    def fail_receipt_begin(**_kwargs: object) -> dict[str, object]:
        raise DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID",
            "simulated durable receipt failure",
        )

    monkeypatch.setattr(receipts, "begin", fail_receipt_begin)
    with pytest.raises(DeploymentCacheRetentionApplyError) as failed:
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)

    assert failed.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID"
    assert not (cache_root / ".retention-activation-history.v2.json").exists()
    assert stale.exists() and current.exists()


def test_retry_persists_receipt_bound_history_after_crash_before_history_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import AirflowLoaderAcknowledgement

    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    operation_id = _authorized_operation_id(authorization)
    original_persist = applier._history.persist
    attempts = 0

    def interrupt_once(
        ack: AirflowLoaderAcknowledgement,
        *,
        expected_revision: str,
        operation_id: str,
    ) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise SystemExit("simulated death after receipt begin")
        return original_persist(
            ack,
            expected_revision=expected_revision,
            operation_id=operation_id,
        )

    monkeypatch.setattr(applier._history, "persist", interrupt_once)
    with pytest.raises(SystemExit):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)

    receipt = receipts.load(operation_id)
    assert receipt is not None and receipt["status"] == "applying"
    assert not (cache_root / ".retention-activation-history.v2.json").exists()
    assert stale.exists() and current.exists()

    report = applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)

    assert report.transaction_status == "committed"
    assert report.activation_history_revision == receipt["activation_history_revision"]
    assert attempts == 2
    assert not stale.exists() and current.exists()


def test_concrete_receipt_store_operation_id_remains_compatible(tmp_path: Path) -> None:
    store = DeploymentCacheRetentionReceiptStore(tmp_path / ".dpone-cache")
    plan_sha256 = "sha256:" + "a" * 64
    review_id = "00000000-0000-4000-8000-000000000001"

    assert store.operation_id(
        environment="dev",
        reviewed_plan_sha256=plan_sha256,
        review_id=review_id,
    ) == retention_operation_id(
        environment="dev",
        reviewed_plan_sha256=plan_sha256,
        review_id=review_id,
    )


def test_retention_replays_receipt_when_process_dies_before_public_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    stale_id = _deployment_id(stale)
    original = receipts.report
    attempts = 0

    def interrupt_once(receipt: dict[str, object]):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise SystemExit("simulated process death after receipt commit")
        return original(receipt)

    monkeypatch.setattr(receipts, "report", interrupt_once)

    with pytest.raises(SystemExit):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    report = applier.apply(
        environment="dev",
        confirm_delete=True,
        promoted_by="ci://retention",
        **authorization,
    )
    assert report.deleted_deployment_ids == (stale_id,)
    assert report.receipt_revision is not None
    assert report.transaction_status == "committed"
    assert current.exists()


def test_committed_wal_from_an_old_operation_aborts_before_rematerialized_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root, stale, current, authorization, _, applier = _prepared_retention(tmp_path)
    stale_id = _deployment_id(stale)
    original_acknowledge = applier._transactions.acknowledge_committed_receipt

    def interrupt_before_wal_prune(*, operation_id: str, deployment_ids: tuple[str, ...]) -> None:
        raise SystemExit(f"simulated death before WAL prune: {operation_id} {deployment_ids!r}")

    monkeypatch.setattr(
        applier._transactions,
        "acknowledge_committed_receipt",
        interrupt_before_wal_prune,
    )
    with pytest.raises(SystemExit):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    assert not stale.exists()

    rematerialized = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    assert _deployment_id(rematerialized) == stale_id
    monkeypatch.setattr(
        applier._transactions,
        "acknowledge_committed_receipt",
        original_acknowledge,
    )

    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    with pytest.raises(DeploymentCacheRetentionApplyError) as drift:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert drift.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert drift.value.details["transaction_status"] == "aborted"
    assert drift.value.details["replacement_deployment_ids"] == [stale_id]
    assert rematerialized.exists()
    assert current.exists()
    assert any(
        item["deployment_id"] == stale_id and item["phase"] == "committed"
        for item in applier._transactions._journal.read_transactions().values()
    )


def test_retention_replay_blocks_candidate_that_gained_protection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import (
        DeploymentCacheRetentionApplyError,
    )

    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    stale_id = _deployment_id(stale)
    old_operation_id = _authorized_operation_id(authorization)

    def interrupt_before_delete(*_args: object, **_kwargs: object) -> object:
        raise SystemExit("simulated process death before delete")

    monkeypatch.setattr(applier._transactions, "delete", interrupt_before_delete)
    with pytest.raises(SystemExit):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    monkeypatch.undo()

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            protected_deployment_ids=(stale_id,),
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert exc.value.details["transaction_status"] == "aborted"
    old_receipt = receipts.load(old_operation_id)
    assert old_receipt is not None and old_receipt["status"] == "aborted"
    assert stale.exists()
    assert current.exists()

    from dpone.runtime.deployment_cache_retention_planner import DeploymentCacheRetentionPlanner

    fresh_plan = DeploymentCacheRetentionPlanner(cache_root).plan(
        environment="dev",
        protected_deployment_ids=(stale_id,),
    )
    report = applier.apply(
        environment="dev",
        confirm_delete=True,
        promoted_by="ci://retention",
        expected_plan_sha256=fresh_plan.plan_sha256,
        protected_deployment_ids=(stale_id,),
    )

    assert report.deleted_deployment_ids == ()
    assert stale.exists()
    assert current.exists()


def test_retention_replay_blocks_protection_drift_when_candidates_are_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    operation_id = _authorized_operation_id(authorization)

    def interrupt_before_delete(*_args: object, **_kwargs: object) -> object:
        raise SystemExit("simulated process death before delete")

    monkeypatch.setattr(applier._transactions, "delete", interrupt_before_delete)
    with pytest.raises(SystemExit):
        applier.apply(environment="dev", confirm_delete=True, promoted_by="ci://retention", **authorization)
    monkeypatch.undo()

    unrelated_protection = "sha256:" + "f" * 64
    with pytest.raises(DeploymentCacheRetentionApplyError) as drift:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            protected_deployment_ids=(unrelated_protection,),
            **authorization,
        )

    assert drift.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert drift.value.details["protected_set_changed"] is True
    assert drift.value.details["no_longer_deletable_ids"] == []
    assert drift.value.details["unexpected_candidate_ids"] == []
    receipt = receipts.load(operation_id)
    assert receipt is not None and receipt["status"] == "aborted"
    assert stale.exists() and current.exists()


def test_retention_blocks_corrupt_existing_receipt_without_mutation(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import (
        DeploymentCacheRetentionApplyError,
    )

    cache_root, stale, current, authorization, _, applier = _prepared_retention(tmp_path)
    operation_id = retention_operation_id(
        environment="dev",
        reviewed_plan_sha256=str(authorization["expected_plan_sha256"]),
    )
    receipt_root = cache_root / ".retention-apply-receipts"
    receipt_root.mkdir(mode=0o700)
    (receipt_root / f"{operation_id.replace(':', '-', 1)}.json").write_text("{", encoding="utf-8")

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID"
    assert stale.exists()
    assert current.exists()


def test_retention_blocks_unknown_receipt_inventory_without_mutation(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import (
        DeploymentCacheRetentionApplyError,
    )

    cache_root, stale, current, authorization, _, applier = _prepared_retention(tmp_path)
    receipt_root = cache_root / ".retention-apply-receipts"
    receipt_root.mkdir(mode=0o700)
    (receipt_root / "unmanaged.json").write_text("{}", encoding="utf-8")

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID"
    assert stale.exists()
    assert current.exists()


def test_retention_blocks_receipt_whose_filename_does_not_match_operation_id(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import (
        DeploymentCacheRetentionApplyError,
    )

    cache_root, stale, current, authorization, receipts, applier = _prepared_retention(tmp_path)
    operation_id = retention_operation_id(
        environment="dev",
        reviewed_plan_sha256=str(authorization["expected_plan_sha256"]),
    )
    receipt = build_retention_apply_receipt(
        operation_id=operation_id,
        status="committed",
        environment="dev",
        promoted_by="ci://retention",
        current_deployment_id=_deployment_id(current),
        reviewed_plan_sha256=str(authorization["expected_plan_sha256"]),
        activation_history_revision="sha256:" + "c" * 64,
        items=[
            {
                "deployment_id": _deployment_id(stale),
                "action": "deleted",
                "reason": "unreferenced",
                "path": stale.as_posix(),
            }
        ],
    )
    assert receipt["operation_id"] == operation_id
    receipt_root = cache_root / ".retention-apply-receipts"
    receipt_root.mkdir(mode=0o700)
    wrong_operation_id = retention_operation_id(
        environment="prod",
        reviewed_plan_sha256=str(authorization["expected_plan_sha256"]),
    )
    wrong_path = receipt_root / f"{wrong_operation_id.replace(':', '-', 1)}.json"
    wrong_path.write_text(json.dumps(receipt), encoding="utf-8")
    wrong_path.chmod(0o600)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID"
    assert stale.exists()
    assert current.exists()


def test_retention_replay_blocks_unsafe_sibling_receipt_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import (
        DeploymentCacheRetentionApplyError,
    )

    cache_root, stale, current, authorization, _, applier = _prepared_retention(tmp_path)

    def interrupt_before_delete(*_args: object, **_kwargs: object) -> object:
        raise SystemExit("simulated process death before delete")

    monkeypatch.setattr(applier._transactions, "delete", interrupt_before_delete)
    with pytest.raises(SystemExit):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    monkeypatch.undo()

    receipt_root = cache_root / ".retention-apply-receipts"
    sibling = receipt_root / f"sha256-{'f' * 64}.json"
    sibling.write_text("{}", encoding="utf-8")
    sibling.chmod(0o640)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID"
    assert stale.exists()
    assert current.exists()


def test_retention_replay_blocks_corrupt_canonical_sibling_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import (
        DeploymentCacheRetentionApplyError,
    )

    cache_root, stale, current, authorization, _, applier = _prepared_retention(tmp_path)

    def interrupt_before_delete(*_args: object, **_kwargs: object) -> object:
        raise SystemExit("simulated process death before delete")

    monkeypatch.setattr(applier._transactions, "delete", interrupt_before_delete)
    with pytest.raises(SystemExit):
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    monkeypatch.undo()

    sibling = cache_root / ".retention-apply-receipts" / f"sha256-{'f' * 64}.json"
    sibling.write_text("{}", encoding="utf-8")
    sibling.chmod(0o600)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID"
    assert stale.exists()
    assert current.exists()


def test_retention_blocks_world_writable_trash_root_before_detach(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_retention_contracts import (
        DeploymentCacheRetentionApplyError,
    )

    cache_root, stale, current, authorization, _, applier = _prepared_retention(tmp_path)
    trash_root = cache_root / ".retention-trash"
    trash_root.mkdir(mode=0o700)
    trash_root.chmod(0o777)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_DETACH_FAILED"
    assert stale.exists()
    assert current.exists()


def test_private_access_mode_accepts_inherited_setgid_without_group_access() -> None:
    assert has_posix_access_mode(stat.S_IFDIR | 0o2700, 0o700) is True
    assert has_posix_access_mode(stat.S_IFDIR | 0o2770, 0o700) is False


def test_retention_rejects_oversized_recovery_journal_before_json_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_common import DeploymentCacheError
    from dpone.runtime.deployment_cache_retention_journal import (
        MAX_RETENTION_JOURNAL_BYTES,
        DeploymentCacheRetentionJournal,
    )

    cache_root = tmp_path / ".dpone-cache"
    cache_root.mkdir()
    journal_path = cache_root / ".retention-recovery.json"
    with journal_path.open("wb") as handle:
        handle.truncate(MAX_RETENTION_JOURNAL_BYTES + 1)

    def forbidden_parse(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("oversized journal reached json parsing")

    monkeypatch.setattr(json, "loads", forbidden_parse)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheRetentionJournal(cache_root).read_transactions()

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_INVALID"


def test_bounded_json_reader_stops_growth_after_initial_stat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.adapters.deployment_cache_files import (
        DeploymentCacheError,
        read_regular_json_object,
    )

    path = tmp_path / "growing.json"
    path.write_text(json.dumps({"payload": "x" * 128}), encoding="utf-8")
    real_fstat = os.fstat
    calls = 0

    def stale_size(descriptor: int) -> os.stat_result:
        nonlocal calls
        calls += 1
        metadata = real_fstat(descriptor)
        if calls == 2:
            values = list(metadata)
            values[6] = 1
            return os.stat_result(values)
        return metadata

    monkeypatch.setattr(os, "fstat", stale_size)

    with pytest.raises(DeploymentCacheError) as exc:
        read_regular_json_object(
            path,
            missing_code="missing",
            invalid_code="invalid",
            label="growing fixture",
            root=tmp_path,
            max_bytes=32,
        )

    assert exc.value.code == "invalid"
    assert "byte limit" in str(exc.value)


def test_activation_snapshot_rejects_oversized_source_before_copy(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_activation import DeploymentCacheActivationSnapshotter
    from dpone.runtime.deployment_cache_common import DeploymentCacheError
    from dpone.runtime.deployment_cache_projection_validator import (
        DeploymentCacheProjectionValidator,
    )

    cache_root = tmp_path / ".dpone-cache"
    source = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    snapshotter = DeploymentCacheActivationSnapshotter(
        cache_root,
        validator=DeploymentCacheProjectionValidator(cache_root),
        max_snapshot_bytes=1,
    )

    with pytest.raises(DeploymentCacheError) as exc:
        snapshotter.prepare(source, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_ACTIVATION_FAILED"
    assert "size limit" in str(exc.value)
    assert not (cache_root / "activations").exists()


def test_retention_evidence_byte_limit_precedes_json_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness.airflow_cache_retention import (
        MAX_DEPLOYMENT_EVIDENCE_BYTES,
        AirflowCacheRetentionError,
        AirflowCacheRetentionService,
    )

    cache_root = tmp_path / ".dpone-cache"
    cache_root.mkdir()
    evidence = tmp_path / "oversized-evidence.json"
    with evidence.open("wb") as handle:
        handle.truncate(MAX_DEPLOYMENT_EVIDENCE_BYTES + 1)

    def forbidden_parse(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("oversized evidence reached json parsing")

    monkeypatch.setattr(json, "loads", forbidden_parse)
    with pytest.raises(AirflowCacheRetentionError) as exc:
        AirflowCacheRetentionService(cache_root=cache_root).plan(
            environment="dev",
            evidence_files=(evidence,),
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_EVIDENCE_INVALID"


def test_retention_evidence_depth_limit_is_fail_closed(tmp_path: Path) -> None:
    from dpone.readiness.airflow_cache_retention import (
        MAX_DEPLOYMENT_EVIDENCE_DEPTH,
        AirflowCacheRetentionError,
        AirflowCacheRetentionService,
    )

    cache_root = tmp_path / ".dpone-cache"
    cache_root.mkdir()
    value: object = {"deployment_id": "sha256:" + "a" * 64}
    for _ in range(MAX_DEPLOYMENT_EVIDENCE_DEPTH + 2):
        value = {"nested": value}
    evidence = tmp_path / "evidence.json"
    evidence.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(AirflowCacheRetentionError) as exc:
        AirflowCacheRetentionService(cache_root=cache_root).plan(
            environment="dev",
            evidence_files=(evidence,),
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_EVIDENCE_INVALID"
