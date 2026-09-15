"""Synthetic real-filesystem resource transaction checks."""

import os
import stat
import subprocess
import sys

import pytest
from tools.dbt_self_service.starter_resource_journal import RESOURCE_PATHS, recovery_report
from tools.dbt_self_service.starter_resource_transaction import apply_resource_plan

from dpone.adapters.project_authoring_lock import project_authoring_lock


def payloads():
    return {path: ("synthetic " + path + "\r\n").encode() for path in RESOURCE_PATHS}


def apply(root, **overrides):
    return apply_resource_plan(
        root,
        payloads(),
        source_revision="a" * 40,
        revalidate_inputs=lambda: None,
        validate_result=lambda: None,
        authoring_lock=project_authoring_lock,
        **overrides,
    )


def test_create_and_identical_retry_are_clean(tmp_path):
    result = apply(tmp_path)
    assert result.passed and len(result.changed_paths) == 16
    assert not recovery_report(tmp_path).pending
    assert {path: (tmp_path / path).read_bytes() for path in RESOURCE_PATHS} == payloads()
    retry = apply(tmp_path)
    assert retry.passed and retry.changed_paths == ()


@pytest.mark.parametrize("mixed", [False, True])
def test_pending_leaf_journal_blocks_entire_inventory_without_mutation(tmp_path, mixed):
    from dpone.manifest.confined_transaction_journal import transaction_journal_name

    for path, content in payloads().items():
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    first = tmp_path / RESOURCE_PATHS[0]
    if mixed:
        first.write_bytes(b"old")
    unchanged = tmp_path / RESOURCE_PATHS[-1]
    journal = unchanged.with_name(transaction_journal_name(unchanged.name))
    journal.write_bytes(b"unresolved")
    before = {path: (tmp_path / path).read_bytes() for path in RESOURCE_PATHS}
    result = apply(tmp_path)
    assert not result.passed and result.recovery_required
    report = recovery_report(tmp_path)
    assert report.pending and journal.relative_to(tmp_path).as_posix() in report.paths
    assert journal.read_bytes() == b"unresolved"
    assert {path: (tmp_path / path).read_bytes() for path in RESOURCE_PATHS} == before


@pytest.mark.parametrize("identical", [False, True])
def test_leaf_journal_created_during_result_validation_prevents_success(tmp_path, identical):
    from dpone.manifest.confined_transaction_journal import transaction_journal_name

    if identical:
        assert apply(tmp_path).passed
    target = tmp_path / RESOURCE_PATHS[-1]
    journal = target.with_name(transaction_journal_name(target.name))

    def validate():
        journal.write_bytes(b"unresolved")

    result = apply_resource_plan(
        tmp_path,
        payloads(),
        source_revision="a" * 40,
        revalidate_inputs=lambda: None,
        validate_result=validate,
        authoring_lock=project_authoring_lock,
    )
    assert not result.passed and result.recovery_required
    assert journal.read_bytes() == b"unresolved"
    assert journal.relative_to(tmp_path).as_posix() in recovery_report(tmp_path).paths


@pytest.mark.parametrize("kind", ["directory", "symlink"])
def test_nonregular_leaf_journal_is_reported_without_following_or_removing(tmp_path, kind):
    from dpone.manifest.confined_transaction_journal import transaction_journal_name

    assert apply(tmp_path).passed
    target = tmp_path / RESOURCE_PATHS[0]
    journal = target.with_name(transaction_journal_name(target.name))
    if kind == "directory":
        journal.mkdir()
    else:
        journal.symlink_to(tmp_path / "absent-foreign-file")
    report = recovery_report(tmp_path)
    assert report.pending and journal.relative_to(tmp_path).as_posix() in report.paths
    assert not apply(tmp_path).passed
    assert journal.is_symlink() if kind == "symlink" else journal.is_dir()


def test_replace_existing_resource_bytes(tmp_path):
    for path in RESOURCE_PATHS:
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"old")
    assert apply(tmp_path).passed
    assert all((tmp_path / path).read_bytes() == data for path, data in payloads().items())
    assert not recovery_report(tmp_path).pending


@pytest.mark.parametrize("existing", [False, True])
def test_caught_failure_compensates_only_owned_writes(tmp_path, existing):
    if existing:
        for path in RESOURCE_PATHS:
            target = tmp_path / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"old")

    def fail(phase, path):
        if phase == "after_mutation":
            raise RuntimeError("PRIVATE_SENTINEL")

    result = apply(tmp_path, phase_hook=fail)
    assert not result.passed
    assert "PRIVATE_SENTINEL" not in repr(result)
    for path in RESOURCE_PATHS:
        if existing:
            assert (tmp_path / path).read_bytes() == b"old"
        else:
            assert not (tmp_path / path).exists()


def test_process_death_leaves_pending_observation_and_blocks_retry(tmp_path):
    def die(phase, path):
        if phase == "after_mutation":
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        apply(tmp_path, phase_hook=die)
    report = recovery_report(tmp_path)
    assert report.pending and report.unresolved
    before = (tmp_path / RESOURCE_PATHS[0]).read_bytes()
    assert not apply(tmp_path).passed
    assert (tmp_path / RESOURCE_PATHS[0]).read_bytes() == before


def test_unknown_paths_reject_before_any_write(tmp_path):
    files = payloads()
    files["../unknown"] = b"no"
    result = apply_resource_plan(
        tmp_path,
        files,
        source_revision="a" * 40,
        revalidate_inputs=lambda: None,
        validate_result=lambda: None,
        authoring_lock=project_authoring_lock,
    )
    assert not result.passed and list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("rollback", [False, True])
def test_existing_permission_bits_survive_apply_or_rollback(tmp_path, rollback):
    first = tmp_path / RESOURCE_PATHS[0]
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(b"old")
    first.chmod(0o755)

    def fail(phase, path):
        if rollback and phase == "after_mutation":
            raise RuntimeError("synthetic")

    result = apply(tmp_path, phase_hook=fail)
    assert result.passed is not rollback
    assert stat.S_IMODE(first.stat().st_mode) == 0o755


def test_foreign_identical_inode_is_preserved_during_compensation(tmp_path):
    first = tmp_path / RESOURCE_PATHS[0]
    replacement = tmp_path / "foreign"

    def interfere(phase, path):
        if phase == "after_mutation":
            replacement.write_bytes(first.read_bytes())
            os.replace(replacement, first)
            raise RuntimeError("synthetic")

    result = apply(tmp_path, phase_hook=interfere)
    assert not result.passed and result.recovery_required
    assert first.read_bytes() == payloads()[RESOURCE_PATHS[0]]
    assert recovery_report(tmp_path).pending


def test_corrupt_prepared_backup_prevents_first_target_mutation(tmp_path):
    first = tmp_path / RESOURCE_PATHS[0]
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(b"old")

    def corrupt(phase, path):
        if phase == "prepared":
            metadata = tmp_path / ".dpone-starter-resource-transactions"
            operation = next(metadata.iterdir())
            (operation / "old/000.bin").write_bytes(b"foreign")

    result = apply(tmp_path, phase_hook=corrupt)
    assert not result.passed and result.recovery_required
    assert first.read_bytes() == b"old"


def test_actual_process_exit_retains_pending_operation(tmp_path):
    script = """
import os, sys
from pathlib import Path
from dpone.adapters.project_authoring_lock import project_authoring_lock
from tools.dbt_self_service.starter_resource_journal import RESOURCE_PATHS
from tools.dbt_self_service.starter_resource_transaction import apply_resource_plan
def die(phase, path):
    if phase == "after_mutation":
        os._exit(91)
apply_resource_plan(
    Path(sys.argv[1]), {path: b"synthetic" for path in RESOURCE_PATHS},
    source_revision="a"*40, revalidate_inputs=lambda: None,
    validate_result=lambda: None, authoring_lock=project_authoring_lock,
    phase_hook=die,
)
"""
    completed = subprocess.run([sys.executable, "-c", script, str(tmp_path)], capture_output=True, timeout=20)
    assert completed.returncode == 91
    report = recovery_report(tmp_path)
    assert report.pending and report.unresolved == (RESOURCE_PATHS[0],)
    assert not apply(tmp_path).passed
    assert (tmp_path / RESOURCE_PATHS[0]).read_bytes() == b"synthetic"


@pytest.mark.parametrize("append_failure", [None, "before", "after"])
def test_third_winner_during_inverse_exchange_is_preserved_and_reported(tmp_path, monkeypatch, append_failure):
    from tools.dbt_self_service import starter_resource_transaction as transaction
    from tools.dbt_self_service.starter_resource_journal import ResourceJournal

    append = ResourceJournal.append

    def fail_append(journal, event):
        affected = event.get("phase") == "RECOVERY_REQUIRED" and bool(event.get("recovery_paths"))
        if not affected or append_failure != "before":
            append(journal, event)
        if affected and append_failure:
            raise OSError("synthetic inverse observation failure")

    monkeypatch.setattr(ResourceJournal, "append", fail_append)

    first = tmp_path / RESOURCE_PATHS[0]
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(b"old")
    exchange = transaction.get_native_atomic_exchange()
    foreign_inode = []

    def raced_exchange(parent, left, right):
        foreign = tmp_path / "foreign"
        foreign.write_bytes(first.read_bytes())
        foreign_inode.append(foreign.stat().st_ino)
        os.replace(foreign, first)
        exchange(parent, left, right)

    monkeypatch.setattr(transaction, "get_native_atomic_exchange", lambda: raced_exchange)

    def fail(phase, path):
        if phase == "after_mutation":
            raise RuntimeError("synthetic")

    result = apply(tmp_path, phase_hook=fail)
    assert result.recovery_required and first.read_bytes() == b"old"
    report = recovery_report(tmp_path)
    paths = report.paths + result.unpersisted_recovery_paths
    preserved = [tmp_path / path for path in paths if (tmp_path / path).is_file()]
    assert any(path.stat().st_ino == foreign_inode[0] for path in preserved)
    if append_failure:
        assert any(
            (tmp_path / path).stat().st_ino == foreign_inode[0]
            for path in result.unpersisted_recovery_paths
            if (tmp_path / path).is_file()
        )
    if append_failure != "before":
        assert (RESOURCE_PATHS[0], False) in report.inverse_outcomes


@pytest.mark.parametrize("phase", ["prepared", "before_apply", "after_mutation", "after_observation", "verified"])
@pytest.mark.parametrize("existing", [False, True])
def test_failure_boundaries_restore_owned_target_bytes(tmp_path, phase, existing):
    if existing:
        for path in RESOURCE_PATHS:
            target = tmp_path / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"old")
    fired = False

    def fail_once(observed, path):
        nonlocal fired
        if observed == phase and not fired:
            fired = True
            raise RuntimeError("synthetic")

    result = apply(tmp_path, phase_hook=fail_once)
    assert fired and not result.passed
    assert not result.recovery_required
    assert not recovery_report(tmp_path).pending
    for path in RESOURCE_PATHS:
        target = tmp_path / path
        assert target.read_bytes() == b"old" if existing else not target.exists()


def test_crash_during_compensation_is_an_unresolved_observation(tmp_path):
    def fail(phase, path):
        if phase == "after_mutation":
            raise RuntimeError("synthetic")
        if phase == "after_compensate_mutation":
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        apply(tmp_path, phase_hook=fail)
    report = recovery_report(tmp_path)
    assert report.pending and report.unresolved == (RESOURCE_PATHS[0],)
    assert not (tmp_path / RESOURCE_PATHS[0]).exists()


def test_cleanup_failure_is_not_claimed_as_success(tmp_path):
    def fail(phase, path):
        if phase == "before_cleanup":
            raise OSError("synthetic")

    result = apply(tmp_path, phase_hook=fail)
    assert not result.passed and result.recovery_required
    assert recovery_report(tmp_path).pending


def test_forward_native_race_reports_retained_third_winner(tmp_path, monkeypatch):
    import tools.dbt_self_service.starter_resource_transaction as writer

    from dpone.manifest.confined_atomic_exchange import get_native_atomic_exchange

    first = tmp_path / RESOURCE_PATHS[0]
    first.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(b"old")
    native = get_native_atomic_exchange()
    original = writer.replace_file_if_digest
    winners = []

    def exchange(parent, target, replacement):
        if len(winners) < 2:
            foreign = tmp_path / "foreign"
            foreign.write_bytes(b"first-foreign" if not winners else b"third-winner")
            winners.append(foreign.stat().st_ino)
            os.replace(foreign, first)
        native(parent, target, replacement)

    def replace(*args, **kwargs):
        return original(*args, **kwargs, atomic_exchange=exchange)

    monkeypatch.setattr(writer, "replace_file_if_digest", replace)
    result = apply(tmp_path)
    assert not result.passed and result.recovery_required and len(winners) == 2
    report = recovery_report(tmp_path)
    assert any((tmp_path / path).is_file() and (tmp_path / path).stat().st_ino == winners[-1] for path in report.paths)
    assert report.discovery_required
    assert (RESOURCE_PATHS[0], False, False) in report.mutation_outcomes


def test_committed_native_cleanup_observation_retains_exact_recovery_path(tmp_path, monkeypatch):
    import tools.dbt_self_service.starter_resource_transaction as writer

    from dpone.manifest.confined_mutations import ConfinedReplaceOutcome

    target = tmp_path / RESOURCE_PATHS[0]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"old")
    retained = target.with_name(".dpone-recovery-" + "a" * 32)
    original = writer.replace_file_if_digest

    def replace(*args, **kwargs):
        outcome = original(*args, **kwargs)
        assert outcome.committed
        retained.write_bytes(b"synthetic retained artifact")
        return ConfinedReplaceOutcome(True, True, retained.name)

    monkeypatch.setattr(writer, "replace_file_if_digest", replace)
    result = apply(tmp_path)
    assert not result.passed and result.recovery_required
    report = recovery_report(tmp_path)
    assert retained.relative_to(tmp_path).as_posix() in report.paths
    assert (RESOURCE_PATHS[0], True, True) in report.mutation_outcomes
    assert target.read_bytes() == payloads()[RESOURCE_PATHS[0]]


def test_failed_outcome_append_reports_unresolved_discovery_obligation(tmp_path, monkeypatch):
    from tools.dbt_self_service.starter_resource_journal import ResourceJournal

    target = tmp_path / RESOURCE_PATHS[0]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b"old")

    def fail(*args):
        raise OSError("synthetic observation write failure")

    monkeypatch.setattr(ResourceJournal, "observe_mutation", fail)
    result = apply(tmp_path)
    report = recovery_report(tmp_path)
    assert not result.passed and report.pending and report.discovery_required
    assert RESOURCE_PATHS[0] in report.unresolved
    assert report.mutation_outcomes == ()


def test_creation_rollback_reports_retained_file_and_owned_directory(tmp_path, monkeypatch):
    from dpone.readiness.airflow_pipeline_source import ConfinedAuthoringFileSystem, ConfinedFileRollbackOutcome

    rollback = ConfinedAuthoringFileSystem.rollback
    target = tmp_path / RESOURCE_PATHS[0]
    retained = target.with_name(".dpone-rollback-" + "b" * 32)

    def preserve(filesystem, created):
        if created.path.as_posix() != RESOURCE_PATHS[0]:
            return rollback(filesystem, created)
        retained.write_bytes(b"synthetic retained file")
        return ConfinedFileRollbackOutcome(
            created.path,
            False,
            True,
            retained.relative_to(tmp_path),
            tuple(item.path for item in created.created_directories),
        )

    def fail(phase, path):
        if phase == "after_mutation":
            raise OSError("synthetic failure")

    monkeypatch.setattr(ConfinedAuthoringFileSystem, "rollback", preserve)
    result = apply(tmp_path, phase_hook=fail)
    assert result.recovery_required
    report = recovery_report(tmp_path)
    assert retained.relative_to(tmp_path).as_posix() in report.paths
    assert (RESOURCE_PATHS[0], False, True) in report.rollback_outcomes


def test_prepared_cleanup_reports_retained_metadata_artifact(tmp_path, monkeypatch):
    from dpone.readiness.airflow_pipeline_source import ConfinedAuthoringFileSystem, ConfinedFileRollbackOutcome

    original = ConfinedAuthoringFileSystem.rollback
    retained = []

    def preserve(filesystem, created):
        if created.path.parts[-2:] != ("new", "015.bin"):
            return original(filesystem, created)
        path = created.path.with_name(".dpone-rollback-" + "c" * 32)
        (tmp_path / path).write_bytes(b"synthetic recovery")
        retained.append(path.as_posix())
        return ConfinedFileRollbackOutcome(created.path, False, True, path)

    monkeypatch.setattr(ConfinedAuthoringFileSystem, "rollback", preserve)
    result = apply(tmp_path)
    report = recovery_report(tmp_path)
    assert result.recovery_required and retained
    assert retained[0] in report.paths
    assert report.status != "INVALID"


@pytest.mark.parametrize("leaf", ["events.jsonl", "manifest.json"])
@pytest.mark.parametrize("sidecar_failure", [None, "call", "fsync", "collision", "lost_parent"])
def test_final_metadata_cleanup_retains_sidecar_or_unpersisted_paths(tmp_path, monkeypatch, leaf, sidecar_failure):
    import tools.dbt_self_service.starter_resource_journal as journal_module

    from dpone.readiness.airflow_pipeline_source import ConfinedAuthoringFileSystem, ConfinedFileRollbackOutcome

    original = ConfinedAuthoringFileSystem.rollback
    write_sidecar = journal_module.write_recovery_sidecar
    retained = []

    def preserve(filesystem, created):
        if created.path.name != leaf:
            return original(filesystem, created)
        path = created.path.with_name(".dpone-rollback-" + "e" * 32)
        os.replace(tmp_path / created.path, tmp_path / path)
        retained.append(path.as_posix())
        return ConfinedFileRollbackOutcome(created.path, False, True, path)

    def fail(*args):
        if sidecar_failure == "call":
            raise OSError("synthetic sidecar persistence failure")
        directory = args[2].path
        if sidecar_failure == "collision":
            (directory / "recovery.json").write_bytes(b"foreign")
        elif sidecar_failure == "lost_parent":
            directory.rename(tmp_path / "retained-moved-operation")
        if sidecar_failure == "fsync":

            def fail_sync(descriptor):
                raise OSError("synthetic fsync failure")

            with monkeypatch.context() as scoped:
                scoped.setattr(os, "fsync", fail_sync)
                write_sidecar(*args)
        else:
            write_sidecar(*args)

    monkeypatch.setattr(ConfinedAuthoringFileSystem, "rollback", preserve)
    if sidecar_failure:
        monkeypatch.setattr(journal_module, "write_recovery_sidecar", fail)
    result = apply(tmp_path)
    assert not result.passed and result.recovery_required and retained
    if sidecar_failure:
        assert retained[0] in result.unpersisted_recovery_paths
    else:
        report = recovery_report(tmp_path)
        assert report.status == "RECOVERY_REQUIRED"
        assert retained[0] in report.paths
        assert not result.unpersisted_recovery_paths
    if sidecar_failure == "lost_parent":
        assert (tmp_path / "retained-moved-operation").is_dir()
        assert list((tmp_path / ".dpone-starter-resource-transactions").iterdir()) == []
    else:
        assert not apply(tmp_path).passed


@pytest.mark.parametrize("kind", ["mutation", "rollback"])
@pytest.mark.parametrize("after_append", [False, True])
def test_observation_append_failure_keeps_known_paths_in_receipt(tmp_path, monkeypatch, kind, after_append):
    import tools.dbt_self_service.starter_resource_transaction as writer
    from tools.dbt_self_service.starter_resource_journal import ResourceJournal

    from dpone.manifest.confined_mutations import ConfinedReplaceOutcome
    from dpone.readiness.airflow_pipeline_source import ConfinedAuthoringFileSystem, ConfinedFileRollbackOutcome

    target = tmp_path / RESOURCE_PATHS[0]
    retained = target.with_name(".dpone-recovery-" + "f" * 32)
    append = ResourceJournal.append
    replace = writer.replace_file_if_digest

    def replace_with_cleanup(*args, **kwargs):
        replace(*args, **kwargs)
        retained.write_bytes(b"synthetic")
        return ConfinedReplaceOutcome(True, True, retained.name)

    def rollback_with_cleanup(filesystem, created):
        retained.write_bytes(b"synthetic")
        return ConfinedFileRollbackOutcome(created.path, False, True, retained.relative_to(tmp_path))

    def append_failure(journal, event):
        affected = (
            "rollback" in event if kind == "rollback" else event.get("phase") == "APPLYING" and "committed" in event
        )
        if not affected or after_append:
            append(journal, event)
        if affected:
            raise OSError("synthetic append failure")

    def fail(phase, path):
        if kind == "rollback" and phase == "after_mutation":
            raise OSError("synthetic")

    if kind == "mutation":
        target.parent.mkdir(parents=True)
        target.write_bytes(b"old")
        monkeypatch.setattr(writer, "replace_file_if_digest", replace_with_cleanup)
    else:
        monkeypatch.setattr(ConfinedAuthoringFileSystem, "rollback", rollback_with_cleanup)
    monkeypatch.setattr(ResourceJournal, "append", append_failure)
    result = apply(tmp_path, phase_hook=fail)
    assert not result.passed and result.recovery_required
    assert retained.relative_to(tmp_path).as_posix() in result.unpersisted_recovery_paths


def test_post_exchange_directory_sync_failure_retains_actual_candidate_outcome(tmp_path, monkeypatch):
    from dpone.manifest import confined_atomic_exchange

    target = tmp_path / RESOURCE_PATHS[0]
    target.parent.mkdir(parents=True)
    target.write_bytes(b"old")
    sync = confined_atomic_exchange.sync_directory
    failed = False

    def fail_once(descriptor):
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("synthetic post-exchange sync failure")
        sync(descriptor)

    monkeypatch.setattr(confined_atomic_exchange, "sync_directory", fail_once)
    result = apply(tmp_path)
    assert failed and not result.passed and result.recovery_required
    assert target.read_bytes() == payloads()[RESOURCE_PATHS[0]]
    report = recovery_report(tmp_path)
    assert (RESOURCE_PATHS[0], True, True) in report.mutation_outcomes
    candidate = target.with_name(f".{target.name}.{result.operation}.new")
    assert candidate.read_bytes() == b"old"
    assert candidate.relative_to(tmp_path).as_posix() in report.paths
