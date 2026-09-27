"""Real BCP process, ambiguity, and durable-custody certification cases."""

from __future__ import annotations

from hashlib import sha256

import pytest
from tests.integration.mssql.mssql_target_local_p1_cases import digest_cases
from tests.integration.mssql.mssql_target_local_p1_support import configured_target_local_stand

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_custody import NativeTargetCustody
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_native_writer import BCP_STAGE_PROOF, NativeStageWriteGrant
from dpone.runtime.sinks.mssql_native_bcp_writer import MssqlNativeBcpWriter
from dpone.runtime.sinks.mssql_native_target_digest import build_target_digest_sql, decode_target_digest_row

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]


@pytest.fixture(scope="module")
def stand():
    return configured_target_local_stand()


def test_real_bcp_positive_terminal_precedes_target_authority(stand, tmp_path) -> None:
    case = digest_cases()[0]
    table = stand.unique_table("positive")
    file = case.sealed_file(tmp_path / "positive.native")
    grant = NativeStageWriteGrant(
        "attempt-positive",
        stand.qualified(table),
        file.path,
        file.rows,
        file.encoded_bytes,
        file.file_sha256,
        sha256(b"positive-grant").hexdigest(),
        BCP_STAGE_PROOF,
    )
    rejects = tmp_path / "positive.rejects"
    writer = MssqlNativeBcpWriter(lambda current, path: stand.launch_bcp(current, path))
    try:
        stand.create_empty_table(table, case)

        outcome = writer.write(grant, rejects_path=rejects)
        digest = decode_target_digest_row(
            stand.single_record(build_target_digest_sql(stand.qualified(table), case.contract, file.rows)),
            expected_rows=file.rows,
        )

        assert outcome.positive_terminal is True
        assert outcome.rows_consumed == file.rows
        assert digest == case.python_digest()
        assert not rejects.exists() or rejects.stat().st_size == 0
        with pytest.raises(ValueError, match="grant_already_launched"):
            writer.write(grant, rejects_path=rejects)
    finally:
        stand.drop_table(table)


def test_real_bcp_lost_ack_is_ambiguous_even_when_rows_arrived(stand, tmp_path) -> None:
    case = digest_cases()[0]
    table = stand.unique_table("lost_ack")
    file = case.sealed_file(tmp_path / "lost-ack.native")
    grant = NativeStageWriteGrant(
        "attempt-lost-ack",
        stand.qualified(table),
        file.path,
        file.rows,
        file.encoded_bytes,
        file.file_sha256,
        sha256(b"lost-ack-grant").hexdigest(),
        BCP_STAGE_PROOF,
    )
    try:
        stand.create_empty_table(table, case)
        writer = MssqlNativeBcpWriter(lambda current, path: stand.launch_bcp(current, path, discard_bcp_output=True))

        outcome = writer.write(grant, rejects_path=tmp_path / "lost-ack.rejects")

        assert outcome.classification == "lost_ack"
        assert outcome.positive_terminal is False
        assert outcome.rows_consumed is None
        assert stand.table_rows(table) == file.rows
    finally:
        stand.drop_table(table)


def test_real_bcp_failure_never_becomes_positive_terminal(stand, tmp_path) -> None:
    case = digest_cases()[0]
    table = stand.unique_table("failure")
    file = case.sealed_file(tmp_path / "failure.native")
    grant = NativeStageWriteGrant(
        "attempt-failure",
        stand.qualified(table),
        file.path,
        file.rows,
        file.encoded_bytes,
        file.file_sha256,
        sha256(b"failure-grant").hexdigest(),
        BCP_STAGE_PROOF,
    )
    writer = MssqlNativeBcpWriter(lambda current, path: stand.launch_bcp(current, path))

    outcome = writer.write(grant, rejects_path=tmp_path / "failure.rejects")

    assert outcome.classification == "failure"
    assert outcome.positive_terminal is False
    assert outcome.rows_consumed is None


def test_real_bcp_cleanup_failure_retains_ambiguity_after_rows_arrive(stand, tmp_path) -> None:
    case = digest_cases()[0]
    table = stand.unique_table("cleanup")
    file = case.sealed_file(tmp_path / "cleanup.native")
    grant = NativeStageWriteGrant(
        "attempt-cleanup",
        stand.qualified(table),
        file.path,
        file.rows,
        file.encoded_bytes,
        file.file_sha256,
        sha256(b"cleanup-grant").hexdigest(),
        BCP_STAGE_PROOF,
    )
    try:
        stand.create_empty_table(table, case)
        writer = MssqlNativeBcpWriter(lambda current, path: stand.launch_bcp(current, path, fail_cleanup=True))

        outcome = writer.write(grant, rejects_path=tmp_path / "cleanup.rejects")

        assert outcome.classification == "cleanup_failed"
        assert outcome.positive_terminal is False
        assert outcome.rows_consumed is None
        assert stand.table_rows(table) == file.rows
    finally:
        stand.drop_table(table)


def test_changed_sealed_file_is_rejected_before_writer_launch(stand, tmp_path) -> None:
    case = digest_cases()[0]
    file = case.sealed_file(tmp_path / "changed.native")
    grant = NativeStageWriteGrant(
        "attempt-changed-file",
        "[dpone_it].[dbo].[synthetic_stage]",
        file.path,
        file.rows,
        file.encoded_bytes,
        file.file_sha256,
        sha256(b"changed-file-grant").hexdigest(),
        BCP_STAGE_PROOF,
    )
    launches = 0

    def unexpected_launch(_grant, _rejects):
        nonlocal launches
        launches += 1
        raise AssertionError("changed input must be rejected before launch")

    writer = MssqlNativeBcpWriter(unexpected_launch)
    file.path.chmod(0o600)
    file.path.write_bytes(file.path.read_bytes() + b"changed")

    with pytest.raises(ValueError, match="file_identity_changed"):
        writer.write(grant, rejects_path=tmp_path / "changed.rejects")
    assert launches == 0


def test_real_bcp_lock_timeout_is_ambiguous_and_cannot_release_custody(stand, tmp_path) -> None:
    case = digest_cases()[0]
    table = stand.unique_table("timeout")
    file = case.sealed_file(tmp_path / "timeout.native")
    grant = NativeStageWriteGrant(
        "attempt-timeout",
        stand.qualified(table),
        file.path,
        file.rows,
        file.encoded_bytes,
        file.file_sha256,
        sha256(b"timeout-grant").hexdigest(),
        BCP_STAGE_PROOF,
    )
    store = SQLiteWindowStore(tmp_path / "timeout-custody.db", clock=lambda: 1.0)
    lease = store.acquire("timeout-target", "writer", 60)
    custody = NativeTargetCustody(store, "timeout-target")
    custody.claim(lease, "d" * 64)
    try:
        stand.create_empty_table(table, case)
        writer = MssqlNativeBcpWriter(lambda current, path: stand.launch_bcp(current, path, timeout_seconds=1))

        with stand.held_table_lock(table):
            outcome = writer.write(grant, rejects_path=tmp_path / "timeout.rejects")

        assert outcome.classification in {"timeout", "custody_lost"}
        assert outcome.positive_terminal is False
        assert custody.inspect(lease).state == "held"

        def no_nonpublication_proof() -> None:
            raise ValueError("unresolved writer")

        with pytest.raises(ValueError, match="unresolved writer"):
            custody.release(
                lease,
                "d" * 64,
                "nonpublication_all_stages_retired",
                assert_release_authority=no_nonpublication_proof,
            )
        assert custody.inspect(lease).state == "held"
    finally:
        stand.drop_table(table)


def test_stable_custody_blocks_v1_and_overlap_after_lease_expiry(tmp_path) -> None:
    now = [1.0]
    store = SQLiteWindowStore(tmp_path / "custody.db", clock=lambda: now[0])
    first = store.acquire("synthetic-target", "first", 1)
    custody = NativeTargetCustody(store, "synthetic-target")
    custody.claim(first, "a" * 64)
    now[0] = 3.0
    recovery = store.acquire("synthetic-target", "recovery", 60)

    assert custody.claim(recovery, "a" * 64).recovery_only is True
    with pytest.raises(WindowContractError, match="custody_held"):
        custody.assert_available_for_v1(recovery)
    with pytest.raises(WindowContractError, match="custody_held"):
        custody.claim(recovery, "b" * 64)
    with pytest.raises(WindowContractError, match="custody_recovery_only"):
        custody.reassert_grant(recovery, "a" * 64)


def test_empty_invocation_uses_distinct_custody_release_reason(tmp_path) -> None:
    store = SQLiteWindowStore(tmp_path / "empty-custody.db", clock=lambda: 1.0)
    lease = store.acquire("synthetic-empty-target", "writer", 60)
    custody = NativeTargetCustody(store, "synthetic-empty-target")
    claim = custody.claim(lease, "c" * 64)
    assert claim.record.state == "held"
    cleared = custody.release(
        lease,
        "c" * 64,
        "empty_completion_cleanup",
        assert_release_authority=lambda: None,
    )

    assert cleared.state == "clear"
    custody.assert_available_for_v1(lease)
