"""Parent input release consumes only exact ordered v4 SqlClient evidence."""

from __future__ import annotations

import os
from dataclasses import asdict, replace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.adapters.mssql_sqlclient_input_custody import FileSqlClientInputCustody
from dpone.app.mssql_sqlclient_parent_input_releaser import SqlClientParentInputReleaser
from dpone.contracts.mssql_native_parent_journal import (
    NativeChunkRetirementReceipt,
    NativeParentRetirementReceipt,
    canonical_digest,
)
from dpone.contracts.mssql_sqlclient_evidence_types import evidence_name
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import (
    SqlClientNativeChunkEvidence,
    canonical_stage_id,
)
from dpone.contracts.mssql_sqlclient_stage_identity import stage_object_identity
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from tests.test_mssql_sqlclient_native_importer import _plan, _project
from tests.test_mssql_tds_writer_launch import setup as setup

H = "a" * 64


def _input_bytes(setup) -> bytes:
    descriptor = setup.plan.input_descriptor.fd
    size = setup.plan.input_descriptor.expected.encoded_bytes
    return os.pread(descriptor, size, 0)


def _parent_receipt(projection, custody, *, ordinal: int, table_suffix: str):
    from dpone.contracts.mssql_native_chunks import NativeChunkReceipt

    attempt = replace(projection.attempt, ordinal=ordinal)
    attempt_digest = attempt_identity_digest(attempt)
    registration = replace(
        projection.registration_receipt,
        attempt_sha256=attempt_digest,
        relative_name=evidence_name(
            attempt_digest,
            projection.registration_receipt.kind,
            projection.registration_receipt.payload_sha256,
        ),
    )
    verification = replace(
        projection.verification_receipt,
        attempt_sha256=attempt_digest,
        relative_name=evidence_name(
            attempt_digest,
            projection.verification_receipt.kind,
            projection.verification_receipt.payload_sha256,
        ),
    )
    stage = replace(projection.stage, table_name=projection.stage.table_name + table_suffix)
    object_identity = stage_object_identity(stage)
    evidence = SqlClientNativeChunkEvidence(
        plan_sha256=custody.plan_sha256,
        window_fingerprint=custody.window_fingerprint,
        projection_sha256=projection.projection_sha256,
        lifecycle_verification_sha256=verification.payload_sha256,
        lifecycle_revision=projection.lifecycle_revision,
        verification_payload_sha256=verification.payload_sha256,
        registration_receipt=registration,
        verification_receipt=verification,
        input_custody=custody,
        stage_identity=stage,
        object_identity=object_identity,
        typed_sum=projection.typed_sum,
    )
    return NativeChunkReceipt(
        ordinal,
        custody.attempt_id,
        canonical_stage_id(object_identity),
        custody.rows,
        custody.encoded_bytes,
        custody.file_sha256,
        custody.typed_digest,
        evidence.to_mapping(),
    )


def _fixture(tmp_path, setup, monkeypatch, *, count: int = 2):
    _, projection = _project(setup, monkeypatch)
    plan = _plan(projection)
    data = _input_bytes(setup)
    custody_store = FileSqlClientInputCustody(tmp_path / "custody")
    parents = []
    custodies = []
    for ordinal in range(count):
        custody = custody_store.retain(
            data,
            plan_sha256=projection.attempt.plan_sha256,
            target_id=plan.target_id,
            run_id=plan.run_id,
            window_fingerprint=plan.window_fingerprint,
            attempt_id=f"{plan.run_id}-{ordinal}-0",
            ordinal=ordinal,
            rows=projection.rows,
            typed_digest=projection.typed_digest,
        )
        custodies.append(custody)
        parents.append(_parent_receipt(projection, custody, ordinal=ordinal, table_suffix=str(ordinal)))

    window_store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = window_store.acquire(plan.target_id, "owner", 60)
    journal = NativeChunkJournal(window_store, lease, plan, parent_schema_version=4)
    journal.begin()
    for ordinal, parent in enumerate(parents):
        journal.attempt(ordinal, 0)
        journal.verified(parent)
    journal.complete(source_eof=True)
    journal.publication.prepared({"stage": "prepared"})
    journal.publication.publication_started({"stage": "prepared"})
    authority = journal.publication.publication_confirmed({"generation": "one"})
    journal.publication.retirement_required(authority)
    journal.publication.retiring()
    retired_chunks = []
    for ordinal, parent in enumerate(parents):
        receipt = NativeChunkRetirementReceipt(
            ordinal,
            parent.attempt_id,
            authority.digest,
            H,
            canonical_digest(asdict(parent)),
            H,
            H,
            H,
            H,
            H,
            H,
        )
        journal.publication.chunk_retired(receipt)
        retired_chunks.append(receipt)
    retirement = journal.publication.retired()
    assert retirement == NativeParentRetirementReceipt(authority.digest, tuple(retired_chunks))
    return journal, custody_store, tuple(custodies), tuple(parents), retirement


def test_releases_two_chunks_in_ordinal_order_and_binds_digest(tmp_path, setup, monkeypatch) -> None:
    journal, store, custodies, _, retirement = _fixture(tmp_path, setup, monkeypatch)
    calls = []
    original = store.release

    def observed(receipt, parent, settled):
        calls.append(receipt.ordinal)
        return original(receipt, parent, settled)

    store.release = observed  # type: ignore[method-assign]
    digest = SqlClientParentInputReleaser(journal, store).release(retirement)

    assert calls == [0, 1]
    assert digest == canonical_digest(
        {
            "schema": "dpone.sqlclient.parent-input-release.v1",
            "retirement_digest": retirement.digest,
            "ordered_custody_ids": [item.durable_object_id for item in custodies],
        }
    )
    assert not tuple((tmp_path / "custody").glob("*.bin"))


def test_missing_files_are_idempotent(tmp_path, setup, monkeypatch) -> None:
    journal, store, _, _, retirement = _fixture(tmp_path, setup, monkeypatch)
    releaser = SqlClientParentInputReleaser(journal, store)
    assert releaser.release(retirement) == releaser.release(retirement)


def test_corrupt_evidence_fails_before_any_release(tmp_path, setup, monkeypatch) -> None:
    journal, store, _, _, retirement = _fixture(tmp_path, setup, monkeypatch)
    journal._data["chunks"]["1"]["receipt"]["consumed_part_evidence"]["schema"] = "wrong"
    before = {path.name for path in (tmp_path / "custody").glob("*.bin")}
    with pytest.raises(ValueError, match="parent_input_release_invalid"):
        SqlClientParentInputReleaser(journal, store).release(retirement)
    assert {path.name for path in (tmp_path / "custody").glob("*.bin")} == before


def test_retirement_mismatch_fails_before_any_release(tmp_path, setup, monkeypatch) -> None:
    journal, store, _, _, retirement = _fixture(tmp_path, setup, monkeypatch)
    wrong_chunks = list(retirement.chunks)
    wrong_chunks[1] = replace(wrong_chunks[1], verification_receipt_sha256="b" * 64)
    wrong = NativeParentRetirementReceipt(retirement.authority_digest, tuple(wrong_chunks))
    before = {path.name for path in (tmp_path / "custody").glob("*.bin")}
    with pytest.raises(ValueError, match="parent_input_release_invalid"):
        SqlClientParentInputReleaser(journal, store).release(wrong)
    assert {path.name for path in (tmp_path / "custody").glob("*.bin")} == before


def test_partial_effect_failure_stops_at_failed_chunk(tmp_path, setup, monkeypatch) -> None:
    journal, store, custodies, _, retirement = _fixture(tmp_path, setup, monkeypatch, count=3)
    original = store.release

    def fail_second(receipt, parent, settled):
        if receipt.ordinal == 1:
            raise OSError("injected")
        return original(receipt, parent, settled)

    store.release = fail_second  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="parent_input_release_failed"):
        SqlClientParentInputReleaser(journal, store).release(retirement)
    with pytest.raises(RuntimeError, match="unavailable"):
        store.observe(custodies[0])
    assert store.observe(custodies[1]) == _input_bytes(setup)
    assert store.observe(custodies[2]) == _input_bytes(setup)
