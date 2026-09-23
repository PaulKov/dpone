"""Source-free inspection binds parent, lifecycle, directory, and P10f bytes."""

from __future__ import annotations

from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_native_chunk_inspection import (
    ExactNativeChunkInspectionEvidence,
    NativeParentJournalInspectionIndex,
)
from dpone.contracts.mssql_native_parent_journal import canonical_digest
from dpone.contracts.mssql_sqlclient_evidence_types import SqlClientEvidenceKind
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import (
    SqlClientInputCustody,
    bind_native_chunk_receipt,
    validate_native_chunk_receipt,
)
from dpone.contracts.mssql_sqlclient_registration import encode_registration
from dpone.contracts.mssql_sqlclient_writer_settlement import (
    decode_writer_settlement,
    encode_writer_settlement,
)
from dpone.contracts.mssql_tds_worker import (
    Contained,
    ContainmentRequired,
    ParentAuthority,
    Retired,
    RetirementRequired,
    TdsAttemptError,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    advance_state,
)
from dpone.ports.mssql_native_chunk_inspection import NativeChunkInspectionReference
from dpone.services.mssql_native_chunk_inspection import (
    NativeChunkInspectionUnknown,
    SourceFreeSqlClientChunkInspector,
)
from dpone.services.mssql_tds_writer_execution import execute_sqlclient_writer
from dpone.services.mssql_tds_writer_launch_validation import build_registration
from dpone.services.mssql_tds_writer_settlement import project_sqlclient_native_chunk, settle_sqlclient_writer
from tests.test_mssql_tds_writer_execution import _install_process, _ready, _result
from tests.test_mssql_tds_writer_launch import Evidence as LaunchEvidence
from tests.test_mssql_tds_writer_launch import setup as setup
from tests.test_mssql_tds_writer_settlement import Verifier, _observation


class _Index:
    def __init__(self, reference):
        self.reference = reference

    def references(self):
        return (self.reference,)


class _Lifecycle:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.calls = 0

    def read(self, identity):
        self.calls += 1
        return self.snapshot if self.snapshot.state.identity == identity else None


class _Directory:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.calls = 0

    def read(self, parent, limits):
        self.calls += 1
        return self.snapshot if (self.snapshot.state.parent, self.snapshot.state.limits) == (parent, limits) else None


class _Evidence:
    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    def read(self, receipt):
        self.calls.append(receipt.kind)
        return self.payloads[receipt.kind]


class _Verifier(Verifier):
    def __init__(self):
        super().__init__(None)

    def observe(self, **kwargs):
        owner = SimpleNamespace(**kwargs)
        owner.observation = kwargs["writer_observation"]
        owner.writer_admission = kwargs["writer_admission"]
        owner.stage = kwargs["stage"]
        owner.content_expectation = kwargs["expectation"]
        return _observation(owner)


def _case(setup, monkeypatch):
    payloads = {}
    original = LaunchEvidence.write

    def capture(self, record, *, deadline):
        receipt = original(self, record, deadline=deadline)
        payloads[record.kind] = record.payload
        return receipt

    monkeypatch.setattr(LaunchEvidence, "write", capture)
    ready, _ = _ready(setup, monkeypatch)
    _install_process(monkeypatch, setup, _result(setup))
    exited = execute_sqlclient_writer(ready, clock_ns=lambda: 1)
    projection = project_sqlclient_native_chunk(settle_sqlclient_writer(exited, _Verifier()))
    # The actor exposes only the last receipt, so reconstruct the earlier exact
    # registration receipt from its captured, already validated payload.
    from dpone.contracts.mssql_sqlclient_evidence import SqlClientEvidenceRecord

    payloads[SqlClientEvidenceKind.REGISTRATION] = encode_registration(
        build_registration(setup.process, setup.plan, setup.plan.attempt)
    )
    registration_receipt = SqlClientEvidenceRecord(
        projection.attempt_sha256,
        SqlClientEvidenceKind.REGISTRATION,
        payloads[SqlClientEvidenceKind.REGISTRATION],
    ).receipt
    verification_receipt = projection.verification_receipt
    custody = SqlClientInputCustody.bind(
        plan_sha256=projection.attempt.plan_sha256,
        target_id=projection.attempt.target_key,
        run_id=projection.attempt.run_id,
        window_fingerprint="window",
        attempt_id=f"{projection.attempt.run_id}-0-{projection.attempt.attempt}",
        ordinal=0,
        rows=projection.rows,
        encoded_bytes=projection.encoded_bytes,
        file_sha256=projection.file_sha256,
        typed_digest=projection.typed_digest,
        durable_object_id="durable-object",
        durable_location_sha256="b" * 64,
    )
    receipt = bind_native_chunk_receipt(
        projection=projection,
        plan_sha256=projection.attempt.plan_sha256,
        window_fingerprint="window",
        attempt_id=custody.attempt_id,
        custody=custody,
    )
    reference = NativeChunkInspectionReference(
        receipt,
        projection.projection_sha256,
        projection.lifecycle_verification_sha256,
        projection.lifecycle_revision,
        registration_receipt,
        verification_receipt,
    )
    inspector = SourceFreeSqlClientChunkInspector(
        _Index(reference),
        _Lifecycle(setup.lifecycle.snapshot),
        _Directory(setup.plan.directory),
        _Evidence(payloads),
    )
    return projection, reference, inspector, payloads


def _for_parent(receipt, identity):
    evidence = validate_native_chunk_receipt(receipt)
    raw_custody = asdict(evidence.input_custody)
    raw_custody.pop("custody_sha256")
    custody = SqlClientInputCustody.bind(
        **{
            **raw_custody,
            "plan_sha256": canonical_digest(identity),
            "window_fingerprint": identity["window_fingerprint"],
        }
    )
    rebound = replace(
        evidence,
        plan_sha256=canonical_digest(identity),
        window_fingerprint=identity["window_fingerprint"],
        input_custody=custody,
    )
    return replace(receipt, consumed_part_evidence=rebound.to_mapping())


def test_inspection_reproduces_byte_identical_projection_without_effects(setup, monkeypatch):
    projection, _, inspector, _ = _case(setup, monkeypatch)
    assert projection.attempt.ordinal == 0

    recovered = inspector.inspect(0, setup.plan.directory.state.limits)

    assert recovered == projection
    assert recovered.projection_sha256 == projection.projection_sha256
    assert inspector._lifecycle.calls == 1
    assert inspector._directories.calls == 1
    assert inspector._evidence.calls == [
        SqlClientEvidenceKind.REGISTRATION,
        SqlClientEvidenceKind.VERIFICATION,
    ]


@pytest.mark.parametrize(
    ("events", "phase"),
    [
        (
            (ContainmentRequired(TdsAttemptError.CLEANUP, ParentAuthority("published", "a" * 64)),),
            TdsAttemptPhase.CONTAINMENT_REQUIRED,
        ),
        (
            (ContainmentRequired(TdsAttemptError.CLEANUP, ParentAuthority("published", "a" * 64)), Contained("b" * 64)),
            TdsAttemptPhase.CONTAINED,
        ),
        (
            (
                ContainmentRequired(TdsAttemptError.CLEANUP, ParentAuthority("published", "a" * 64)),
                Contained("b" * 64),
                RetirementRequired(),
            ),
            TdsAttemptPhase.RETIREMENT_REQUIRED,
        ),
        (
            (
                ContainmentRequired(TdsAttemptError.CLEANUP, ParentAuthority("published", "a" * 64)),
                Contained("b" * 64),
                RetirementRequired(),
                Retired("c" * 64),
            ),
            TdsAttemptPhase.RETIRED,
        ),
    ],
)
def test_inspection_reproduces_original_projection_after_retirement_progress(setup, monkeypatch, events, phase):
    projection, _, inspector, _ = _case(setup, monkeypatch)
    snapshot = inspector._lifecycle.snapshot
    state = snapshot.state
    for event in events:
        state = advance_state(state, event, expected_phase=state.phase)
    assert state.phase is phase
    inspector._lifecycle.snapshot = TdsAttemptSnapshot(state, snapshot.revision + len(events))

    assert inspector.inspect(0, setup.plan.directory.state.limits) == projection


@pytest.mark.parametrize(
    "tamper",
    ("parent", "registration", "verification", "lifecycle", "directory", "replay"),
)
def test_inspection_fails_closed_for_tamper_replay_and_incomplete_recovery(setup, monkeypatch, tamper):
    projection, reference, inspector, payloads = _case(setup, monkeypatch)
    if tamper == "parent":
        inspector._index = _Index(replace(reference, projection_sha256="f" * 64))
    elif tamper == "registration":
        payloads[SqlClientEvidenceKind.REGISTRATION] += b" "
    elif tamper == "verification":
        payloads[SqlClientEvidenceKind.VERIFICATION] += b" "
    elif tamper == "lifecycle":
        inspector._lifecycle.snapshot = replace(
            inspector._lifecycle.snapshot,
            state=replace(inspector._lifecycle.snapshot.state, verification_sha256="f" * 64),
            revision=projection.lifecycle_revision + 1,
        )
    elif tamper == "directory":
        inspector._directories.snapshot = None
    else:
        inspector._index = _Index(replace(reference, receipt=replace(reference.receipt, ordinal=1)))

    with pytest.raises(NativeChunkInspectionUnknown, match="inspection_unknown"):
        inspector.inspect(0, setup.plan.directory.state.limits)


def test_inspection_rejects_reencoded_settlement_with_changed_input_expectation(setup, monkeypatch):
    _, reference, inspector, payloads = _case(setup, monkeypatch)
    record = decode_writer_settlement(payloads[SqlClientEvidenceKind.VERIFICATION])
    changed = replace(record, expectation=replace(record.expectation, file_sha256="e" * 64))
    payload = encode_writer_settlement(changed)
    from dpone.contracts.mssql_sqlclient_evidence import SqlClientEvidenceRecord

    receipt = SqlClientEvidenceRecord(
        reference.registration_receipt.attempt_sha256,
        SqlClientEvidenceKind.VERIFICATION,
        payload,
    ).receipt
    payloads[SqlClientEvidenceKind.VERIFICATION] = payload
    inspector._index = _Index(
        replace(
            reference,
            receipt=replace(reference.receipt, file_sha256="e" * 64),
            lifecycle_verification_sha256=receipt.payload_sha256,
            verification_receipt=receipt,
        )
    )
    inspector._lifecycle.snapshot = replace(
        inspector._lifecycle.snapshot,
        state=replace(
            inspector._lifecycle.snapshot.state,
            verification_sha256=receipt.payload_sha256,
        ),
    )

    with pytest.raises(NativeChunkInspectionUnknown, match="inspection_unknown"):
        inspector.inspect(0, setup.plan.directory.state.limits)


def test_parent_adapter_enumerates_only_exact_v4_coordinates(setup, monkeypatch):
    projection, reference, _, _ = _case(setup, monkeypatch)
    identity = {
        "run_id": projection.attempt.run_id,
        "window_fingerprint": "window",
        "transport": {"backend": "mssql_sqlclient"},
    }
    receipt = _for_parent(reference.receipt, identity)
    data = {
        "version": 4,
        "phase": "stage_complete",
        "identity": identity,
        "chunks": {"0": {"attempt": projection.attempt.attempt, "phase": "verified", "receipt": asdict(receipt)}},
    }

    assert NativeParentJournalInspectionIndex(SimpleNamespace(data=data)).references() == (
        replace(reference, receipt=receipt),
    )

    data["chunks"]["0"]["attempt"] += 1
    with pytest.raises(ValueError, match="inspection_unknown"):
        NativeParentJournalInspectionIndex(SimpleNamespace(data=data)).references()


def test_parent_adapter_preserves_multiple_chunk_ordinal_order(setup, monkeypatch):
    projection, reference, _, _ = _case(setup, monkeypatch)
    identity = {
        "run_id": projection.attempt.run_id,
        "window_fingerprint": "window",
        "transport": {"backend": "mssql_sqlclient"},
    }
    first = _for_parent(reference.receipt, identity)
    raw_evidence = dict(first.consumed_part_evidence)
    raw_custody = dict(raw_evidence["input_custody"])
    raw_custody.pop("custody_sha256")
    second_custody = SqlClientInputCustody.bind(
        **{
            **raw_custody,
            "ordinal": 1,
            "attempt_id": f"{projection.attempt.run_id}-1-0",
        }
    )
    raw_evidence["input_custody"] = asdict(second_custody)
    second = replace(
        first,
        ordinal=1,
        attempt_id=f"{projection.attempt.run_id}-1-0",
        consumed_part_evidence=raw_evidence,
    )
    data = {
        "version": 4,
        "phase": "stage_complete",
        "identity": identity,
        "chunks": {
            "1": {"attempt": 0, "phase": "verified", "receipt": asdict(second)},
            "0": {"attempt": projection.attempt.attempt, "phase": "verified", "receipt": asdict(first)},
        },
    }

    references = NativeParentJournalInspectionIndex(SimpleNamespace(data=data)).references()

    assert tuple(item.receipt.ordinal for item in references) == (0, 1)
    assert tuple(item.receipt.attempt_id for item in references) == (
        f"{projection.attempt.run_id}-0-{projection.attempt.attempt}",
        f"{projection.attempt.run_id}-1-0",
    )


@pytest.mark.parametrize("replay", ("plan", "window"))
def test_parent_index_rejects_self_consistent_cross_parent_replay(setup, monkeypatch, replay):
    projection, reference, _, _ = _case(setup, monkeypatch)
    identity = {
        "run_id": projection.attempt.run_id,
        "window_fingerprint": "window",
        "transport": {"backend": "mssql_sqlclient"},
    }
    other = dict(identity)
    if replay == "window":
        other["window_fingerprint"] = "other-window"
    else:
        other["source_query_id"] = "other-query"
    receipt = _for_parent(reference.receipt, other)
    data = {
        "version": 4,
        "phase": "stage_complete",
        "identity": identity,
        "chunks": {
            "0": {
                "attempt": projection.attempt.attempt,
                "phase": "verified",
                "receipt": asdict(receipt),
            }
        },
    }

    with pytest.raises(ValueError, match="inspection_unknown"):
        NativeParentJournalInspectionIndex(SimpleNamespace(data=data)).references()


def test_exact_evidence_adapter_rejects_changed_bytes(setup, monkeypatch):
    _, reference, _, payloads = _case(setup, monkeypatch)

    class Reader:
        def read(self, relative_name, byte_count, payload_sha256):
            return payloads[SqlClientEvidenceKind.REGISTRATION] + b" "

    with pytest.raises(ValueError, match="inspection_unknown"):
        ExactNativeChunkInspectionEvidence(Reader()).read(reference.registration_receipt)
