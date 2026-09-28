"""Synthetic target completion exercises real store CAS and publication counters."""

from dataclasses import replace

import pytest

from dpone.contracts.clickhouse_cluster_publication import AuthorityMutationResult, AuthorityMutationStatus
from dpone.contracts.quality_replay import ReplayQualityEvidenceError, TargetAcceptanceError, unavailable_observation
from dpone.ports.target_acceptance import BoundedTargetAcceptanceReader
from dpone.runtime.governance.quality_replay import receipt_already_accepted
from dpone.runtime.sinks.clickhouse_replay_quality import ClickHouseReplayQualityStore
from tests.test_quality_replay_runtime import Rig, acceptance_policy


class Reader(BoundedTargetAcceptanceReader):
    def __init__(self):
        self.rig = None
        self.scans = 0
        self.verifications = 0
        self.failure = None
        self.change = None

    def require_ready(self, **kwargs):
        assert set(kwargs) == {"cluster", "database", "table"}

    def verify_generation(self, request, *, deadline):
        self.verifications += 1
        if request.table == self.rig.candidate.target_table:
            assert self.rig.ddl.dispatches == 0
        else:
            assert self.rig.authority.current.record.quality_reader == request.reader_token

    def collect(self, request, *, deadline):
        self.scans += 1
        assert self.rig.capsule().state == "TARGET_PENDING"
        assert self.rig.authority.current.record.quality_reader == request.reader_token
        if self.failure:
            raise self.failure
        value = unavailable_observation(
            request, replica=self.rig.catalog.inventory(request.cluster).hosts[0], attempt_id="a" * 32
        )
        value.update(
            row_count=2,
            null_counts=dict.fromkeys(request.null_columns, 0),
            distinct_counts=dict.fromkeys(request.distinct_columns, 2),
            warnings=[],
        )
        if self.change:
            self.change(value)
        return value


class TargetRig(Rig):
    def __init__(self, quality=None):
        self.reader = Reader()
        super().__init__(quality or acceptance_policy(target=True))
        self.reader.rig = self

    def fresh_store(self):
        return ClickHouseReplayQualityStore(
            self.catalog, lambda _: self.authority, target_acceptance_reader=self.reader
        )


def test_original_and_complete_replay_share_proof_without_rescan():
    rig = TargetRig()
    rig.publish(complete=True)
    original = rig.capsule()
    assert original.version.endswith(".v2")
    assert original.state == "COMPLETE"
    assert set(original.core["acceptance"]) == {"source", "staged"}
    assert original.target["capture_boundary"] == "committed_generation_before_governance_complete"
    assert "captured_before_cleanup" not in original.target
    receipt, result = rig.fresh_session().replay(rig.config)
    assert receipt.report.passed and result["kind"].endswith(".v2")
    assert set(result["acceptance"]) == {"source", "staged", "target"}
    assert rig.capsule().payload == original.payload
    assert rig.reader.scans == 1
    assert rig.ddl.dispatches == 1
    assert rig.authority.current.record.quality_reader is None


@pytest.mark.parametrize("uncertain", [False, True])
def test_timeout_keeps_pending_and_only_known_quiescence_retires_guard(uncertain):
    rig = TargetRig()
    rig.publish()
    rig.reader.failure = TargetAcceptanceError("INCOMPLETE", quiescent=not uncertain)
    with pytest.raises(TargetAcceptanceError) as caught:
        rig.fresh_session().replay(rig.config)
    assert caught.value.replay_details["target_commit"] == "proven"
    assert rig.capsule().state == "TARGET_PENDING"
    assert (rig.authority.current.record.quality_reader is not None) == uncertain
    assert rig.ddl.dispatches == 1
    if not uncertain:
        rig.reader.failure = None
        rig.fresh_session().replay(rig.config)
        assert rig.capsule().state == "COMPLETE" and rig.reader.scans == 2


@pytest.mark.parametrize("metric", [None, True, -1, 2**64])
def test_invalid_target_is_terminal_and_never_reobserved(metric):
    rig = TargetRig()
    rig.publish()
    rig.reader.change = lambda value: value.update(row_count=metric)
    with pytest.raises(TargetAcceptanceError):
        rig.fresh_session().replay(rig.config)
    failed = rig.capsule().payload
    assert rig.capsule().state == "FAILED"
    with pytest.raises(ReplayQualityEvidenceError, match="FAILED"):
        rig.fresh_session().replay(rig.config)
    assert rig.capsule().payload == failed and rig.reader.scans == 1


def test_unexpected_worker_error_keeps_guard_and_never_consumes_receipt():
    rig = TargetRig()
    rig.publish()
    rig.reader.failure = RuntimeError("synthetic lifecycle uncertainty")
    session = rig.fresh_session()
    with pytest.raises(RuntimeError):
        session.replay(rig.config)
    assert session.accepted_receipt is None
    assert rig.authority.current.record.quality_reader is not None


@pytest.mark.parametrize("boundary", ["pending", "complete", "release"])
def test_ack_loss_never_emits_success_or_retries_mutation(boundary):
    rig = TargetRig()
    rig.publish()
    original_cas = rig.authority.compare_and_swap
    hits = []

    def lose_ack(current, desired):
        result = original_cas(current, desired)
        state = rig.capsule().state
        match = (
            (boundary == "pending" and state == "TARGET_PENDING")
            or (boundary == "complete" and state == "COMPLETE")
            or (boundary == "release" and desired.quality_reader is None)
        )
        if match:
            hits.append(1)
            return AuthorityMutationResult(AuthorityMutationStatus.OUTCOME_UNKNOWN)
        return result

    rig.authority.compare_and_swap = lose_ack
    session = rig.fresh_session()
    with pytest.raises(ReplayQualityEvidenceError):
        session.replay(rig.config)
    assert len(hits) == 1
    assert session.accepted_receipt is None
    if boundary != "release":
        assert rig.authority.current.record.quality_reader is not None
    assert rig.ddl.dispatches == 1


def test_receipt_identity_only_skips_after_verified_release():
    rig = TargetRig()
    rig.publish(complete=True)
    execution = rig.session.execution
    execution.replay_session = rig.session
    assert receipt_already_accepted(execution, rig.session.accepted_receipt, config=rig.config)
    assert not receipt_already_accepted(execution, object(), config=rig.config)
    assert not receipt_already_accepted(execution, None, config=rig.config)


def test_complete_envelope_capacity_rejects_before_publication_dispatch():
    rig = TargetRig()
    rig.stage()
    key = (rig.config.target_schema, rig.candidate.target_table)
    # PREPARED fits, but the completed proof repeats these requested columns.
    plan = rig.store._pending[key]["target_plan"]
    plan["columns"] = [["x" * 130000, "UInt64"]]
    with pytest.raises(ReplayQualityEvidenceError):
        rig.service.publish(rig.config, rig.candidate, staged_rows=2)
    assert rig.ddl.dispatches == 0


@pytest.mark.parametrize("mode", ["required", "warn_only"])
def test_explicit_unavailable_record_is_pending_required_or_complete_warn_only(mode):
    quality = acceptance_policy(target=True)
    quality["acceptance"]["mode"] = mode
    rig = TargetRig(quality)
    rig.publish()
    rig.reader.change = lambda value: value.update(
        row_count=None, null_counts={}, distinct_counts={}, warnings=["target_acceptance_metric_probe_unavailable"]
    )
    if mode == "required":
        with pytest.raises(TargetAcceptanceError, match="INCOMPLETE"):
            rig.fresh_session().replay(rig.config)
        assert rig.capsule().state == "TARGET_PENDING"
    else:
        rig.fresh_session().replay(rig.config)
        assert rig.capsule().state == "COMPLETE"
        assert rig.capsule().target["warnings"] == ["target_acceptance_metric_probe_unavailable"]
    assert rig.authority.current.record.quality_reader is None


def test_late_result_is_not_complete_even_if_reader_returns_metrics(monkeypatch):
    rig = TargetRig()
    rig.publish()
    times = iter([100, 161])
    monkeypatch.setattr("dpone.runtime.governance.quality_replay.monotonic", lambda: next(times))
    with pytest.raises(TargetAcceptanceError, match="INCOMPLETE"):
        rig.fresh_session().replay(rig.config)
    assert rig.capsule().state == "TARGET_PENDING"
    assert rig.authority.current.record.quality_reader is None


def test_competing_reader_and_successor_cannot_enter_during_observation():
    from dpone.runtime.sinks.clickhouse_full_refresh_publication import SCHEDULER_IDENTITY_OPTION

    rig = TargetRig()
    rig.publish()
    successor = replace(rig.config, options={**rig.config.options, SCHEDULER_IDENTITY_OPTION: "next"})

    def compete(value):
        with pytest.raises(ReplayQualityEvidenceError, match="INCOMPLETE") as competing:
            rig.fresh_session().replay(rig.config)
        assert competing.value.replay_details["target_commit"] == "proven"
        with pytest.raises(ReplayQualityEvidenceError, match="INCOMPLETE"):
            rig.service.prepare_admission(successor)

    rig.reader.change = compete
    rig.fresh_session().replay(rig.config)
    assert rig.reader.scans == 1 and rig.capsule().state == "COMPLETE"


def test_target_receipt_is_consumed_under_guard_before_verified_release(monkeypatch):
    rig = TargetRig()
    rig.publish()
    session = rig.fresh_session()
    real_accept = session.execution.accept_state

    def guarded_accept(receipt, *, load_config):
        assert rig.authority.current.record.quality_reader is not None
        assert rig.capsule().state == "COMPLETE"
        assert session.accepted_receipt is None
        return real_accept(receipt, load_config=load_config)

    monkeypatch.setattr(session.execution, "accept_state", guarded_accept)
    receipt, _ = session.replay(rig.config)
    assert session.accepted_receipt is receipt
    assert rig.authority.current.record.quality_reader is None


def test_original_finalization_uses_bounded_reader_and_no_legacy_target_probe():
    from types import SimpleNamespace

    from dpone.runtime.artifacts import InMemoryRowsArtifact
    from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricSnapshot
    from dpone.runtime.governance.finalization import LoadGovernanceFinalizationCoordinator
    from dpone.runtime.sinks.load_payload import LoadPayload
    from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
    from dpone.runtime.sources.base import ExtractResult
    from tests.test_native_load_governance_finalization import _load_record
    from tests.test_quality_replay_runtime import SCHEMA

    rig = TargetRig()
    rig.session.execution.replay_session = rig.session
    captured = []

    class Probe:
        def collect(self, request):
            assert request.side != "target"
            captured.append(request.side)
            return AcceptanceMetricSnapshot(
                request.side, row_count=2, columns=("id",), dataset=request.dataset_identity
            )

    class Sink:
        metric_probe = Probe()
        published = None

        def stage_payload(self, *_args):
            return rig.handle

        def finalize_staged_load(self, config, handle):
            self.published = rig.service.publish(config, rig.candidate, staged_rows=2)
            return LoadResult(
                inserted_rows=2,
                updated_rows=0,
                total_rows=2,
                staging_rows=2,
                commit_outcome=AtomicCommitOutcome.COMMITTED,
                commit_receipt_id=self.published.authority.operation_id,
            )

        def cleanup_staged_load(self, handle):
            if self.published:
                rig.service.cleanup(self.published)

        def abort_staged_load(self, handle):
            assert rig.ddl.dispatches == 0

    artifact = InMemoryRowsArtifact([{"id": 1}, {"id": 2}])
    result = LoadGovernanceFinalizationCoordinator().load(
        source=SimpleNamespace(metric_probe=Probe()),
        sink=Sink(),
        load_config=rig.config,
        payload=LoadPayload(artifact=artifact, schema=SCHEMA),
        extract_result=ExtractResult(artifact=artifact, schema=SCHEMA),
        load_record=_load_record(),
        quality_execution=rig.session.execution,
    )
    assert captured == ["source", "staged"]
    assert rig.reader.scans == 1 and rig.capsule().state == "COMPLETE"
    assert result.quality_gate_receipt is rig.session.accepted_receipt
    assert set(result.reconciliation_metrics["acceptance_metrics"]["snapshots"]) == {"source", "staged", "target"}


def test_post_completion_bookkeeping_failure_keeps_committed_truth():
    from types import SimpleNamespace

    from dpone.runtime.governance.finalization_support import preserve_completed_replay_truth

    error = RuntimeError("unsafe driver detail")
    preserve_completed_replay_truth(
        error,
        {"replayed_from": {"run_id": "original", "load_id": "original-load"}},
        SimpleNamespace(run_id="current", load_id="current-load"),
    )
    assert error.replay_details == {
        "target_commit": "proven",
        "governance": "blocked",
        "error_code": "DPONE_REPLAY_QUALITY_EVIDENCE_INCOMPLETE",
        "original_run_id": "original",
        "original_load_id": "original-load",
        "current_run_id": "current",
        "current_load_id": "current-load",
    }


def test_nonselected_replica_is_not_accepted_even_when_admitted():
    rig = TargetRig()
    rig.publish()
    rig.reader.change = lambda value: value.update(replica=rig.catalog.inventory("one_shard").hosts[-1])
    with pytest.raises(TargetAcceptanceError, match="MISMATCH"):
        rig.fresh_session().replay(rig.config)
    assert rig.capsule().state == "FAILED"


def test_retaining_another_target_does_not_retain_successful_reader():
    rig = TargetRig()
    rig.publish()
    session = rig.fresh_session()
    other = replace(rig.config, target_table="other_target")

    def retain_other(value):
        session.store.retain_guard(other)

    rig.reader.change = retain_other
    session.replay(rig.config)
    assert session.accepted_receipt is not None
    assert rig.authority.current.record.quality_reader is None


def test_normally_returning_with_retained_guard_never_accepts_receipt():
    rig = TargetRig()
    rig.publish()
    session = rig.fresh_session()
    rig.reader.change = lambda value: session.store.retain_guard(rig.config)
    with pytest.raises(ReplayQualityEvidenceError, match="INCOMPLETE"):
        session.replay(rig.config)
    assert session.accepted_receipt is None
    assert rig.authority.current.record.quality_reader is not None
