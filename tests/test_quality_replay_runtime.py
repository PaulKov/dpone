"""Synthetic committed replay through the real publication and quality services."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.contracts.quality_replay import QualityReplayCapsule, ReplayQualityEvidenceError
from dpone.governance.quality import QualityProbeSnapshot
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricRun, AcceptanceMetricSnapshot
from dpone.runtime.governance.finalization import LoadGovernanceFinalizationCoordinator
from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.governance.quality_execution import QualityExecutionSnapshot, QualityGateExecution
from dpone.runtime.governance.quality_replay import QualityReplaySession
from dpone.runtime.sinks.clickhouse_cluster_full_refresh_publication import (
    ClickHouseClusterFullRefreshPublicationService,
)
from dpone.runtime.sinks.clickhouse_full_refresh_publication import REPLAY_OPTION, SCHEDULER_IDENTITY_OPTION
from dpone.runtime.sinks.clickhouse_replay_quality import ClickHouseReplayQualityStore
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult
from dpone.runtime.sources.base import ExtractResult
from tests.test_clickhouse_cluster_full_refresh_publication import _Authority, _Bootstrap, _Catalog, _config
from tests.test_clickhouse_cluster_full_refresh_publication import _Ddl as PublicationDdl
from tests.test_native_load_governance_finalization import _load_record
from tests.test_runtime_etl_processor_split import StubLogger, StubSink, StubSource

SCHEMA = [("id", "Int32")]
GATES = {"gates": [{"id": "rows", "type": "row_count_reconciliation"}]}


class Authority(_Authority):
    def require_ready(self, cluster, database, hosts):
        assert cluster and database and len(hosts) == 2


class Ddl(PublicationDdl):
    lose_read_ack = False

    def find_entries(self, cluster, token):
        if self.lose_read_ack:
            self.lose_read_ack = False
            raise ConnectionError("synthetic read acknowledgement lost")
        return super().find_entries(cluster, token)


class Rig:
    def __init__(self, quality=None):
        self.catalog, self.authority = _Catalog(), Authority()
        self.ddl = Ddl(self.catalog)
        base = _config()
        self.config = replace(base, options={**base.options, "quality": quality or GATES})
        self.store = self.fresh_store()
        self.service = ClickHouseClusterFullRefreshPublicationService(
            self.catalog,
            lambda _: self.authority,
            self.ddl,
            _Bootstrap(),
            quality_store=self.store,
        )
        self.candidate = replace(self.config, target_table="candidate")
        self.handle = StagedLoadHandle(self.candidate, SCHEMA, 2)
        self.session = self.fresh_session(store=self.store)

    def fresh_store(self):
        return ClickHouseReplayQualityStore(self.catalog, lambda _: self.authority)

    def fresh_session(self, config=None, store=None):
        config = config or self.config
        execution = QualityGateExecution(
            QualityExecutionSnapshot.from_load_config(config),
            run_id="synthetic-run",
            load_id="synthetic-load",
        )
        return QualityReplaySession(store or self.fresh_store(), config, execution)

    def stage(self):
        execution = self.session.execution
        execution.select_boundary("pre_commit", load_config=self.config)
        receipt = execution.evaluate(
            load_config=self.config,
            boundary="pre_commit",
            source_snapshot=QualityProbeSnapshot(row_count=2),
            target_snapshot=QualityProbeSnapshot(row_count=2),
        )
        acceptance = AcceptanceMetricRun(execution.snapshot.acceptance_policy)
        if acceptance.policy.enabled:
            for side in acceptance.policy.requested_sides:
                acceptance.add(
                    AcceptanceMetricSnapshot(
                        side,
                        row_count=2,
                        columns=("id",),
                        dataset=f"synthetic:{side}",
                        null_counts={"id": 0} if acceptance.policy.null_counts != "off" else {},
                    )
                )
        self.session.prepare(
            config=self.config,
            handle=self.handle,
            extract_result=SimpleNamespace(schema=SCHEMA),
            receipt=receipt,
            acceptance=acceptance,
        )

    def publish(self, complete=False):
        self.stage()
        receipt = self.service.publish(self.config, self.candidate, staged_rows=2)
        if complete:
            self.session.finish_original(self.config)
        self.service.cleanup(receipt)

    def overwrite_record(self, **changes):
        current = self.authority.current
        self.authority.compare_and_swap(current, replace(current.record, **changes))

    def capsule(self):
        return QualityReplayCapsule.parse(self.authority.current.record.quality_evidence)


def test_lost_read_ack_fresh_session_reconciles_and_completes_without_redispatch():
    rig = Rig()
    rig.stage()
    rig.ddl.lose_read_ack = True
    with pytest.raises(ConnectionError, match="acknowledgement"):
        rig.service.publish(rig.config, rig.candidate, staged_rows=2)
    assert rig.catalog.committed and rig.capsule().state == "PREPARED"
    admitted = rig.service.prepare_admission(rig.config)
    receipt, evidence = rig.fresh_session(admitted).replay(admitted)
    assert receipt.report.passed and evidence["replayed_from"]["load_id"] == "synthetic-load"
    assert rig.capsule().state == "COMPLETE"
    assert rig.authority.current.record.quality_reader is None
    assert rig.ddl.dispatches == rig.ddl.cleanup_dispatches == 1


def test_successful_publication_supports_two_fresh_replays_without_mutation():
    rig = Rig()
    rig.publish(complete=True)
    immutable_core = rig.capsule().core_digest
    for _ in range(2):
        admitted = rig.service.prepare_admission(rig.config)
        receipt, evidence = rig.fresh_session(admitted).replay(admitted)
        assert receipt.report.passed and evidence["core_digest"] == immutable_core
        assert rig.authority.current.record.quality_reader is None
    assert rig.ddl.dispatches == rig.ddl.cleanup_dispatches == 1


@pytest.mark.parametrize("bad_evidence", ["missing", "tampered", "failed"])
def test_missing_tampered_or_terminal_failed_capsule_never_authorizes_replay(bad_evidence):
    rig = Rig()
    rig.publish()
    capsule = rig.capsule()
    raw = {
        "missing": None,
        "tampered": capsule.payload.replace('"row_count":2', '"row_count":3', 1),
        "failed": capsule.advance("FAILED", authority_version=rig.authority.current.version + 1).payload,
    }[bad_evidence]
    rig.overwrite_record(quality_evidence=raw)
    with pytest.raises(ReplayQualityEvidenceError):
        rig.fresh_session().replay(rig.config)
    assert rig.ddl.dispatches == 1
    assert rig.authority.current.record.quality_reader is None


@pytest.mark.parametrize("drift", ["config", "policy", "schema", "generation", "inventory"])
def test_exact_bindings_reject_drift_after_completed_publication(drift):
    rig = Rig()
    rig.publish(complete=True)
    config = rig.config
    if drift == "config":
        config = replace(config, batch_size=7)
    elif drift == "policy":
        config = replace(
            config,
            options={**config.options, "quality": {"gates": [{"id": "other", "type": "min_rows", "threshold": 1}]}},
        )
    elif drift == "schema":
        rig.catalog.new = replace(rig.catalog.new, schema_digest="different-schema")
    elif drift == "generation":
        rig.catalog.new = replace(rig.catalog.new, uuid="successor")
    else:
        rig.catalog.inventory_address = "127.0.0.9"
    with pytest.raises(ReplayQualityEvidenceError):
        rig.fresh_session(config).replay(config)
    assert rig.ddl.dispatches == 1


def acceptance_policy(*, target=False):
    return {
        "acceptance": {
            "enabled": True,
            "mode": "required",
            "capture": {"source": True, "staged": True, "target": target},
        }
    }


def test_acceptance_only_source_staged_records_survive_fresh_replay():
    rig = Rig(acceptance_policy())
    rig.publish(complete=True)
    _, evidence = rig.fresh_session().replay(rig.config)
    assert set(evidence["acceptance"]) == {"source", "staged"}
    assert all(value["row_count"] == 2 for value in evidence["acceptance"].values())


@pytest.mark.parametrize("state", ["PREPARED", "TARGET_PENDING", "FAILED"])
def test_slot_reuse_cannot_retire_unfinished_governance(state):
    rig = Rig()
    rig.publish()
    if state != "PREPARED":
        rig.overwrite_record(
            quality_evidence=rig.capsule().advance(state, authority_version=rig.authority.current.version + 1).payload
        )
    successor = replace(rig.config, options={**rig.config.options, SCHEDULER_IDENTITY_OPTION: "next-invocation"})
    with pytest.raises(ReplayQualityEvidenceError, match="INCOMPLETE"):
        rig.service.prepare_admission(successor)
    assert rig.ddl.dispatches == 1


def test_untrusted_dto_does_not_declare_store_capability():
    config = replace(_config(), options={**_config().options, "quality": GATES})
    execution = QualityGateExecution(QualityExecutionSnapshot.from_load_config(config), run_id="run", load_id="load")
    with pytest.raises(ReplayQualityEvidenceError, match="UNSUPPORTED"):
        QualityReplaySession(SimpleNamespace(committed=lambda _: {}), config, execution)


def test_target_capture_is_unsupported_before_source_or_publication():
    with pytest.raises(ReplayQualityEvidenceError, match="UNSUPPORTED"):
        Rig(acceptance_policy(target=True))


def test_processor_replay_uses_real_authority_with_source_and_mutation_traps():
    rig = Rig()
    rig.publish(complete=True)

    class Source(StubSource):
        def extract(self, *_args):
            pytest.fail("committed replay performed source extraction")

        def get_incremental_state(self, *_args):
            pytest.fail("committed replay read source state")

    class Sink(StubSink):
        quality_replay_store = rig.fresh_store()

        def prepare_runtime_admission(self, config, **_kwargs):
            return rig.service.prepare_admission(config)

        def replay_result(self, config):
            return config.options.get(REPLAY_OPTION)

        def load(self, *_args):
            pytest.fail("committed replay performed mutation")

    for _ in range(2):
        result = ETLProcessor(Source(None), Sink(), etl_logger=StubLogger()).run(rig.config)
        assert result["status"] == "success", result
        assert result["reconciliation_metrics"]["quality_replay"]["replayed_from"]["run_id"] == "synthetic-run"
    assert rig.ddl.dispatches == rig.ddl.cleanup_dispatches == 1


def test_config_cannot_change_between_negotiation_and_replay():
    rig = Rig()
    rig.publish(complete=True)
    session = rig.fresh_session()
    changed = replace(rig.config, batch_size=7)
    with pytest.raises(ReplayQualityEvidenceError, match="MISMATCH"):
        session.replay(changed)


@pytest.mark.parametrize("count", [None, True, -1])
def test_acceptance_required_invalid_row_observation_is_not_success(count):
    rig = Rig(acceptance_policy())
    rig.publish()
    core = rig.capsule().core
    core["acceptance"]["source"]["row_count"] = count
    rig.overwrite_record(quality_evidence=QualityReplayCapsule.prepare(core).payload)
    with pytest.raises(ReplayQualityEvidenceError):
        rig.fresh_session().replay(rig.config)


@pytest.mark.parametrize("mutation", ["side_missing", "field_missing", "unknown_warning", "wrong_side", "not_captured"])
def test_acceptance_malformed_side_records_fail_closed(mutation):
    rig = Rig(acceptance_policy())
    rig.publish()
    core = rig.capsule().core
    source = core["acceptance"]["source"]
    if mutation == "side_missing":
        core["acceptance"].pop("source")
    elif mutation == "field_missing":
        source.pop("row_count")
    elif mutation == "unknown_warning":
        source["warnings"] = ["unknown_warning"]
    elif mutation == "wrong_side":
        source["side"] = "target"
    else:
        source["captured_before_cleanup"] = False
    rig.overwrite_record(quality_evidence=QualityReplayCapsule.prepare(core).payload)
    with pytest.raises(ReplayQualityEvidenceError):
        rig.fresh_session().replay(rig.config)


def test_explicit_warn_only_unavailable_snapshot_remains_visible():
    quality = acceptance_policy()
    quality["acceptance"]["mode"] = "warn_only"
    rig = Rig(quality)
    rig.publish()
    core = rig.capsule().core
    core["acceptance"]["source"].update(row_count=None, warnings=["acceptance_metric_probe_failed"])
    rig.overwrite_record(quality_evidence=QualityReplayCapsule.prepare(core).payload)
    _, evidence = rig.fresh_session().replay(rig.config)
    assert evidence["acceptance"]["source"]["warnings"] == ["acceptance_metric_probe_failed"]


def test_required_explicit_metric_selection_cannot_disappear_from_recorded_columns():
    quality = acceptance_policy()
    quality["acceptance"]["checks"] = {"null_counts": ["id"]}
    rig = Rig(quality)
    rig.publish()
    core = rig.capsule().core
    core["acceptance"]["source"].update(columns=[], null_counts={})
    rig.overwrite_record(quality_evidence=QualityReplayCapsule.prepare(core).payload)
    with pytest.raises(ReplayQualityEvidenceError):
        rig.fresh_session().replay(rig.config)


def test_managed_successor_is_fenced_while_generation_read_guard_is_held():
    rig = Rig()
    rig.publish(complete=True)
    successor = replace(rig.config, options={**rig.config.options, SCHEDULER_IDENTITY_OPTION: "next"})
    with rig.fresh_store().committed(rig.config):
        with pytest.raises(ReplayQualityEvidenceError, match="INCOMPLETE"):
            rig.service.prepare_admission(successor)
    assert rig.service.prepare_admission(successor) == successor


@pytest.mark.parametrize("source_rows", [1, 2])
def test_original_coordinator_stages_quality_before_actual_publication(source_rows):
    rig = Rig()
    rig.session.execution.replay_session = rig.session

    class Sink:
        published = None

        def stage_payload(self, _config, _payload):
            return rig.handle

        def finalize_staged_load(self, config, _handle):
            self.published = rig.service.publish(config, rig.candidate, staged_rows=2)
            return LoadResult(
                inserted_rows=2,
                updated_rows=0,
                total_rows=2,
                staging_rows=2,
                commit_outcome=AtomicCommitOutcome.COMMITTED,
                commit_receipt_id=self.published.authority.operation_id,
            )

        def cleanup_staged_load(self, _handle):
            if self.published is not None:
                rig.service.cleanup(self.published)

        def abort_staged_load(self, _handle):
            assert rig.ddl.dispatches == 0

    artifact = InMemoryRowsArtifact([{"id": value} for value in range(source_rows)])
    args = dict(
        sink=Sink(),
        load_config=rig.config,
        payload=LoadPayload(artifact=artifact, schema=SCHEMA),
        extract_result=ExtractResult(artifact=artifact, schema=SCHEMA),
        load_record=_load_record(),
        quality_execution=rig.session.execution,
    )
    if source_rows == 1:
        with pytest.raises(RuntimeError, match="quality gates failed"):
            LoadGovernanceFinalizationCoordinator().load(**args)
        assert rig.ddl.dispatches == 0 and rig.authority.current is None
    else:
        assert LoadGovernanceFinalizationCoordinator().load(**args).total_rows == 2
        assert rig.capsule().state == "COMPLETE"
        assert rig.fresh_session().replay(rig.config)[0].report.passed
        assert rig.ddl.dispatches == rig.ddl.cleanup_dispatches == 1
