"""A failed capability negotiation must never use the committed-warning fallback."""

from dataclasses import replace

import pytest

from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError
from dpone.contracts.quality_replay import ReplayQualityEvidenceError
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.sinks.clickhouse_full_refresh_publication import REPLAY_OPTION, SCHEDULER_IDENTITY_OPTION
from dpone.runtime.sinks.clickhouse_replay_quality import ClickHouseReplayQualityStore
from tests.test_quality_replay_runtime import Rig
from tests.test_runtime_etl_processor_split import StubLogger, StubSink, StubSource


@pytest.mark.parametrize("error", [ConnectionError("synthetic read failed"), ClusterPublicationError("X", "synthetic")])
def test_readiness_failure_after_proven_commit_is_blocking(error):
    rig = Rig()
    rig.publish(complete=True)

    class UnavailableStore(ClickHouseReplayQualityStore):
        def require_ready(self, _config):
            raise error

    class Source(StubSource):
        def extract(self, *_args):
            pytest.fail("source extraction on committed replay")

    class Sink(StubSink):
        quality_replay_store = UnavailableStore(rig.catalog, lambda _: rig.authority)

        def prepare_runtime_admission(self, config, **_kwargs):
            return rig.service.prepare_admission(config)

        def replay_result(self, config):
            return config.options.get(REPLAY_OPTION)

        def load(self, *_args):
            pytest.fail("target mutation on committed replay")

    with pytest.raises(ReplayQualityEvidenceError, match="UNSUPPORTED") as observed:
        ETLProcessor(Source(None), Sink(), etl_logger=StubLogger()).run(rig.config)
    assert observed.value.blocks_committed_success
    assert rig.ddl.dispatches == 1
    assert rig.authority.current.record.phase.value == "COMPLETED"


def test_reset_authority_version_rejects_future_completion_before_guard_mutation():
    rig = Rig()
    rig.publish(complete=True)
    completed = rig.capsule()
    assert completed.completion_authority_version > 0
    reset = replace(rig.authority.current, version=0)
    rig.authority.current = reset
    with pytest.raises(ReplayQualityEvidenceError, match="MISMATCH"):
        rig.fresh_session().replay(rig.config)
    assert rig.authority.current is reset
    assert rig.ddl.dispatches == 1


def test_reset_authority_version_cannot_retire_quality_for_successor():
    rig = Rig()
    rig.publish(complete=True)
    reset = replace(rig.authority.current, version=0)
    rig.authority.current = reset
    successor = replace(rig.config, options={**rig.config.options, SCHEDULER_IDENTITY_OPTION: "successor"})
    with pytest.raises(ReplayQualityEvidenceError, match="MISMATCH"):
        rig.service.prepare_admission(successor)
    assert rig.authority.current is reset
    assert rig.ddl.dispatches == 1
