"""Historical loader injection must survive delegation to canonical strategies."""

from functools import partial
from types import SimpleNamespace

import pytest

from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.strategies.postgres.file_export_loader import PostgresFileExportLoader
from dpone.runtime.sinks.strategies.postgres.internal_query_loader import PostgresInternalQueryLoader
from tests.test_postgres_strategy_preservation import RecordingConnector, config, payload
from tests.test_runtime_postgres_strategy_split import StubCopy, StubCursor, StubLogger


@pytest.mark.parametrize("entry", ["internal", "load", "load_standard", "load_with_truncate", "load_with_exchange"])
@pytest.mark.parametrize("mode", [None, "exchange"])
@pytest.mark.parametrize("sample_failure", [False, True])
def test_legacy_loader_honors_injected_target_policy_and_sample(tmp_path, entry, mode, sample_failure):
    connector = RecordingConnector()
    connector.connection = SimpleNamespace(cursor=lambda: StubCursor(StubCopy()))
    logger = StubLogger()
    cfg = config(overwrite_type=mode)
    cfg.log_sample_rows = 1
    calls = []
    sample_error = RuntimeError("custom sample failed")

    def ensure_target(actual_config, schema):
        assert actual_config is cfg
        assert schema == [("id", "integer")]
        calls.append("target")
        return False

    def ensure_technical(actual_config):
        assert actual_config is cfg
        calls.append("technical")

    def sample(actual_config, limit):
        assert actual_config is cfg
        assert limit == 1
        assert not any(op[0] == "COMMIT" for op in connector.operations)
        calls.append("sample")
        if sample_failure:
            raise sample_error

    manager = SimpleNamespace(ensure_target_table=ensure_target, ensure_technical_columns=ensure_technical)
    path = tmp_path / "input.csv"
    if entry == "internal":
        loader = PostgresInternalQueryLoader(connector, logger, manager, sample)
        invoke = partial(loader.load, cfg, payload())
    else:
        path.write_text("1\n2\n")
        artifact = FileExportArtifact(str(path), columns=["id"], compressed=False, format="csv")
        batch = LoadPayload(artifact, [("id", "integer")])
        loader = PostgresFileExportLoader(connector, logger, manager, sample)
        args = (cfg, batch) if entry == "load" else (cfg, batch, artifact)
        invoke = partial(getattr(loader, entry), *args)

    if sample_failure:
        with pytest.raises(RuntimeError) as raised:
            invoke()
        assert raised.value is sample_error
        assert connector.operations[-1][0] == "ROLLBACK"
        assert not any(op[0] == "COMMIT" for op in connector.operations)
    else:
        assert invoke().inserted_rows == 2
        assert connector.operations[-1][0] == "COMMIT"
    assert calls == ["technical" if mode == "exchange" else "target", "sample"]
    assert not any(event == "TARGET" for event, _ in logger.progress)
    assert not any(op[0].startswith("SELECT *") for op in connector.operations)
    assert not path.exists()
