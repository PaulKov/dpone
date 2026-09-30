"""Default application composition from hydrated endpoints through publication."""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from types import SimpleNamespace

import pytest
from tests.integration.mssql.mssql_live_support import clickhouse_connector, mssql_connector
from tools.mssql_stress_governance import governed_mssql_route

from dpone.adapters.mssql_native_recovery_journal import MssqlNativeRecoveryJournalReader
from dpone.app.mssql_native_recovery_application import MssqlNativeRecoveryApplication
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.lineage.audit import LoadIdentityService
from dpone.runtime.mssql_native_application import MssqlNativeApplicationRuntime, _NativeRuntimeAssembly
from dpone.runtime.process_logging import etl_logger
from dpone.runtime.sources.clickhouse import ClickHouseSource
from dpone.runtime.sources.clickhouse_native_source import NativeQueryArtifact

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql, pytest.mark.integration_clickhouse]


@pytest.mark.parametrize("import_backend", ["bcp", "mssql_sqlclient"])
@pytest.mark.parametrize("fault", ["none", "postpublication", "pre_eof"])
def test_default_native_runtime_executes_hydrated_route(
    tmp_path, monkeypatch: pytest.MonkeyPatch, import_backend: str, fault: str
) -> None:
    if import_backend == "mssql_sqlclient" and os.environ.get("DPONE_RUN_SQLCLIENT_LIVE") != "1":
        pytest.skip("set DPONE_RUN_SQLCLIENT_LIVE=1 inside the certified Linux x86-64 runner")
    source_connector = clickhouse_connector()
    target_connector = mssql_connector()
    suffix = uuid.uuid4().hex[:16]
    source_table = f"dpone_default_native_source_{suffix}"
    target_table = f"dpone_default_native_target_{suffix}"
    layout_version = 2 if import_backend == "mssql_sqlclient" else 1
    try:
        source_connector.execute_query(
            f"CREATE TABLE `{source_table}` (row_key Int64, ratio Nullable(Float64), "
            "text_value Nullable(String), happened_at Nullable(DateTime64(6, 'UTC'))) "
            "ENGINE=MergeTree ORDER BY row_key"
        )
        source_connector.execute_query(
            f"INSERT INTO `{source_table}` VALUES "
            "(1,-1.5,'alpha','2024-02-29 23:59:59.999999'),"
            "(2,0.0,'',NULL),(3,NULL,NULL,'2030-06-01 12:30:45.123456'),"
            "(3,NULL,NULL,'2030-06-01 12:30:45.123456')"
        )
        target_connector.execute_query(
            f"CREATE TABLE [dbo].[{target_table}] ("
            "[row_key] bigint NOT NULL,[ratio] float NULL,[text_value] nvarchar(max) NULL,"
            "[happened_at] datetime2(6) NULL)"
        )
        with governed_mssql_route(
            target_connector,
            target_database=target_connector.database,
            target_schema="dbo",
            target_table=target_table,
        ) as route:
            sink = route.sink(logger=etl_logger)
            source = ClickHouseSource(source_connector, sink.logger, sink_connector=target_connector)
            execution = {
                "import_backend": import_backend,
                "verification_backend": "target_local",
                "chunking": {"mode": "bounded_stream", "checkpointing": "resumable", "parallelism": 1},
                "native_chunks": {
                    "max_total_encoded_bytes": 8 << 20,
                    "stage_allocated_bytes_stop_threshold": 1 << 30,
                    "max_rows": 1 if fault == "pre_eof" else 1024,
                    "max_bytes": 4 << 20,
                    "max_row_bytes": 1 << 20,
                    "max_pending": 1,
                    "max_staging_tables": 32,
                },
            }
            if import_backend == "mssql_sqlclient":
                execution["layout_version"] = layout_version
            config = LoadConfig(
                "source",
                "target",
                source_connector.database,
                source_table,
                "dbo",
                target_table,
                target_database=target_connector.database,
                staging_database=target_connector.database,
                staging_schema="dbo",
                load_strategy=LoadStrategy.FULL_REFRESH,
                options={
                    "source_type": "clickhouse",
                    "sink_type": "mssql",
                    "lineage": False,
                    "native_transfer": {
                        "wire": {"mode": "typed_binary", "binary_format": "mssql_native"},
                        "execution": execution,
                    },
                },
            )
            process = SimpleNamespace(
                name="default_native_live",
                load_config=config,
                source_obj=source,
                sink_obj=sink,
                load_identity_service=LoadIdentityService(),
                raw_config={
                    "runtime": {
                        "storage": {
                            "work_dir": str(tmp_path / "work"),
                            "checkpoint_dir": str(tmp_path / "state"),
                            "evidence_dir": str(tmp_path / "evidence"),
                            "debug_dir": str(tmp_path / "debug"),
                            "min_free_bytes": 1,
                        }
                    }
                },
                ensure_runtime_bindings=lambda: None,
            )
            original_evidence = _NativeRuntimeAssembly._evidence
            original_rows = NativeQueryArtifact.iter_native_rows
            if fault == "postpublication":
                monkeypatch.setattr(
                    _NativeRuntimeAssembly,
                    "_evidence",
                    lambda *_args, **_kwargs: (_ for _ in ()).throw(
                        RuntimeError("synthetic_postpublication_evidence_failure")
                    ),
                )
                with pytest.raises(RuntimeError, match="synthetic_postpublication_evidence_failure"):
                    MssqlNativeApplicationRuntime(process).run(config, owner=f"default-{suffix}")
                monkeypatch.setattr(_NativeRuntimeAssembly, "_evidence", original_evidence)
            elif fault == "pre_eof":

                def interrupted_rows(artifact):
                    for ordinal, row in enumerate(original_rows(artifact)):
                        if ordinal == 2:
                            raise RuntimeError("synthetic_source_interrupted_before_eof")
                        yield row

                monkeypatch.setattr(NativeQueryArtifact, "iter_native_rows", interrupted_rows)
                with pytest.raises(RuntimeError, match="synthetic_source_interrupted_before_eof"):
                    MssqlNativeApplicationRuntime(process).run(config, owner=f"default-{suffix}")
                monkeypatch.setattr(NativeQueryArtifact, "iter_native_rows", original_rows)
            if fault != "none":
                journals = list((tmp_path / "state" / "mssql-native").glob("*.sqlite"))
                assert len(journals) == 1
                reader = MssqlNativeRecoveryJournalReader(journals[0])
                inventory = reader.inspect_all()
                assert len(inventory["items"]) == 1
                invocation_id = inventory["items"][0]["invocation_id"]
                if fault == "postpublication":
                    assert inventory["items"][0]["state"] == "PUBLISHED"

                class SourceForbidden:
                    def __getattr__(self, _name):
                        pytest.fail("source access is forbidden during MSSQL native recovery")

                source.connector = SourceForbidden()
                action = "resume" if fault == "postpublication" else "retire"
                recovered = MssqlNativeRecoveryApplication().execute(
                    process,
                    journal_root=journals[0],
                    invocation_id=invocation_id,
                    action=action,
                    owner=f"recovery-{suffix}",
                    confirmed=True,
                )
                expected_states = {"CUSTODY_RELEASED"} if fault == "postpublication" else {"EMPTY_STAGING", "RETIRED"}
                assert recovered["state"] in expected_states
            else:
                result = MssqlNativeApplicationRuntime(process).run(config, owner=f"default-{suffix}")
                assert result.status == "success"
                assert result.extracted_rows == 4
            actual = target_connector.get_records(
                f"SELECT [row_key],[ratio],[text_value],[happened_at] FROM [dbo].[{target_table}] ORDER BY [row_key]"
            )
            expected = [
                (1, -1.5, "alpha", datetime(2024, 2, 29, 23, 59, 59, 999999)),
                (2, 0.0, "", None),
                (3, None, None, datetime(2030, 6, 1, 12, 30, 45, 123456)),
                (3, None, None, datetime(2030, 6, 1, 12, 30, 45, 123456)),
            ]
            assert actual == ([] if fault == "pre_eof" else expected)
    finally:
        target_connector.execute_query(f"DROP TABLE IF EXISTS [dbo].[{target_table}]")
        source_connector.execute_query(f"DROP TABLE IF EXISTS `{source_table}`")
        target_connector.close()
        source_connector.close()
