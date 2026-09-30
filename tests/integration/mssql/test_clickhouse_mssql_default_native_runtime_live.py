"""Default application composition from hydrated endpoints through publication."""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from types import SimpleNamespace

import pytest
from tests.integration.mssql.mssql_live_support import clickhouse_connector, mssql_connector
from tests.integration.mssql.mssql_sqlclient_mutation_cases import inject_stage_mutation
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


@pytest.mark.parametrize(
    ("import_backend", "fault"),
    [(backend, fault) for backend in ("bcp", "mssql_sqlclient") for fault in ("none", "postpublication", "pre_eof")]
    + [
        ("mssql_sqlclient", f"{stage}:{mutation}")
        for stage in ("raw", "prepared")
        for mutation in ("insert", "update", "delete", "truncate", "delete_insert")
    ],
)
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
    cleanup_stages: list[str] = []
    try:
        source_connector.execute_query(
            f"CREATE TABLE `{source_table}` (row_key Int64, ratio Nullable(Float64), "
            "text_value Nullable(String), happened_at Nullable(DateTime64(6, 'UTC')), "
            "nullable_key Nullable(Int64), required_ratio Float64, "
            "required_text String, required_at DateTime64(6, 'UTC')) "
            "ENGINE=MergeTree ORDER BY row_key"
        )
        source_connector.execute_query(
            f"INSERT INTO `{source_table}` VALUES "
            "(1,-1.5,'alpha','2024-02-29 23:59:59.999999',NULL,-2.5,'λ','2024-01-01'),"
            "(2,0.0,'',NULL,-9223372036854775808,0.0,'','2024-01-02'),"
            "(3,NULL,NULL,'2030-06-01 12:30:45.123456',9223372036854775807,3.5,'tail','2024-01-03'),"
            "(3,NULL,NULL,'2030-06-01 12:30:45.123456',9223372036854775807,3.5,'tail','2024-01-03')"
        )
        target_connector.execute_query(
            f"CREATE TABLE [dbo].[{target_table}] ("
            "[row_key] bigint NOT NULL,[ratio] float NULL,[text_value] nvarchar(max) NULL,"
            "[happened_at] datetime2(6) NULL,[nullable_key] bigint NULL,[required_ratio] float NOT NULL,"
            "[required_text] nvarchar(max) NOT NULL,[required_at] datetime2(6) NOT NULL)"
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
            elif ":" in fault:
                stage, mutation = fault.split(":", 1)
                with monkeypatch.context() as injected:
                    observed = inject_stage_mutation(injected, target_connector, stage, mutation, cleanup_stages)
                    diagnostic = (
                        "persisted_hash_count"
                        if mutation in {"insert", "delete", "truncate"}
                        else "stage_mutation_detected"
                        if stage == "raw"
                        else "prepared_content_changed"
                    )
                    with pytest.raises(ValueError, match=f"mssql_native.{diagnostic}"):
                        MssqlNativeApplicationRuntime(process).run(config, owner=f"default-{suffix}")
                    assert observed == [fault]
                assert target_connector.get_records(f"SELECT COUNT_BIG(*) FROM [dbo].[{target_table}]") == [(0,)]
            if fault != "none":
                journals = list((tmp_path / "state" / "mssql-native").glob("*.sqlite"))
                assert len(journals) == 1
                reader = MssqlNativeRecoveryJournalReader(journals[0])
                inventory = reader.inspect_all()
                assert len(inventory["items"]) == 1
                invocation_id = inventory["items"][0]["invocation_id"]
                if fault == "postpublication":
                    assert inventory["items"][0]["state"] == "PUBLISHED"
                elif ":" in fault:
                    assert inventory["items"][0]["state"] == "VERIFIED_EOF"
                    assert reader.load(invocation_id).projection["publication"]["phase"] == "prepared"

                class SourceForbidden:
                    def __getattr__(self, _name):
                        pytest.fail("source access is forbidden during MSSQL native recovery")

                source.connector = SourceForbidden()
                action = "retire" if fault == "pre_eof" else "resume"

                def recover():
                    return MssqlNativeRecoveryApplication().execute(
                        process,
                        journal_root=journals[0],
                        invocation_id=invocation_id,
                        action=action,
                        owner=f"recovery-{suffix}",
                        confirmed=True,
                    )

                if ":" in fault:
                    with pytest.raises(ValueError, match=f"mssql_native.{diagnostic}"):
                        recover()
                    snapshot = reader.load(invocation_id)
                    assert snapshot.custody is not None and snapshot.custody.state == "held"
                else:
                    recovered = recover()
                    expected_states = (
                        {"CUSTODY_RELEASED"} if fault == "postpublication" else {"EMPTY_STAGING", "RETIRED"}
                    )
                    assert recovered["state"] in expected_states
            else:
                result = MssqlNativeApplicationRuntime(process).run(config, owner=f"default-{suffix}")
                assert result.status == "success"
                assert result.extracted_rows == 4
            actual = target_connector.get_records(
                f"SELECT [row_key],[ratio],[text_value],[happened_at],[nullable_key],"
                f"[required_ratio],[required_text],[required_at] FROM [dbo].[{target_table}] ORDER BY [row_key]"
            )
            expected = [
                (1, -1.5, "alpha", datetime(2024, 2, 29, 23, 59, 59, 999999), None, -2.5, "λ", datetime(2024, 1, 1)),
                (2, 0.0, "", None, -(1 << 63), 0.0, "", datetime(2024, 1, 2)),
                (
                    3,
                    None,
                    None,
                    datetime(2030, 6, 1, 12, 30, 45, 123456),
                    (1 << 63) - 1,
                    3.5,
                    "tail",
                    datetime(2024, 1, 3),
                ),
                (
                    3,
                    None,
                    None,
                    datetime(2030, 6, 1, 12, 30, 45, 123456),
                    (1 << 63) - 1,
                    3.5,
                    "tail",
                    datetime(2024, 1, 3),
                ),
            ]
            assert actual == ([] if fault == "pre_eof" or ":" in fault else expected)
    finally:
        # These objects belong only to this disposable synthetic fixture. Runtime
        # must retain corrupted-stage custody; fixture teardown then removes its
        # own physical objects without manufacturing a successful journal state.
        for qualified in cleanup_stages:
            target_connector.execute_query(f"DROP TABLE IF EXISTS {qualified}")
        target_connector.execute_query(f"DROP TABLE IF EXISTS [dbo].[{target_table}]")
        source_connector.execute_query(f"DROP TABLE IF EXISTS `{source_table}`")
        target_connector.close()
        source_connector.close()
