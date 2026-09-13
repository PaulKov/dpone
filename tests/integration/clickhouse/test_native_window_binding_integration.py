"""Real ClickHouse name binding and exact half-open native timestamp windows."""

from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from tests.integration.mssql.clickhouse_mssql_delivery_support import redacted_live
from tests.test_mssql_native_policy import config
from tools.native_delivery_live_benchmark import approved
from tools.native_delivery_local.environment import Environment
from tools.native_delivery_local.source_guard import ddl_lock

from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.sources.clickhouse_native_source import ClickHouseNativeSource

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_clickhouse]


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("dtype", ["DateTime64(6, 'UTC')", "DateTime('UTC')"])
@redacted_live
def test_native_window_uses_physical_temporal_column(tmp_path, nullable, dtype):
    if not approved():
        pytest.skip("DDA disposable environment is not explicitly approved")
    environment = Environment(root=tmp_path)
    connector = environment.clickhouse()
    table = "dda_window_" + uuid4().hex
    relation = f"`dda_synthetic`.`{table}`"
    source_type = f"Nullable({dtype})" if nullable else dtype
    start, end = datetime(2026, 1, 1), datetime(2026, 1, 2)
    tick = timedelta(microseconds=1) if "64" in dtype else timedelta(seconds=1)
    times = [start - tick, start, start, end - tick, end]
    if nullable:
        times.append(None)
    artifact = None
    source_uuid = None
    try:
        with ddl_lock(tmp_path, writer=True):
            connector.execute_query(
                f"CREATE TABLE {relation} (event_at {source_type}) ENGINE=MergeTree ORDER BY tuple()"
            )
            source_uuid = connector.get_records(
                "SELECT toString(uuid) FROM system.tables WHERE database='dda_synthetic' AND name=%(table)s",
                {"table": table},
            )[0][0]
            connector.connection.execute(f"INSERT INTO {relation} VALUES", [(value,) for value in times])
        value = config("partition_replace")
        value.source_schema, value.source_table = "dda_synthetic", table
        value.options.update(
            mssql_native_window={"column": "event_at", "anchor": "data_interval_end", "lookback": "P1D"},
            interval={"interval_end": "2026-01-02T00:00:00Z"},
        )
        result = ClickHouseNativeSource(
            connector, schema_guard_factory=lambda _: ddl_lock(tmp_path, writer=False)
        ).extract(value)
        artifact = result.artifact
        assert sorted(row["event_at"] for row in artifact.iter_native_rows()) == [start, start, end - tick]
        assert artifact.rows_exported == 3
    finally:
        try:
            if artifact is not None:
                assert artifact.terminate(ArtifactTerminalOutcome.ABORT).cleanup_succeeded
            if source_uuid is not None:
                with ddl_lock(tmp_path, writer=True):
                    current = connector.get_records(
                        "SELECT toString(uuid) FROM system.tables WHERE database='dda_synthetic' AND name=%(table)s",
                        {"table": table},
                    )
                    assert [tuple(row) for row in current] == [(source_uuid,)]
                    connector.execute_query(f"DROP TABLE {relation}")
        finally:
            connector.close()
