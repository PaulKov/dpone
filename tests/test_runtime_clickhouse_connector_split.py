from __future__ import annotations

from pathlib import Path

from dpone.runtime.connectors.clickhouse import ClickHouseConnector


class DummyLogger:
    def __init__(self):
        self.progress = []
        self.errors = []
        self.warnings = []

    def log_etl_progress(self, event, payload):
        self.progress.append((event, payload))

    def log_etl_error(self, message, payload):
        self.errors.append((message, payload))

    def warning(self, message, *args):
        self.warnings.append((message, args))


class FakeClient:
    def __init__(self):
        self.executed = []
        self.iter_rows = []
        self.disconnected = False

    def execute(self, query, params=None, with_column_types=False, settings=None):
        stored_params = list(params) if isinstance(params, list) else params
        self.executed.append((query, stored_params, with_column_types, settings))
        if "FROM system.columns" in str(query):
            return [("id", "Int32"), ("name", "String")]
        if with_column_types:
            # Header probes intentionally return zero rows; column metadata must
            # still be available for streaming as_dict zip.
            if "LIMIT 0" in str(query).upper():
                return [], [("id", "UInt64"), ("name", "String")]
            if "cnt" in str(query):
                return [((7,),)][0], [("cnt", "UInt64")]
            return [(1, "a")], [("id", "UInt64"), ("name", "String")]
        return [(1,), (2,)]

    def execute_iter(self, query, params=None):
        return iter(self.iter_rows)

    def disconnect(self):
        self.disconnected = True


class FlakyMetadataClient(FakeClient):
    def __init__(self, *, failures: int):
        super().__init__()
        self.failures = failures

    def execute(self, query, params=None, with_column_types=False, settings=None):
        self.executed.append((query, params, with_column_types, settings))
        if self.failures > 0:
            self.failures -= 1
            raise RuntimeError("Code: 517. Metadata on replica is not up to date with common metadata in Zookeeper")
        return []


class StubConnector(ClickHouseConnector):
    def __init__(self):
        super().__init__(
            host="localhost",
            port=9000,
            database="db",
            user="default",
            password="",
            logger=DummyLogger(),
        )
        self._client = FakeClient()


def test_clickhouse_connector_facade_builds_basic_queries():
    connector = StubConnector()
    assert connector.build_select_query("db", "tbl", ["a", "b"], limit=10, offset=5) == (
        "SELECT `a`, `b` FROM `db`.`tbl` LIMIT 10 OFFSET 5"
    )
    assert connector.query_max_column("db.tbl", "id") == 1


def test_clickhouse_streaming_as_dict_keeps_business_columns_when_header_probe_empty():
    """Regression: WHERE 1=0 probe returns no rows; col_names must still resolve.

    Without column metadata, every streamed row becomes {} and MSSQL BCP loads
    NULL business columns while lineage (__dpone__*) still attaches.
    """
    connector = StubConnector()
    connector._client.iter_rows = [(1, "a"), (2, "b")]

    batches = list(
        connector.get_records_streaming(
            "SELECT id, name FROM db.tbl",
            batch_size=10,
            as_dict=True,
        )
    )

    assert batches == [[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]]
    header_calls = [
        item for item in connector._client.executed if item[2] is True and "LIMIT 0" in str(item[0]).upper()
    ]
    assert header_calls, "streaming as_dict must resolve headers via with_column_types"


def test_clickhouse_describe_result_columns_uses_metadata_not_sample_rows():
    connector = StubConnector()
    assert connector.describe_result_columns("SELECT id, name FROM db.tbl") == ["id", "name"]


def test_clickhouse_connector_generates_partitions_and_counts_rows(monkeypatch):
    connector = StubConnector()

    def fake_get_records(query, params=None, as_dict=False):
        if as_dict and "partition_date" in query:
            return [{"partition_date": "2026-03-01"}, {"partition_date": "2026-03-02"}]
        if as_dict and "COUNT(*)" in query:
            return [{"cnt": 3}]
        return []

    monkeypatch.setattr(connector, "get_records", fake_get_records)

    parts = connector.generate_date_partitions(
        "2026-03-01",
        "2026-03-02",
        partition_by="day",
        date_column="dt",
        current_date=None,
    )
    assert parts == [
        ("2026-03-01", "toDate(`dt`) = '2026-03-01'"),
        ("2026-03-02", "toDate(`dt`) = '2026-03-02'"),
    ]

    nonempty = connector.discover_nonempty_partitions(
        schema="db",
        table="tbl",
        date_column="dt",
        date_from="2026-03-01",
        date_to="2026-03-31",
        partition_by="day",
    )
    assert nonempty == {"2026-03-01", "2026-03-02"}
    assert connector.count_rows_by_date("db", "tbl", "dt", "2026-03-01") == 3


def test_clickhouse_connector_import_from_csv_batches(tmp_path: Path):
    connector = StubConnector()
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("id,name\n1,a\n2,b\n", encoding="utf-8")

    connector.import_from_csv("db", "tbl", str(csv_path), batch_rows=1)

    inserts = [x for x in connector._client.executed if str(x[0]).startswith("INSERT INTO")]
    assert len(inserts) == 2
    assert inserts[0][1] == [(1, "a")]
    assert inserts[1][1] == [(2, "b")]


def test_clickhouse_connector_close_disconnects_client():
    connector = StubConnector()
    connector.close()
    assert connector._client is None


def test_clickhouse_connector_retries_transient_cluster_metadata_ddl(monkeypatch):
    monkeypatch.setenv("DPONE_CLICKHOUSE_DDL_RETRY_BACKOFF_SECONDS", "0")
    connector = StubConnector()
    connector._client = FlakyMetadataClient(failures=2)

    connector.execute_query("ALTER TABLE `db`.`tbl` ON CLUSTER `dwh` ADD COLUMN IF NOT EXISTS `x` UInt8")

    assert len(connector._client.executed) == 3
    assert connector.logger.warnings
    assert "retryable ClickHouse metadata lag" in connector.logger.warnings[0][0]
    settings = connector._client.executed[0][3]
    assert settings["distributed_ddl_output_mode"] == "null_status_on_timeout"
    assert settings["distributed_ddl_task_timeout"] == 30


def test_clickhouse_on_cluster_ddl_defaults_tolerate_inactive_replicas():
    connector = StubConnector()
    connector.execute_query("CREATE TABLE IF NOT EXISTS `db`.`t` ON CLUSTER `dwh` (id UInt8) ENGINE = Memory")
    settings = connector._client.executed[-1][3]
    assert settings["distributed_ddl_output_mode"] == "null_status_on_timeout"
    assert settings["distributed_ddl_task_timeout"] == 30


def test_clickhouse_non_cluster_ddl_does_not_inject_distributed_ddl_settings():
    connector = StubConnector()
    connector.execute_query("CREATE TABLE IF NOT EXISTS `db`.`t` (id UInt8) ENGINE = Memory")
    settings = connector._client.executed[-1][3]
    assert settings is not None
    assert settings.get("skip_unavailable_shards") == 1
    assert "distributed_ddl_output_mode" not in settings


def test_clickhouse_get_records_skips_unavailable_shards():
    connector = StubConnector()
    connector.get_records("SELECT 1")
    settings = connector._client.executed[-1][3]
    assert settings["skip_unavailable_shards"] == 1


def test_clickhouse_connector_does_not_retry_metadata_lag_for_insert(monkeypatch):
    monkeypatch.setenv("DPONE_CLICKHOUSE_DDL_RETRY_BACKOFF_SECONDS", "0")
    connector = StubConnector()
    connector._client = FlakyMetadataClient(failures=1)

    try:
        connector.execute_query("INSERT INTO `db`.`tbl` SELECT * FROM `db`.`src`")
    except RuntimeError as exc:
        assert "Metadata on replica is not up to date" in str(exc)
    else:  # pragma: no cover - defensive
        raise AssertionError("INSERT should not be retried automatically")

    assert len(connector._client.executed) == 1
