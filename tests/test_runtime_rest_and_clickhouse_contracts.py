from __future__ import annotations

from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.api_sources import get_api_source_defaults, list_api_source_types
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.api_registry import list_registered_api_provider_specs
from dpone.runtime.artifacts import FileExportArtifact
from dpone.runtime.connectors.api.rest import GenericRestConnector, RestCredentials
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.api.rest import GenericRestSource


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return FakeResponse(self.payloads.pop(0))


def test_rest_api_defaults_and_load_config_derivation() -> None:
    defaults = get_api_source_defaults("rest")
    cfg = LoadConfigBuilder().build(
        {
            "source": {"type": "api", "api_type": "rest", "options": {"resource": "orders"}},
            "sink": {
                "type": "mssql",
                "connection_id": "mssql-dwh",
                "table": {"schema": "landing_api", "name": "orders"},
                "strategy": {"mode": "full_refresh"},
            },
        }
    )

    assert "rest" in list_api_source_types()
    assert defaults.connection_id() == "api__rest"
    assert cfg.source_conn_id == "api__rest"
    assert cfg.source_schema == "api__rest"
    assert cfg.source_table == "orders"
    assert {spec.api_type for spec in list_registered_api_provider_specs()} >= {"rest"}


def test_generic_rest_connector_handles_bearer_auth_and_cursor_pagination() -> None:
    session = FakeSession(
        [
            {"data": {"items": [{"id": 1}], "next_cursor": "abc"}},
            {"data": {"items": [{"id": 2}], "next_cursor": None}},
        ]
    )
    connector = GenericRestConnector(
        RestCredentials(endpoint="https://api.example.com", token="token"), session=session
    )

    rows = list(
        connector.iter_rows(
            {
                "path": "/v1/orders",
                "records_path": "data.items",
                "pagination": {"type": "cursor", "cursor_param": "cursor", "next_cursor_path": "data.next_cursor"},
            }
        )
    )

    assert rows == [{"id": 1}, {"id": 2}]
    assert session.calls[0][2]["headers"]["Authorization"] == "Bearer token"
    assert session.calls[1][2]["params"]["cursor"] == "abc"


def test_generic_rest_source_streams_rows_with_configured_schema() -> None:
    session = FakeSession([{"items": [{"id": 1, "amount": "9.99"}]}])
    connector = GenericRestConnector(RestCredentials(endpoint="https://api.example.com"), session=session)
    source = GenericRestSource(connector=connector, sink_connector=None, logger=None)
    cfg = LoadConfig(
        source_conn_id="api__rest",
        target_conn_id="mssql-dwh",
        source_schema="api__rest",
        source_table="orders",
        target_schema="landing_api",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "records_path": "items",
            "columns": [{"name": "id", "type": "bigint"}, {"name": "amount", "type": "decimal(18,2)"}],
        },
    )

    result = source.extract(cfg, None)

    assert result.schema == [("id", "bigint"), ("amount", "decimal(18,2)")]
    assert list(result.artifact._iterator) == [{"id": 1, "amount": "9.99"}]


def test_clickhouse_sink_loads_file_artifact_with_native_insert(tmp_path: Path) -> None:
    data_file = tmp_path / "orders.bcp"
    data_file.write_text("1\t10.5\n2\t20.0\n", encoding="utf-8")

    class FakeClickHouseClient:
        def __init__(self):
            self.inserts = []

        def execute(self, query, params=None):
            if query.startswith("INSERT"):
                self.inserts.append((query, params))
            return [(2,)]

    class FakeConnector:
        def __init__(self):
            self.connection = FakeClickHouseClient()
            self.queries = []

        def execute_query(self, query, params=None):
            self.queries.append(query)
            return 0

        def get_records(self, query, params=None, as_dict=False):
            return [(2,)]

    cfg = LoadConfig(
        source_conn_id="mssql-source",
        target_conn_id="clickhouse-landing",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=1,
    )
    sink = ClickHouseSink(FakeConnector())
    result = sink.load(
        cfg,
        LoadPayload(
            artifact=FileExportArtifact(str(data_file), ["id", "amount"], format="mssql-delimited"),
            schema=[("id", "bigint"), ("amount", "decimal(18,2)")],
        ),
    )

    assert result.inserted_rows == 2
    joined = "\n".join(sink.connector.queries)
    assert "CREATE TABLE `landing`.`orders__dpone_staging_" in joined
    assert "RENAME TABLE" in joined
    assert "TRUNCATE TABLE `landing`.`orders`" not in joined
    assert len(sink.connector.connection.inserts) == 2


def test_clickhouse_sink_reports_exact_target_nullability() -> None:
    class Connector:
        def get_records(self, query, params=None, as_dict=False):
            if "EXISTS TABLE" in query:
                return [(1,)]
            return [("id", "Int32"), ("note", "Nullable(String)")]

    cfg = LoadConfig(
        source_conn_id="postgres-source",
        target_conn_id="clickhouse-landing",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["id"],
    )

    columns = ClickHouseSink(Connector()).get_target_columns(cfg)

    assert [(column.name, column.dtype, column.nullable) for column in columns] == [
        ("id", "Int32", False),
        ("note", "Nullable(String)", True),
    ]


def test_clickhouse_replace_uses_shadow_swap_without_mutation(tmp_path: Path) -> None:
    data_file = tmp_path / "orders.bcp"
    data_file.write_text("3\t30.0\n", encoding="utf-8")

    class FakeClickHouseClient:
        def __init__(self):
            self.inserts = []

        def execute(self, query, params=None):
            if query.startswith("INSERT"):
                self.inserts.append((query, params))
            return [(1,)]

    class FakeConnector:
        def __init__(self):
            self.connection = FakeClickHouseClient()
            self.queries = []

        def execute_query(self, query, params=None):
            self.queries.append(query)
            return 0

        def get_records(self, query, params=None, as_dict=False):
            if "EXISTS TABLE" in query:
                return [(1,)]
            return [(1,)]

    cfg = LoadConfig(
        source_conn_id="mssql-source",
        target_conn_id="clickhouse-landing",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.REPLACE,
        custom_predicate="id >= 3",
        batch_size=1,
    )
    sink = ClickHouseSink(FakeConnector())

    result = sink.load(
        cfg,
        LoadPayload(
            artifact=FileExportArtifact(str(data_file), ["id", "amount"], format="mssql-delimited"),
            schema=[("id", "bigint"), ("amount", "decimal(18,2)")],
        ),
    )

    joined = "\n".join(sink.connector.queries)
    assert result.inserted_rows == 1
    assert "ALTER TABLE" not in joined
    assert "DELETE WHERE" not in joined
    assert "CREATE TABLE `landing`.`orders__dpone_shadow_" in joined
    assert "WHERE NOT (id >= 3)" in joined
    assert "RENAME TABLE" in joined


def test_clickhouse_incremental_merge_default_uses_lightweight_delete_insert(tmp_path: Path) -> None:
    data_file = tmp_path / "orders.bcp"
    data_file.write_text("1\t10.0\n", encoding="utf-8")

    class FakeClickHouseClient:
        def execute(self, query, params=None):
            return [(1,)]

    class FakeConnector:
        host = "localhost"
        port = 9000
        database = "default"
        user = "default"
        password = ""
        secure = False
        compression = False
        connect_timeout = 10
        send_receive_timeout = 300
        settings = {}
        application_name = "dpone-test"

        def __init__(self):
            self.connection = FakeClickHouseClient()
            self.queries = []

        def execute_query(self, query, params=None):
            self.queries.append(query)
            return 1

        def get_records(self, query, params=None, as_dict=False):
            self.queries.append(query)
            if "EXISTS TABLE" in query:
                return [(1,)]
            if " IS NULL" in query:
                return [(0,)]
            if "HAVING count() > 1" in query:
                return [(0,)]
            return [(1,)]

    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="ch",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key="id",
    )
    sink = ClickHouseSink(FakeConnector())

    sink.load(
        cfg,
        LoadPayload(
            artifact=FileExportArtifact(str(data_file), ["id", "amount"], format="mssql-delimited", estimated_rows=1),
            schema=[("id", "bigint"), ("amount", "decimal(18,2)")],
        ),
    )

    joined = "\n".join(sink.connector.queries)
    assert "DELETE FROM `landing`.`orders`" in joined
    assert "SETTINGS mutations_sync = 1" in joined
    assert "ALTER TABLE `landing`.`orders` DELETE" not in joined
    assert "INSERT INTO `landing`.`orders` SELECT * FROM" in joined


def test_clickhouse_rejects_null_staging_key_before_target_lookup_or_mutation(
    tmp_path: Path,
) -> None:
    data_file = tmp_path / "orders.bcp"
    data_file.write_text("\\N\t10.0\n", encoding="utf-8")

    class FakeClickHouseClient:
        def execute(self, query, params=None):
            return [(1,)]

    class FakeConnector:
        host = "localhost"
        port = 9000
        database = "default"
        user = "default"
        password = ""
        secure = False
        compression = False
        connect_timeout = 10
        send_receive_timeout = 300
        settings = {}
        application_name = "dpone-test"

        def __init__(self):
            self.connection = FakeClickHouseClient()
            self.queries = []

        def execute_query(self, query, params=None):
            self.queries.append(query)
            return 1

        def get_records(self, query, params=None, as_dict=False):
            self.queries.append(query)
            if " IS NULL" in query:
                return [(1,)]
            if "HAVING count() > 1" in query:
                return [(0,)]
            if "EXISTS TABLE" in query:
                return [(0,)]
            return [(1,)]

    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="ch",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key="id",
    )
    sink = ClickHouseSink(FakeConnector())

    with pytest.raises(ValueError) as error:
        sink.load(
            cfg,
            LoadPayload(
                artifact=FileExportArtifact(
                    str(data_file),
                    ["id", "amount"],
                    format="mssql-delimited",
                    estimated_rows=1,
                ),
                schema=[("id", "bigint"), ("amount", "decimal(18,2)")],
            ),
        )

    assert getattr(error.value, "code", None) == ("DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_NULL")
    queries = sink.connector.queries
    null_probe = next(index for index, query in enumerate(queries) if " IS NULL" in query)
    assert all("EXISTS TABLE" not in query for query in queries[: null_probe + 1])
    assert all("RENAME TABLE" not in query and "DELETE FROM" not in query for query in queries)
    assert any("DROP TABLE" in query for query in queries)


def test_clickhouse_mutation_policy_requires_opt_in(tmp_path: Path) -> None:
    data_file = tmp_path / "orders.bcp"
    data_file.write_text("1\t10.0\n", encoding="utf-8")

    class FakeConnector:
        host = "localhost"
        port = 9000
        database = "default"
        user = "default"
        password = ""
        secure = False
        compression = False
        connect_timeout = 10
        send_receive_timeout = 300
        settings = {}
        application_name = "dpone-test"
        connection = type("Client", (), {"execute": lambda self, query, params=None: [(1,)]})()

        def execute_query(self, query, params=None):
            return 1

        def get_records(self, query, params=None, as_dict=False):
            if "EXISTS TABLE" in query:
                return [(1,)]
            if " IS NULL" in query:
                return [(0,)]
            if "HAVING count() > 1" in query:
                return [(0,)]
            return [(1,)]

    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="ch",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key="id",
        merge_policy="mutation_delete_insert",
    )

    with pytest.raises(ValueError, match="non-recommended"):
        ClickHouseSink(FakeConnector()).load(
            cfg,
            LoadPayload(
                artifact=FileExportArtifact(
                    str(data_file), ["id", "amount"], format="mssql-delimited", estimated_rows=1
                ),
                schema=[("id", "bigint"), ("amount", "decimal(18,2)")],
            ),
        )


def test_clickhouse_partition_replace_uses_replace_partition_from_staging(tmp_path: Path) -> None:
    data_file = tmp_path / "orders.bcp"
    data_file.write_text("1\t2026-06-03\n", encoding="utf-8")

    class FakeClickHouseClient:
        def execute(self, query, params=None):
            return [(1,)]

    class FakeConnector:
        host = "localhost"
        port = 9000
        database = "default"
        user = "default"
        password = ""
        secure = False
        compression = False
        connect_timeout = 10
        send_receive_timeout = 300
        settings = {}
        application_name = "dpone-test"

        def __init__(self):
            self.connection = FakeClickHouseClient()
            self.queries = []

        def execute_query(self, query, params=None):
            self.queries.append(query)
            return 1

        def get_records(self, query, params=None, as_dict=False):
            self.queries.append(query)
            if "EXISTS TABLE" in query:
                return [(1,)]
            if "SELECT DISTINCT" in query:
                return [("2026-06-03",)]
            return [(1,)]

    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="ch",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        partition={"column": "business_date", "max_partitions_per_run": 4},
    )
    sink = ClickHouseSink(FakeConnector())

    result = sink.load(
        cfg,
        LoadPayload(
            artifact=FileExportArtifact(
                str(data_file), ["id", "business_date"], format="mssql-delimited", estimated_rows=1
            ),
            schema=[("id", "bigint"), ("business_date", "date")],
        ),
    )

    joined = "\n".join(sink.connector.queries)
    assert result.replaced_rows == 1
    assert "CREATE TABLE `landing`.`orders__dpone_staging_" in joined
    assert " AS `landing`.`orders`" in joined
    assert "ALTER TABLE `landing`.`orders` REPLACE PARTITION '2026-06-03' FROM" in joined


def test_clickhouse_partition_replace_uses_physical_partition_value_expression(tmp_path: Path) -> None:
    data_file = tmp_path / "orders.bcp"
    data_file.write_text("1\t2026-06-03\n", encoding="utf-8")

    class FakeClickHouseClient:
        def execute(self, query, params=None):
            return [(1,)]

    class FakeConnector:
        host = "localhost"
        port = 9000
        database = "default"
        user = "default"
        password = ""
        secure = False
        compression = False
        connect_timeout = 10
        send_receive_timeout = 300
        settings = {}
        application_name = "dpone-test"

        def __init__(self):
            self.connection = FakeClickHouseClient()
            self.queries = []

        def execute_query(self, query, params=None):
            self.queries.append(query)
            return 1

        def get_records(self, query, params=None, as_dict=False):
            self.queries.append(query)
            if "EXISTS TABLE" in query:
                return [(1,)]
            if "SELECT DISTINCT toYYYYMM(business_date)" in query:
                return [(202606,)]
            return [(1,)]

    cfg = LoadConfig(
        source_conn_id="src",
        target_conn_id="ch",
        source_schema="dbo",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        partition={
            "column": "business_date",
            "value_expression": "toYYYYMM(business_date)",
            "max_partitions_per_run": 4,
        },
    )
    sink = ClickHouseSink(FakeConnector())

    result = sink.load(
        cfg,
        LoadPayload(
            artifact=FileExportArtifact(
                str(data_file), ["id", "business_date"], format="mssql-delimited", estimated_rows=1
            ),
            schema=[("id", "bigint"), ("business_date", "date")],
        ),
    )

    joined = "\n".join(sink.connector.queries)
    assert result.replaced_rows == 1
    assert "SELECT DISTINCT toYYYYMM(business_date) AS __dpone_partition_value FROM" in joined
    assert "ALTER TABLE `landing`.`orders` REPLACE PARTITION 202606 FROM" in joined
