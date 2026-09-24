"""Pre-provisioned databases do not require database-creation privileges."""

from __future__ import annotations

import pytest

from dpone.runtime.clickhouse_database_provisioning import ensure_database


class Connector:
    def __init__(self, rows=None, error=None):
        self.rows = list(rows or [])
        self.error = error
        self.statements = []

    def execute_query(self, sql):
        self.statements.append(sql)
        if self.error:
            raise self.error

    def get_records(self, sql, params=None):
        self.statements.append(sql)
        result = self.rows.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_creation_success_does_not_require_catalog_reads():
    connector = Connector()
    ensure_database(connector, database="landing", statement="CREATE DATABASE IF NOT EXISTS `landing`")
    assert len(connector.statements) == 1


@pytest.mark.parametrize("cluster,rows", [(None, [[(1,)]]), ("example_cluster", [[(3,)], [(3,)]])])
def test_preprovisioned_database_survives_create_denial(cluster, rows):
    connector = Connector(rows, RuntimeError("Code: 497. DB::Exception: denied (ACCESS_DENIED)"))
    ensure_database(connector, database="landing", statement="CREATE DATABASE IF NOT EXISTS `landing`", cluster=cluster)
    if cluster:
        assert "skip_unavailable_shards=0" in connector.statements[-1]


@pytest.mark.parametrize(
    "cluster,rows",
    [
        (None, [[(0,)]]),
        (None, [[]]),
        (None, [RuntimeError("catalog unavailable")]),
        ("example_cluster", [[(3,)], [(2,)]]),
        ("example_cluster", [[(0,)]]),
    ],
)
def test_missing_or_unverified_database_preserves_denial(cluster, rows):
    error = RuntimeError("Code: 497. DB::Exception: denied")
    connector = Connector(rows, error)
    with pytest.raises(RuntimeError) as raised:
        ensure_database(
            connector, database="landing", statement="CREATE DATABASE IF NOT EXISTS `landing`", cluster=cluster
        )
    assert raised.value is error


@pytest.mark.parametrize("error", [RuntimeError("timeout"), RuntimeError("Code: 62. syntax error")])
def test_other_errors_are_not_suppressed(error):
    connector = Connector(error=error)
    with pytest.raises(RuntimeError) as raised:
        ensure_database(connector, database="landing", statement="CREATE DATABASE IF NOT EXISTS `landing`")
    assert raised.value is error
    assert len(connector.statements) == 1


def test_native_permission_code_through_exception_chain():
    class ServerError(Exception):
        code = 497

    error = RuntimeError("connector failed")
    error.__cause__ = ServerError("restricted operation")
    ensure_database(Connector([[(1,)]], error), database="landing", statement="CREATE DATABASE")


@pytest.mark.parametrize("rows", [[("1",)], [(True,)], [(1,), (1,)], [(1, 1)]])
def test_malformed_probe_does_not_waive_denial(rows):
    error = RuntimeError("Code: 497. denied")
    with pytest.raises(RuntimeError) as raised:
        ensure_database(Connector([rows], error), database="landing", statement="CREATE DATABASE")
    assert raised.value is error


@pytest.mark.parametrize("store_kind", ["load", "step", "route_step", "sink"])
def test_all_stores_preserve_table_permission_failure(store_kind):
    from dpone.config import LoadConfig, LoadStrategy
    from dpone.runtime.sinks.clickhouse_sql_mixin import ClickHouseSqlMixin
    from dpone.runtime.state.clickhouse import ClickHouseLoadAuditStorage, ClickHouseLoadStepAuditStorage
    from dpone.runtime.state.load_step_audit import ClickHouseLoadStepAuditStorage as RouteStepStorage

    table_error = RuntimeError("table permission denied")

    class RestrictedConnector(Connector):
        def execute_query(self, sql):
            self.statements.append(sql)
            if sql.startswith("CREATE DATABASE"):
                raise RuntimeError("Code: 497. denied")
            raise table_error

    connector = RestrictedConnector([[(1,)]])
    with pytest.raises(RuntimeError) as raised:
        if store_kind == "load":
            ClickHouseLoadAuditStorage(connector, schema="landing").create_load_table()
        elif store_kind == "step":
            ClickHouseLoadStepAuditStorage(connector, schema="landing").create_step_table()
        elif store_kind == "route_step":
            RouteStepStorage(connector, schema="landing")._ensure_table()
        else:
            sink = ClickHouseSqlMixin()
            sink.connector = connector
            config = LoadConfig(
                source_conn_id="source",
                target_conn_id="target",
                source_schema="public",
                source_table="source_events",
                target_schema="landing",
                target_table="target_fact",
                load_strategy=LoadStrategy.FULL_REFRESH,
            )
            sink._table = lambda _config: "`landing`.`target_fact`"
            sink._create_table_from_columns_sql(config, ["id Int64"], if_not_exists=True)
    assert raised.value is table_error
