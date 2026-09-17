from __future__ import annotations

from typing import Any

from dpone.runtime.sinks.clickhouse_full_refresh_catalog import ClickHouseFullRefreshCatalog


class _Connector:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.query_ids: list[str | None] = []
        self.rows: list[Any] = []

    def get_records(self, query: str) -> list[Any]:
        self.queries.append(query)
        return self.rows

    def execute_query(self, query: str, *, query_id: str | None = None) -> None:
        self.queries.append(query)
        self.query_ids.append(query_id)


def test_catalog_projects_database_and_table_identity() -> None:
    connector = _Connector()
    catalog = ClickHouseFullRefreshCatalog(connector)
    connector.rows = [("Atomic",)]

    assert catalog.database_engine("analytics") == "Atomic"

    connector.rows = [("target", "uuid-1", "MergeTree", "marker")]
    tables = catalog.tables("analytics", ("target", "candidate"))

    assert tables["target"].uuid == "uuid-1"
    assert tables["target"].comment == "marker"
    assert "system.tables" in connector.queries[-1]


def test_catalog_renders_single_non_retried_publication_statements() -> None:
    connector = _Connector()
    catalog = ClickHouseFullRefreshCatalog(connector)

    catalog.create_marker("analytics", "marker", '{"operation_id":"one"}')
    catalog.exchange("analytics", "target", "candidate", query_id="exchange-id")
    catalog.rename("analytics", "candidate", "target", query_id="rename-id")
    catalog.drop("analytics", "candidate")

    assert connector.queries == [
        'CREATE TABLE `analytics`.`marker` (operation_id String) ENGINE = TinyLog COMMENT \'{"operation_id":"one"}\'',
        "EXCHANGE TABLES `analytics`.`target` AND `analytics`.`candidate`",
        "RENAME TABLE `analytics`.`candidate` TO `analytics`.`target`",
        "DROP TABLE `analytics`.`candidate`",
    ]
    assert connector.query_ids == [None, "exchange-id", "rename-id", None]
