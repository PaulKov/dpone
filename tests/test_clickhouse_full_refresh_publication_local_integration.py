"""Opt-in local Docker proof for bounded full-refresh publication.

Run from the repository root after starting the integration ClickHouse service:

    DPONE_RUN_INTEGRATION=1 DPONE_RUN_INTEGRATION_LIVE=1 \
      uv run pytest tests/test_clickhouse_full_refresh_publication_local_integration.py -q

This is local engine evidence, not production topology certification.
"""

from __future__ import annotations

import base64
import json
import os
import uuid
from dataclasses import replace
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION, LoadStrategy
from dpone.runtime.connectors.clickhouse import ClickHouseConnector
from dpone.runtime.sinks.clickhouse_full_refresh_catalog import ClickHouseFullRefreshCatalog
from dpone.runtime.sinks.clickhouse_full_refresh_contract import publication_invocation_id
from dpone.runtime.sinks.clickhouse_full_refresh_publication import (
    SCHEDULER_IDENTITY_OPTION,
    ClickHouseFullRefreshPublicationService,
)

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.integration_clickhouse]

if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)
if str(os.getenv("DPONE_RUN_INTEGRATION_LIVE", "0")).lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Live integration tests are disabled", allow_module_level=True)


class _HttpConnector:
    def __init__(self) -> None:
        port = os.getenv("DPONE_IT_CH_HTTP_PORT_FORWARD", "58123")
        self._url = f"http://127.0.0.1:{port}/"
        user = os.getenv("DPONE_IT_CH_USER", "default")
        password = os.getenv("DPONE_IT_CH_PASSWORD", "dpone")
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self._headers = {"Authorization": f"Basic {token}"}

    def execute_query(self, query: str, *, query_id: str | None = None) -> None:
        self._request(query, query_id=query_id)

    def get_records(self, query: str):  # noqa: ANN201
        payload = self._request(f"{query} FORMAT JSONCompactEachRow")
        return [tuple(json.loads(line)) for line in payload.splitlines() if line]

    def _request(self, query: str, *, query_id: str | None = None) -> str:
        url = self._url if query_id is None else f"{self._url}?{urlencode({'query_id': query_id})}"
        request = Request(url, data=query.encode("utf-8"), headers=self._headers, method="POST")
        with urlopen(request, timeout=15) as response:  # noqa: S310 - fixed localhost integration endpoint
            return response.read().decode("utf-8")


class _LostReplyCatalog(ClickHouseFullRefreshCatalog):
    def __init__(self, connector: _HttpConnector) -> None:
        super().__init__(connector)
        self.exchange_calls = 0

    def exchange(self, database: str, target: str, candidate: str, *, query_id: str) -> None:
        self.exchange_calls += 1
        super().exchange(database, target, candidate, query_id=query_id)
        raise TimeoutError("injected response loss after server mutation")


def _config(database: str, target: str) -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="source_model",
        target_schema=database,
        target_table=target,
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "run_id": "local-publication-run",
            SOURCE_BYTE_BUDGET_OPTION: 1024,
            SCHEDULER_IDENTITY_OPTION: publication_invocation_id(
                scheduler_run_id="local-publication-run",
                process_id="local-integration",
            ),
        },
    )


def test_real_atomic_database_recovers_lost_exchange_reply_without_replay() -> None:
    connector = _HttpConnector()
    database = f"dpone_pub_{uuid.uuid4().hex[:12]}"
    target, candidate = "target", "candidate"
    connector.execute_query(f"CREATE DATABASE `{database}` ENGINE = Atomic")
    try:
        connector.execute_query(f"CREATE TABLE `{database}`.`{target}` (id UInt64) ENGINE = MergeTree ORDER BY tuple()")
        connector.execute_query(
            f"CREATE TABLE `{database}`.`{candidate}` (id UInt64) ENGINE = MergeTree ORDER BY tuple()"
        )
        connector.execute_query(f"INSERT INTO `{database}`.`{target}` VALUES (1)")
        connector.execute_query(f"INSERT INTO `{database}`.`{candidate}` VALUES (2), (3)")
        catalog = _LostReplyCatalog(connector)
        service = ClickHouseFullRefreshPublicationService(catalog)
        config = _config(database, target)

        receipt = service.publish(config, replace(config, target_table=candidate), staged_rows=2)

        assert receipt.recovered_after_error is True
        assert catalog.exchange_calls == 1
        assert connector.get_records(f"SELECT groupArray(id) FROM `{database}`.`{target}`") == [(["2", "3"],)]

        service.cleanup(receipt)

        names = connector.get_records(
            f"SELECT name FROM system.tables WHERE database = '{database}' AND name != '{target}' ORDER BY name"
        )
        assert names == []
    finally:
        connector.execute_query(f"DROP DATABASE IF EXISTS `{database}` SYNC")


@pytest.mark.parametrize("driver,port", [("native", 59000), ("http", 58123)])
def test_runtime_connector_applies_deterministic_query_id(driver: str, port: int) -> None:
    """Exercise both production transports, not only the integration HTTP shim."""

    connector = ClickHouseConnector(
        host="127.0.0.1",
        port=port,
        database="default",
        user=os.getenv("DPONE_IT_CH_USER", "default"),
        password=os.getenv("DPONE_IT_CH_PASSWORD", "dpone"),
        driver=driver,
        compression=False,
    )
    query_id = f"dpone-full-refresh-live-{driver}-{uuid.uuid4().hex}"
    try:
        connector.execute_query("SELECT 1", query_id=query_id)
        connector.execute_query("SYSTEM FLUSH LOGS")
        rows = connector.get_records(
            "SELECT count() FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
        assert rows == [(1,)]
    finally:
        connector.close()
