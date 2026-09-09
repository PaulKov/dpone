from __future__ import annotations

import json
from pathlib import Path

from dpone.manifest.loader import ManifestLoaderRouter
from dpone.services.backfill_service import BackfillCommandService
from dpone.services.manifest import ManifestCommandContext

_MANIFEST = """
name: orders_backfill

source:
  type: mssql
  connection_id: mssql_oltp
  connection_type: env
  table:
    schema: dbo
    name: orders

sink:
  type: clickhouse
  connection_id: clickhouse_dwh
  connection_type: env
  table:
    schema: analytics
    name: orders
  strategy:
    mode: backfill
    backfill:
      inner_mode: partition_replace
      backfill_id: campaign-a
      state:
        backend: audit_schema
        schema: DWH_Tech
      chunk:
        column: business_date
        from: "2025-01-01"
        to: "2025-01-10"
        step: 3d
"""


def test_backfill_status_reads_audit_schema_state_store(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = tmp_path / "orders_backfill.yml"
    manifest.write_text(_MANIFEST.lstrip(), encoding="utf-8")
    connector = _ReadConnector(
        {
            "kind": "dpone.backfill_ledger",
            "schema_version": "2",
            "run_key": "campaign-a",
            "dataset": "analytics.orders",
            "inner_mode": "partition_replace",
            "chunk_config": {"column": "business_date"},
            "chunks": [
                {"index": 1, "start": "2025-01-01", "end": "2025-01-04", "idempotency_key": "a", "status": "success"},
                {"index": 2, "start": "2025-01-04", "end": "2025-01-07", "idempotency_key": "b", "status": "pending"},
            ],
        }
    )
    service = BackfillCommandService(sink_connector=connector, sink_type="clickhouse")

    payload = service.status(path=manifest, manifest_ctx=_ctx())

    assert payload["kind"] == "dpone.backfill_status"
    assert payload["state_backend"] == "audit_schema"
    assert payload["state_capabilities"]["lock_scope"] == "local_cache"
    assert payload["state_capabilities"]["distributed_lock"] is False
    assert payload["counts"] == {"success": 1, "pending": 3}
    assert any("`DWH_Tech`.`__dpone__backfill_campaigns` FINAL" in str(sql) for sql, _ in connector.calls)


def test_backfill_status_resolves_audit_schema_connector_when_not_injected(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = tmp_path / "orders_backfill.yml"
    manifest.write_text(_MANIFEST.lstrip(), encoding="utf-8")
    connector = _ReadConnector(_ledger())
    service = BackfillCommandService(connector_resolver=_Resolver(connector))

    payload = service.status(path=manifest, manifest_ctx=_ctx())

    assert payload["state_backend"] == "audit_schema"
    assert payload["counts"] == {"success": 1, "pending": 3}


def test_backfill_doctor_warns_when_state_lock_is_not_distributed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = tmp_path / "orders_backfill.yml"
    manifest.write_text(_MANIFEST.lstrip(), encoding="utf-8")
    service = BackfillCommandService(sink_connector=_ReadConnector(_ledger()), sink_type="clickhouse")

    payload = service.doctor(path=manifest, manifest_ctx=_ctx())

    assert any("state lock is not distributed" in warning for warning in payload["warnings"])


def _ctx() -> ManifestCommandContext:
    return ManifestCommandContext(registry_paths=(), loader=ManifestLoaderRouter(registry_paths=()))


def _ledger() -> dict[str, object]:
    return {
        "kind": "dpone.backfill_ledger",
        "schema_version": "2",
        "run_key": "campaign-a",
        "dataset": "analytics.orders",
        "inner_mode": "partition_replace",
        "chunk_config": {"column": "business_date"},
        "chunks": [
            {"index": 1, "start": "2025-01-01", "end": "2025-01-04", "idempotency_key": "a", "status": "success"},
            {"index": 2, "start": "2025-01-04", "end": "2025-01-07", "idempotency_key": "b", "status": "pending"},
        ],
    }


class _ReadConnector:
    def __init__(self, ledger: dict[str, object]) -> None:
        self.ledger = ledger
        self.calls: list[tuple[object, object | None]] = []

    def get_records(self, sql, params=None, as_dict=False):
        self.calls.append((sql, params))
        if "dpone_backfill_journal_shape:clickhouse" in str(sql):
            rows = [
                {
                    "column_name": "journal_id",
                    "data_type": "UInt64",
                    "default_kind": "DEFAULT",
                    "default_expression": "generateSnowflakeID()",
                    "engine_full": "ReplacingMergeTree(journal_id) ORDER BY run_key",
                }
            ]
            return rows if as_dict else [tuple(rows[0].values())]
        if "__dpone__backfill_chunks" in str(sql):
            return []
        details_json = json.dumps(self.ledger, ensure_ascii=False)
        if as_dict:
            return [{"journal_id": 1, "details_json": details_json}]
        return [(1, details_json)]

    def execute_query(self, sql, params=None):
        self.calls.append((sql, params))

    def quote_identifier(self, value: str) -> str:
        return f"[{value}]"

    def qualified_name(self, schema: str, table: str) -> str:
        return f"[{schema}].[{table}]"


class _Resolver:
    def __init__(self, connector: _ReadConnector) -> None:
        self.connector = connector

    def resolve(self, load_config, backfill_options):
        return self.connector
