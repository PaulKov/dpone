from __future__ import annotations

import json
from pathlib import Path

from dpone.backfill.state import BACKFILL_LEDGER_SCHEMA_VERSION
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
      advisor:
        optimize_for: speed
      chunk:
        column: business_date
        from: "2025-01-01"
        to: "2025-05-01"
        step: 1d
"""


def _write_manifest(tmp_path: Path) -> Path:
    manifest = tmp_path / "orders_backfill.yml"
    manifest.write_text(_MANIFEST.lstrip(), encoding="utf-8")
    return manifest


def _ctx() -> ManifestCommandContext:
    return ManifestCommandContext(registry_paths=(), loader=ManifestLoaderRouter(registry_paths=()))


def test_backfill_plan_advisor_uses_persisted_chunk_evidence(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)
    service = BackfillCommandService()
    plan = service.plan(path=manifest, manifest_ctx=_ctx())
    chunks = [
        dict(
            plan["chunks"][0],
            status="success",
            rows_loaded=1_200_000,
            started_at="2026-07-01T00:00:00+00:00",
            finished_at="2026-07-01T00:02:00+00:00",
        ),
        dict(
            plan["chunks"][1],
            status="success",
            rows_loaded=900_000,
            started_at="2026-07-01T00:02:00+00:00",
            finished_at="2026-07-01T00:03:30+00:00",
        ),
    ]
    ledger_path = Path(plan["state_path"])
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps(
            {
                "kind": "dpone.backfill_ledger",
                "schema_version": BACKFILL_LEDGER_SCHEMA_VERSION,
                "run_key": plan["run_key"],
                "dataset": plan["dataset"],
                "inner_mode": plan["inner_mode"],
                "chunk_config": plan["chunk_config"],
                "chunks": chunks,
            }
        ),
        encoding="utf-8",
    )

    payload = service.plan(path=manifest, manifest_ctx=_ctx(), advisor=True)

    assert payload["advisor"]["recommendation"] == "increase_window_and_parallelism"
    assert payload["advisor"]["recommended_step"] == "2d"
    assert payload["advisor"]["recommended_max_parallel_chunks"] == 4
    assert payload["advisor"]["recommended_overrides"] == {
        "chunk.step": "2d",
        "parallel_workers": 4,
    }
    assert payload["advisor"]["evidence"]["rows_per_second"] == 10_000


def test_backfill_plan_advisor_uses_external_load_step_evidence(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)
    evidence = tmp_path / "load_steps.json"
    evidence.write_text(
        json.dumps(
            {
                "load_steps": [
                    {
                        "step_id": "source_read",
                        "status": "succeeded",
                        "details_json": {"throughput": {"rows_per_second": 50_000, "row_count": 100_000}},
                    },
                    {
                        "step_id": "sink_finalize",
                        "status": "succeeded",
                        "details_json": {"throughput": {"rows_per_second": 2_000, "row_count": 100_000}},
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    payload = BackfillCommandService().plan(
        path=manifest,
        manifest_ctx=_ctx(),
        advisor=True,
        advisor_evidence_paths=(evidence,),
    )

    assert payload["advisor"]["recommendation"] == "tune_sink_finalize_before_parallelism"
    assert payload["advisor"]["evidence"]["bottleneck_stage"] == "sink_finalize"
    assert payload["advisor"]["evidence_sources"] == ["chunk_ledger", "load_steps"]
