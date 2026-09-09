from __future__ import annotations

import json
import logging
from types import SimpleNamespace

from dpone.backfill.mapping import (
    AIRFLOW_MAPPING_ITEM_ENV,
    build_airflow_mapping_plan,
    serialize_airflow_mapping_item,
)
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.process_types import ProcessResult
from dpone.gitops.airflow_xcom_outcome import _backfill_section
from dpone.services.airflow_mapping_context import (
    AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY,
    AirflowMappingContextService,
)
from dpone.services.run_manifest import RunManifestService


def _config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        load_strategy=LoadStrategy.BACKFILL,
        options={
            "source_type": "mssql",
            "sink_type": "postgres",
            "backfill": {
                "inner_mode": "partition_replace",
                "parallel_workers": 1,
                "chunk": {
                    "column": "d",
                    "from": "2025-01-01",
                    "to": "2025-01-04",
                    "step": "1d",
                },
                "state": {"backend": "audit_schema", "require_distributed_lock": True},
            },
        },
    )


def _item_json() -> str:
    plan = build_airflow_mapping_plan(
        _config(),
        {"mode": "summary", "max_items": 2, "max_active": 2, "pool": "history"},
    )
    return serialize_airflow_mapping_item(plan, plan.items[0])


def test_mapping_context_service_is_empty_without_provider_input() -> None:
    assert AirflowMappingContextService().from_environ({}) == {}


def test_mapping_context_service_parses_provider_input_once_at_composition_root() -> None:
    context = AirflowMappingContextService().from_environ({AIRFLOW_MAPPING_ITEM_ENV: _item_json()})

    selection = context[AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY]
    assert selection.item_index == 0
    assert selection.chunk_indexes == (1, 2)


def test_run_manifest_service_injects_mapping_selection_into_run_context(tmp_path) -> None:
    seen: list[object] = []

    class Process:
        def __init__(self, config, config_path=None):
            del config, config_path

        def run(self, context=None, dag_id=None, execution_date=None):
            del dag_id, execution_date
            seen.append(context.config[AIRFLOW_MAPPING_SELECTION_CONTEXT_KEY])
            return ProcessResult(
                status="success",
                inserted_rows=0,
                updated_rows=0,
                final_rows=0,
                extracted_rows=0,
                duration_seconds=0.0,
                errors=[],
            )

    spec = SimpleNamespace(name="orders", selector=None, config="cfg", config_path=tmp_path / "m.yml")
    manifest = SimpleNamespace(processes=(spec,), path=tmp_path / "m.yml")

    class Loader:
        def load(self, path, *, metadata_only=True):
            del path, metadata_only
            return manifest

    context_config = AirflowMappingContextService().from_environ({AIRFLOW_MAPPING_ITEM_ENV: _item_json()})
    result = RunManifestService(process_factory=Process).run(
        path=tmp_path / "m.yml",
        manifest_ctx=SimpleNamespace(loader=Loader()),
        run_context_config=context_config,
    )

    assert result.passed
    assert seen[0].chunk_indexes == (1, 2)


def test_run_command_rejects_invalid_mapping_item_before_manifest_hydration(monkeypatch, capsys) -> None:
    from dpone.commands import run_cmd

    hydrated = False

    def build_manifest(*args, **kwargs):
        nonlocal hydrated
        hydrated = True
        raise AssertionError("manifest hydration must not run")

    monkeypatch.setenv(AIRFLOW_MAPPING_ITEM_ENV, '{"predicate":"must not be accepted"}')
    monkeypatch.setattr(run_cmd, "build_manifest_context", build_manifest)
    code = run_cmd.cmd_run(
        SimpleNamespace(
            path="manifest.yaml",
            pipeline=None,
            select=[],
            exclude=[],
            state=None,
            sample=None,
            target=None,
            format="json",
            selector=None,
            run_id=None,
            retry_attempts=0,
            retry_backoff_seconds=0,
        ),
        ctx=object(),
        logger=logging.getLogger("test"),
    )

    payload = json.loads(capsys.readouterr().out)
    assert code == 2
    assert hydrated is False
    assert "DPONE_AIRFLOW_MAPPING_ITEM_INVALID" in payload["result"]["errors"][0]


def test_xcom_backfill_summary_keeps_bounded_mapping_and_omits_chunk_ledger() -> None:
    mapping = {
        "mode": "summary",
        "plan_fingerprint": "sha256:" + "a" * 64,
        "backfill_plan_hash": "sha256:" + "b" * 64,
        "item_index": 0,
        "first_chunk_index": 1,
        "last_chunk_index": 2,
        "chunks_count": 2,
        "item_status": "success",
    }
    inline = {
        "result": {
            "details": {
                "backfill": {
                    "run_key": "campaign",
                    "chunks_total": 4,
                    "chunks": [{"index": 1, "predicate": "must not enter XCom"}],
                    "mapping": mapping,
                }
            }
        }
    }

    summary = _backfill_section(inline)

    assert summary is not None
    assert summary["mapping"] == mapping
    assert "chunks" not in summary
    assert "predicate" not in str(summary)
