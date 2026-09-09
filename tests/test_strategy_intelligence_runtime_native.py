from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.config import LoadStrategy
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.strategy_intelligence.certification import (
    StrategyCertificationArtifactWriter,
    StrategyCertificationMatrixService,
)
from dpone.strategy_intelligence.native_transfer import NativeTransferPlanBuilder, NativeTransferRequest
from dpone.strategy_intelligence.preflight import NativeFastPathPreflightService


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _auto_config() -> dict:
    return {
        "name": "public_orders__auto",
        "source": {
            "type": "postgres",
            "connection_id": "pg",
            "table": {"schema": "public", "name": "orders"},
            "options": {
                "estimated_rows": 50_000_000,
                "changed_percent": 0.2,
                "delete_percent": 0.05,
                "partition_column": "business_date",
            },
        },
        "sink": {
            "type": "mssql",
            "connection_id": "mssql",
            "table": {"schema": "landing", "name": "orders"},
            "strategy": {"mode": "auto", "unique_key": ["order_id"]},
        },
    }


def _write_manifest(path: Path) -> None:
    path.write_text(
        """
name: public_orders__auto
source:
  type: postgres
  connection_id: pg
  connection_type: env
  table: {schema: public, name: orders}
  options:
    estimated_rows: 50000000
    changed_percent: 0.2
    delete_percent: 0.05
    partition_column: business_date
sink:
  type: mssql
  connection_id: mssql
  connection_type: env
  table: {schema: landing, name: orders}
  strategy:
    mode: auto
    unique_key: [order_id]
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_load_config_builder_compiles_auto_strategy_and_adaptive_batching() -> None:
    load_config = LoadConfigBuilder().build(_auto_config())

    assert load_config.load_strategy is LoadStrategy.PARTITION_REPLACE
    assert load_config.partition == {"column": "business_date", "values_from_staging": True}
    assert load_config.batch_size == 100_000
    assert load_config.options["strategy_intelligence"]["decision"]["requested_mode"] == "auto"
    assert load_config.options["strategy_intelligence"]["decision"]["strategy_mode"] == "partition_replace"
    assert load_config.options["strategy_intelligence"]["decision"]["native_fast_path"] == "postgres_copy_to_mssql_bcp"


def test_plan_cli_explain_strategy_embeds_decision_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    manifest = tmp_path / "orders.yaml"
    _write_manifest(manifest)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["plan", str(manifest), "--explain-strategy", "--format", "json"])

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["strategy"]["mode"] == "partition_replace"
    assert payload["strategy_intelligence"]["decision"]["strategy_mode"] == "partition_replace"
    assert payload["strategy_intelligence"]["decision"]["adaptive_batching"]["initial_batch_size"] == 100_000


def test_native_fast_path_preflight_reports_missing_and_present_tools() -> None:
    service = NativeFastPathPreflightService(tool_resolver=lambda name: f"/usr/bin/{name}" if name == "bcp" else None)

    result = service.check_path("postgres", "mssql")

    assert result.path_id == "postgres_copy_to_mssql_bcp"
    assert result.ready is False
    assert result.tools["bcp"].available is True
    assert result.tools["psycopg"].available is False


def test_strategy_repair_cli_writes_actionable_plan(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "strategy",
                "repair-plan",
                "01HY0000000000000000000000",
                "--source-type",
                "postgres",
                "--sink-type",
                "mssql",
                "--strategy",
                "partition_replace",
                "--failed-stage",
                "finalize",
                "--partition",
                "2026-01-01",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["safe_to_auto_resume"] is False
    assert payload["commands"][0].startswith("dpone resync")


def test_strategy_certification_artifact_writer_emits_json_and_markdown(tmp_path: Path) -> None:
    matrix = StrategyCertificationMatrixService().build()
    artifact = StrategyCertificationArtifactWriter(base_dir=tmp_path).write(matrix)

    assert artifact.json_path.exists()
    assert artifact.markdown_path.exists()
    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))
    markdown = artifact.markdown_path.read_text(encoding="utf-8")
    assert payload["entries"]
    assert "# dpone strategy certification matrix" in markdown
    assert "postgres" in markdown


def test_strategy_native_transfer_evidence_cli_writes_bundle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    plan = NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="postgres",
            sink_type="mssql",
            source_table="public.events",
            target_table="dbo.events",
            strategy="cdc_apply",
            unique_key=("event_id",),
            source_options={
                "cdc": {"state_boundary": "logical_lsn"},
                "partitioning": {"column": "event_id", "max_partitions": 2},
            },
            sink_options={"deletes": {"mode": "soft_delete"}},
        )
    )
    plan_json = tmp_path / "plan.json"
    plan_json.write_text(json.dumps({"native_transfer_plan": plan.to_dict()}), encoding="utf-8")
    payload_args: list[str] = []
    for artifact in plan.evidence_contract["required_artifacts"]:
        payload_path = tmp_path / artifact
        payload_path.write_text(json.dumps({"artifact": artifact, "status": "passed"}), encoding="utf-8")
        payload_args.extend(["--payload", f"{artifact}={payload_path}"])

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "strategy",
                "native-transfer-evidence",
                "--run-id",
                "01J00000000000000000000000",
                "--plan-json",
                str(plan_json),
                "--output-dir",
                str(tmp_path / "evidence"),
                "--format",
                "json",
                *payload_args,
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert Path(payload["index_path"]).exists()
    assert Path(payload["markdown_path"]).exists()
    index = json.loads(Path(payload["index_path"]).read_text(encoding="utf-8"))
    assert index["contract"]["route"] == "postgres_to_mssql"
    assert len(index["artifacts"]) == len(plan.evidence_contract["required_artifacts"])
