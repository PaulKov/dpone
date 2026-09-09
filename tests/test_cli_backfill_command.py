from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config.mssql_strategy_contract import MSSQLStrategyContractError
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
      chunk:
        column: business_date
        from: "2025-01-01"
        to: "2025-01-10"
        step: 3d
"""

_NON_BACKFILL_MANIFEST = _MANIFEST.replace("mode: backfill", "mode: full_refresh")

_MSSQL_BACKFILL_MANIFEST = """
name: postgres_mssql_backfill

source:
  type: postgres
  connection_id: postgres_oltp
  connection_type: env
  table:
    schema: public
    name: orders
  options:
    export_format: csv

sink:
  type: mssql
  connection_id: mssql_dwh
  connection_type: env
  table:
    schema: dbo
    name: orders
  strategy:
    mode: backfill
    backfill:
      inner_mode: replace
      chunk:
        column: id
        kind: integer
        from: 1
        to: 200
        step: 1
"""


def _write_manifest(tmp_path: Path, text: str = _MANIFEST) -> Path:
    manifest = tmp_path / "orders_backfill.yml"
    manifest.write_text(text.lstrip(), encoding="utf-8")
    return manifest


def _ctx() -> ManifestCommandContext:
    return ManifestCommandContext(registry_paths=(), loader=ManifestLoaderRouter(registry_paths=()))


def test_backfill_plan_builds_deterministic_chunk_plan(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)

    payload = BackfillCommandService().plan(path=manifest, manifest_ctx=_ctx())

    assert payload["kind"] == "dpone.backfill_plan"
    assert payload["dataset"] == "analytics.orders"
    assert payload["inner_mode"] == "partition_replace"
    assert payload["chunks_total"] == 4
    assert payload["counts"] == {"pending": 4}
    first_chunk = payload["chunks"][0]
    assert first_chunk["portable_scope"] == {
        "column": "business_date",
        "kind": "range",
        "version": 1,
        "lower": {"inclusive": True, "value": {"type": "date", "value": "2025-01-01"}},
        "upper": {"inclusive": False, "value": {"type": "date", "value": "2025-01-04"}},
    }
    assert len(first_chunk["portable_scope_sha256"]) == 64
    assert "predicate" not in first_chunk
    assert not Path(payload["state_path"]).exists()  # planning moves no state


def test_backfill_plan_can_attach_advisor_recommendation(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)

    payload = BackfillCommandService().plan(path=manifest, manifest_ctx=_ctx(), advisor=True)

    assert payload["advisor"]["schema_version"] == "dpone.backfill.advisor.v1"
    assert payload["advisor"]["recommended_max_parallel_chunks"] == 1


def test_backfill_plan_advisor_reads_failed_vendor_matrix_evidence(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)
    evidence = tmp_path / "certification_report.json"
    evidence.write_text(
        json.dumps(
            {
                "certification_report": {
                    "passed": False,
                    "total_cases": 4,
                    "failed_cases": 1,
                    "blockers": ["matrix_rows.mismatch:postgres_to_mssql__backfill"],
                }
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

    assert payload["advisor"]["recommendation"] == "resolve_vendor_certification_failures"
    assert payload["advisor"]["recommended_max_parallel_chunks"] == 1
    assert payload["advisor"]["evidence"]["failed_matrix_cases"] == 1
    assert payload["advisor"]["evidence_sources"] == ["chunk_ledger", "matrix_report"]


def test_backfill_plan_rejects_non_backfill_manifests(tmp_path: Path) -> None:
    manifest = _write_manifest(tmp_path, _NON_BACKFILL_MANIFEST)

    with pytest.raises(ValueError, match="requires sink.strategy.mode: backfill"):
        BackfillCommandService().plan(path=manifest, manifest_ctx=_ctx())


def test_backfill_plan_requires_chunk_window_or_overrides(tmp_path: Path) -> None:
    text = _MANIFEST.replace(
        '      chunk:\n        column: business_date\n        from: "2025-01-01"\n        to: "2025-01-10"\n        step: 3d\n',
        "",
    )
    manifest = _write_manifest(tmp_path, text)

    class _ForbiddenStateFactory:
        def build(self, *_args, **_kwargs):
            raise AssertionError("invalid plan reached state resolution")

    with pytest.raises(ValueError, match="No backfill.chunk window configured"):
        BackfillCommandService(state_store_factory=_ForbiddenStateFactory()).plan(
            path=manifest,
            manifest_ctx=_ctx(),
        )


def test_backfill_overrides_reshape_the_window(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)

    payload = BackfillCommandService().plan(
        path=manifest,
        manifest_ctx=_ctx(),
        overrides={"from": "2025-02-01", "to": "2025-02-02", "step": "1d", "inner_mode": "replace"},
    )

    assert payload["chunk_config"]["from"] == "2025-02-01"
    assert payload["inner_mode"] == "replace"
    assert payload["chunks_total"] == 2


@pytest.mark.parametrize("command", ("plan", "status", "cancel", "doctor", "run", "retry_failed"))
def test_mssql_cli_override_is_validated_before_state_plan_or_run_io(
    tmp_path: Path,
    command: str,
) -> None:
    manifest = _write_manifest(tmp_path, _MSSQL_BACKFILL_MANIFEST)

    class _ForbiddenStateFactory:
        calls = 0

        def build(self, *_args, **_kwargs):
            self.calls += 1
            raise AssertionError("invalid override reached state resolution")

    class _ForbiddenRunService:
        calls = 0

        def run(self, **_kwargs):
            self.calls += 1
            raise AssertionError("invalid override reached manifest execution")

    state_factory = _ForbiddenStateFactory()
    run_service = _ForbiddenRunService()
    service = BackfillCommandService(
        state_store_factory=state_factory,
        run_service=run_service,
    )
    kwargs = {
        "path": manifest,
        "manifest_ctx": _ctx(),
        "overrides": {"inner_mode": "incremental_append"},
    }
    if command == "cancel":
        kwargs.update(reason="operator stop", requested_by="qa")
    elif command == "run":
        kwargs["execute"] = True

    with pytest.raises(MSSQLStrategyContractError) as raised:
        getattr(service, command)(**kwargs)

    assert raised.value.blocker == "mssql.strategy.backfill.incremental_append_publication"
    assert state_factory.calls == 0
    assert run_service.calls == 0


def test_backfill_run_is_dry_run_by_default(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)

    payload = BackfillCommandService().run(path=manifest, manifest_ctx=_ctx(), execute=False)

    assert payload["kind"] == "dpone.backfill_plan"
    assert payload["executed"] is False
    assert payload["next_actions"] == ["re-run with --execute to load the pending chunks"]


def test_backfill_status_reads_persisted_ledger(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)
    service = BackfillCommandService()
    plan = service.plan(path=manifest, manifest_ctx=_ctx())

    ledger_path = Path(plan["state_path"])
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    committed = [dict(chunk, status="success" if chunk["index"] <= 2 else "pending") for chunk in plan["chunks"]]
    ledger_path.write_text(
        json.dumps(
            {
                "kind": "dpone.backfill_ledger",
                "schema_version": "2",
                "run_key": plan["run_key"],
                "dataset": plan["dataset"],
                "inner_mode": plan["inner_mode"],
                "chunk_config": plan["chunk_config"],
                "chunks": committed,
            }
        ),
        encoding="utf-8",
    )

    payload = service.status(path=manifest, manifest_ctx=_ctx())

    assert payload["kind"] == "dpone.backfill_status"
    assert payload["counts"] == {"success": 2, "pending": 2}


def test_backfill_doctor_recommends_retry_failed_for_failed_chunks(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)
    service = BackfillCommandService()
    plan = service.plan(path=manifest, manifest_ctx=_ctx())
    ledger_path = Path(plan["state_path"])
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    chunks = [
        dict(chunk, status="success" if chunk["index"] == 1 else "failed" if chunk["index"] == 2 else "pending")
        for chunk in plan["chunks"]
    ]
    ledger_path.write_text(
        json.dumps(
            {
                "kind": "dpone.backfill_ledger",
                "schema_version": "2",
                "run_key": plan["run_key"],
                "dataset": plan["dataset"],
                "inner_mode": plan["inner_mode"],
                "chunk_config": plan["chunk_config"],
                "chunks": chunks,
            }
        ),
        encoding="utf-8",
    )

    payload = service.doctor(path=manifest, manifest_ctx=_ctx())

    assert payload["status"] == "warning"
    assert payload["next_actions"] == [
        f"dpone backfill retry-failed {manifest} --execute",
        f"dpone backfill status {manifest} --format json",
    ]


def test_backfill_doctor_recommends_resume_for_pending_chunks(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)
    service = BackfillCommandService()
    plan = service.plan(path=manifest, manifest_ctx=_ctx())
    ledger_path = Path(plan["state_path"])
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    chunks = [dict(chunk, status="success" if chunk["index"] <= 2 else "pending") for chunk in plan["chunks"]]
    ledger_path.write_text(
        json.dumps(
            {
                "kind": "dpone.backfill_ledger",
                "schema_version": "2",
                "run_key": plan["run_key"],
                "dataset": plan["dataset"],
                "inner_mode": plan["inner_mode"],
                "chunk_config": plan["chunk_config"],
                "chunks": chunks,
            }
        ),
        encoding="utf-8",
    )

    payload = service.doctor(path=manifest, manifest_ctx=_ctx())

    assert payload["status"] == "warning"
    assert payload["next_actions"] == [f"dpone backfill resume {manifest}"]


def test_backfill_doctor_passes_completed_campaign(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)
    service = BackfillCommandService()
    plan = service.plan(path=manifest, manifest_ctx=_ctx())
    ledger_path = Path(plan["state_path"])
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps(
            {
                "kind": "dpone.backfill_ledger",
                "schema_version": "2",
                "run_key": plan["run_key"],
                "dataset": plan["dataset"],
                "inner_mode": plan["inner_mode"],
                "chunk_config": plan["chunk_config"],
                "chunks": [dict(chunk, status="success") for chunk in plan["chunks"]],
            }
        ),
        encoding="utf-8",
    )

    payload = service.doctor(path=manifest, manifest_ctx=_ctx())

    assert payload["status"] == "passed"
    assert payload["next_actions"] == []


def test_backfill_cancel_marks_campaign_without_running_data(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)
    service = BackfillCommandService()
    plan = service.plan(path=manifest, manifest_ctx=_ctx())
    ledger_path = Path(plan["state_path"])
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        json.dumps(
            {
                "kind": "dpone.backfill_ledger",
                "schema_version": "2",
                "run_key": plan["run_key"],
                "dataset": plan["dataset"],
                "inner_mode": plan["inner_mode"],
                "chunk_config": plan["chunk_config"],
                "chunks": plan["chunks"],
            }
        ),
        encoding="utf-8",
    )

    payload = service.cancel(path=manifest, manifest_ctx=_ctx(), reason="operator stop", requested_by="qa")

    assert payload["kind"] == "dpone.backfill_cancel"
    assert payload["status"] == "cancel_requested"
    assert payload["cancel_reason"] == "operator stop"


def test_backfill_retry_failed_executes_with_failed_only_policy(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = _write_manifest(tmp_path)

    class _Report:
        def to_dict(self):
            return {
                "passed": False,
                "result": {
                    "details": {
                        "backfill": {
                            "operation_status": "success",
                            "chunks_selected": 1,
                        }
                    }
                },
            }

    class _RunService:
        def __init__(self) -> None:
            self.mutated_options = None

        def run(self, **kwargs):
            load_config = BackfillCommandService()._load_config(
                path=manifest,
                manifest_ctx=_ctx(),
                selector=None,
            )
            mutated = kwargs["load_config_mutator"](load_config)
            self.mutated_options = mutated.options["backfill"]
            return _Report()

    run_service = _RunService()
    payload = BackfillCommandService(run_service=run_service).retry_failed(path=manifest, manifest_ctx=_ctx())

    assert payload["kind"] == "dpone.backfill_retry_failed"
    assert payload["operation_passed"] is True
    assert run_service.mutated_options["retry_policy"] == "failed_only"


def test_backfill_retry_failed_command_exits_zero_for_successful_operation(monkeypatch, capsys) -> None:
    from dpone.commands import backfill_cmd

    class _Service:
        def retry_failed(self, **kwargs):
            return {
                "kind": "dpone.backfill_retry_failed",
                "passed": False,
                "operation_passed": True,
                "result": {
                    "details": {
                        "backfill": {
                            "operation_status": "success",
                            "chunks_selected": 1,
                        }
                    }
                },
            }

    monkeypatch.setattr(backfill_cmd, "BackfillCommandService", lambda: _Service())
    args = SimpleNamespace(
        path="orders.yml",
        registry=[],
        selector=None,
        column=None,
        window_from=None,
        window_to=None,
        step=None,
        kind=None,
        inner_mode=None,
        parallel_workers=None,
        state_dir=None,
        backfill_id=None,
        max_chunks=None,
        predicate_dialect=None,
        run_id=None,
        dag_id=None,
        execution_date=None,
        format="json",
    )

    code = backfill_cmd.cmd_backfill_retry_failed(args, ctx=object(), logger=logging.getLogger("test"))
    output = json.loads(capsys.readouterr().out)

    assert code == 0
    assert output["operation_passed"] is True
