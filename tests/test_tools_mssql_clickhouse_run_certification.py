from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import yaml

from dpone.contracts.process_types import ProcessResult
from dpone.runtime.lineage.partition_checkpoint_store import JsonlPartitionCheckpointStore
from dpone.services.run_manifest import RunManifestResult


def _load_tool_module():
    path = Path("tools/mssql_clickhouse_run_certification.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_clickhouse_run_certification", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _result(manifest: Path, run_id: str, *, inserted_rows: int) -> RunManifestResult:
    return RunManifestResult(
        manifest=str(manifest),
        process="mssql_to_clickhouse_native_run_cert",
        selector=None,
        run_id=run_id,
        passed=True,
        result=ProcessResult(
            status="success",
            extracted_rows=inserted_rows,
            inserted_rows=inserted_rows,
            updated_rows=0,
            final_rows=inserted_rows,
            duration_seconds=0.1,
            errors=[],
        ),
    )


def test_build_manifest_uses_dpone_run_path_and_sql_backed_state(tmp_path: Path) -> None:
    module = _load_tool_module()
    manifest_path = tmp_path / "manifest.yml"
    config = module.LiveRunCertificationConfig(
        rows=1000,
        source_schema="dbo",
        source_table="orders",
        target_database="analytics",
        target_table="orders_ch",
        state_schema="etl_state",
        checkpoint_table="dpone_partition_checkpoints",
        evidence_dir=tmp_path / "evidence",
        mssql_params={"host": "127.0.0.1", "password": "secret", "bcp_path": "bcp"},
        clickhouse_params={"host": "127.0.0.1", "password": "secret"},
        partition_column="order_id",
        target_rows_per_partition=500,
        export_workers=2,
        load_workers=2,
        bcp_batch_size=1000,
    )

    module.write_manifest(manifest_path, config)

    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    assert manifest["runtime"] == {"compatibility": {"legacy_runtime_connections": "explicit_only"}}
    assert manifest["source"]["type"] == "mssql"
    assert manifest["source"]["connection_type"] == "params"
    assert json.loads(manifest["source"]["connection_id"])["bcp_path"] == "bcp"
    assert manifest["source"]["options"]["partitioning"]["export_workers"] == 2
    assert manifest["source"]["options"]["partitioning"]["load_workers"] == 2
    assert manifest["sink"]["type"] == "clickhouse"
    assert manifest["sink"]["strategy"]["mode"] == "incremental_append"
    assert manifest["sink"]["options"]["native_transfer"]["require_artifact_checksum"] is True
    assert manifest["sink"]["options"]["runtime_evidence"]["output_dir"] == str(config.evidence_dir / "runtime")
    assert manifest["state"] == {
        "type": "mssql",
        "connection_type": "params",
        "connection_id": manifest["source"]["connection_id"],
        "vault_mount_point": "local-params-explicit",
        "table": {"schema": "etl_state"},
        "partition_checkpoint_table": {"schema": "etl_state", "name": "dpone_partition_checkpoints"},
    }


def test_certify_dpone_run_executes_two_real_run_callbacks_and_writes_evidence(tmp_path: Path) -> None:
    module = _load_tool_module()
    manifest_path = tmp_path / "manifest.yml"
    manifest_path.write_text("source: {}\n", encoding="utf-8")
    runtime_dir = tmp_path / "runtime"
    checkpoint_store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    calls: list[str] = []

    def run_once(path: Path, *, run_id: str) -> RunManifestResult:
        assert path == manifest_path
        calls.append(run_id)
        inserted = 2 if len(calls) == 1 else 0
        checkpoint_store.upsert(module._checkpoint(index=0, status="committed"))
        checkpoint_store.upsert(module._checkpoint(index=1, status="committed"))
        _write_runtime_report(
            runtime_dir, run_id, skip=0 if inserted else 2, retry=2 if inserted else 0, inserted=inserted
        )
        return _result(manifest_path, run_id, inserted_rows=inserted)

    result = module.certify_dpone_run(
        manifest_path=manifest_path,
        evidence_dir=tmp_path / "evidence",
        runtime_report_dir=runtime_dir,
        checkpoint_store=checkpoint_store,
        run_once=run_once,
    )

    assert calls == ["dpone-native-transfer-first", "dpone-native-transfer-second"]
    assert result.passed is True
    assert (tmp_path / "evidence" / "native_transfer_run_certification.json").exists()
    assert (tmp_path / "evidence" / "native_transfer_run_certification.md").exists()


def _write_runtime_report(path: Path, run_id: str, *, skip: int, retry: int, inserted: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / f"native_transfer_runtime_{run_id}.json").write_text(
        json.dumps(
            {
                "schema_version": "dpone.native_transfer.runtime_report.v1",
                "run_id": run_id,
                "resume_plan": {"summary": {"skip": skip, "retry": retry}},
                "load_result": {"inserted_rows": inserted},
                "passed": True,
            }
        ),
        encoding="utf-8",
    )
