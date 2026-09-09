#!/usr/bin/env python3
"""Materialize and exact-verify the wide MSSQL dbt relation for route certification."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

_TOOLS_DIR = Path(__file__).resolve().parent
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

import mssql_clickhouse_wide_type_certification as wide  # noqa: E402
import mssql_dbt_wide_result as result_helpers  # noqa: E402
from local_route_certification_receipt import (  # noqa: E402
    LocalSourceSnapshot,
    capture_local_source_snapshot,
)
from local_route_certification_validation import prepare_create_once_directory  # noqa: E402
from mssql_dbt_wide_config import add_mssql_args  # noqa: E402
from mssql_dbt_wide_evidence import (  # noqa: E402
    assert_dbt_wide_artifact_secret_free,
    mssql_connection_sha256,
    relation_generation_sha256,
)
from mssql_dbt_wide_result import (  # noqa: E402
    DbtWideMaterializationResult,
    write_evidence,
)
from mssql_dbt_wide_schema import dbt_wide_schema_metrics  # noqa: E402
from mssql_dbt_wide_values import dbt_wide_value_metrics  # noqa: E402

_MODEL_UNIQUE_ID = "model.dpone_mssql_clickhouse_wide.wide_dbt_result"


@dataclass(frozen=True, slots=True)
class DbtWideMaterializationConfig:
    """Inputs for one local SQL Server dbt materialization proof."""

    source_schema: str
    release_id: str
    source_table: str
    target_schema: str
    target_table: str
    rows: int
    source_column_count: int
    output_dir: Path
    project_dir: Path
    target_path: Path
    mssql_params: dict[str, Any]


def dbt_command(config: DbtWideMaterializationConfig) -> tuple[str, ...]:
    """Return the exact non-partial dbt invocation used by the live producer."""

    return (
        "dbt",
        "run",
        "--project-dir",
        str(config.project_dir),
        "--profiles-dir",
        str(config.project_dir),
        "--target-path",
        str(config.target_path),
        "--no-partial-parse",
        "--vars",
        json.dumps(
            {"source_schema": config.source_schema, "source_table": config.source_table},
            separators=(",", ":"),
            sort_keys=True,
        ),
    )


def run_live_materialization(config: DbtWideMaterializationConfig) -> DbtWideMaterializationResult:
    """Run dbt against local MSSQL and verify relation, schema, keys, and calculation."""

    if config.rows != 10_000 or config.source_column_count != 201:
        raise ValueError("dbt_wide_release_shape_must_be_10000_rows_by_201_columns")
    if config.target_table != "wide_dbt_result":
        raise ValueError("dbt_wide_target_table_must_match_model_name")
    prepare_create_once_directory(config.output_dir)
    prepare_create_once_directory(config.target_path)
    snapshot = capture_local_source_snapshot()
    environment = _dbt_environment(config)
    completed = subprocess.run(
        dbt_command(config),
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if completed.returncode != 0:
        result = _failure_result(config, snapshot, "dbt_run_failed")
        write_evidence(
            config.output_dir,
            result,
            forbidden_secret_values=(str(config.mssql_params.get("password") or ""),),
        )
        raise RuntimeError("dbt_wide_materialization_failed")

    run_results_path = config.target_path / "run_results.json"
    manifest_path = config.target_path / "manifest.json"
    _require_dbt_success(run_results_path, manifest_path, config)
    forbidden_secrets = (str(config.mssql_params.get("password") or ""),)
    assert_dbt_wide_artifact_secret_free(run_results_path, forbidden_secret_values=forbidden_secrets)
    assert_dbt_wide_artifact_secret_free(manifest_path, forbidden_secret_values=forbidden_secrets)
    run_results_copy = result_helpers.retain_artifact(run_results_path, config.output_dir, "dbt_run_results.json")
    manifest_copy = result_helpers.retain_artifact(manifest_path, config.output_dir, "dbt_manifest.json")
    metrics = _relation_metrics(config)
    passed = (
        metrics["source_count"] == metrics["target_count"] == config.rows
        and metrics["distinct_key_count"] == config.rows
        and metrics["source_column_count"] == config.source_column_count
        and metrics["target_column_count"] == config.source_column_count + 1
        and metrics["schema_mismatch_count"] == 0
        and metrics["canonical_source_mismatch_count"] == 0
        and metrics["source_schema_sha256"] == metrics["passthrough_schema_sha256"]
        and metrics["source_data_sha256"] == metrics["passthrough_data_sha256"]
        and metrics["calculated_column_type"] == "decimal(38,8)"
        and metrics["calculated_column_nullable"] is True
        and metrics["calculated_mismatch_count"] == 0
        and capture_local_source_snapshot() == snapshot
    )
    generation_payload = {
        "mssql_connection_sha256": mssql_connection_sha256(config.mssql_params),
        "output_data_rows": metrics["target_count"],
        "output_data_sha256": metrics["output_data_sha256"],
        "output_relation": f"{config.target_schema}.{config.target_table}",
        "passthrough_schema_sha256": metrics["passthrough_schema_sha256"],
    }
    result = DbtWideMaterializationResult(
        created_at=result_helpers.now(),
        release_id=config.release_id,
        git_head_sha=snapshot.git_head_sha,
        source_snapshot_sha256=snapshot.source_snapshot_sha256,
        worktree_dirty=snapshot.worktree_dirty,
        project_sha256=_project_sha256(config.project_dir),
        mssql_connection_sha256=str(generation_payload["mssql_connection_sha256"]),
        source_relation=f"{config.source_schema}.{config.source_table}",
        output_relation=f"{config.target_schema}.{config.target_table}",
        model_unique_id=_MODEL_UNIQUE_ID,
        materialization="table",
        source_count=metrics["source_count"],
        target_count=metrics["target_count"],
        distinct_key_count=metrics["distinct_key_count"],
        source_column_count=metrics["source_column_count"],
        target_column_count=metrics["target_column_count"],
        schema_mismatch_count=metrics["schema_mismatch_count"],
        canonical_source_schema_sha256=str(metrics["canonical_source_schema_sha256"]),
        canonical_source_mismatch_count=metrics["canonical_source_mismatch_count"],
        source_schema_sha256=str(metrics["source_schema_sha256"]),
        passthrough_schema_sha256=str(metrics["passthrough_schema_sha256"]),
        source_data_sha256=str(metrics["source_data_sha256"]),
        passthrough_data_sha256=str(metrics["passthrough_data_sha256"]),
        output_data_sha256=str(metrics["output_data_sha256"]),
        output_data_rows=metrics["target_count"],
        relation_generation_sha256=relation_generation_sha256(generation_payload),
        calculated_column_type=str(metrics["calculated_column_type"]),
        calculated_column_nullable=bool(metrics["calculated_column_nullable"]),
        calculated_mismatch_count=metrics["calculated_mismatch_count"],
        run_results_path=run_results_copy.name,
        run_results_sha256=result_helpers.file_sha256(run_results_copy),
        manifest_path=manifest_copy.name,
        manifest_sha256=result_helpers.file_sha256(manifest_copy),
        passed=passed,
        error=None if passed else "dbt_wide_exact_reconciliation_failed",
    )
    write_evidence(
        config.output_dir,
        result,
        forbidden_secret_values=(str(config.mssql_params.get("password") or ""),),
    )
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-schema", required=True)
    parser.add_argument("--target-table", default="wide_dbt_result")
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--source-column-count", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--project-dir", type=Path, default=Path("tests/fixtures/mssql-clickhouse-wide-dbt"))
    parser.add_argument("--target-path", type=Path, required=True)
    add_mssql_args(parser)
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> DbtWideMaterializationConfig:
    project_dir = args.project_dir.expanduser().resolve()
    expected_project_dir = (Path(__file__).resolve().parents[1] / "tests/fixtures/mssql-clickhouse-wide-dbt").resolve()
    if project_dir != expected_project_dir:
        raise ValueError("dbt_wide_project_must_be_checked_in_fixture")
    return DbtWideMaterializationConfig(
        source_schema=args.source_schema,
        release_id=args.release_id,
        source_table=args.source_table,
        target_schema=args.target_schema,
        target_table=args.target_table,
        rows=args.rows,
        source_column_count=args.source_column_count,
        output_dir=args.output_dir.expanduser().resolve(),
        project_dir=project_dir,
        target_path=args.target_path.expanduser().resolve(),
        mssql_params=wide._mssql_params(args),
    )


def main(argv: list[str] | None = None) -> int:
    config = build_config(parse_args(argv))
    result = run_live_materialization(config)
    payload = json.loads((config.output_dir / "mssql_dbt_wide_materialization.json").read_text(encoding="utf-8"))
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.passed and payload["evidence_status"] == "PASS" else 1


def _relation_metrics(config: DbtWideMaterializationConfig) -> dict[str, Any]:
    connector = wide._mssql_connector(cast(wide.WideTypeCertificationConfig, config))
    source = f"{wide._mssql_ident(config.source_schema)}.{wide._mssql_ident(config.source_table)}"
    target = f"{wide._mssql_ident(config.target_schema)}.{wide._mssql_ident(config.target_table)}"
    try:
        schema = dbt_wide_schema_metrics(
            connector,
            source_schema=config.source_schema,
            source_table=config.source_table,
            target_schema=config.target_schema,
            target_table=config.target_table,
        )
        values = dbt_wide_value_metrics(
            connector,
            source_schema=config.source_schema,
            source_table=config.source_table,
            target_schema=config.target_schema,
            target_table=config.target_table,
            rows=config.rows,
        )
        return {
            "source_count": _scalar(connector, f"SELECT COUNT_BIG(*) FROM {source}"),
            "target_count": _scalar(connector, f"SELECT COUNT_BIG(*) FROM {target}"),
            "distinct_key_count": _scalar(connector, f"SELECT COUNT_BIG(DISTINCT [order_id]) FROM {target}"),
            "source_column_count": schema.source_column_count,
            "target_column_count": schema.target_column_count,
            "schema_mismatch_count": schema.mismatch_count,
            "canonical_source_schema_sha256": schema.canonical_source_sha256,
            "canonical_source_mismatch_count": schema.canonical_source_mismatch_count,
            "source_schema_sha256": schema.source_sha256,
            "passthrough_schema_sha256": schema.passthrough_sha256,
            "source_data_sha256": values.source_sha256,
            "passthrough_data_sha256": values.passthrough_sha256,
            "output_data_sha256": values.output_sha256,
            "calculated_column_type": schema.calculated_column_type,
            "calculated_column_nullable": schema.calculated_column_nullable,
            "calculated_mismatch_count": _scalar(
                connector,
                f"""
SELECT COUNT_BIG(*)
FROM {target}
WHERE
    ([dbt_calculated_amount] IS NULL AND [amount] IS NOT NULL)
    OR ([dbt_calculated_amount] IS NOT NULL AND [amount] IS NULL)
    OR [dbt_calculated_amount] <> CAST([amount] * CAST(2 AS decimal(18, 4)) AS decimal(38, 8))
""",
            ),
        }
    finally:
        wide._close_quietly(connector)


def _scalar(connector: Any, sql: str) -> int:
    rows = connector.get_records(sql)
    if not rows:
        raise RuntimeError("dbt_wide_metric_missing")
    return int(rows[0][0])


def _require_dbt_success(run_results_path: Path, manifest_path: Path, config: DbtWideMaterializationConfig) -> None:
    payload = json.loads(run_results_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    results = payload.get("results") if isinstance(payload, dict) else None
    matched = [item for item in results or [] if item.get("unique_id") == _MODEL_UNIQUE_ID]
    if len(matched) != 1 or matched[0].get("status") != "success":
        raise RuntimeError("dbt_wide_run_result_not_successful")
    nodes = manifest.get("nodes") if isinstance(manifest, dict) else None
    node = nodes.get(_MODEL_UNIQUE_ID) if isinstance(nodes, dict) else None
    if not isinstance(node, dict):
        raise RuntimeError("dbt_wide_manifest_node_missing")
    expected_relation = f'"{config.mssql_params["database"]}"."{config.target_schema}"."{config.target_table}"'
    if (
        node.get("unique_id") != _MODEL_UNIQUE_ID
        or node.get("package_name") != "dpone_mssql_clickhouse_wide"
        or node.get("name") != config.target_table
        or node.get("database") != config.mssql_params["database"]
        or node.get("schema") != config.target_schema
        or node.get("relation_name") != expected_relation
        or node.get("config", {}).get("materialized") != "table"
        or matched[0].get("relation_name") != expected_relation
    ):
        raise RuntimeError("dbt_wide_materialized_relation_mismatch")


def _failure_result(
    config: DbtWideMaterializationConfig,
    snapshot: LocalSourceSnapshot,
    error: str,
) -> DbtWideMaterializationResult:
    return result_helpers.failure_result(
        config,
        snapshot,
        error,
        project_sha256=_project_sha256(config.project_dir),
    )


def _project_sha256(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        relative = path.relative_to(root)
        if relative.parts[0] in {"target", "logs"} or path.name == ".user.yml":
            continue
        digest.update(relative.as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return "sha256:" + digest.hexdigest()


def _dbt_environment(config: DbtWideMaterializationConfig) -> dict[str, str]:
    params = config.mssql_params
    return {
        **os.environ,
        "DPONE_DBT_WIDE_SCHEMA": config.target_schema,
        "DPONE_IT_MSSQL_HOST": str(params["host"]),
        "DPONE_IT_MSSQL_PORT": str(params["port"]),
        "DPONE_IT_MSSQL_PORT_FORWARD": str(params["port"]),
        "DPONE_IT_MSSQL_DATABASE": str(params["database"]),
        "DPONE_IT_MSSQL_USER": str(params["username"]),
        "DPONE_IT_MSSQL_PASSWORD": str(params["password"]),
        "DPONE_IT_MSSQL_DRIVER": str(params["driver"]),
    }


if __name__ == "__main__":
    raise SystemExit(main())
