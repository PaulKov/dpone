from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator


def _load_tool_module():
    path = Path("tools/mssql_dbt_wide_materialization.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_dbt_wide_materialization", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _passing_result(module, root: Path):
    connection_sha256 = "sha256:" + "2" * 64
    output_data_sha256 = "sha256:" + "4" * 64
    relation_generation = module.relation_generation_sha256(
        {
            "mssql_connection_sha256": connection_sha256,
            "output_data_rows": 10_000,
            "output_data_sha256": output_data_sha256,
            "output_relation": "dbt_calc.wide_dbt_result",
            "passthrough_schema_sha256": "sha256:" + "f" * 64,
        }
    )
    return module.DbtWideMaterializationResult(
        created_at="2026-08-10T00:00:00Z",
        release_id="0.74.0",
        git_head_sha="a" * 40,
        source_snapshot_sha256="sha256:" + "b" * 64,
        worktree_dirty=False,
        project_sha256="sha256:" + "c" * 64,
        mssql_connection_sha256=connection_sha256,
        source_relation="src.orders",
        output_relation="dbt_calc.wide_dbt_result",
        model_unique_id="model.dpone_mssql_clickhouse_wide.wide_dbt_result",
        materialization="table",
        source_count=10_000,
        target_count=10_000,
        distinct_key_count=10_000,
        source_column_count=201,
        target_column_count=202,
        schema_mismatch_count=0,
        canonical_source_schema_sha256=__import__("mssql_dbt_wide_schema").canonical_wide_source_schema_sha256(201),
        canonical_source_mismatch_count=0,
        source_schema_sha256="sha256:" + "f" * 64,
        passthrough_schema_sha256="sha256:" + "f" * 64,
        source_data_sha256="sha256:" + "3" * 64,
        passthrough_data_sha256="sha256:" + "3" * 64,
        output_data_sha256=output_data_sha256,
        output_data_rows=10_000,
        relation_generation_sha256=relation_generation,
        calculated_column_type="decimal(38,8)",
        calculated_column_nullable=True,
        calculated_mismatch_count=0,
        run_results_path="dbt_run_results.json",
        run_results_sha256="sha256:"
        + __import__("hashlib").sha256((root / "dbt_run_results.json").read_bytes()).hexdigest(),
        manifest_path="dbt_manifest.json",
        manifest_sha256="sha256:" + __import__("hashlib").sha256((root / "dbt_manifest.json").read_bytes()).hexdigest(),
        passed=True,
    )


def test_dbt_wide_result_is_exact_commit_bound_and_self_hashed(tmp_path: Path) -> None:
    module = _load_tool_module()
    (tmp_path / "dbt_run_results.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "dbt_manifest.json").write_text("{}\n", encoding="utf-8")
    result = _passing_result(module, tmp_path)
    connection_sha256 = result.mssql_connection_sha256
    output_data_sha256 = result.output_data_sha256

    path = module.write_evidence(tmp_path, result)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "dpone.dbt_sqlserver.wide_materialization.v1"
    assert payload["environment_class"] == "LOCAL_DOCKER"
    assert payload["evidence_status"] == "PASS"
    assert payload["output_relation"] == "dbt_calc.wide_dbt_result"
    assert payload["target_column_count"] == 202
    assert payload["schema_mismatch_count"] == 0
    assert payload["canonical_source_mismatch_count"] == 0
    assert payload["calculated_column_nullable"] is True
    assert payload["source_schema_sha256"] == payload["passthrough_schema_sha256"]
    assert payload["source_data_sha256"] == payload["passthrough_data_sha256"]
    assert payload["output_data_sha256"] == output_data_sha256
    assert payload["calculated_column_type"] == "decimal(38,8)"
    assert payload["calculated_mismatch_count"] == 0
    assert payload["production_certification"] == "UNVERIFIED"
    assert payload["evidence_sha256"].startswith("sha256:")
    schema = json.loads(
        Path("docs/schemas/dpone.dbt-sqlserver-wide-materialization.v1.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(payload)

    verified = __import__("mssql_dbt_wide_evidence").require_exact_dbt_wide_evidence(
        path,
        release_id="0.74.0",
        git_head_sha="a" * 40,
        source_snapshot_sha256="sha256:" + "b" * 64,
        output_relation="dbt_calc.wide_dbt_result",
        connection_sha256=connection_sha256,
        expected_rows=10_000,
        expected_source_columns=201,
        expected_target_columns=202,
    )
    assert verified["evidence_status"] == "PASS"


def test_dbt_wide_command_pins_project_target_and_source_relation(tmp_path: Path) -> None:
    module = _load_tool_module()
    config = module.DbtWideMaterializationConfig(
        source_schema="src",
        release_id="0.74.0",
        source_table="orders",
        target_schema="dbt_calc",
        target_table="wide_dbt_result",
        rows=10_000,
        source_column_count=201,
        output_dir=tmp_path / "evidence",
        project_dir=Path("tests/fixtures/mssql-clickhouse-wide-dbt"),
        target_path=tmp_path / "target",
        mssql_params={
            "host": "127.0.0.1",
            "port": 1433,
            "database": "dpone_it",
            "username": "sa",
            "password": "secret",
            "driver": "ODBC Driver 18 for SQL Server",
        },
    )

    command = module.dbt_command(config)

    assert command[:2] == ("dbt", "run")
    assert "--no-partial-parse" in command
    assert str(config.project_dir) in command
    variables = json.loads(command[command.index("--vars") + 1])
    assert variables == {"source_schema": "src", "source_table": "orders"}
    environment = module._dbt_environment(config)
    assert environment["DPONE_IT_MSSQL_HOST"] == "127.0.0.1"
    assert environment["DPONE_IT_MSSQL_PORT_FORWARD"] == "1433"
    assert environment["DPONE_IT_MSSQL_DATABASE"] == "dpone_it"


def test_dbt_wide_failure_evidence_conforms_to_public_schema(tmp_path: Path) -> None:
    module = _load_tool_module()
    config = module.DbtWideMaterializationConfig(
        source_schema="src",
        release_id="0.74.0",
        source_table="orders",
        target_schema="dbt_calc",
        target_table="wide_dbt_result",
        rows=10,
        source_column_count=201,
        output_dir=tmp_path,
        project_dir=Path("tests/fixtures/mssql-clickhouse-wide-dbt"),
        target_path=tmp_path / "target",
        mssql_params={
            "host": "127.0.0.1",
            "port": 1433,
            "database": "dpone_it",
            "username": "sa",
            "password": "secret",
            "driver": "ODBC Driver 18 for SQL Server",
        },
    )
    snapshot = module.LocalSourceSnapshot("a" * 40, "sha256:" + "b" * 64, False)
    path = module.write_evidence(tmp_path, module._failure_result(config, snapshot, "dbt_run_failed"))
    schema = json.loads(
        Path("docs/schemas/dpone.dbt-sqlserver-wide-materialization.v1.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(json.loads(path.read_text(encoding="utf-8")))

    with pytest.raises(FileExistsError):
        module.write_evidence(tmp_path, module._failure_result(config, snapshot, "retry_failed"))


def test_pass_evidence_is_not_published_before_secret_verification(tmp_path: Path) -> None:
    module = _load_tool_module()
    (tmp_path / "dbt_run_results.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "dbt_manifest.json").write_text('{"compiled":"actual-password"}\n', encoding="utf-8")
    result = _passing_result(module, tmp_path)

    with pytest.raises(ValueError, match="upstream_evidence_secret_material_detected"):
        module.write_evidence(tmp_path, result, forbidden_secret_values=("actual-password",))

    assert not (tmp_path / "mssql_dbt_wide_materialization.json").exists()
    assert not tuple(tmp_path.glob(".mssql-dbt-wide-pending-*"))


def test_pass_evidence_rejects_suspicious_literal_without_external_secret_context(tmp_path: Path) -> None:
    module = _load_tool_module()
    (tmp_path / "dbt_run_results.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "dbt_manifest.json").write_text('{"metadata":{"value":"plain-secret"}}\n', encoding="utf-8")
    result = _passing_result(module, tmp_path)

    with pytest.raises(ValueError, match="upstream_evidence_secret_material_detected"):
        module.write_evidence(tmp_path, result)

    assert not (tmp_path / "mssql_dbt_wide_materialization.json").exists()


def test_dbt_secret_scan_distinguishes_macro_resource_identity_from_secret_field(tmp_path: Path) -> None:
    module = _load_tool_module()
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "macros": {
                    "macro.dbt_sqlserver.sqlserver__create_schema_with_authorization": {
                        "name": "sqlserver__create_schema_with_authorization"
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    module.assert_dbt_wide_artifact_secret_free(manifest)

    manifest.write_text(json.dumps({"metadata": {"password": "not-public"}}), encoding="utf-8")
    with pytest.raises(ValueError, match="upstream_evidence_secret_material_detected"):
        module.assert_dbt_wide_artifact_secret_free(manifest)


def test_dbt_evidence_rejects_symlinked_retained_artifact(tmp_path: Path) -> None:
    module = _load_tool_module()
    outside = tmp_path / "outside"
    outside.mkdir()
    external = outside / "manifest.json"
    external.write_text("{}\n", encoding="utf-8")
    (tmp_path / "dbt_run_results.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "dbt_manifest.json").symlink_to(external)
    result = _passing_result(module, tmp_path)

    with pytest.raises(ValueError, match="artifact_path_invalid"):
        module.write_evidence(tmp_path, result)

    assert not (tmp_path / "mssql_dbt_wide_materialization.json").exists()
