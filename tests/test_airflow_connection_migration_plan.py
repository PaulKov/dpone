from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.security_redaction import REDACTION_TOKEN

ROOT = Path(__file__).resolve().parents[1]


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


def test_connection_check_returns_legacy_registry_migration_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    code, _, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr

    registry_path = tmp_path / "platform" / "connection-registries" / "dev.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["connections"]["mssql_dev"] = {
        "type": "mssql",
        "connection_type": "vault",
        "vault_path": "dpone/dev/credentials/mssql_dev",
        "connection": {
            "host": "mssql.local",
            "port": 1433,
            "database": "dwh",
            "password": "nested-password-value",
            "headers": {"api_token": "nested-token-value"},
        },
        "api_token": "token-value",
        "password": "password-value",
    }
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=True), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev", "--format", "json"],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert {error["code"] for error in payload["errors"]} == {
        "DPONE_LEGACY_CONNECTION_REGISTRY_ENTRY_FOUND",
        "DPONE_SECRET_VALUE_IN_REGISTRY",
    }
    legacy_error = next(
        error for error in payload["errors"] if error["code"] == "DPONE_LEGACY_CONNECTION_REGISTRY_ENTRY_FOUND"
    )
    assert legacy_error["docs_url"] == "docs/errors/DPONE_LEGACY_CONNECTION_REGISTRY_ENTRY_FOUND.md"
    assert (ROOT / legacy_error["docs_url"]).is_file()
    plan = payload["connection_registry_migration_plan"]
    assert (
        GitOpsSchemaValidator().validate(
            plan,
            expected_kind="dpone.connection-registry-migration-plan.v1",
        )
        == ()
    )
    assert plan["kind"] == "dpone.connection-registry-migration-plan.v1"
    assert plan["schema"] == "dpone.connection-registry-migration-plan.v1"
    assert plan["mode"] == "plan"
    assert plan["apply"] is False
    assert plan["environment"] == "dev"
    assert plan["registry_path"] == "platform/connection-registries/dev.yaml"
    assert plan["summary"] == {
        "legacy_entries": 1,
        "manual_review_required": True,
        "apply_supported": False,
    }
    assert len(plan["changes"]) == 1
    change = plan["changes"][0]
    assert change["action"] == "replace_legacy_connection_entry"
    assert change["connection_ref"] == "mssql_dev"
    assert change["detected"] == {
        "connection_type": "vault",
        "vault_path": "[REDACTED]",
    }
    assert change["proposed_entry"] == {
        "type": "mssql",
        "connection": {"host": "mssql.local", "port": 1433, "database": "dwh"},
        "credentials": {
            "resolver": "vault_kv",
            "mount": "kv",
            "kv_version": 2,
            "path": "dpone/dev/credentials/mssql_dev",
            "fields": {"username": "username", "password": "password"},
            "version_policy": "latest",
            "resolution_scope": "workload_start",
        },
    }
    assert "--- before/platform/connection-registries/dev.yaml#connections.mssql_dev" in change["unified_diff"]
    assert "+++ after/platform/connection-registries/dev.yaml#connections.mssql_dev" in change["unified_diff"]
    serialized_plan = json.dumps(plan).lower()
    assert "token-value" not in serialized_plan
    assert "password-value" not in serialized_plan
    assert "nested-token-value" not in serialized_plan
    assert "nested-password-value" not in serialized_plan
    assert "token-value" not in stdout
    assert "password-value" not in stdout


def test_connection_check_text_output_shows_manual_registry_fix_hint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    code, _, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr

    registry_path = tmp_path / "platform" / "connection-registries" / "dev.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["connections"]["mssql_dev"] = {
        "type": "mssql",
        "connection_type": "vault",
        "vault_path": "dpone/dev/credentials/mssql_dev",
        "connection": {
            "host": "mssql.local",
            "port": 1433,
            "database": "dwh",
            "password": "nested-password-value",
            "headers": {"api_token": "nested-token-value"},
        },
        "api_token": "token-value",
        "password": "password-value",
    }
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=True), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        ["check", "pipelines/orders_daily", "--connections", "--environment", "dev"],
        capsys,
    )

    assert code == 1, stderr
    assert "dpone check connections: FAILED" in stdout
    assert "- error: DPONE_LEGACY_CONNECTION_REGISTRY_ENTRY_FOUND:" in stdout
    assert "- connection refs: clickhouse_dev, mssql_dev" in stdout
    assert "  docs: https://paulkov.github.io/dpone/errors/DPONE_LEGACY_CONNECTION_REGISTRY_ENTRY_FOUND/" in stdout
    assert "  fix manual: migrate_legacy_connection_entry" in stdout
    assert "Registry migration plan: platform/connection-registries/dev.yaml" in stdout
    assert "--- before/platform/connection-registries/dev.yaml#connections.mssql_dev" in stdout
    assert "+++ after/platform/connection-registries/dev.yaml#connections.mssql_dev" in stdout
    assert "+credentials:" in stdout
    assert "password-value" not in stdout
    assert "token-value" not in stdout
    assert "nested-password-value" not in stdout
    assert "nested-token-value" not in stdout


def test_static_check_returns_legacy_authoring_migration_plan_without_secret_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    code, _, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr

    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    pipeline = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    pipeline["processes"][0]["source"].pop("connection_ref", None)
    pipeline["processes"][0]["source"].update(
        {
            "connection_type": "vault",
            "vault_path": "dpone/dev/credentials/mssql_dev",
            "api_token": "token-value",
            "options": [{"api_token": "list-token-value"}],
            "headers": {"api_token": "nested-token-value"},
            "password": "password-value",
        }
    )
    pipeline_path.write_text(yaml.safe_dump(pipeline, sort_keys=True), encoding="utf-8")

    code, stdout, stderr = _run_cli(["check", "pipelines/orders_daily", "--format", "json"], capsys)

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert {error["code"] for error in payload["errors"]} == {"DPONE_LEGACY_CONNECTION_CONFIG_FOUND"}
    assert payload["errors"][0]["docs_url"] == "docs/errors/DPONE_LEGACY_CONNECTION_CONFIG_FOUND.md"
    assert (ROOT / payload["errors"][0]["docs_url"]).is_file()
    fixes = payload["errors"][0]["fixes"]
    assert fixes == [
        {
            "id": "plan_authoring_connection_ref_migration",
            "safety": "safe",
            "command": "dpone fix pipelines/orders_daily --plan",
        },
        {
            "id": "apply_authoring_connection_ref_migration",
            "safety": "manual",
            "command": "dpone fix pipelines/orders_daily --apply",
        },
    ]
    plan = payload["airflow_authoring_migration_plan"]
    assert (
        GitOpsSchemaValidator().validate(
            plan,
            expected_kind="dpone.airflow-authoring-migration-plan.v1",
        )
        == ()
    )
    assert plan["kind"] == "dpone.airflow-authoring-migration-plan.v1"
    assert plan["schema"] == "dpone.airflow-authoring-migration-plan.v1"
    assert plan["source_path"] == "pipelines/orders_daily/pipeline.yaml"
    assert plan["summary"] == {
        "legacy_sections": 1,
        "manual_review_required": True,
        "apply_supported": False,
    }
    assert len(plan["changes"]) == 1
    change = plan["changes"][0]
    assert change["action"] == "replace_legacy_connection_config"
    assert change["path"] == "processes[0].source"
    assert change["suggested_connection_ref"] == "mssql_dev"
    assert change["detected"] == {
        "connection_type": "vault",
        "vault_path": REDACTION_TOKEN,
    }
    assert change["proposed_section"]["connection_ref"] == "mssql_dev"
    assert "connection_type" not in change["proposed_section"]
    assert "vault_path" not in change["proposed_section"]
    assert "headers" not in change["proposed_section"]
    assert "options" not in change["proposed_section"]
    assert "--- before/pipelines/orders_daily/pipeline.yaml#processes[0].source" in change["unified_diff"]
    assert "+++ after/pipelines/orders_daily/pipeline.yaml#processes[0].source" in change["unified_diff"]
    serialized_plan = json.dumps(plan).lower()
    assert "password-value" not in serialized_plan
    assert "token-value" not in serialized_plan
    assert "nested-token-value" not in serialized_plan
    assert "list-token-value" not in serialized_plan
    assert "dpone/dev/credentials/mssql_dev" not in serialized_plan


def test_static_check_text_output_shows_safe_authoring_fix_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    code, _, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr

    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    pipeline = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    source = pipeline["processes"][0]["source"]
    source.pop("connection_ref", None)
    source.update(
        {
            "connection_type": "vault",
            "vault_path": "dpone/dev/credentials/mssql_dev",
            "api_token": "token-value",
            "password": "password-value",
        }
    )
    pipeline_path.write_text(yaml.safe_dump(pipeline, sort_keys=True), encoding="utf-8")

    code, stdout, stderr = _run_cli(["check", "pipelines/orders_daily"], capsys)

    assert code == 1, stderr
    assert "dpone check: FAILED" in stdout
    assert "- error: DPONE_LEGACY_CONNECTION_CONFIG_FOUND:" in stdout
    assert "  docs: https://paulkov.github.io/dpone/errors/DPONE_LEGACY_CONNECTION_CONFIG_FOUND/" in stdout
    assert "  fix safe: dpone fix pipelines/orders_daily --plan" in stdout
    assert "  fix manual: dpone fix pipelines/orders_daily --apply" in stdout
    assert "password-value" not in stdout
    assert "token-value" not in stdout
    assert "dpone/dev/credentials/mssql_dev" not in stdout


def test_fix_text_plan_shows_redacted_diff_without_applying(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    code, _, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr

    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    pipeline = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    source = pipeline["processes"][0]["source"]
    source.pop("connection_ref", None)
    source.update(
        {
            "connection_type": "vault",
            "vault_path": "dpone/dev/credentials/mssql_dev",
            "api_token": "token-value",
            "password": "password-value",
        }
    )
    pipeline_path.write_text(yaml.safe_dump(pipeline, sort_keys=True), encoding="utf-8")
    original_text = pipeline_path.read_text(encoding="utf-8")

    code, stdout, stderr = _run_cli(["fix", "pipelines/orders_daily", "--plan"], capsys)

    assert code == 0, stderr
    assert "dpone fix: PLAN" in stdout
    assert "- target: pipelines/orders_daily/pipeline.yaml" in stdout
    assert "- mode: plan" in stdout
    assert "- legacy sections: 1" in stdout
    assert "- applied sections: 0" in stdout
    assert "- planned ref: processes[0].source -> mssql_dev" in stdout
    assert "- action: review the redacted diff, then run dpone fix pipelines/orders_daily --apply" in stdout
    assert "- details: rerun with --format json for the full authoring migration plan" in stdout
    assert "- plan_modify: processes[0].source" in stdout
    assert "--- before/pipelines/orders_daily/pipeline.yaml#processes[0].source" in stdout
    assert "+++ after/pipelines/orders_daily/pipeline.yaml#processes[0].source" in stdout
    assert "connection_ref: mssql_dev" in stdout
    assert "password-value" not in stdout
    assert "token-value" not in stdout
    assert "dpone/dev/credentials/mssql_dev" not in stdout
    assert "dpone self-service: OK" not in stdout
    assert pipeline_path.read_text(encoding="utf-8") == original_text


def test_fix_text_plan_normalizes_absolute_target_in_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    code, _, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr

    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    pipeline = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    source = pipeline["processes"][0]["source"]
    source.pop("connection_ref", None)
    source.update(
        {
            "connection_type": "legacy",
        }
    )
    pipeline_path.write_text(yaml.safe_dump(pipeline, sort_keys=True), encoding="utf-8")

    code, stdout, stderr = _run_cli(["fix", str(tmp_path / "pipelines" / "orders_daily"), "--plan"], capsys)

    assert code == 0, stderr
    assert "dpone fix: PLAN" in stdout
    assert "- target: pipelines/orders_daily/pipeline.yaml" in stdout
    assert "- action: review the redacted diff, then run dpone fix pipelines/orders_daily --apply" in stdout
    assert str(tmp_path) not in stdout
    assert "Traceback" not in stdout

    code, stdout, stderr = _run_cli(
        ["fix", str(tmp_path / "pipelines" / "orders_daily"), "--plan", "--format", "json"],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert (
        GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.airflow-authoring-fix.v1",
        )
        == ()
    )
    assert payload["target"] == "pipelines/orders_daily/pipeline.yaml"
    assert str(tmp_path) not in stdout


def test_fix_plans_and_applies_legacy_authoring_migration_safely(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    code, _, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr

    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    pipeline = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    source = pipeline["processes"][0]["source"]
    source.pop("connection_ref", None)
    source.update(
        {
            "connection_type": "vault",
            "vault_path": "dpone/dev/credentials/mssql_dev",
            "api_token": "token-value",
            "password": "password-value",
        }
    )
    pipeline_path.write_text(yaml.safe_dump(pipeline, sort_keys=True), encoding="utf-8")

    code, stdout, stderr = _run_cli(["fix", "pipelines/orders_daily", "--plan", "--format", "json"], capsys)

    assert code == 0, stderr
    assert "password-value" not in stdout
    assert "token-value" not in stdout
    assert "dpone/dev/credentials/mssql_dev" not in stdout
    payload = json.loads(stdout)
    assert (
        GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.airflow-authoring-fix.v1",
        )
        == ()
    )
    assert payload["kind"] == "dpone.airflow-authoring-fix.v1"
    assert payload["schema"] == "dpone.airflow-authoring-fix.v1"
    assert payload["mode"] == "plan"
    assert payload["apply"] is False
    assert payload["applied"] is False
    assert payload["target"] == "pipelines/orders_daily/pipeline.yaml"
    assert payload["summary"] == {
        "legacy_sections": 1,
        "applied_sections": 0,
        "no_op": False,
    }
    assert len(payload["migration_plan"]["changes"]) == 1
    assert payload["migration_plan"]["changes"][0]["suggested_connection_ref"] == "mssql_dev"
    assert payload["changes"][0]["action"] == "plan_modify"
    unchanged = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))["processes"][0]["source"]
    assert unchanged["connection_type"] == "vault"
    assert unchanged["vault_path"] == "dpone/dev/credentials/mssql_dev"
    assert unchanged["api_token"] == "token-value"
    assert unchanged["password"] == "password-value"

    code, stdout, stderr = _run_cli(["fix", "pipelines/orders_daily", "--apply", "--format", "json"], capsys)

    assert code == 0, stderr
    assert "password-value" not in stdout
    assert "token-value" not in stdout
    payload = json.loads(stdout)
    assert (
        GitOpsSchemaValidator().validate(
            payload,
            expected_kind="dpone.airflow-authoring-fix.v1",
        )
        == ()
    )
    assert payload["mode"] == "apply"
    assert payload["apply"] is True
    assert payload["applied"] is True
    assert payload["summary"] == {
        "legacy_sections": 1,
        "applied_sections": 1,
        "no_op": False,
    }
    assert payload["changes"][0]["action"] == "modify"

    migrated = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))["processes"][0]["source"]
    assert migrated["connection_ref"] == "mssql_dev"
    assert migrated["type"] == "mssql"
    assert "connection_type" not in migrated
    assert "vault_path" not in migrated
    assert "api_token" not in migrated
    assert "password" not in migrated

    code, stdout, stderr = _run_cli(["fix", "pipelines/orders_daily", "--plan", "--format", "json"], capsys)

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["mode"] == "plan"
    assert payload["apply"] is False
    assert payload["applied"] is False
    assert payload["summary"] == {
        "legacy_sections": 0,
        "applied_sections": 0,
        "no_op": True,
    }
    assert payload["changes"] == []
    assert "migration_plan" not in payload


def test_fix_apply_text_summarizes_migration_and_next_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, _, stderr = _run_cli(["init", "project", "--airflow", "--format", "json"], capsys)
    assert code == 0, stderr
    code, _, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr

    pipeline_path = tmp_path / "pipelines" / "orders_daily" / "pipeline.yaml"
    pipeline = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    source = pipeline["processes"][0]["source"]
    source.pop("connection_ref", None)
    source.update(
        {
            "connection_type": "vault",
            "vault_path": "dpone/dev/credentials/mssql_dev",
            "api_token": "token-value",
            "password": "password-value",
        }
    )
    pipeline_path.write_text(yaml.safe_dump(pipeline, sort_keys=True), encoding="utf-8")

    code, stdout, stderr = _run_cli(["fix", "pipelines/orders_daily", "--apply"], capsys)

    assert code == 0, stderr
    assert "dpone fix: APPLIED" in stdout
    assert "- target: pipelines/orders_daily/pipeline.yaml" in stdout
    assert "- mode: apply" in stdout
    assert "- legacy sections: 1" in stdout
    assert "- applied sections: 1" in stdout
    assert "- planned ref: processes[0].source -> mssql_dev" in stdout
    assert "- modify: processes[0].source" in stdout
    assert "- action: run dpone check pipelines/orders_daily to verify the migrated authoring source" in stdout
    assert "- details: rerun with --format json for the full authoring migration plan" in stdout
    assert "password-value" not in stdout
    assert "token-value" not in stdout
    assert "dpone self-service: OK" not in stdout

    migrated = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))["processes"][0]["source"]
    assert migrated["connection_ref"] == "mssql_dev"
    assert "connection_type" not in migrated
    assert "vault_path" not in migrated

    code, stdout, stderr = _run_cli(["fix", "pipelines/orders_daily", "--plan"], capsys)

    assert code == 0, stderr
    assert "dpone fix: NO_OP" in stdout
    assert "- target: pipelines/orders_daily/pipeline.yaml" in stdout
    assert "- legacy sections: 0" in stdout
    assert "- applied sections: 0" in stdout
    assert "- action: run dpone check pipelines/orders_daily to verify the pipeline" in stdout
    assert "dpone self-service: OK" not in stdout
