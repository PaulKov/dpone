"""Exercise the documented author model with real offline dbt and public CLI."""

import hashlib
import importlib.metadata
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED

ROOT = Path(__file__).parents[1]
DEMO = ROOT / "examples/dbt-inline-publishing"
TUTORIAL = ROOT / "docs/dbt-inline-publishing.md"
MODEL = "competitive_pricing"


def _tutorial_block(language: str) -> str:
    section = TUTORIAL.read_text(encoding="utf-8").split("## 1. Add publishing metadata", 1)[1]
    match = re.search(rf"```{language}\n(.*?)\n```", section, re.DOTALL)
    assert match is not None, f"Tutorial must contain a complete {language} example"
    return match[1] + "\n"


def _prepare_project(tmp_path: Path) -> Path:
    """Keep actual platform inputs; replace models with the documented example."""

    project = tmp_path / "project"
    (project / "models").mkdir(parents=True)
    for relative in ("dbt_project.yml", "dpone/dbt-publish-profiles.yml", "profiles/profiles.yml"):
        destination = project / relative
        destination.parent.mkdir(exist_ok=True)
        shutil.copyfile(DEMO / relative, destination)
    _normalize_example_identities(project)
    (project / "models" / f"{MODEL}.sql").write_text(_tutorial_block("sql"), encoding="utf-8")
    (project / "models/schema.yml").write_text(_tutorial_block("yaml"), encoding="utf-8")
    return project


def _normalize_example_identities(project: Path) -> None:
    """Change only explicit database identities in temporary platform inputs."""

    project_path = project / "dbt_project.yml"
    config = yaml.safe_load(project_path.read_text(encoding="utf-8"))
    config["models"]["dpone_dbt_demo"]["+database"] = "analytics_demo"
    project_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    profile_path = project / "profiles/profiles.yml"
    profile = yaml.safe_load(profile_path.read_text(encoding="utf-8"))
    profile["dpone_dbt_demo"]["outputs"]["parse"]["database"] = "analytics_demo"
    profile_path.write_text(yaml.safe_dump(profile), encoding="utf-8")

    policy_path = project / "dpone/dbt-publish-profiles.yml"
    policy = yaml.safe_load(policy_path.read_text(encoding="utf-8"))
    sink = policy["profiles"]["mssql_to_clickhouse_mart"]["sink"]
    sink["target_schema"] = "analytics_demo"
    sink["staging_schema"] = "dpone_stage"
    sink["options"]["load_governance"]["audit"]["state_schema"] = "dpone_stage"
    policy_path.write_text(yaml.safe_dump(policy), encoding="utf-8")


def _environment(project: Path) -> dict[str, str]:
    """Use the existing isolated environment with synthetic parse-only values."""

    environment = DbtInvocationContext.canonical().environment(home=str(project / ".parse-home"))
    return environment | {
        "PYTHONDONTWRITEBYTECODE": "1",
        "DPONE_DBT_DEMO_SQLSERVER_USER": "parse_only",
        "DPONE_DBT_DEMO_SQLSERVER_PASSWORD": "parse_only",
    }


def _run(project: Path, executable: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(Path(sys.executable).with_name(executable)), *arguments],
        cwd=project,
        env=_environment(project),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def _parse(project: Path) -> None:
    """Missing pinned dependencies fail this acceptance case; never use a fixture."""

    assert importlib.metadata.version("dbt-core") == DBT_SQLSERVER_1_12_CERTIFIED.dbt_core_version
    assert importlib.metadata.version("dbt-sqlserver") == DBT_SQLSERVER_1_12_CERTIFIED.adapter_version
    completed = _run(
        project,
        "dbt",
        "--no-send-anonymous-usage-stats",
        "--no-use-colors",
        "parse",
        "--profiles-dir",
        "profiles",
        "--no-partial-parse",
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    manifest = json.loads((project / "target/manifest.json").read_bytes())
    assert (
        manifest["nodes"][f"model.dpone_dbt_demo.{MODEL}"]["raw_code"]
        == (project / "models" / f"{MODEL}.sql").read_text(encoding="utf-8").strip()
    )


def _tree_hashes(project: Path) -> dict[str, str]:
    return {
        path.relative_to(project).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in project.rglob("*")
        if path.is_file()
    }


def test_tutorial_model_passes_real_parse_check_and_explain(tmp_path: Path) -> None:
    project = _prepare_project(tmp_path)
    _parse(project)
    before = _tree_hashes(project)

    checked = _run(project, "dpone", "dbt", "check", ".", "--format", "json")
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert checked.stderr == ""
    report = json.loads(checked.stdout)
    assert report["schema"] == "dpone.dbt-publish-compile.v2"
    assert report["passed"] and len(report["models"]) == 1
    assert report["release_id"] is None and report["artifacts"] == {}

    explained = _run(project, "dpone", "dbt", "explain", MODEL, "--format", "json")
    assert explained.returncode == 0, explained.stdout + explained.stderr
    assert explained.stderr == ""
    model = json.loads(explained.stdout)
    assert model["schema"] == "dpone.dbt-publish-explain.v1"
    assert model["model"] == f"model.dpone_dbt_demo.{MODEL}"
    assert model["source_relation"] == {"database": "analytics_demo", "schema": "pricing_pricing", "name": MODEL}
    assert model["intent"]["profile"] == "mssql_to_clickhouse_mart"
    assert model["intent"]["workflow"] == "competitive_pricing"
    assert model["intent"]["target"] == {"schema": None, "table": None}
    assert model["resolved_strategy"]["mode"] == "incremental_merge"
    assert model["resolved_strategy"]["unique_key"] == ["product_id", "date_id"]
    assert model["route_capability"]["evidence_status"] == "UNVERIFIED"

    text = _run(project, "dpone", "dbt", "check", ".")
    assert text.returncode == 0, text.stdout + text.stderr
    assert text.stderr == ""
    assert "dbt -> dpone publish: PASS" in text.stdout
    assert "validated publishing models: 1; workflows: 1" in text.stdout
    assert "published models:" not in text.stdout
    assert _tree_hashes(project) == before


@pytest.mark.parametrize("invalid_input", ["nullable_key", "unauthorized_strategy"])
def test_tutorial_invalid_contract_or_strategy_is_rejected(tmp_path: Path, invalid_input: str) -> None:
    project = _prepare_project(tmp_path)
    if invalid_input == "nullable_key":
        path = project / "models/schema.yml"
        schema = yaml.safe_load(path.read_text(encoding="utf-8"))
        column = next(column for column in schema["models"][0]["columns"] if column["name"] == "product_id")
        column.pop("constraints")
        path.write_text(yaml.safe_dump(schema), encoding="utf-8")
    else:
        path = project / "models" / f"{MODEL}.sql"
        path.write_text(
            path.read_text(encoding="utf-8").replace("'incremental_merge'", "'full_refresh'"), encoding="utf-8"
        )
    _parse(project)
    before = _tree_hashes(project)
    for arguments in (("check", "."), ("explain", MODEL)):
        completed = _run(project, "dpone", "dbt", *arguments, "--format", "json")
        assert completed.returncode == 1, completed.stdout + completed.stderr
        assert completed.stderr == ""
        error = json.loads(completed.stdout)
        assert error["schema"] == "dpone.error.v1"
        assert error["path"] == f"models/{MODEL}.sql"
        assert error["code"] == (
            "DPONE_DBT_UNIQUE_KEY_NULLABLE" if invalid_input == "nullable_key" else "DPONE_DBT_STRATEGY_UNRESOLVED"
        )
    assert _tree_hashes(project) == before
