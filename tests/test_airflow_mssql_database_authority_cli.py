from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from dpone.cli import main as cli_main


@pytest.mark.parametrize(
    ("missing_role", "expected_code"),
    (
        ("target", "DPONE_MSSQL_TARGET_DATABASE_AUTHORITY_REQUIRED"),
        ("staging", "DPONE_MSSQL_STAGING_DATABASE_AUTHORITY_REQUIRED"),
        ("state", "DPONE_MSSQL_STATE_DATABASE_AUTHORITY_REQUIRED"),
    ),
)
def test_dpone_check_reports_missing_governed_database_pin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    missing_role: str,
    expected_code: str,
) -> None:
    monkeypatch.chdir(tmp_path)
    _run(["init", "project", "--airflow", "--format", "json"], capsys)
    _run(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "postgres-to-clickhouse-full-refresh",
            "--airflow",
            "--format",
            "json",
        ],
        capsys,
    )
    _author_governed_route(tmp_path, missing_role=missing_role)

    code, stdout, stderr = _run(
        [
            "check",
            "pipelines/orders_daily",
            "--connections",
            "--environment",
            "dev",
            "--format",
            "json",
        ],
        capsys,
    )
    payload = json.loads(stdout)

    assert code == 1, stderr
    assert [error["code"] for error in payload["errors"]] == [expected_code]
    assert payload["errors"][0]["docs_url"] == f"docs/errors/{expected_code}.md"
    assert payload["errors"][0]["fixes"] == [{"id": "declare_mssql_database_authorities", "safety": "manual"}]


def _author_governed_route(root: Path, *, missing_role: str) -> None:
    pipeline_path = root / "pipelines" / "orders_daily" / "pipeline.yaml"
    pipeline = yaml.safe_load(pipeline_path.read_text(encoding="utf-8"))
    process = pipeline["processes"][0]
    process["sink"] = {
        "type": "mssql",
        "connection_ref": "mssql_dev",
        "table": {"schema": "dbo", "name": "orders"},
        "staging": {"database": "DWH_Stage", "schema": "staging"},
        "strategy": {"mode": "full_refresh"},
    }
    process["state"] = {
        "type": "mssql",
        "connection_ref": "mssql_state",
        "atomicity": "target_atomic",
        "provisioning": "external",
    }
    pipeline_path.write_text(yaml.safe_dump(pipeline, sort_keys=False), encoding="utf-8")

    domain_path = root / "domains" / "sales.yaml"
    domain = yaml.safe_load(domain_path.read_text(encoding="utf-8"))
    domain["workloads"]["orders_daily"]["connection_refs"] = [
        "postgres_dev",
        "mssql_dev",
        "mssql_state",
    ]
    domain_path.write_text(yaml.safe_dump(domain, sort_keys=False), encoding="utf-8")

    binding_path = root / "environments" / "dev" / "binding-set.yaml"
    bindings = yaml.safe_load(binding_path.read_text(encoding="utf-8"))
    bindings["bindings"]["mssql_state"] = {"connection_ref": "mssql_state"}
    binding_path.write_text(yaml.safe_dump(bindings, sort_keys=False), encoding="utf-8")

    registry_path = root / "platform" / "connection-registries" / "dev.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    target = registry["connections"]["mssql_dev"]
    target["connection"]["database"] = "DWH"
    target["connection"]["database_authorities"] = {
        "DWH": _pin(7, "1"),
        "DWH_Stage": _pin(8, "2"),
    }
    state = deepcopy(target)
    state["connection"]["database"] = "Example_System"
    state["connection"]["database_authorities"] = {"Example_System": _pin(9, "3")}
    registry["connections"]["mssql_state"] = state
    if missing_role == "target":
        target["connection"]["database_authorities"].pop("DWH")
    elif missing_role == "staging":
        target["connection"]["database_authorities"].pop("DWH_Stage")
    else:
        state["connection"]["database_authorities"].clear()
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")


def _pin(database_id: int, token: str) -> dict[str, object]:
    return {
        "database_id": database_id,
        "create_token": f"2026-08-16T00:00:0{token}.0000000",
        "database_guid": f"{token * 8}-{token * 4}-{token * 4}-{token * 4}-{token * 12}",
    }


def _run(
    args: list[str],
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err
