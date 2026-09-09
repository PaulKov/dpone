from __future__ import annotations

import tomllib
from pathlib import Path

from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED


def test_certified_toolchain_matches_installation_extra() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    dependencies = set(project["project"]["optional-dependencies"]["dbt-mssql"])
    contract = DBT_SQLSERVER_1_12_CERTIFIED

    assert dependencies == {
        f"dbt-core=={contract.dbt_core_version}",
        f"{contract.adapter_distribution}=={contract.adapter_version}",
    }
