"""CLI tests for `dpone gitops airflow deps`."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.airflow_dag_spec_repo import dag_declaration, standard_repo


def test_gitops_airflow_deps_renders_markdown(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    workload_set = standard_repo(tmp_path, dags={"DAG__marketing": dag_declaration()})
    from dpone.cli.main import main as cli_main

    with pytest.raises(SystemExit) as exc:
        cli_main(
            [
                "gitops",
                "airflow",
                "deps",
                "--workload-set",
                workload_set.relative_to(tmp_path).as_posix(),
                "--format",
                "md",
            ]
        )
    assert exc.value.code == 0
