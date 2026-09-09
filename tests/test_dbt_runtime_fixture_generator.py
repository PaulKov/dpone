from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from tools.dbt_self_service import generate_runtime_fixture

from dpone.runtime.dbt_project_bundle import build_dbt_project_bundle

ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "dbt-runtime-correctness-v1"


def test_runtime_fixture_generator_requires_environment_provenance() -> None:
    with pytest.raises(SystemExit):
        generate_runtime_fixture._parser().parse_args(("--profiles-dir", "profiles", "--output-dir", "output"))


def test_runtime_fixture_provenance_binds_source_environment_and_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    shutil.copytree(FIXTURE, project)
    output = tmp_path / "generated"
    manifest = b'{"metadata":{"dbt_schema_version":"v12"}}\n'
    run_results = b'{"metadata":{"dbt_schema_version":"v6"}}\n'
    monkeypatch.setattr(
        generate_runtime_fixture,
        "_source_commit",
        lambda: "a" * 40,
    )

    generate_runtime_fixture._publish(
        output,
        manifest=manifest,
        run_results=run_results,
        target_name="runtime_fixture",
        project=project,
        environment_id="approved-mssql-01",
        database_version="2022-CU-current",
    )

    provenance = json.loads((output / "provenance.json").read_text(encoding="utf-8"))
    assert provenance["source_commit"] == "a" * 40
    assert provenance["source_tree_sha256"] == (build_dbt_project_bundle(project).bundle.archive_sha256)
    assert provenance["environment_id"] == "approved-mssql-01"
    assert provenance["database_version"] == "2022-CU-current"
    assert provenance["generated_at"].endswith("+00:00")
    assert "<approved-environment>" in provenance["generator_command"]
    assert (output / "manifest.v12.json").read_bytes() == manifest
    assert (output / "run-results.v6.json").read_bytes() == run_results


@pytest.mark.parametrize("value", ("", "../prod", "prod env", "x" * 129))
def test_runtime_fixture_provenance_rejects_unsafe_labels(value: str) -> None:
    with pytest.raises(ValueError):
        generate_runtime_fixture._safe_label(value, "environment id")
