from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import tomllib
import warnings
from importlib import import_module
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
READER_SRC = REPO_ROOT / "packages/dpone-airflow-pack/src"
READER_PYPROJECT = REPO_ROOT / "packages/dpone-airflow-pack/pyproject.toml"
PROVIDER_SRC = REPO_ROOT / "packages/apache-airflow-providers-dpone/src"
PROVIDER_PYPROJECT = REPO_ROOT / "packages/apache-airflow-providers-dpone/pyproject.toml"
for source_root in (READER_SRC, PROVIDER_SRC):
    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))


def test_airflow_pack_provider_imports_without_full_dpone() -> None:
    script = textwrap.dedent(
        """
        import importlib
        import json
        import sys

        module = importlib.import_module("dpone_airflow_pack")
        print(json.dumps({
            "module": module.__name__,
            "has_builder": hasattr(module, "build_dpone_gitops_task_group_from_pack"),
            "airflow_loaded": "airflow" in sys.modules,
            "dpone_loaded": "dpone" in sys.modules,
        }, sort_keys=True))
        """
    )
    env = {**os.environ, "PYTHONPATH": str(READER_SRC)}

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path.home(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload == {
        "airflow_loaded": False,
        "dpone_loaded": False,
        "has_builder": True,
        "module": "dpone_airflow_pack",
    }


def test_airflow_pack_legacy_exports_do_not_import_airflow() -> None:
    script = textwrap.dedent(
        """
        import json
        import sys

        from dpone_airflow_pack import (
            build_dpone_gitops_task_group_from_pack,
            load_dpone_airflow_pack,
            validate_dpone_artifacts,
        )

        print(json.dumps({
            "airflow_loaded": "airflow" in sys.modules,
            "builder": callable(build_dpone_gitops_task_group_from_pack),
            "loader": callable(load_dpone_airflow_pack),
            "validator": callable(validate_dpone_artifacts),
        }, sort_keys=True))
        """
    )
    env = {**os.environ, "PYTHONPATH": str(READER_SRC)}

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path.home(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "airflow_loaded": False,
        "builder": True,
        "loader": True,
        "validator": True,
    }


def test_legacy_provider_facade_remains_available_without_formal_provider() -> None:
    script = textwrap.dedent(
        """
        import json
        import sys
        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            from dpone_airflow_pack import DponeDag, load_dpone_dags

        print(json.dumps({
            "airflow_loaded": "airflow" in sys.modules,
            "dag": DponeDag.__name__,
            "loader": callable(load_dpone_dags),
            "warnings": len(caught),
        }, sort_keys=True))
        """
    )
    env = {**os.environ, "PYTHONPATH": str(READER_SRC)}

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path.home(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "airflow_loaded": False,
        "dag": "DponeDag",
        "loader": True,
        "warnings": 1,
    }


def test_canonical_provider_namespace_imports_without_full_dpone() -> None:
    script = textwrap.dedent(
        """
        import importlib
        import json
        import sys

        module = importlib.import_module("airflow.providers.dpone")
        print(json.dumps({
            "module": module.__name__,
            "has_loader": hasattr(module, "load_dpone_dags"),
            "dpone_loaded": "dpone" in sys.modules,
            "vault_loaded": "vault_kv_client" in sys.modules,
        }, sort_keys=True))
        """
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join((str(PROVIDER_SRC), str(READER_SRC)))}

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path.home(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload == {
        "dpone_loaded": False,
        "has_loader": True,
        "module": "airflow.providers.dpone",
        "vault_loaded": False,
    }


def test_airflow_pack_provider_declares_no_heavy_runtime_dependencies() -> None:
    text = READER_PYPROJECT.read_text(encoding="utf-8")

    assert 'name = "dpone-airflow-pack"' in text
    for forbidden in (
        "dpone==",
        "dpone[",
        "clickhouse",
        "pyodbc",
        "pandas",
        "polars",
        "connectorx",
    ):
        assert forbidden not in text


def test_reader_and_formal_provider_wheels_own_distinct_namespaces() -> None:
    reader = tomllib.loads(READER_PYPROJECT.read_text(encoding="utf-8"))
    provider = tomllib.loads(PROVIDER_PYPROJECT.read_text(encoding="utf-8"))

    assert reader["tool"]["uv"]["build-backend"]["module-name"] == "dpone_airflow_pack"
    assert provider["tool"]["uv"]["build-backend"]["module-name"] == "airflow.providers.dpone"


def test_formal_provider_declares_airflow_discovery_entry_point() -> None:
    pyproject = tomllib.loads(PROVIDER_PYPROJECT.read_text(encoding="utf-8"))

    assert pyproject["project"]["entry-points"]["apache_airflow_provider"] == {
        "provider_info": "airflow.providers.dpone:get_provider_info"
    }


def test_formal_provider_declares_kubernetes_operator_runtime_dependency() -> None:
    pyproject = tomllib.loads(PROVIDER_PYPROJECT.read_text(encoding="utf-8"))
    dependencies = pyproject["project"]["dependencies"]

    assert any(item.startswith("apache-airflow-providers-cncf-kubernetes") for item in dependencies)


def test_provider_info_matches_distribution_metadata() -> None:
    pyproject = tomllib.loads(PROVIDER_PYPROJECT.read_text(encoding="utf-8"))
    module = import_module("airflow.providers.dpone")

    info = module.get_provider_info()

    assert info["package-name"] == pyproject["project"]["name"]
    assert info["versions"] == [pyproject["project"]["version"]]


def test_canonical_provider_namespace_exports_structured_error_type() -> None:
    canonical = import_module("airflow.providers.dpone")
    deployment_index = import_module("dpone_airflow_pack.deployment_index")

    assert canonical.AirflowDeploymentIndexError is deployment_index.AirflowDeploymentIndexError
    assert "AirflowDeploymentIndexError" in canonical.__all__


def test_canonical_provider_namespace_exports_index_model_types() -> None:
    canonical = import_module("airflow.providers.dpone")
    deployment_index = import_module("dpone_airflow_pack.deployment_index")

    assert canonical.AirflowDeploymentIndex is deployment_index.AirflowDeploymentIndex
    assert canonical.AirflowIndexArtifact is deployment_index.AirflowIndexArtifact
    assert canonical.CacheResolver is deployment_index.CacheResolver
    assert "AirflowDeploymentIndex" in canonical.__all__
    assert "AirflowIndexArtifact" in canonical.__all__
    assert "CacheResolver" in canonical.__all__


def test_canonical_provider_namespace_exports_semantic_refresh_callables() -> None:
    canonical = import_module("airflow.providers.dpone")
    semantic_refresh = import_module("dpone_airflow_pack.semantic_refresh_airflow")

    assert canonical.SemanticRefreshAirflowCallables is semantic_refresh.SemanticRefreshAirflowCallables
    assert "SemanticRefreshAirflowCallables" in canonical.__all__


def test_legacy_provider_facade_exports_warn_once_per_process() -> None:
    sys.modules.pop("dpone_airflow_pack", None)
    legacy = import_module("dpone_airflow_pack")
    canonical = import_module("airflow.providers.dpone")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", DeprecationWarning)
        assert legacy.DponeDag is canonical.DponeDag
        assert legacy.DponeTaskGroup is canonical.DponeTaskGroup
        assert legacy.AirflowDeploymentIndex is canonical.AirflowDeploymentIndex
        assert legacy.AirflowDeploymentIndexError is canonical.AirflowDeploymentIndexError
        assert legacy.AirflowIndexArtifact is canonical.AirflowIndexArtifact
        assert legacy.load_dpone_dags is canonical.load_dpone_dags

    deprecations = [item for item in caught if issubclass(item.category, DeprecationWarning)]
    assert len(deprecations) == 1
    assert "airflow.providers.dpone" in str(deprecations[0].message)


def test_legacy_package_getattr_restores_loaded_dag_schedule_submodule(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    legacy = import_module("dpone_airflow_pack")
    dag_schedule = import_module("dpone_airflow_pack.dag_schedule")
    monkeypatch.delattr(legacy, "dag_schedule", raising=False)

    assert getattr(legacy, "dag_schedule") is dag_schedule
