from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[1]
READER_ROOT = ROOT / "packages" / "dpone-airflow-pack"
PROVIDER_ROOT = ROOT / "packages" / "apache-airflow-providers-dpone"


def test_formal_provider_and_reader_have_single_responsibilities() -> None:
    reader = tomllib.loads((READER_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    provider = tomllib.loads((PROVIDER_ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert reader["project"]["name"] == "dpone-airflow-pack"
    assert reader["tool"]["uv"]["build-backend"]["module-name"] == "dpone_airflow_pack"
    assert "entry-points" not in reader["project"]

    assert provider["project"]["name"] == "apache-airflow-providers-dpone"
    assert provider["tool"]["uv"]["build-backend"]["module-name"] == "airflow.providers.dpone"
    assert provider["project"]["entry-points"]["apache_airflow_provider"] == {
        "provider_info": "airflow.providers.dpone:get_provider_info"
    }
    assert provider["project"]["version"] == reader["project"]["version"]
    assert f"dpone-airflow-pack=={reader['project']['version']}" in provider["project"]["dependencies"]
    assert "apache-airflow>=2.10,<3.4" in provider["project"]["dependencies"]


def test_formal_provider_namespace_import_is_parse_safe() -> None:
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
    pythonpath = os.pathsep.join((str(PROVIDER_ROOT / "src"), str(READER_ROOT / "src")))
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path.home(),
        env={**os.environ, "PYTHONPATH": pythonpath},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "dpone_loaded": False,
        "has_loader": True,
        "module": "airflow.providers.dpone",
        "vault_loaded": False,
    }


def test_formal_provider_owns_typing_and_reader_keeps_its_marker() -> None:
    canonical = PROVIDER_ROOT / "src" / "airflow" / "providers" / "dpone"
    legacy = READER_ROOT / "src" / "dpone_airflow_pack"

    assert (canonical / "__init__.py").is_file()
    assert (canonical / "__init__.pyi").is_file()
    assert (canonical / "py.typed").is_file()
    assert (legacy / "py.typed").is_file()
    old_canonical = READER_ROOT / "src" / "airflow" / "providers" / "dpone"
    assert not (old_canonical / "__init__.py").exists()
    assert not (old_canonical / "__init__.pyi").exists()
    assert not (old_canonical / "py.typed").exists()


def test_provider_info_names_the_formal_distribution() -> None:
    script = textwrap.dedent(
        """
        import json
        from airflow.providers.dpone import get_provider_info
        print(json.dumps(get_provider_info(), sort_keys=True))
        """
    )
    pythonpath = os.pathsep.join((str(PROVIDER_ROOT / "src"), str(READER_ROOT / "src")))
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path.home(),
        env={**os.environ, "PYTHONPATH": pythonpath},
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["package-name"] == "apache-airflow-providers-dpone"


def test_provider_version_has_no_manually_duplicated_release_constant() -> None:
    source = (READER_ROOT / "src/dpone_airflow_pack/provider.py").read_text(encoding="utf-8")

    assert "_PROVIDER_VERSION_FALLBACK" not in source
    assert '_READER_PACKAGE_NAME = "dpone-airflow-pack"' in source
