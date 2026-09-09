from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_validator():
    path = ROOT / "tools" / "dbt_self_service" / "validate_workflow_inputs.py"
    spec = importlib.util.spec_from_file_location("dpone_dbt_workflow_input_allowlist", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_validate_workflow_inputs_accepts_allowlisted_values(monkeypatch) -> None:
    module = _load_validator()
    monkeypatch.setenv("DPONE_VERSION", "0.73.20")
    monkeypatch.setenv("AIRFLOW_VERSION", "3.2.0")
    monkeypatch.setenv("PYTHON_VERSION", "3.12")
    monkeypatch.setenv("DPONE_RELEASE_ID", "sha256:" + "a" * 64)

    assert module.main(["validate", "DPONE_VERSION", "AIRFLOW_VERSION", "PYTHON_VERSION", "DPONE_RELEASE_ID"]) == 0


def test_validate_workflow_inputs_rejects_shell_metacharacters(monkeypatch) -> None:
    module = _load_validator()
    monkeypatch.setenv("DPONE_VERSION", '1.0.0"; id; echo "')

    assert module.main(["validate", "DPONE_VERSION"]) == 4
