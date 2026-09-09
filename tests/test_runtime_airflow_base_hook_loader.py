from __future__ import annotations

import sys
import types

from dpone.runtime.credentials.airflow_base_hook import load_airflow_base_hook


def _install_module(monkeypatch, module_name: str, **attrs: object) -> None:
    module = types.ModuleType(module_name)
    if module_name in {"airflow", "airflow.sdk", "airflow.sdk.bases", "airflow.hooks"}:
        module.__path__ = []
    for key, value in attrs.items():
        setattr(module, key, value)
    monkeypatch.setitem(sys.modules, module_name, module)


def _clear_airflow_modules(monkeypatch) -> None:
    for module_name in list(sys.modules):
        if module_name == "airflow" or module_name.startswith("airflow."):
            monkeypatch.delitem(sys.modules, module_name, raising=False)


def test_load_airflow_base_hook_prefers_airflow_3_sdk_path(monkeypatch) -> None:
    class Airflow3BaseHook:
        pass

    class LegacyBaseHook:
        pass

    _clear_airflow_modules(monkeypatch)
    _install_module(monkeypatch, "airflow")
    _install_module(monkeypatch, "airflow.sdk")
    _install_module(monkeypatch, "airflow.sdk.bases")
    _install_module(monkeypatch, "airflow.sdk.bases.hook", BaseHook=Airflow3BaseHook)
    _install_module(monkeypatch, "airflow.hooks")
    _install_module(monkeypatch, "airflow.hooks.base", BaseHook=LegacyBaseHook)

    assert load_airflow_base_hook() is Airflow3BaseHook


def test_load_airflow_base_hook_supports_airflow_2_legacy_path(monkeypatch) -> None:
    class LegacyBaseHook:
        pass

    _clear_airflow_modules(monkeypatch)
    _install_module(monkeypatch, "airflow")
    _install_module(monkeypatch, "airflow.hooks")
    _install_module(monkeypatch, "airflow.hooks.base", BaseHook=LegacyBaseHook)

    assert load_airflow_base_hook() is LegacyBaseHook


def test_load_airflow_base_hook_returns_none_without_airflow(monkeypatch) -> None:
    _clear_airflow_modules(monkeypatch)
    _install_module(monkeypatch, "airflow")

    assert load_airflow_base_hook() is None
