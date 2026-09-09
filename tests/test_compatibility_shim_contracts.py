from __future__ import annotations

import importlib
import types
import warnings

import pytest


@pytest.mark.parametrize(
    ("module_name", "message"),
    [
        ("dpone.source", "`dpone.source` is deprecated"),
        ("dpone.sink", "`dpone.sink` is deprecated"),
        ("dpone.sql_helpers", "`dpone.sql_helpers` is deprecated"),
        ("dpone.state", "`dpone.state` is deprecated"),
        ("dpone.xmin", "`dpone.xmin` is deprecated"),
        ("dpone.credentials", "`dpone.credentials` is deprecated"),
        ("dpone.etl", "`dpone.etl` is deprecated"),
        ("dpone.etl_logging", "`dpone.etl_logging` is deprecated"),
        ("dpone.reconciliation", "`dpone.reconciliation` is deprecated"),
    ],
)
def test_deprecated_runtime_shims_warn_and_delegate_lazily(module_name: str, message: str) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        module = importlib.reload(importlib.import_module(module_name))

    assert any(message in str(warning.message) for warning in caught)

    sentinel = object()
    module._RUNTIME = types.SimpleNamespace(Sentinel=sentinel)  # type: ignore[attr-defined]

    assert module.Sentinel is sentinel
    assert "Sentinel" in dir(module)


def test_deprecated_runtime_shim_dir_falls_back_when_runtime_import_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    import dpone.source as source

    source._RUNTIME = None  # type: ignore[attr-defined]
    monkeypatch.setattr(source, "_runtime", lambda: (_ for _ in ()).throw(RuntimeError("boom")))

    assert "__getattr__" in dir(source)


@pytest.mark.parametrize(
    ("module_name", "public_name"),
    [
        ("dpone", "LoadStrategy"),
        ("dpone.core", "ETLConfigurationError"),
        ("dpone.core.etl_types", "DependencyConfig"),
        ("dpone.core.runtime", "RunContext"),
        ("dpone.dag", "DependencyConfig"),
        ("dpone.yaml_config_handler", "DependencyConfig"),
    ],
)
def test_lazy_export_packages_cache_known_symbols_and_reject_unknown_names(module_name: str, public_name: str) -> None:
    module = importlib.import_module(module_name)

    value = getattr(module, public_name)

    assert getattr(module, public_name) is value
    with pytest.raises(AttributeError, match="missing_symbol"):
        getattr(module, "missing_symbol")
