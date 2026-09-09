from __future__ import annotations

from types import SimpleNamespace

import dpone_airflow_pack.dag_materializer as dag_materializer
import dpone_airflow_pack.provider as provider
import pytest


def test_dag_materializer_prefers_airflow_sdk_on_airflow_3(monkeypatch) -> None:
    class SdkDag:
        pass

    imports: list[str] = []

    def import_module(name: str) -> object:
        imports.append(name)
        if name == "airflow.sdk":
            return SimpleNamespace(DAG=SdkDag)
        raise AssertionError(f"unexpected legacy import: {name}")

    monkeypatch.setattr(dag_materializer, "import_module", import_module)

    dag_class, _ = dag_materializer._airflow_dag_types()

    assert dag_class is SdkDag
    assert imports == ["airflow.sdk"]


def test_dag_materializer_falls_back_to_airflow_2_dag(monkeypatch) -> None:
    class LegacyDag:
        pass

    imports: list[str] = []

    def import_module(name: str) -> object:
        imports.append(name)
        if name == "airflow.sdk":
            raise ModuleNotFoundError("No module named 'airflow.sdk'", name="airflow.sdk")
        if name == "airflow":
            return SimpleNamespace(DAG=LegacyDag)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(dag_materializer, "import_module", import_module)

    dag_class, _ = dag_materializer._airflow_dag_types()

    assert dag_class is LegacyDag
    assert imports == ["airflow.sdk", "airflow"]


@pytest.mark.parametrize(
    ("loader_name", "primary_module"),
    [
        ("_airflow_dag_types", "airflow.sdk"),
        ("_task_group_class", "airflow.sdk"),
        ("_empty_operator_class", "airflow.providers.standard.operators.empty"),
    ],
)
@pytest.mark.parametrize(
    "primary_error",
    [
        ImportError("installed Airflow public API is broken"),
        ModuleNotFoundError(
            "No module named 'airflow.transitive_dependency'",
            name="airflow.transitive_dependency",
        ),
        AttributeError("installed Airflow public API is missing a required symbol"),
    ],
)
def test_dag_materializer_fails_closed_for_broken_installed_public_api(
    monkeypatch: pytest.MonkeyPatch,
    loader_name: str,
    primary_module: str,
    primary_error: Exception,
) -> None:
    imports: list[str] = []

    def import_module(name: str) -> object:
        imports.append(name)
        if name == primary_module:
            if isinstance(primary_error, AttributeError):

                class BrokenModule:
                    def __getattr__(self, symbol: str) -> object:
                        del symbol
                        raise primary_error

                return BrokenModule()
            raise primary_error
        raise AssertionError(f"unexpected legacy import: {name}")

    monkeypatch.setattr(dag_materializer, "import_module", import_module)

    with pytest.raises(type(primary_error), match=str(primary_error)):
        getattr(dag_materializer, loader_name)()

    assert imports == [primary_module]


@pytest.mark.parametrize(
    ("loader_name", "primary_module", "legacy_module", "symbol"),
    [
        ("_task_group_class", "airflow.sdk", "airflow.utils.task_group", "TaskGroup"),
        (
            "_empty_operator_class",
            "airflow.providers.standard.operators.empty",
            "airflow.operators.empty",
            "EmptyOperator",
        ),
    ],
)
def test_dag_materializer_falls_back_only_when_primary_module_is_absent(
    monkeypatch: pytest.MonkeyPatch,
    loader_name: str,
    primary_module: str,
    legacy_module: str,
    symbol: str,
) -> None:
    class LegacyType:
        pass

    imports: list[str] = []

    def import_module(name: str) -> object:
        imports.append(name)
        if name == primary_module:
            raise ModuleNotFoundError(f"No module named {primary_module!r}", name=primary_module)
        if name == legacy_module:
            return SimpleNamespace(**{symbol: LegacyType})
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(dag_materializer, "import_module", import_module)

    assert getattr(dag_materializer, loader_name)() is LegacyType
    assert imports == [primary_module, legacy_module]


def test_provider_task_group_falls_back_only_when_airflow_sdk_is_absent(monkeypatch) -> None:
    class LegacyTaskGroup:
        pass

    imports: list[str] = []

    def import_module(name: str) -> object:
        imports.append(name)
        if name == "airflow.sdk":
            raise ModuleNotFoundError("No module named 'airflow.sdk'", name="airflow.sdk")
        if name == "airflow.utils.task_group":
            return SimpleNamespace(TaskGroup=LegacyTaskGroup)
        raise AssertionError(f"unexpected import: {name}")

    monkeypatch.setattr(provider, "import_module", import_module)

    assert provider._airflow_task_group_class() is LegacyTaskGroup
    assert imports == ["airflow.sdk", "airflow.utils.task_group"]


@pytest.mark.parametrize(
    "sdk_error",
    [
        ImportError("installed airflow.sdk is broken"),
        ModuleNotFoundError("No module named 'airflow.sdk.internal'", name="airflow.sdk.internal"),
    ],
)
def test_provider_task_group_fails_closed_for_broken_installed_sdk(
    monkeypatch: pytest.MonkeyPatch,
    sdk_error: Exception,
) -> None:
    imports: list[str] = []

    def import_module(name: str) -> object:
        imports.append(name)
        raise sdk_error

    monkeypatch.setattr(provider, "import_module", import_module)

    with pytest.raises(type(sdk_error), match=str(sdk_error)):
        provider._airflow_task_group_class()

    assert imports == ["airflow.sdk"]
