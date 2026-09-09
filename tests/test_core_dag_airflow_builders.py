from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.contracts.process_types import DependencyConfig
from dpone.dag.yaml_types import ProcessNode


class FakeDAG:
    pass


class FakeTaskGroup:
    def __init__(self, *, group_id: str, dag: FakeDAG, tooltip: str) -> None:
        self.group_id = group_id
        self.dag = dag
        self.tooltip = tooltip


class FakeETLOperator:
    def __init__(
        self,
        *,
        task_id: str,
        config_path: str,
        dag: FakeDAG,
        task_group: FakeTaskGroup | None = None,
    ) -> None:
        self.task_id = task_id
        self.config_path = config_path
        self.dag = dag
        self.task_group = task_group
        self.downstream: list[FakeETLOperator] = []

    def __rshift__(self, other: FakeETLOperator) -> FakeETLOperator:
        self.downstream.append(other)
        return other


@pytest.fixture()
def fake_airflow_modules(monkeypatch: pytest.MonkeyPatch) -> types.SimpleNamespace:
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG

    airflow_utils = types.ModuleType("airflow.utils")
    task_group_module = types.ModuleType("airflow.utils.task_group")
    task_group_module.TaskGroup = FakeTaskGroup

    operators = types.ModuleType("operators")
    etl_operator = types.ModuleType("operators.etl_operator")
    etl_operator.ETLOperator = FakeETLOperator

    for module_name, module in {
        "airflow": airflow,
        "airflow.utils": airflow_utils,
        "airflow.utils.task_group": task_group_module,
        "operators": operators,
        "operators.etl_operator": etl_operator,
    }.items():
        monkeypatch.setitem(sys.modules, module_name, module)

    sys.modules.pop("dpone.core.dag_builder", None)
    sys.modules.pop("dpone.core.task_group_builder", None)
    importlib.invalidate_caches()

    return types.SimpleNamespace(dag=FakeDAG())


def process_node(
    name: str,
    *,
    config_path: str = "batch.yaml",
    selector: str | None = None,
    dependencies: list[DependencyConfig] | None = None,
    task_group: str | None = None,
) -> ProcessNode:
    return ProcessNode(
        config=SimpleNamespace(name=name),
        config_path=Path(config_path),
        selector=selector,
        dependencies=dependencies or [],
        task_group=task_group,
    )


def test_resolve_yaml_path_adds_suffix_and_respects_absolute_paths(fake_airflow_modules: types.SimpleNamespace) -> None:
    dag_builder = importlib.import_module("dpone.core.dag_builder")

    assert dag_builder._resolve_yaml_path("orders", Path("/repo/manifests")) == Path("/repo/manifests/orders.yaml")
    assert dag_builder._resolve_yaml_path("/tmp/orders.yml", Path("/repo/manifests")) == Path("/tmp/orders.yml")


def test_build_dag_from_yaml_uses_single_task_fast_path(
    fake_airflow_modules: types.SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dag_builder = importlib.import_module("dpone.core.dag_builder")
    node = process_node("orders", config_path=str(tmp_path / "orders.yaml"))
    managers: list[Any] = []

    class FakeDependencyManager:
        def __init__(self, base_path: Path | None) -> None:
            self.base_path = base_path
            self.loaded_path: Path | None = None
            managers.append(self)

        def load_process_chain(self, full_yaml_path: Path) -> None:
            self.loaded_path = full_yaml_path

        def get_execution_plan(self) -> list[ProcessNode]:
            return [node]

    monkeypatch.setattr(dag_builder, "DependencyManager", FakeDependencyManager)

    tasks = dag_builder.build_dag_from_yaml(fake_airflow_modules.dag, "orders", base_path=tmp_path)

    assert managers[0].base_path == tmp_path
    assert managers[0].loaded_path == tmp_path / "orders.yaml"
    assert list(tasks) == ["orders"]
    assert tasks["orders"].config_path == f"{tmp_path / 'orders.yaml'}#orders"


def test_validate_dependencies_returns_errors_without_leaking_exceptions(
    fake_airflow_modules: types.SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dag_builder = importlib.import_module("dpone.core.dag_builder")

    class FailingDependencyManager:
        def __init__(self, base_path: Path | None) -> None:
            self.base_path = base_path

        def load_process_chain(self, full_yaml_path: Path) -> None:
            raise RuntimeError(f"cannot load {full_yaml_path}")

    monkeypatch.setattr(dag_builder, "DependencyManager", FailingDependencyManager)

    assert dag_builder.validate_dependencies("missing.yaml", Path("/repo")) == [
        "Ошибка при загрузке зависимостей: cannot load /repo/missing.yaml"
    ]


def test_get_dependency_tree_returns_backward_compatible_root_payload(
    fake_airflow_modules: types.SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dag_builder = importlib.import_module("dpone.core.dag_builder")
    root = process_node("root", config_path=str(tmp_path / "root.yaml"))

    class FakeDependencyManager:
        def __init__(self, base_path: Path | None) -> None:
            self.base_path = base_path

        def load_process_chain(self, full_yaml_path: Path) -> ProcessNode:
            return root

    monkeypatch.setattr(dag_builder, "DependencyManager", FakeDependencyManager)

    assert dag_builder.get_dependency_tree("root", tmp_path) == {"root": "root"}


def test_task_group_builder_collects_groups_and_builds_dependencies(
    fake_airflow_modules: types.SimpleNamespace,
) -> None:
    task_group_builder = importlib.import_module("dpone.core.task_group_builder")
    upstream = process_node("extract_orders", selector="extract", task_group="extractors")
    downstream = process_node(
        "load_orders",
        selector="load",
        dependencies=[DependencyConfig(path="extract_orders")],
        task_group="loaders",
    )

    builder = task_group_builder.TaskGroupBuilder(fake_airflow_modules.dag)
    builder.collect_task_groups([upstream, downstream])
    assert sorted(builder.task_groups) == ["extractors", "loaders"]

    upstream_task = builder.create_task(upstream)
    downstream_task = builder.create_task(downstream)
    builder.build_dependencies([upstream, downstream])

    assert upstream_task.task_group.group_id == "extractors"
    assert downstream_task.task_group.group_id == "loaders"
    assert upstream_task.downstream == [downstream_task]


def test_task_group_builder_validates_unknown_groups_and_dependencies(
    fake_airflow_modules: types.SimpleNamespace,
) -> None:
    task_group_builder = importlib.import_module("dpone.core.task_group_builder")
    node = process_node(
        "load_orders",
        dependencies=[
            DependencyConfig(path="missing_task"),
            DependencyConfig(path="", group="missing_group"),
        ],
    )

    builder = task_group_builder.TaskGroupBuilder(fake_airflow_modules.dag)

    assert builder.validate_task_groups([node]) == [
        "Unknown dependency 'missing_task' in task 'load_orders'",
        "Unknown group 'missing_group' in task 'load_orders'. Available groups: []",
    ]
