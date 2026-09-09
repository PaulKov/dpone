"""Построение Airflow DAG из YAML-конфигов dpone."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from airflow import DAG

from dpone.config.env import MANIFEST_DIR
from dpone.core.task_group_builder import TaskGroupBuilder
from dpone.dag import DependencyManager

if TYPE_CHECKING:
    from dpone.dag.yaml_types import ProcessNode

from operators.etl_operator import ETLOperator


def _resolve_yaml_path(yaml_path: str, base_path: Path | None = None) -> Path:
    """Разрешает абсолютный путь к YAML файлу."""
    if not yaml_path.endswith((".yaml", ".yml")):
        yaml_path = f"{yaml_path}.yaml"

    yaml_path_obj = Path(yaml_path)

    if yaml_path_obj.is_absolute():
        return yaml_path_obj

    search_base = base_path if base_path is not None else MANIFEST_DIR
    return search_base / yaml_path


def _create_single_task(dag: DAG, node: ProcessNode) -> ETLOperator:
    """Создаёт одну ETL задачу (без сложной логики/TaskGroup)."""
    from operators.etl_operator import ETLOperator

    return ETLOperator(
        task_id=node.name,
        # node.ref = "<yaml_path>#<selector>" — важно для batch manifests
        config_path=node.ref,
        dag=dag,
    )


def _create_tasks_from_execution_plan(
    dag: DAG,
    execution_plan: list,
) -> dict[str, ETLOperator]:
    """Создаёт ETL задачи из плана выполнения с поддержкой TaskGroup."""
    builder = TaskGroupBuilder(dag)

    builder.collect_task_groups(execution_plan)

    errors = builder.validate_task_groups(execution_plan)
    if errors:
        raise ValueError(f"Task group validation failed: {errors}")

    for node in execution_plan:
        builder.create_task(node)

    # Зависимости строятся через builder.build_dependencies (из depends_on и групп).
    builder.build_dependencies(execution_plan)

    return builder.tasks


def build_dag_from_yaml(
    dag: DAG,
    etl_process_name: str,
    *,
    base_path: Path | None = None,
) -> dict[str, ETLOperator]:
    """Создаёт задачи DAG по YAML-конфигурации с автоматической обработкой зависимостей.

    Поддерживает:
    - legacy manifests (1 YAML == 1 процесс)
    - batch manifests (1 YAML == много процессов)
    """
    full_yaml_path = _resolve_yaml_path(etl_process_name, base_path)

    dependency_manager = DependencyManager(base_path)
    dependency_manager.load_process_chain(full_yaml_path)

    execution_plan = dependency_manager.get_execution_plan()

    # Fast path: ровно одна задача и нет TaskGroup
    if len(execution_plan) == 1 and not execution_plan[0].task_group:
        node = execution_plan[0]
        single_task = _create_single_task(dag, node)
        return {node.name: single_task}

    return _create_tasks_from_execution_plan(dag, execution_plan)


def validate_dependencies(yaml_path: str, base_path: Path | None = None) -> list[str]:
    """Валидирует зависимости в YAML конфигурации."""
    dependency_manager = DependencyManager(base_path)

    try:
        full_yaml_path = _resolve_yaml_path(yaml_path, base_path)
        dependency_manager.load_process_chain(full_yaml_path)
        return dependency_manager.validate_dependencies()
    except Exception as e:
        return [f"Ошибка при загрузке зависимостей: {str(e)}"]


def get_dependency_tree(yaml_path: str, base_path: Path | None = None) -> dict[str, any]:
    """Возвращает дерево зависимостей для отладки.

    NOTE: в текущей версии dpone не реализует полноценный dependency_tree API.
    """
    dependency_manager = DependencyManager(base_path)
    full_yaml_path = _resolve_yaml_path(yaml_path, base_path)
    root_node = dependency_manager.load_process_chain(full_yaml_path)

    # Backward-compat placeholder: вернуть хотя бы корневую ноду
    return {"root": root_node.name}
